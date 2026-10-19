//! End-to-end timed service over msrs + copper-rs, with the real clock.
//!
//! DAG: `ingress → watchdog → egress`.
//!   * `ingress` (`msrs_transport::CycleIngressTask<Feed>`) emits a `Cycle`
//!     every iteration: the drained feeds plus the cycle time.
//!   * `watchdog` (`msrs_core::FsmBatchTask<WatchdogMachine>`) runs the
//!     statig watchdog and forwards every alarm of a cycle as one `Batch`.
//!   * `egress` (`msrs_transport::BatchEgressTask<Alarm>`) sends each alarm
//!     back to the transport.
//!
//! `main` runs the live service (feeds, a silence longer than the timeout,
//! more feeds, then silence until the run ends), then replays the recorded cycles through a fresh machine.
//! The trip decision depends on time, and the time it used is in the log, so
//! the replay must reproduce the live alarms bit-for-bit.

use crossbeam_channel::{unbounded, Receiver, Sender};
use cu29::prelude::*;
use msrs_core::{run_step_all, Batch, RtConfig, Trigger};
use msrs_transport::{BatchEgressTask, Cycle, CycleIngressTask, Transport, TransportDriver};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

mod watchdog;
use watchdog::{Alarm, Feed, WatchdogMachine};

// Crate-root aliases referenced from copperconfig.ron.
pub type WatchdogIngress = CycleIngressTask<Feed>;
pub type WatchdogFsm = msrs_core::FsmBatchTask<WatchdogMachine>;
pub type WatchdogEgress = BatchEgressTask<Alarm>;
pub type FeedCycle = Cycle<Feed>;
pub type AlarmBatch = Batch<Alarm>;

#[copper_runtime(config = "copperconfig.ron")]
struct WatchdogApplication {}

gen_cumsgs!("copperconfig.ron");

const SLAB_SIZE: Option<usize> = Some(1024 * 1024 * 10);
const LOOP_PERIOD: Duration = Duration::from_millis(5);
const RUN_FOR: Duration = Duration::from_millis(400);

/// In-process transport: feeds on a schedule (with a long silence in the
/// middle) and forwards alarms to `main` until the DAG side disconnects.
struct Pulse {
    results: Sender<Alarm>,
}

impl Transport for Pulse {
    type Inbound = Feed;
    type Outbound = Alarm;

    fn run(self, rx_out: Receiver<Alarm>, tx_in: Sender<Feed>) -> Result<(), String> {
        let feeder = std::thread::spawn(move || {
            let burst = |seq: std::ops::Range<u32>| {
                for n in seq {
                    if tx_in.send(Feed(n)).is_err() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            };
            burst(0..5);
            std::thread::sleep(Duration::from_millis(150)); // 3x the timeout
            burst(5..8);
        });
        while let Ok(alarm) = rx_out.recv() {
            self.results.send(alarm).map_err(|e| e.to_string())?;
        }
        feeder.join().map_err(|_| "feeder panicked".to_string())
    }
}

fn run_live(log_base: &Path) -> Vec<Alarm> {
    let (res_tx, res_rx) = unbounded::<Alarm>();
    let handles = TransportDriver::spawn(Pulse { results: res_tx }, RtConfig::normal());
    WatchdogIngress::install(handles.from_transport);
    WatchdogEgress::install(handles.to_transport);

    let mut application = WatchdogApplication::builder()
        .with_log_path(log_base.to_str().expect("utf-8 log path"), SLAB_SIZE)
        .expect("Failed to setup logger.")
        .build()
        .expect("Failed to create application.");

    application
        .start_all_tasks()
        .expect("start_all_tasks failed");
    let start = Instant::now();
    while start.elapsed() < RUN_FOR {
        application
            .run_one_iteration()
            .expect("run_one_iteration failed");
        std::thread::sleep(LOOP_PERIOD);
    }
    application.stop_all_tasks().expect("stop_all_tasks failed");
    // Dropping the app flushes the log and disconnects egress, which ends the
    // transport's receive loop.
    drop(application);

    let _ = handles.join.join();
    res_rx.try_iter().collect()
}

/// Recorded per-copperlist payloads: each ingress cycle and the alarms the
/// watchdog emitted for it.
struct Recorded {
    cycles: Vec<Cycle<Feed>>,
    alarms: Vec<Alarm>,
}

fn read_recorded(log_base: &Path) -> Recorded {
    use cu29::bincode::{config::standard, decode_from_std_read};

    let ids = <CuMsgs as MatchingTasks>::get_all_task_ids();
    assert_eq!(
        ids,
        ["ingress", "watchdog", "egress"],
        "unexpected copperlist slot order: {ids:?}"
    );

    let read = match UnifiedLoggerBuilder::new()
        .file_base_name(log_base)
        .build()
        .expect("open unified log for reading")
    {
        UnifiedLogger::Read(r) => r,
        UnifiedLogger::Write(_) => panic!("expected a read-only unified logger"),
    };
    let mut reader = UnifiedLoggerIOReader::new(read, UnifiedLogType::CopperList);

    let mut cycles = Vec::new();
    let mut alarms = Vec::new();
    while let Ok(cl) = decode_from_std_read::<CopperList<CuMsgs>, _, _>(&mut reader, standard()) {
        // CycleIngressTask emits on every iteration, so every copperlist has
        // a cycle; a missing one would be a gap the replay cannot cover.
        let cycle = cl.msgs.0 .0.payload().expect("copperlist without a cycle");
        cycles.push(cycle.clone());
        if let Some(Batch(out)) = cl.msgs.0 .1.payload() {
            alarms.extend(out.iter().cloned());
        }
    }
    Recorded { cycles, alarms }
}

/// Re-run a fresh machine over the recorded cycles — no runtime, no clock.
fn replay(cycles: &[Cycle<Feed>]) -> Vec<Alarm> {
    let mut machine = WatchdogMachine::default();
    cycles
        .iter()
        .flat_map(|c| run_step_all(&mut machine, &Trigger::Message(c)))
        .collect()
}

fn main() {
    let log_base =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/watchdog-logs/wd.copper");
    let log_dir = log_base.parent().unwrap();
    let _ = std::fs::remove_dir_all(log_dir);
    std::fs::create_dir_all(log_dir).expect("create log dir");

    let live = run_live(&log_base);
    for a in &live {
        println!("[live] {a:?}");
    }
    let kinds: Vec<&str> = live
        .iter()
        .map(|a| match a {
            Alarm::Armed => "armed",
            Alarm::Tripped { .. } => "tripped",
            Alarm::Recovered => "recovered",
        })
        .collect();
    assert_eq!(
        kinds,
        ["armed", "tripped", "recovered", "tripped"],
        "live run did not arm, trip on silence, recover, and trip after the last feed"
    );

    let recorded = read_recorded(&log_base);
    let fed: usize = recorded.cycles.iter().map(|c| c.events.len()).sum();
    println!(
        "[replay] recovered {} cycle(s) carrying {} feed(s) and {} alarm(s) from the log",
        recorded.cycles.len(),
        fed,
        recorded.alarms.len()
    );
    let replayed = replay(&recorded.cycles);
    for a in &replayed {
        println!("[replay] {a:?}");
    }

    assert_eq!(
        replayed, recorded.alarms,
        "replay diverged from the alarms recorded in the log"
    );
    if replayed == live {
        println!("REPLAY DETERMINISM: OK (live == replay, bit-identical)");
    } else {
        eprintln!("REPLAY DETERMINISM: FAILED\n  live:   {live:?}\n  replay: {replayed:?}");
        std::process::exit(1);
    }
}
