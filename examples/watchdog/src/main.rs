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
use msrs_core::{
    read_copperlists, replay_gate, run_step_all, Batch, GateReport, LogEnd, Recorded, RtConfig,
    Trigger,
};
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

/// Recorded steps: each ingress cycle and the alarms emitted for it (`None`
/// when the step emitted nothing).
fn read_recorded(log_base: &Path) -> Vec<Recorded<Cycle<Feed>, Option<AlarmBatch>>> {
    let ids = <CuMsgs as MatchingTasks>::get_all_task_ids();
    assert_eq!(
        ids,
        ["ingress", "watchdog", "egress"],
        "unexpected copperlist slot order: {ids:?}"
    );
    let (copperlists, end) = read_copperlists::<CuMsgs>(log_base).expect("read unified log");
    assert_eq!(end, LogEnd::Eof, "log did not end cleanly");
    copperlists
        .iter()
        .map(|cl| Recorded {
            id: cl.id,
            // CycleIngressTask emits on every iteration, so every copperlist
            // has a cycle; a missing one would be a gap the replay cannot cover.
            input: cl
                .msgs
                .0
                 .0
                .payload()
                .expect("copperlist without a cycle")
                .clone(),
            output: cl.msgs.0 .1.payload().cloned(),
        })
        .collect()
}

/// Re-run a fresh machine over the recorded cycles — no runtime, no clock.
fn replay(recorded: &[Recorded<Cycle<Feed>, Option<AlarmBatch>>]) -> GateReport {
    let mut machine = WatchdogMachine::default();
    replay_gate(recorded, |c| {
        let out = run_step_all(&mut machine, &Trigger::Message(c));
        (!out.is_empty()).then_some(Batch(out))
    })
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
    let fed: usize = recorded.iter().map(|r| r.input.events.len()).sum();
    let alarms: Vec<Alarm> = recorded
        .iter()
        .filter_map(|r| r.output.as_ref())
        .flat_map(|b| b.0.iter().cloned())
        .collect();
    println!(
        "[replay] recovered {} cycle(s) carrying {} feed(s) and {} alarm(s) from the log",
        recorded.len(),
        fed,
        alarms.len()
    );
    assert_eq!(
        alarms, live,
        "the log does not hold the alarms the live run sent"
    );

    let report = replay(&recorded);
    println!("[replay] {report:?}");
    assert!(
        report.passed(),
        "replay diverged or the log has gaps: {report:?}"
    );

    // Negative control: drop the feeds of the first cycle that armed the
    // watchdog. The gate must notice, or it proves nothing.
    let mut perturbed = recorded.clone();
    let armed = perturbed
        .iter_mut()
        .find(|r| {
            r.output
                .as_ref()
                .is_some_and(|b| b.0.contains(&Alarm::Armed))
        })
        .expect("a cycle that armed");
    armed.input.events.clear();
    let control = replay(&perturbed);
    assert!(
        !control.passed(),
        "negative control passed: the gate is vacuous"
    );
    println!(
        "[replay] negative control diverged as required ({} divergent step(s))",
        control.divergent
    );
    println!("REPLAY DETERMINISM: OK (log == replay, bit-identical; ids contiguous)");
}
