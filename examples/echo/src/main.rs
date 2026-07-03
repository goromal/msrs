//! End-to-end echo service over msrs + copper-rs.
//!
//! DAG: `ingress → echo → egress`.
//!   * `ingress` (`msrs_transport::IngressTask<String>`) drains inbound messages
//!     from the in-process transport onto the copperlist.
//!   * `echo` (`msrs_core::FsmTask<EchoMachine>`) runs the statig-backed echo FSM.
//!   * `egress` (`msrs_transport::EgressTask<String>`) forwards echoes back to the
//!     transport.
//!
//! `main` does two things in one process so `cargo run -p echo` demonstrates the
//! whole gate:
//!   1. **[live]** feed 3 strings through the transport, collect the echoes, and
//!      write a copper-rs unified log.
//!   2. **[replay]** read the recorded ingress inputs back out of that log and
//!      re-run the deterministic FSM over them, reproducing the echoes. The live
//!      and replay sequences are compared bit-for-bit with `diff`.

use crossbeam_channel::{unbounded, Receiver, Sender};
use cu29::prelude::*;
use msrs_core::{run_step, RtConfig, Trigger};
use msrs_transport::{EgressTask, IngressTask, Transport, TransportDriver};
use std::path::{Path, PathBuf};
use std::time::Duration;

mod echo;
use echo::{EchoMachine, EchoMsg};

// ---------------------------------------------------------------------------
// Task type aliases referenced (by name) from the generated runtime code.
// Path-qualified generic types are hard for the RON codegen to parse, so we
// expose plain crate-root aliases and name those in copperconfig.ron.
// ---------------------------------------------------------------------------

/// Source task: transport → DAG.
pub type EchoIngress = IngressTask<EchoMsg>;
/// Transform task: the statig echo FSM.
pub type EchoFsm = msrs_core::FsmTask<EchoMachine>;
/// Sink task: DAG → transport.
pub type EchoEgress = EgressTask<EchoMsg>;

// The copper application + its copperlist payload type (`CuMsgs`).
#[copper_runtime(config = "copperconfig.ron")]
struct EchoApplication {}

gen_cumsgs!("copperconfig.ron");

const SLAB_SIZE: Option<usize> = Some(1024 * 1024 * 10);

// ---------------------------------------------------------------------------
// In-process transport: feeds a fixed list of strings inbound and collects the
// echoes flowing back outbound, forwarding them to `main` over `results`.
// ---------------------------------------------------------------------------

struct Feeder {
    inputs: Vec<String>,
    results: Sender<String>,
}

impl Transport for Feeder {
    type Inbound = EchoMsg;
    type Outbound = EchoMsg;

    fn run(self, rx_out: Receiver<EchoMsg>, tx_in: Sender<EchoMsg>) -> Result<(), String> {
        // Feed every input into the DAG.
        for msg in &self.inputs {
            tx_in
                .send(EchoMsg::new(msg.clone()))
                .map_err(|e| e.to_string())?;
        }
        // Collect exactly one echo per input, forwarding its body to main.
        for _ in 0..self.inputs.len() {
            match rx_out.recv_timeout(Duration::from_secs(5)) {
                Ok(echoed) => self.results.send(echoed.0).map_err(|e| e.to_string())?,
                Err(_) => return Err("timed out waiting for echo".to_string()),
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Live run.
// ---------------------------------------------------------------------------

fn run_live(log_base: &Path, inputs: &[&str]) -> Vec<String> {
    let (res_tx, res_rx) = unbounded::<String>();
    let feeder = Feeder {
        inputs: inputs.iter().map(|s| s.to_string()).collect(),
        results: res_tx,
    };

    // Spawn the transport on its own driver thread, then hand its DAG-side
    // channel ends to the ingress/egress tasks BEFORE the runtime is built.
    let handles = TransportDriver::spawn(feeder, RtConfig::normal());
    IngressTask::<EchoMsg>::install(handles.from_transport);
    EgressTask::<EchoMsg>::install(handles.to_transport);

    let mut application = EchoApplication::builder()
        .with_log_path(log_base.to_str().expect("utf-8 log path"), SLAB_SIZE)
        .expect("Failed to setup logger.")
        .build()
        .expect("Failed to create application.");

    application
        .start_all_tasks()
        .expect("start_all_tasks failed");
    // Ingress drains at most one message per iteration; run a few extra so all
    // messages traverse ingress → echo → egress.
    for _ in 0..(inputs.len() + 3) {
        application
            .run_one_iteration()
            .expect("run_one_iteration failed");
    }
    application.stop_all_tasks().expect("stop_all_tasks failed");

    // Drop the application so the unified logger flushes to disk before we read.
    drop(application);

    // Collect the echoes observed on the transport's outbound side.
    let mut live = Vec::new();
    while live.len() < inputs.len() {
        match res_rx.recv_timeout(Duration::from_secs(2)) {
            Ok(v) => live.push(v),
            Err(_) => break,
        }
    }
    let _ = handles.join.join();
    live
}

// ---------------------------------------------------------------------------
// Replay: read the recorded copperlists back and re-drive the pure FSM.
// ---------------------------------------------------------------------------

/// Recorded per-copperlist payloads: the ingress input and the echo output.
struct Recorded {
    ingress_inputs: Vec<String>,
    echo_outputs: Vec<String>,
}

fn read_recorded(log_base: &Path) -> Recorded {
    use cu29::bincode::{config::standard, decode_from_std_read};

    // Slot order in the copperlist tuple matches `get_all_task_ids()`; assert the
    // mapping we rely on (slot 0 = ingress output, slot 1 = echo output).
    let ids = <CuMsgs as MatchingTasks>::get_all_task_ids();
    assert_eq!(
        ids,
        ["ingress", "echo", "egress"],
        "unexpected copperlist slot order: {ids:?}"
    );

    let logger = UnifiedLoggerBuilder::new()
        .file_base_name(log_base)
        .build()
        .expect("open unified log for reading");
    let read = match logger {
        UnifiedLogger::Read(r) => r,
        UnifiedLogger::Write(_) => panic!("expected a read-only unified logger"),
    };
    let mut reader = UnifiedLoggerIOReader::new(read, UnifiedLogType::CopperList);

    let mut ingress_inputs = Vec::new();
    let mut echo_outputs = Vec::new();
    // Iterate every recorded copperlist until the stream is exhausted.
    while let Ok(cl) = decode_from_std_read::<CopperList<CuMsgs>, _, _>(&mut reader, standard()) {
        if let Some(input) = cl.msgs.0 .0.payload() {
            ingress_inputs.push(input.0.clone());
        }
        if let Some(out) = cl.msgs.0 .1.payload() {
            echo_outputs.push(out.0.clone());
        }
    }
    Recorded {
        ingress_inputs,
        echo_outputs,
    }
}

/// Deterministically reproduce the echoes by re-running the FSM over the
/// recorded ingress inputs — no copper runtime involved.
fn replay(inputs: &[String]) -> Vec<String> {
    let mut machine = EchoMachine::default();
    inputs
        .iter()
        .filter_map(|msg| {
            run_step(&mut machine, &Trigger::Message(&EchoMsg::new(msg.clone()))).map(|out| out.0)
        })
        .collect()
}

fn main() {
    let log_base =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/echo-logs/echo.copper");
    std::fs::create_dir_all(log_base.parent().unwrap()).expect("create log dir");
    // Start each run from a clean log family so the reader sees only this run.
    if let Some(dir) = log_base.parent() {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }

    let inputs = ["hello", "world", "copper"];

    // 1. Live run through the copper DAG + in-process transport.
    let live = run_live(&log_base, &inputs);
    for e in &live {
        println!("[live] {}", e);
    }
    assert_eq!(
        live.len(),
        inputs.len(),
        "live run did not echo every input"
    );

    // 2. Replay from the recorded unified log.
    let recorded = read_recorded(&log_base);
    println!(
        "[replay] recovered {} ingress input(s) and {} echo output(s) from the log",
        recorded.ingress_inputs.len(),
        recorded.echo_outputs.len()
    );
    let replayed = replay(&recorded.ingress_inputs);
    for e in &replayed {
        println!("[replay] {}", e);
    }

    // Cross-check: the FSM replay reproduces exactly the echo outputs the live
    // run recorded in the log.
    assert_eq!(
        replayed, recorded.echo_outputs,
        "replay diverged from the echo outputs recorded in the log"
    );

    // 3. Determinism gate: compare the two independently produced sequences
    // bit-for-bit with `diff`.
    let live_path = std::env::temp_dir().join("echo_live.txt");
    let replay_path = std::env::temp_dir().join("echo_replay.txt");
    std::fs::write(&live_path, live.join("\n") + "\n").expect("write live file");
    std::fs::write(&replay_path, replayed.join("\n") + "\n").expect("write replay file");

    let status = std::process::Command::new("diff")
        .arg(&live_path)
        .arg(&replay_path)
        .status()
        .expect("run diff");

    if status.success() {
        println!("REPLAY DETERMINISM: OK (diff empty)");
    } else {
        eprintln!("REPLAY DETERMINISM: FAILED (diff non-empty)");
        std::process::exit(1);
    }
}
