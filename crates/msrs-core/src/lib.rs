//! msrs-core: pure building blocks for deterministic FSM-driven services.
pub mod trigger;
pub use trigger::Trigger;
pub mod effects;
pub use effects::{Effects, ExtraEmits};
pub mod store;
pub use store::Store;
pub mod rt;
pub use rt::{RtConfig, SchedPolicy};
pub mod fsm_task;
pub use fsm_task::{run_step, run_step_all, Batch, FsmBatchTask, FsmSpec, FsmTask};
pub mod replay;
pub use replay::{read_copperlists, replay_gate, GateReport, LogEnd, Recorded};
