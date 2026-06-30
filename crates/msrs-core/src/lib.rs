//! msrs-core: pure building blocks for deterministic FSM-driven services.
pub mod trigger;
pub use trigger::Trigger;
pub mod effects;
pub use effects::Effects;
pub mod store;
pub use store::Store;
pub mod rt;
pub use rt::{RtConfig, SchedPolicy};
