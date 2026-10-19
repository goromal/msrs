//! msrs-transport: IPC plug-in contract and DAG bridge.
pub mod driver;
pub mod registry;
pub mod tasks;
pub mod transport;
pub use driver::{DriverHandles, TransportDriver};
pub use tasks::{
    cycle_step, egress_step, ingress_step, BatchEgressTask, Cycle, CycleIngressTask, EgressTask,
    IngressTask, DEFAULT_MAX_BATCH,
};
pub use transport::{Transport, TransportEvent};
