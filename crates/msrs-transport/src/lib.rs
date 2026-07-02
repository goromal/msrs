//! msrs-transport: IPC plug-in contract and DAG bridge.
pub mod transport;
pub mod driver;
pub mod tasks;
pub use driver::{DriverHandles, TransportDriver};
pub use transport::{Transport, TransportEvent};
pub use tasks::{EgressTask, IngressTask, egress_step, ingress_step};
