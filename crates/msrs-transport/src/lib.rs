//! msrs-transport: IPC plug-in contract and DAG bridge.
pub mod transport;
pub mod driver;
pub use driver::{DriverHandles, TransportDriver};
pub use transport::{Transport, TransportEvent};
