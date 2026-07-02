//! The IPC plug-in contract.

use crossbeam_channel::{Receiver, Sender};

/// A middleware binding. Runs on its own driver thread, owning the middleware
/// executor/loop. It reads DAG outputs from `rx_out` and publishes them, and
/// forwards externally-received messages to the DAG via `tx_in`. It never
/// touches copper-rs internals.
pub trait Transport: Send + 'static {
    /// Messages flowing middleware → DAG.
    type Inbound: Send + 'static;
    /// Messages flowing DAG → middleware.
    type Outbound: Send + 'static;

    /// Run until shutdown (channel disconnect). Return `Err` on fatal fault.
    fn run(
        self,
        rx_out: Receiver<Self::Outbound>,
        tx_in: Sender<Self::Inbound>,
    ) -> Result<(), String>;
}

/// An inbound event as seen by the DAG: a payload or a transport-level error,
/// so faults enter the replay graph instead of being swallowed on the thread.
#[derive(Debug, Clone)]
pub enum TransportEvent<T> {
    Message(T),
    Error(String),
}
