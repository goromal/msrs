//! The IPC plug-in contract.

use bincode::{Decode, Encode};
use crossbeam_channel::{Receiver, Sender};
use cu29::prelude::Reflect;
use serde::{Deserialize, Serialize};

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
///
/// Derives the full copper payload set, so `TransportEvent<T>` can be used
/// directly as an `IngressTask` / `FsmSpec::In` payload whenever `T` is itself
/// a payload type.
#[derive(Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize, Reflect)]
pub enum TransportEvent<T> {
    Message(T),
    Error(String),
}

/// Copper preallocates payload slots with `Default`; the value is never
/// observed as a real event (message presence is tracked by `CuMsg`).
impl<T: Default> Default for TransportEvent<T> {
    fn default() -> Self {
        TransportEvent::Message(T::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cu29::prelude::CuMsgPayload;

    fn assert_payload<P: CuMsgPayload>() {}

    #[test]
    fn transport_event_is_a_copper_payload() {
        assert_payload::<TransportEvent<u32>>();
        assert_eq!(TransportEvent::<u32>::default(), TransportEvent::Message(0));
    }
}
