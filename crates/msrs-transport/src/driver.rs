//! Spawns a transport on its own thread and owns the bridge channels.

use crate::transport::Transport;
use crossbeam_channel::{unbounded, Receiver, Sender};
use msrs_core::RtConfig;
use std::thread::{self, JoinHandle};

/// Handles the DAG side uses to talk to a spawned transport.
pub struct DriverHandles<In, Out> {
    /// DAG egress pushes outbound messages here.
    pub to_transport: Sender<Out>,
    /// DAG ingress drains inbound messages here.
    pub from_transport: Receiver<In>,
    pub join: JoinHandle<Result<(), String>>,
}

pub struct TransportDriver;

impl TransportDriver {
    /// Spawn `transport` on a dedicated thread with `rt` applied to it.
    pub fn spawn<T: Transport>(transport: T, rt: RtConfig) -> DriverHandles<T::Inbound, T::Outbound> {
        let (to_transport, rx_out) = unbounded::<T::Outbound>();
        let (tx_in, from_transport) = unbounded::<T::Inbound>();
        let join = thread::spawn(move || {
            let _ = rt.apply(); // best-effort RT on the driver thread
            transport.run(rx_out, tx_in)
        });
        DriverHandles { to_transport, from_transport, join }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::Transport;
    use crossbeam_channel::{Receiver, Sender};

    struct Loopback;
    impl Transport for Loopback {
        type Inbound = i32;
        type Outbound = i32;
        fn run(self, rx_out: Receiver<i32>, tx_in: Sender<i32>) -> Result<(), String> {
            // Echo each outbound back inbound until the DAG side disconnects.
            while let Ok(v) = rx_out.recv() {
                tx_in.send(v * 10).map_err(|e| e.to_string())?;
            }
            Ok(())
        }
    }

    #[test]
    fn loopback_round_trips() {
        let h = TransportDriver::spawn(Loopback, RtConfig::normal());
        h.to_transport.send(5).unwrap();
        assert_eq!(h.from_transport.recv().unwrap(), 50);
        drop(h.to_transport); // disconnect → thread exits
        assert!(h.join.join().unwrap().is_ok());
    }
}
