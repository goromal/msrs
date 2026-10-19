//! Spawns a transport on its own thread and owns the bridge channels.

use crate::transport::Transport;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
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
    ///
    /// Both channels are unbounded: if the transport produces faster than the
    /// DAG drains, the inbound backlog (and latency) grows without limit. Use
    /// [`TransportDriver::spawn_bounded`] to cap it.
    pub fn spawn<T: Transport>(
        transport: T,
        rt: RtConfig,
    ) -> DriverHandles<T::Inbound, T::Outbound> {
        let (tx_in, from_transport) = unbounded::<T::Inbound>();
        Self::spawn_with(transport, rt, tx_in, from_transport)
    }

    /// Like [`TransportDriver::spawn`], but the inbound (transport → DAG)
    /// channel holds at most `inbound_capacity` messages.
    ///
    /// When it is full, `tx_in.send` blocks the driver thread (backpressure
    /// into the middleware); a transport that prefers to shed load should use
    /// `tx_in.try_send` and drop or count the overflow itself. The outbound
    /// channel stays unbounded so the deterministic loop never blocks on
    /// egress.
    pub fn spawn_bounded<T: Transport>(
        transport: T,
        rt: RtConfig,
        inbound_capacity: usize,
    ) -> DriverHandles<T::Inbound, T::Outbound> {
        let (tx_in, from_transport) = bounded::<T::Inbound>(inbound_capacity);
        Self::spawn_with(transport, rt, tx_in, from_transport)
    }

    fn spawn_with<T: Transport>(
        transport: T,
        rt: RtConfig,
        tx_in: Sender<T::Inbound>,
        from_transport: Receiver<T::Inbound>,
    ) -> DriverHandles<T::Inbound, T::Outbound> {
        let (to_transport, rx_out) = unbounded::<T::Outbound>();
        let join = thread::spawn(move || {
            let _ = rt.apply(); // best-effort RT on the driver thread
            transport.run(rx_out, tx_in)
        });
        DriverHandles {
            to_transport,
            from_transport,
            join,
        }
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

    /// Pushes `n` messages inbound with `try_send`, reporting how many fit.
    struct Burst {
        n: i32,
        accepted: Sender<usize>,
    }
    impl Transport for Burst {
        type Inbound = i32;
        type Outbound = i32;
        fn run(self, _rx_out: Receiver<i32>, tx_in: Sender<i32>) -> Result<(), String> {
            let ok = (0..self.n).filter(|v| tx_in.try_send(*v).is_ok()).count();
            self.accepted.send(ok).map_err(|e| e.to_string())
        }
    }

    #[test]
    fn bounded_inbound_caps_backlog() {
        let (acc_tx, acc_rx) = crossbeam_channel::unbounded();
        let h = TransportDriver::spawn_bounded(
            Burst {
                n: 10,
                accepted: acc_tx,
            },
            RtConfig::normal(),
            4,
        );
        assert_eq!(acc_rx.recv().unwrap(), 4);
        assert!(h.join.join().unwrap().is_ok());
        assert_eq!(h.from_transport.len(), 4);
    }
}
