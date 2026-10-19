//! copper-rs source/sink tasks bridging the transport channels to the DAG.
//!
//! # Design: channel injection (Route B)
//!
//! `IngressTask<T>` and `EgressTask<T>` need their channel ends at construction
//! time, but the copper runtime constructs tasks by calling `new()` without any
//! caller-supplied context beyond the RON `ComponentConfig`.  The copper
//! `Resources` mechanism (`ResourceManager` / `ResourceBindings`) is designed
//! for *board-level hardware* resources (serial ports, buses) wired through RON
//! config — there is no way to put an arbitrary `Receiver<T>` into a
//! `ResourceManager` from Rust code rather than from a config file.  Route (A)
//! is therefore not workable.
//!
//! Route (B): a process-wide registry (see [`crate::registry`]) stores the
//! channel ends before the runtime is constructed.  The application calls
//! `IngressTask::install(rx)` and `EgressTask::install(tx)` before building the
//! copper runtime.  When the generated runtime code calls
//! `IngressTask::new(...)` / `EgressTask::new(...)`, `new()` takes the matching
//! channel end out of the registry.  Service-owned tasks use the same registry
//! through [`crate::registry::install`] / [`crate::registry::take`].
//!
//! # Choosing an ingress task
//!
//! * [`IngressTask`] forwards at most one message per iteration. Under
//!   sustained input a backlog builds up and a downstream `FsmTask` never sees
//!   a `Tick`.
//! * [`CycleIngressTask`] emits a [`Cycle`] on *every* iteration: a bounded
//!   batch of drained messages plus the cycle timestamp. Time therefore
//!   reaches the FSM (and the unified log) regardless of input load, which is
//!   what watchdogs and deadlines need. Pair it with `FsmBatchTask` and
//!   [`BatchEgressTask`] when one cycle can produce several outputs.

use crate::registry;
use bincode::{Decode, Encode};
use crossbeam_channel::{Receiver, Sender};
use cu29::prelude::*;
use msrs_core::Batch;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Copper-agnostic step helpers
// ---------------------------------------------------------------------------

/// Drain at most one inbound message (non-blocking). Returns the payload to
/// place on the source task's output, or `None` to emit an empty message.
pub fn ingress_step<T>(rx: &Receiver<T>) -> Option<T> {
    rx.try_recv().ok()
}

/// Forward a sink task's input payload (if present) to the outbound channel.
/// Returns `Err` if the transport side has hung up.
pub fn egress_step<T>(tx: &Sender<T>, payload: Option<T>) -> Result<(), String> {
    match payload {
        Some(v) => tx.send(v).map_err(|e| e.to_string()),
        None => Ok(()),
    }
}

/// Drain up to `max_batch` inbound messages (non-blocking) and stamp the
/// batch with `now_ns`, which the caller samples *after* the drain so every
/// event in the batch is known to precede it.
pub fn cycle_step<T>(rx: &Receiver<T>, max_batch: usize, now_ns: u64) -> Cycle<T> {
    let events: Vec<T> = rx.try_iter().take(max_batch).collect();
    Cycle {
        now_ns,
        events,
        pending: rx.len() as u64,
    }
}

// ---------------------------------------------------------------------------
// IngressTask<T>
// ---------------------------------------------------------------------------

/// A copper source task that drains inbound messages from the transport channel.
///
/// Each `process()` cycle calls [`ingress_step`]: if a message is ready, it is
/// set as the output payload; otherwise the output is cleared (empty slot on
/// the CopperList). Either way the output's `tov` is stamped with the cycle
/// time, so the unified log records the time a downstream `FsmTask` uses for
/// `Tick` and replay can reproduce it.
///
/// Before building the copper runtime, the caller must install the receive end
/// of the transport channel:
/// ```ignore
/// IngressTask::<MyPayload>::install(rx);
/// ```
#[derive(Reflect)]
pub struct IngressTask<T> {
    rx: Receiver<T>,
}

impl<T: Send + 'static> IngressTask<T> {
    /// Install the inbound channel end for `T`. Must be called once, before the
    /// copper runtime constructs this task.  Panics if a slot for `T` is already
    /// occupied (double-install guard).
    pub fn install(rx: Receiver<T>) {
        registry::install::<Self, _>(rx);
    }
}

impl<T> Freezable for IngressTask<T> {}

impl<T> CuSrcTask for IngressTask<T>
where
    T: CuMsgPayload + Send + 'static,
{
    type Output<'m> = CuMsg<T>;
    type Resources<'r> = ();

    fn new(_config: Option<&ComponentConfig>, _resources: Self::Resources<'_>) -> CuResult<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            rx: registry::take::<Self, _>()?,
        })
    }

    fn process<'o>(&mut self, ctx: &CuContext, new_msg: &mut Self::Output<'o>) -> CuResult<()> {
        match ingress_step(&self.rx) {
            Some(v) => new_msg.set_payload(v),
            None => new_msg.clear_payload(),
        }
        new_msg.tov = Tov::Time(ctx.now());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CycleIngressTask<T>
// ---------------------------------------------------------------------------

/// One ingress cycle: the messages drained this iteration and the cycle time.
///
/// Emitted by [`CycleIngressTask`] on every iteration, including those with no
/// events, so a downstream FSM receives time continuously and the timestamp is
/// recorded in the unified log for replay.
#[derive(Default, Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize, Reflect)]
pub struct Cycle<T> {
    /// Runtime clock time (ns), sampled after the drain.
    pub now_ns: u64,
    /// Messages drained this cycle, in arrival order (at most `max_batch`).
    pub events: Vec<T>,
    /// Messages still queued after the drain. Non-zero means input is
    /// outpacing the loop; the service decides how to react.
    pub pending: u64,
}

/// Default batch cap for [`CycleIngressTask`] when RON sets no `max_batch`.
pub const DEFAULT_MAX_BATCH: usize = 64;

/// A copper source task that emits a [`Cycle`] every iteration.
///
/// Drains up to `max_batch` messages (RON component config, default
/// [`DEFAULT_MAX_BATCH`]) and stamps the cycle with the clock read after the
/// drain:
/// ```ron
/// ( id: "ingress", type: "MyCycleIngress", config: { "max_batch": 32 } ),
/// ```
/// Install the receive end before building the runtime, exactly as with
/// [`IngressTask`]:
/// ```ignore
/// CycleIngressTask::<MyPayload>::install(rx);
/// ```
#[derive(Reflect)]
pub struct CycleIngressTask<T> {
    rx: Receiver<T>,
    max_batch: usize,
}

impl<T: Send + 'static> CycleIngressTask<T> {
    /// Install the inbound channel end for `T`. Panics on double install.
    pub fn install(rx: Receiver<T>) {
        registry::install::<Self, _>(rx);
    }
}

impl<T> Freezable for CycleIngressTask<T> {}

impl<T> CuSrcTask for CycleIngressTask<T>
where
    T: CuMsgPayload + Send + 'static,
{
    type Output<'m> = CuMsg<Cycle<T>>;
    type Resources<'r> = ();

    fn new(config: Option<&ComponentConfig>, _resources: Self::Resources<'_>) -> CuResult<Self>
    where
        Self: Sized,
    {
        let max_batch = match config {
            Some(cfg) => cfg.get::<u32>("max_batch")?.map(|v| v as usize),
            None => None,
        }
        .unwrap_or(DEFAULT_MAX_BATCH);
        if max_batch == 0 {
            return Err(CuError::from("CycleIngressTask max_batch must be > 0"));
        }
        Ok(Self {
            rx: registry::take::<Self, _>()?,
            max_batch,
        })
    }

    fn process<'o>(&mut self, ctx: &CuContext, new_msg: &mut Self::Output<'o>) -> CuResult<()> {
        let mut cycle = cycle_step(&self.rx, self.max_batch, 0);
        let now = ctx.now();
        cycle.now_ns = now.as_nanos();
        new_msg.set_payload(cycle);
        new_msg.tov = Tov::Time(now);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// EgressTask<T>
// ---------------------------------------------------------------------------

/// A copper sink task that forwards DAG output payloads to the transport channel.
///
/// Each `process()` cycle calls [`egress_step`]: if the input has a payload, it
/// is cloned and sent on the outbound channel.  An empty input (no payload) is
/// silently ignored.
///
/// Before building the copper runtime, the caller must install the send end of
/// the transport channel:
/// ```ignore
/// EgressTask::<MyPayload>::install(tx);
/// ```
#[derive(Reflect)]
pub struct EgressTask<T> {
    tx: Sender<T>,
}

impl<T: Send + 'static> EgressTask<T> {
    /// Install the outbound channel end for `T`. Must be called once, before the
    /// copper runtime constructs this task.  Panics if a slot for `T` is already
    /// occupied (double-install guard).
    pub fn install(tx: Sender<T>) {
        registry::install::<Self, _>(tx);
    }
}

impl<T> Freezable for EgressTask<T> {}

impl<T> CuSinkTask for EgressTask<T>
where
    T: CuMsgPayload + Send + 'static,
{
    type Input<'m> = CuMsg<T>;
    type Resources<'r> = ();

    fn new(_config: Option<&ComponentConfig>, _resources: Self::Resources<'_>) -> CuResult<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            tx: registry::take::<Self, _>()?,
        })
    }

    fn process<'i>(&mut self, _ctx: &CuContext, input: &Self::Input<'i>) -> CuResult<()> {
        egress_step(&self.tx, input.payload().cloned()).map_err(CuError::from)
    }
}

// ---------------------------------------------------------------------------
// BatchEgressTask<T>
// ---------------------------------------------------------------------------

/// A copper sink task that forwards every value of a [`Batch`] (for example
/// the output of `FsmBatchTask`) to the transport channel, in order.
///
/// ```ignore
/// BatchEgressTask::<MyPayload>::install(tx);
/// ```
#[derive(Reflect)]
pub struct BatchEgressTask<T> {
    tx: Sender<T>,
}

impl<T: Send + 'static> BatchEgressTask<T> {
    /// Install the outbound channel end for `T`. Panics on double install.
    pub fn install(tx: Sender<T>) {
        registry::install::<Self, _>(tx);
    }
}

impl<T> Freezable for BatchEgressTask<T> {}

impl<T> CuSinkTask for BatchEgressTask<T>
where
    T: CuMsgPayload + Send + 'static,
{
    type Input<'m> = CuMsg<Batch<T>>;
    type Resources<'r> = ();

    fn new(_config: Option<&ComponentConfig>, _resources: Self::Resources<'_>) -> CuResult<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            tx: registry::take::<Self, _>()?,
        })
    }

    fn process<'i>(&mut self, _ctx: &CuContext, input: &Self::Input<'i>) -> CuResult<()> {
        if let Some(Batch(values)) = input.payload() {
            for v in values {
                egress_step(&self.tx, Some(v.clone())).map_err(CuError::from)?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;

    #[test]
    fn ingress_passes_through_and_handles_empty() {
        let (tx, rx) = unbounded::<i32>();
        assert_eq!(ingress_step(&rx), None); // empty
        tx.send(7).unwrap();
        assert_eq!(ingress_step(&rx), Some(7));
    }

    #[test]
    fn egress_forwards_present_payload_only() {
        let (tx, rx) = unbounded::<i32>();
        egress_step(&tx, None).unwrap();
        assert!(rx.try_recv().is_err());
        egress_step(&tx, Some(9)).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 9);
    }

    #[test]
    fn cycle_step_drains_bounded_batch_and_reports_pending() {
        let (tx, rx) = unbounded::<i32>();
        for v in 0..5 {
            tx.send(v).unwrap();
        }
        let c = cycle_step(&rx, 3, 77);
        assert_eq!(c.events, vec![0, 1, 2]);
        assert_eq!(c.pending, 2);
        assert_eq!(c.now_ns, 77);
        let c = cycle_step(&rx, 3, 78);
        assert_eq!(c.events, vec![3, 4]);
        assert_eq!(c.pending, 0);
        // An empty cycle still carries time.
        let c = cycle_step(&rx, 3, 79);
        assert!(c.events.is_empty());
        assert_eq!(c.now_ns, 79);
    }

    // Each task test uses its own payload type so registry slots don't collide
    // across parallel tests.
    #[derive(Default, Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize, Reflect)]
    struct IngressProbe(u32);

    #[test]
    fn ingress_task_stamps_tov_on_empty_cycles() {
        let (_tx, rx) = unbounded::<IngressProbe>();
        IngressTask::<IngressProbe>::install(rx);
        let mut task = IngressTask::<IngressProbe>::new(None, ()).unwrap();
        let (ctx, mock) = CuContext::new_mock_clock();
        mock.set_value(1234);
        let mut out = CuMsg::<IngressProbe>::new(None);
        task.process(&ctx, &mut out).unwrap();
        assert!(out.payload().is_none());
        assert!(matches!(out.tov, Tov::Time(t) if t.as_nanos() == 1234));
    }

    #[derive(Default, Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize, Reflect)]
    struct CycleProbe(u32);

    #[test]
    fn cycle_ingress_task_emits_every_iteration() {
        let (tx, rx) = unbounded::<CycleProbe>();
        CycleIngressTask::<CycleProbe>::install(rx);
        let mut task = CycleIngressTask::<CycleProbe>::new(None, ()).unwrap();
        let (ctx, mock) = CuContext::new_mock_clock();
        let mut out = CuMsg::<Cycle<CycleProbe>>::new(None);

        mock.set_value(10);
        task.process(&ctx, &mut out).unwrap();
        let c = out.payload().expect("cycle emitted with no input");
        assert_eq!((c.now_ns, c.events.len()), (10, 0));

        tx.send(CycleProbe(1)).unwrap();
        tx.send(CycleProbe(2)).unwrap();
        mock.set_value(20);
        task.process(&ctx, &mut out).unwrap();
        let c = out.payload().unwrap();
        assert_eq!(c.now_ns, 20);
        assert_eq!(c.events, vec![CycleProbe(1), CycleProbe(2)]);
    }

    #[derive(Default, Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize, Reflect)]
    struct BatchProbe(u32);

    #[test]
    fn batch_egress_forwards_each_value_in_order() {
        let (tx, rx) = unbounded::<BatchProbe>();
        BatchEgressTask::<BatchProbe>::install(tx);
        let mut task = BatchEgressTask::<BatchProbe>::new(None, ()).unwrap();
        let ctx = CuContext::new_mock_clock().0;
        let input = CuMsg::new(Some(Batch(vec![BatchProbe(1), BatchProbe(2)])));
        task.process(&ctx, &input).unwrap();
        assert_eq!(
            rx.try_iter().collect::<Vec<_>>(),
            [BatchProbe(1), BatchProbe(2)]
        );
    }
}
