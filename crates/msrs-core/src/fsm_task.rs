//! `FsmTask<M>` — the keystone copper-rs adapter.
//!
//! This module bridges the pure, copper-agnostic FSM world ([`Trigger`] /
//! [`Effects`]) to a concrete copper-rs [`CuTask`]. Business logic lives behind
//! the [`FsmSpec`] trait, which knows nothing about copper: it inspects a
//! [`Trigger`], mutates its own internal store, and emits output via
//! [`Effects`]. [`FsmTask`] wraps such a spec and drives it from copper's
//! `process()` loop, converting payload presence into a `Trigger` and writing
//! any emitted output back onto the copper output message.
//!
//! The split exists so the FSM step is unit-testable *without* a running copper
//! runtime: see [`run_step`] / [`run_step_all`] and the tests at the bottom of
//! this file.

use crate::effects::Effects;
use crate::trigger::Trigger;

/// Describes a machine's payload types and its single dispatch step.
///
/// Implementors own whatever state they need (a [`crate::Store`], a statig
/// `StateMachine`, etc.). `step` is the one entry point copper calls per cycle.
pub trait FsmSpec {
    /// Inbound message payload type.
    type In;
    /// Outbound message payload type.
    type Out;

    /// Inspect `trigger`, mutate internal store, emit output via `effects`.
    ///
    /// Under [`FsmTask`] a step may emit at most once (more is a runtime
    /// error); under [`FsmBatchTask`] it may emit any number of times.
    fn step(&mut self, trigger: &Trigger<'_, Self::In>, effects: &mut Effects<Self::Out>);
}

/// Run a single FSM step in isolation and return its (at most one) output.
///
/// Copper-agnostic: this is the unit-test seam for machines driven by
/// [`FsmTask`].
///
/// # Panics
///
/// Panics if the step emitted more than once, mirroring the error
/// [`FsmTask::process`] returns at runtime, so an accidental double emit fails
/// the test instead of being silently dropped. Machines that emit several
/// outputs per step should be tested with [`run_step_all`].
pub fn run_step<M: FsmSpec>(machine: &mut M, trigger: &Trigger<'_, M::In>) -> Option<M::Out> {
    let mut effects = Effects::default();
    machine.step(trigger, &mut effects);
    effects.take_one().unwrap_or_else(|e| panic!("{e}"))
}

/// Run a single FSM step in isolation and return every output, in emission
/// order. The unit-test seam for machines driven by [`FsmBatchTask`], and for
/// asserting "exactly one output" without relying on [`run_step`]'s panic.
pub fn run_step_all<M: FsmSpec>(machine: &mut M, trigger: &Trigger<'_, M::In>) -> Vec<M::Out> {
    let mut effects = Effects::default();
    machine.step(trigger, &mut effects);
    effects.take()
}

// ---------------------------------------------------------------------------
// copper-rs CuTask adapters
// ---------------------------------------------------------------------------

use cu29::prelude::*;

/// Build the trigger for one `process()` cycle.
///
/// A present payload is a `Message`. An empty input is a `Tick` stamped with
/// the input's time of validity, which the upstream task wrote and the unified
/// log recorded (msrs ingress tasks stamp every cycle, even empty ones), so the
/// tick time can be replayed bit-exactly. Only when upstream left `tov` unset
/// does this fall back to reading the clock, which is not replayable.
fn trigger_for<'a, T>(ctx: &CuContext, input: &'a CuMsg<T>) -> Trigger<'a, T>
where
    T: CuMsgPayload,
{
    match input.payload() {
        Some(msg) => Trigger::Message(msg),
        None => Trigger::Tick(match input.tov {
            Tov::Time(t) => t.as_nanos(),
            Tov::Range(r) => r.start.as_nanos(),
            Tov::None => ctx.now().as_nanos(),
        }),
    }
}

/// A generic copper-rs transform task that owns an [`FsmSpec`] machine.
///
/// Input payload is `M::In`, output payload is `M::Out`. Each `process()`
/// builds a [`Trigger`] (`Message` if a payload arrived, else `Tick` carrying
/// the input's recorded time of validity), runs the FSM step, and writes the
/// emitted value with `set_payload` (or `clear_payload` when nothing was
/// emitted). Emitting more than once in a step returns a `CuError` rather than
/// dropping values; use [`FsmBatchTask`] for multi-output machines.
///
/// `Tick` fires only on cycles where no message arrived, so under sustained
/// input a machine may never tick. Services with deadlines or watchdogs should
/// consume `msrs_transport::Cycle` batches instead, which carry the cycle time
/// on every iteration.
#[derive(Reflect)]
pub struct FsmTask<M> {
    machine: M,
}

impl<M: Default> Default for FsmTask<M> {
    fn default() -> Self {
        Self {
            machine: M::default(),
        }
    }
}

// No persisted state across freeze/thaw beyond the machine itself; the machine
// type is not (yet) required to be Freezable, so a no-op freeze/thaw is used.
// (Determinism replay of the FSM store is out of scope for Task 6.)
impl<M> Freezable for FsmTask<M> {}

impl<M> CuTask for FsmTask<M>
where
    M: FsmSpec + Default + Send + Sync + 'static,
    M::In: CuMsgPayload,
    M::Out: CuMsgPayload,
{
    // `input_msg!(T)` expands to `CuMsg<T>`; for a generic payload we name the
    // concrete type directly. `CuMsg<T>` impls `CuMsgPack` (the `Input` bound).
    type Input<'m> = CuMsg<M::In>;
    // `output_msg!(T)` expands to `CuMsg<T>`; `CuMsg<T>` impls `CuMsgPayload`'s
    // companion `CuMsgPack`, and the `Output` slot wants `CuMsgPayload` on the
    // payload — satisfied because `M::Out: CuMsgPayload`.
    type Output<'m> = CuMsg<M::Out>;
    type Resources<'r> = ();

    fn new(_config: Option<&ComponentConfig>, _resources: Self::Resources<'_>) -> CuResult<Self>
    where
        Self: Sized,
    {
        Ok(Self::default())
    }

    fn process(
        &mut self,
        ctx: &CuContext,
        input: &Self::Input<'_>,
        output: &mut Self::Output<'_>,
    ) -> CuResult<()> {
        let trigger = trigger_for(ctx, input);
        let mut effects = Effects::default();
        self.machine.step(&trigger, &mut effects);

        // Forward the emitted output; a double emit is a fault, not a drop.
        match effects.take_one() {
            Ok(Some(out)) => {
                output.set_payload(out);
                output.tov = Tov::Time(ctx.now());
            }
            Ok(None) => output.clear_payload(),
            Err(e) => {
                output.clear_payload();
                return Err(CuError::from(e.to_string()));
            }
        }
        Ok(())
    }
}

/// A batch of payloads carried as one copper message.
///
/// The output payload of [`FsmBatchTask`] (every value a step emitted, in
/// order) and the input of `msrs_transport::BatchEgressTask`. `Vec<T>` cannot
/// be a copper payload directly (no `TypePath` under reflect-off), hence the
/// newtype.
#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    bincode::Encode,
    bincode::Decode,
    serde::Serialize,
    serde::Deserialize,
    Reflect,
)]
pub struct Batch<T>(pub Vec<T>);

/// Like [`FsmTask`], but forwards every value a step emits as one
/// [`Batch`] message, so machines may emit any number of outputs per step
/// (for example one per event in an ingress cycle). An empty step clears the
/// output payload. Test with [`run_step_all`].
#[derive(Reflect)]
pub struct FsmBatchTask<M> {
    machine: M,
}

impl<M: Default> Default for FsmBatchTask<M> {
    fn default() -> Self {
        Self {
            machine: M::default(),
        }
    }
}

impl<M> Freezable for FsmBatchTask<M> {}

impl<M> CuTask for FsmBatchTask<M>
where
    M: FsmSpec + Default + Send + Sync + 'static,
    M::In: CuMsgPayload,
    M::Out: CuMsgPayload,
{
    type Input<'m> = CuMsg<M::In>;
    type Output<'m> = CuMsg<Batch<M::Out>>;
    type Resources<'r> = ();

    fn new(_config: Option<&ComponentConfig>, _resources: Self::Resources<'_>) -> CuResult<Self>
    where
        Self: Sized,
    {
        Ok(Self::default())
    }

    fn process(
        &mut self,
        ctx: &CuContext,
        input: &Self::Input<'_>,
        output: &mut Self::Output<'_>,
    ) -> CuResult<()> {
        let trigger = trigger_for(ctx, input);
        let mut effects = Effects::default();
        self.machine.step(&trigger, &mut effects);

        let outs = effects.take();
        if outs.is_empty() {
            output.clear_payload();
        } else {
            output.set_payload(Batch(outs));
            output.tov = Tov::Time(ctx.now());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default, Reflect)]
    struct EchoMachine {
        seen: u32,
    }

    impl FsmSpec for EchoMachine {
        type In = String;
        type Out = String;
        fn step(&mut self, trigger: &Trigger<'_, String>, effects: &mut Effects<String>) {
            if let Some(msg) = trigger.message() {
                self.seen += 1;
                effects.emit(format!("echo {}: {}", self.seen, msg));
            }
        }
    }

    #[test]
    fn message_produces_echo() {
        let mut m = EchoMachine::default();
        let input = "hi".to_string();
        assert_eq!(
            run_step(&mut m, &Trigger::Message(&input)).as_deref(),
            Some("echo 1: hi")
        );
    }

    #[test]
    fn tick_produces_nothing() {
        let mut m = EchoMachine::default();
        assert_eq!(run_step(&mut m, &Trigger::Tick(123)), None);
    }

    /// Emits twice per message.
    #[derive(Default)]
    struct DoubleEmitter;

    impl FsmSpec for DoubleEmitter {
        type In = u32;
        type Out = u32;
        fn step(&mut self, trigger: &Trigger<'_, u32>, effects: &mut Effects<u32>) {
            if let Some(v) = trigger.message() {
                effects.emit(*v);
                effects.emit(*v + 1);
            }
        }
    }

    #[test]
    #[should_panic(expected = "emitted 2 outputs")]
    fn run_step_panics_on_double_emit() {
        let mut m = DoubleEmitter;
        let _ = run_step(&mut m, &Trigger::Message(&1));
    }

    #[test]
    fn run_step_all_returns_every_emit() {
        let mut m = DoubleEmitter;
        assert_eq!(run_step_all(&mut m, &Trigger::Message(&1)), vec![1, 2]);
        assert!(run_step_all(&mut m, &Trigger::Tick(0)).is_empty());
    }

    /// Records the tick time it was given.
    #[derive(Default)]
    struct TickProbe {
        last_tick: Option<u64>,
    }

    impl FsmSpec for TickProbe {
        type In = u32;
        type Out = u32;
        fn step(&mut self, trigger: &Trigger<'_, u32>, _effects: &mut Effects<u32>) {
            if let Trigger::Tick(ns) = trigger {
                self.last_tick = Some(*ns);
            }
        }
    }

    fn task_ctx() -> CuContext {
        let (ctx, mock) = CuContext::new_mock_clock();
        mock.set_value(999);
        ctx
    }

    #[test]
    fn fsm_task_ticks_at_recorded_input_tov_not_clock() {
        let ctx = task_ctx();
        let mut task = FsmTask::<TickProbe>::default();
        let mut input = CuMsg::<u32>::new(None);
        input.tov = Tov::Time(CuTime::from(42u64));
        let mut output = CuMsg::<u32>::new(None);
        task.process(&ctx, &input, &mut output).unwrap();
        assert_eq!(task.machine.last_tick, Some(42));
    }

    #[test]
    fn fsm_task_errors_on_double_emit() {
        let ctx = task_ctx();
        let mut task = FsmTask::<DoubleEmitter>::default();
        let input = CuMsg::new(Some(1u32));
        let mut output = CuMsg::<u32>::new(None);
        assert!(task.process(&ctx, &input, &mut output).is_err());
        assert!(output.payload().is_none());
    }

    #[test]
    fn fsm_batch_task_forwards_every_emit() {
        let ctx = task_ctx();
        let mut task = FsmBatchTask::<DoubleEmitter>::default();
        let mut output = CuMsg::<Batch<u32>>::new(None);
        task.process(&ctx, &CuMsg::new(Some(5u32)), &mut output)
            .unwrap();
        assert_eq!(output.payload(), Some(&Batch(vec![5, 6])));
        task.process(&ctx, &CuMsg::<u32>::new(None), &mut output)
            .unwrap();
        assert!(output.payload().is_none());
    }
}
