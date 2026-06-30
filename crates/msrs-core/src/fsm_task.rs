//! `FsmTask<M>` — the keystone copper-rs adapter.
//!
//! This module bridges the pure, copper-agnostic FSM world ([`Trigger`] /
//! [`Effects`]) to a concrete copper-rs [`CuTask`]. Business logic lives behind
//! the [`FsmSpec`] trait, which knows nothing about copper: it inspects a
//! [`Trigger`], mutates its own internal store, and emits 0/1 output via
//! [`Effects`]. [`FsmTask`] wraps such a spec and drives it from copper's
//! `process()` loop, converting payload presence into a `Trigger` and writing
//! any emitted output back onto the copper output message.
//!
//! The split exists so the FSM step is unit-testable *without* a running copper
//! runtime: see [`run_step`] and the tests at the bottom of this file.

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

    /// Inspect `trigger`, mutate internal store, emit 0/1 output via `effects`.
    fn step(&mut self, trigger: &Trigger<'_, Self::In>, effects: &mut Effects<Self::Out>);
}

/// Run a single FSM step in isolation and return its (at most one) output.
///
/// Copper-agnostic: this is the unit-test seam. [`FsmTask::process`] calls it.
pub fn run_step<M: FsmSpec>(machine: &mut M, trigger: &Trigger<'_, M::In>) -> Option<M::Out> {
    let mut effects = Effects::default();
    machine.step(trigger, &mut effects);
    effects.take_one()
}

// ---------------------------------------------------------------------------
// copper-rs CuTask adapter
// ---------------------------------------------------------------------------

use cu29::prelude::*;

/// A generic copper-rs transform task that owns an [`FsmSpec`] machine.
///
/// Input payload is `M::In`, output payload is `M::Out`. Each `process()`:
/// builds a [`Trigger`] (`Message` if a payload arrived, else `Tick` carrying
/// the context clock's nanosecond timestamp), runs the FSM step via
/// [`run_step`], and writes the emitted value with `set_payload` (or
/// `clear_payload` when nothing was emitted).
#[derive(Reflect)]
pub struct FsmTask<M> {
    machine: M,
}

impl<M: Default> Default for FsmTask<M> {
    fn default() -> Self {
        Self { machine: M::default() }
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
        // Build the trigger from payload presence.
        let trigger = match input.payload() {
            Some(msg) => Trigger::Message(msg),
            None => Trigger::Tick(ctx.now().as_nanos()),
        };

        // Drive the FSM step and forward any emitted output.
        match run_step(&mut self.machine, &trigger) {
            Some(out) => {
                output.set_payload(out);
                output.tov = Tov::Time(ctx.now());
            }
            None => output.clear_payload(),
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
}
