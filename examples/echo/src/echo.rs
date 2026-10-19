//! The echo service: a pure [`Store`] plus a `statig`-backed [`FsmSpec`].
//!
//! `EchoStore` holds the business state (a monotonic message counter) and
//! exposes a single pure query, [`EchoStore::echo`]. `EchoMachine` wraps a real
//! `statig` state machine that, on each inbound message, calls into the store
//! and emits the formatted echo — bridging the pure world to copper-rs via
//! [`FsmSpec`].
//!
//! Copper message payloads must implement `TypePath` (a reflection trait). Under
//! cu29 rc2's default (reflect-off) build, only primitive types get `TypePath`,
//! so `String` cannot be a payload directly. [`EchoMsg`] is a thin newtype that
//! derives the full `CuMsgPayload` bound set (and `TypePath` via the stub
//! `Reflect` derive); the pure store still speaks plain `&str`/`String`.

use bincode::{Decode, Encode};
use cu29::prelude::Reflect;
use msrs_core::store::Store;
use msrs_core::{Effects, FsmSpec, Trigger};
use serde::{Deserialize, Serialize};
use statig::blocking::InitializedStateMachine;
use statig::prelude::*;

/// The copper-rs message payload for the echo DAG: a wrapped `String`.
#[derive(Default, Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize, Reflect)]
pub struct EchoMsg(pub String);

impl EchoMsg {
    pub fn new(s: impl Into<String>) -> Self {
        EchoMsg(s.into())
    }
}

// ---------------------------------------------------------------------------
// EchoStore — the pure business-logic layer.
// ---------------------------------------------------------------------------

/// Pure store for the echo service.
#[derive(Default)]
pub struct EchoStore {
    /// Number of messages echoed so far.
    pub count: u64,
}

impl Store for EchoStore {}

impl EchoStore {
    /// Pure: format an echo of `msg` (does not mutate; caller bumps count).
    pub fn echo(&self, msg: &str) -> String {
        format!("echo {}: {}", self.count + 1, msg)
    }
}

// ---------------------------------------------------------------------------
// statig state machine.
//
// The machine genuinely drives the echo: its single `running` state handles an
// inbound `EchoEvent`, calls `EchoStore::echo`, records the output on the
// context, and bumps the store's counter. `EchoMachine::step` dispatches into
// this machine via `handle_with_context` on every `Trigger::Message`.
// ---------------------------------------------------------------------------

/// External mutable context injected into `statig` handlers at dispatch time.
#[derive(Default)]
pub struct EchoCtx {
    /// The pure store the handler queries and mutates.
    pub store: EchoStore,
    /// Output produced by the last dispatch (drained by `step`).
    pub emitted: Option<String>,
}

/// The event fed to the state machine: one inbound message body.
pub struct EchoEvent(pub String);

/// The machine storage (`&mut self` inside handlers). Single-state.
#[derive(Default)]
pub struct EchoGate;

#[state_machine(initial = "State::running()", state(derive(Debug)))]
impl EchoGate {
    /// The only state: echo every message via the store, stay running.
    #[state]
    fn running(context: &mut EchoCtx, event: &EchoEvent) -> Outcome<State> {
        context.emitted = Some(context.store.echo(&event.0));
        context.store.count += 1;
        Handled
    }
}

// ---------------------------------------------------------------------------
// EchoMachine — the FsmSpec that copper's FsmTask drives.
// ---------------------------------------------------------------------------

/// FSM spec for the echo task. Owns a live `statig` state machine plus its
/// context; each `Message` trigger dispatches through the machine.
///
/// The `statig` machine is initialized eagerly in `Default`, because
/// `init_with_context` runs the initial state's entry actions (and those of its
/// superstates). Initializing lazily inside `step` would run them during, and
/// attribute them to, the first trigger. No `Reflect` derive is needed: the
/// reflect-off blanket `impl<T: 'static> Reflect for T` covers it, and
/// `FsmTask`'s own `Reflect` derive does not bound its `machine` field.
pub struct EchoMachine {
    sm: InitializedStateMachine<EchoGate>,
    ctx: EchoCtx,
}

impl Default for EchoMachine {
    fn default() -> Self {
        let mut ctx = EchoCtx::default();
        let sm = EchoGate
            .uninitialized_state_machine()
            .init_with_context(&mut ctx);
        Self { sm, ctx }
    }
}

impl FsmSpec for EchoMachine {
    type In = EchoMsg;
    type Out = EchoMsg;

    fn step(&mut self, trigger: &Trigger<'_, EchoMsg>, effects: &mut Effects<EchoMsg>) {
        if let Some(msg) = trigger.message() {
            self.ctx.emitted = None;
            self.sm
                .handle_with_context(&EchoEvent(msg.0.clone()), &mut self.ctx);
            if let Some(out) = self.ctx.emitted.take() {
                effects.emit(EchoMsg(out));
            }
        }
        // Tick → no output (nothing to echo).
    }
}

impl EchoMachine {
    /// Debug view of the current statig state (for evidence/inspection).
    pub fn state_name(&self) -> String {
        format!("{:?}", self.sm.state())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use msrs_core::run_step;

    #[test]
    fn echo_formats_with_next_index() {
        let s = EchoStore::default();
        assert_eq!(s.echo("hi"), "echo 1: hi");
    }

    #[test]
    fn echo_uses_current_count() {
        let s = EchoStore { count: 4 };
        assert_eq!(s.echo("hi"), "echo 5: hi");
    }

    #[test]
    fn machine_echoes_on_message_and_counts_up() {
        let mut m = EchoMachine::default();
        let a = EchoMsg::new("hello");
        let b = EchoMsg::new("world");
        assert_eq!(
            run_step(&mut m, &Trigger::Message(&a)),
            Some(EchoMsg::new("echo 1: hello"))
        );
        assert_eq!(
            run_step(&mut m, &Trigger::Message(&b)),
            Some(EchoMsg::new("echo 2: world"))
        );
    }

    #[test]
    fn machine_emits_nothing_on_tick() {
        let mut m = EchoMachine::default();
        assert_eq!(run_step(&mut m, &Trigger::Tick(123)), None);
    }

    #[test]
    fn statig_machine_is_initialized_before_first_step() {
        let mut m = EchoMachine::default();
        // Eager init: the real statig machine is live before any trigger.
        assert_eq!(m.state_name(), "Running");
        let a = EchoMsg::new("x");
        let _ = run_step(&mut m, &Trigger::Message(&a));
        assert_eq!(m.state_name(), "Running");
    }
}
