//! The watchdog service: a pure [`Store`] plus a `statig`-backed [`FsmSpec`]
//! that consumes one [`Cycle`] per iteration. See `SPEC.md` for the diagram.

use bincode::{Decode, Encode};
use cu29::prelude::Reflect;
use msrs_core::store::Store;
use msrs_core::{Effects, FsmSpec, Trigger};
use msrs_transport::Cycle;
use serde::{Deserialize, Serialize};
use statig::blocking::InitializedStateMachine;
use statig::prelude::*;

/// Inbound heartbeat: a sequence number.
#[derive(Default, Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize, Reflect)]
pub struct Feed(pub u32);

/// Outbound alarm transitions.
#[derive(Default, Debug, Clone, PartialEq, Encode, Decode, Serialize, Deserialize, Reflect)]
pub enum Alarm {
    /// First feed seen; the watchdog is now supervising.
    #[default]
    Armed,
    /// No feed for longer than the timeout.
    Tripped { silent_ns: u64 },
    /// Feeds resumed after a trip.
    Recovered,
}

/// Feed silence that trips the alarm.
pub const TIMEOUT_NS: u64 = 50_000_000;

// ---------------------------------------------------------------------------
// WatchdogStore — the pure business-logic layer.
// ---------------------------------------------------------------------------

/// Pure store: when the last feed arrived, and how long silence may last.
pub struct WatchdogStore {
    timeout_ns: u64,
    last_feed_ns: Option<u64>,
}

impl Default for WatchdogStore {
    fn default() -> Self {
        Self::new(TIMEOUT_NS)
    }
}

impl Store for WatchdogStore {}

impl WatchdogStore {
    pub fn new(timeout_ns: u64) -> Self {
        Self {
            timeout_ns,
            last_feed_ns: None,
        }
    }

    /// Mutation: a feed was drained in the cycle at `now_ns`.
    pub fn record_feed(&mut self, now_ns: u64) {
        self.last_feed_ns = Some(now_ns);
    }

    /// Pure: time since the last feed (0 before any feed).
    pub fn silent_ns(&self, now_ns: u64) -> u64 {
        self.last_feed_ns.map_or(0, |t| now_ns.saturating_sub(t))
    }

    /// Pure: has the feed been silent for longer than the timeout?
    pub fn expired(&self, now_ns: u64) -> bool {
        self.last_feed_ns.is_some() && self.silent_ns(now_ns) > self.timeout_ns
    }
}

// ---------------------------------------------------------------------------
// statig state machine.
// ---------------------------------------------------------------------------

/// Context injected into handlers: the store, the cycle time, and outputs.
#[derive(Default)]
pub struct WatchdogCtx {
    pub store: WatchdogStore,
    /// `Cycle::now_ns` of the cycle being dispatched.
    pub now_ns: u64,
    /// Alarms produced during the current cycle (drained by `step`).
    pub emitted: Vec<Alarm>,
}

/// Events dispatched into the machine, in order, for each cycle.
pub enum WatchdogEvent {
    /// One drained heartbeat.
    Feed,
    /// End-of-cycle deadline check.
    Check,
}

#[derive(Default)]
pub struct WatchdogGate;

#[state_machine(initial = "State::idle()", state(derive(Debug)))]
impl WatchdogGate {
    #[state]
    fn idle(context: &mut WatchdogCtx, event: &WatchdogEvent) -> Outcome<State> {
        match event {
            WatchdogEvent::Feed => {
                context.store.record_feed(context.now_ns);
                context.emitted.push(Alarm::Armed);
                Transition(State::armed())
            }
            WatchdogEvent::Check => Handled,
        }
    }

    #[state]
    fn armed(context: &mut WatchdogCtx, event: &WatchdogEvent) -> Outcome<State> {
        match event {
            WatchdogEvent::Feed => {
                context.store.record_feed(context.now_ns);
                Handled
            }
            WatchdogEvent::Check if context.store.expired(context.now_ns) => {
                let silent_ns = context.store.silent_ns(context.now_ns);
                context.emitted.push(Alarm::Tripped { silent_ns });
                Transition(State::tripped())
            }
            WatchdogEvent::Check => Handled,
        }
    }

    #[state]
    fn tripped(context: &mut WatchdogCtx, event: &WatchdogEvent) -> Outcome<State> {
        match event {
            WatchdogEvent::Feed => {
                context.store.record_feed(context.now_ns);
                context.emitted.push(Alarm::Recovered);
                Transition(State::armed())
            }
            WatchdogEvent::Check => Handled,
        }
    }
}

// ---------------------------------------------------------------------------
// WatchdogMachine — the FsmSpec that copper's FsmBatchTask drives.
// ---------------------------------------------------------------------------

/// FSM spec: one `Cycle<Feed>` in, any number of `Alarm`s out.
pub struct WatchdogMachine {
    sm: InitializedStateMachine<WatchdogGate>,
    ctx: WatchdogCtx,
}

impl Default for WatchdogMachine {
    fn default() -> Self {
        // Eager init so initial entry actions never run inside the first step.
        let mut ctx = WatchdogCtx::default();
        let sm = WatchdogGate
            .uninitialized_state_machine()
            .init_with_context(&mut ctx);
        Self { sm, ctx }
    }
}

impl FsmSpec for WatchdogMachine {
    type In = Cycle<Feed>;
    type Out = Alarm;

    fn step(&mut self, trigger: &Trigger<'_, Cycle<Feed>>, effects: &mut Effects<Alarm>) {
        // CycleIngressTask emits every iteration, so there is no Tick path:
        // time arrives (and is logged) as `Cycle::now_ns`.
        let Some(cycle) = trigger.message() else {
            return;
        };
        self.ctx.now_ns = cycle.now_ns;
        for _ in &cycle.events {
            self.sm
                .handle_with_context(&WatchdogEvent::Feed, &mut self.ctx);
        }
        self.sm
            .handle_with_context(&WatchdogEvent::Check, &mut self.ctx);
        for alarm in self.ctx.emitted.drain(..) {
            effects.emit(alarm);
        }
    }
}

impl WatchdogMachine {
    /// Debug view of the current statig state.
    pub fn state_name(&self) -> String {
        format!("{:?}", self.sm.state())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use msrs_core::run_step_all;

    fn cycle(now_ns: u64, feeds: u32) -> Cycle<Feed> {
        Cycle {
            now_ns,
            events: (0..feeds).map(Feed).collect(),
            pending: 0,
        }
    }

    fn step(m: &mut WatchdogMachine, now_ns: u64, feeds: u32) -> Vec<Alarm> {
        run_step_all(m, &Trigger::Message(&cycle(now_ns, feeds)))
    }

    #[test]
    fn store_expires_only_after_timeout() {
        let mut s = WatchdogStore::new(10);
        assert!(!s.expired(1_000)); // never fed
        s.record_feed(100);
        assert!(!s.expired(110));
        assert!(s.expired(111));
        assert_eq!(s.silent_ns(111), 11);
    }

    #[test]
    fn idle_ignores_check() {
        let mut m = WatchdogMachine::default();
        assert_eq!(m.state_name(), "Idle");
        assert!(step(&mut m, 10 * TIMEOUT_NS, 0).is_empty());
        assert_eq!(m.state_name(), "Idle");
    }

    #[test]
    fn idle_arms_on_feed() {
        let mut m = WatchdogMachine::default();
        assert_eq!(step(&mut m, 0, 1), vec![Alarm::Armed]);
        assert_eq!(m.state_name(), "Armed");
    }

    #[test]
    fn armed_stays_armed_on_feed_and_within_timeout() {
        let mut m = WatchdogMachine::default();
        step(&mut m, 0, 1);
        assert!(step(&mut m, TIMEOUT_NS, 0).is_empty()); // guard rejects
        assert!(step(&mut m, TIMEOUT_NS + 1, 3).is_empty()); // feeds refresh
        assert!(step(&mut m, 2 * TIMEOUT_NS + 1, 0).is_empty());
        assert_eq!(m.state_name(), "Armed");
    }

    #[test]
    fn armed_trips_after_timeout() {
        let mut m = WatchdogMachine::default();
        step(&mut m, 0, 1);
        assert_eq!(
            step(&mut m, TIMEOUT_NS + 1, 0),
            vec![Alarm::Tripped {
                silent_ns: TIMEOUT_NS + 1
            }]
        );
        assert_eq!(m.state_name(), "Tripped");
    }

    #[test]
    fn tripped_recovers_on_feed() {
        let mut m = WatchdogMachine::default();
        step(&mut m, 0, 1);
        step(&mut m, TIMEOUT_NS + 1, 0);
        assert!(step(&mut m, 3 * TIMEOUT_NS, 0).is_empty()); // check: handled
        assert_eq!(step(&mut m, 3 * TIMEOUT_NS, 1), vec![Alarm::Recovered]);
        assert_eq!(m.state_name(), "Armed");
    }

    #[test]
    fn feeds_in_a_cycle_are_dispatched_before_its_check() {
        let mut m = WatchdogMachine::default();
        // One cycle: arm, then check at the same time: no trip.
        assert_eq!(step(&mut m, 5, 2), vec![Alarm::Armed]);
    }
}
