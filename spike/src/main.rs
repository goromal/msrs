//! Throwaway integration spike for copper-rs (cu29 1.0.0-rc2) + statig 0.4.
//!
//! Goal: discover & exercise the EXACT runtime/trait/dispatch signatures the
//! rest of `msrs` depends on. Run with `/tmp/xcargo run` from this directory.

use cu29::prelude::*;
use std::path::Path;

// ---------------------------------------------------------------------------
// 1. copper-rs source task: an incrementing i32 counter.
// ---------------------------------------------------------------------------

#[derive(Default, Reflect)]
pub struct Counter {
    count: i32,
}

// `Freezable` is required by all three task traits (for keyframe logging /
// determinism replay). The default impl is a no-op; we persist `count` so the
// task state survives a freeze/thaw cycle. The blanket default `impl Freezable
// for Counter {}` also compiles, but we show the real encode/decode shape here.
impl Freezable for Counter {
    fn freeze<E: cu29::bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), cu29::bincode::error::EncodeError> {
        cu29::bincode::Encode::encode(&self.count, encoder)
    }

    fn thaw<D: cu29::bincode::de::Decoder>(
        &mut self,
        decoder: &mut D,
    ) -> Result<(), cu29::bincode::error::DecodeError> {
        self.count = cu29::bincode::Decode::decode(decoder)?;
        Ok(())
    }
}

impl CuSrcTask for Counter {
    type Resources<'r> = ();
    type Output<'m> = output_msg!(i32);

    fn new(_config: Option<&ComponentConfig>, _resources: Self::Resources<'_>) -> CuResult<Self>
    where
        Self: Sized,
    {
        Ok(Self::default())
    }

    fn process(&mut self, ctx: &CuContext, output: &mut Self::Output<'_>) -> CuResult<()> {
        self.count += 1;
        // Write the payload into the CuMsg.
        output.set_payload(self.count);
        // Timestamp via the context clock (CuContext derefs to RobotClock,
        // so `ctx.now()` works; `ctx.clock.now()` is the explicit form).
        output.tov = Tov::Time(ctx.now());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 2. copper-rs sink task: prints the received i32.
// ---------------------------------------------------------------------------

#[derive(Default, Reflect)]
pub struct Printer;

impl Freezable for Printer {} // no state -> default no-op freeze/thaw is fine.

impl CuSinkTask for Printer {
    type Resources<'r> = ();
    type Input<'m> = input_msg!(i32);

    fn new(_config: Option<&ComponentConfig>, _resources: Self::Resources<'_>) -> CuResult<Self>
    where
        Self: Sized,
    {
        Ok(Self)
    }

    fn process(&mut self, _ctx: &CuContext, input: &Self::Input<'_>) -> CuResult<()> {
        // Read the payload out of the CuMsg: `.payload()` -> Option<&T>.
        if let Some(value) = input.payload() {
            println!("[copper sink] received i32 = {}", value);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 3. The #[copper_runtime] application struct.
// ---------------------------------------------------------------------------

#[copper_runtime(config = "copperconfig.ron")]
struct SpikeApplication {}

const SLAB_SIZE: Option<usize> = Some(1024 * 1024 * 10);

// ---------------------------------------------------------------------------
// 4. A minimal statig 0.4 state machine that owns an external mutable context.
// ---------------------------------------------------------------------------

mod fsm {
    use statig::prelude::*;

    /// External mutable context injected at dispatch time via
    /// `handle_with_context`. Mapped to the `context` parameter of handlers.
    #[derive(Debug, Default)]
    pub struct Ctx {
        pub transitions: u32,
    }

    pub struct Toggle;

    /// The machine storage (`&mut self` inside handlers). A 2-state machine:
    /// `closed` <-> `open`.
    #[derive(Default)]
    pub struct Gate;

    #[state_machine(initial = "State::closed()", state(derive(Debug)))]
    impl Gate {
        // Handler signature that compiles in statig 0.4: statig injects
        // `context: &mut Ctx` (the external context) and `event: &Toggle`.
        // `&mut self` is the machine storage. Returns `Outcome<State>`.
        #[state]
        fn closed(context: &mut Ctx, event: &Toggle) -> Outcome<State> {
            let _ = event;
            context.transitions += 1;
            println!("[statig] closed -> open (transition #{})", context.transitions);
            Transition(State::open())
        }

        #[state]
        fn open(context: &mut Ctx, event: &Toggle) -> Outcome<State> {
            let _ = event;
            context.transitions += 1;
            println!("[statig] open -> closed (transition #{})", context.transitions);
            Transition(State::closed())
        }
    }
}

fn run_statig() {
    use fsm::*;
    use statig::prelude::*;

    let mut ctx = Ctx::default();
    // Build & initialize. When a context is used, init also needs it.
    let mut machine = Gate::default().uninitialized_state_machine().init_with_context(&mut ctx);

    println!("[statig] initial state = {:?}", machine.state());
    for _ in 0..3 {
        // Dispatch an event WITH the external context.
        machine.handle_with_context(&Toggle, &mut ctx);
        println!("[statig] now in state = {:?}", machine.state());
    }
    println!("[statig] total transitions recorded in context = {}", ctx.transitions);
}

// ---------------------------------------------------------------------------
// 5. main(): set up the unified logger, step the runtime N times, run statig.
// ---------------------------------------------------------------------------

fn main() {
    // --- copper-rs ---
    let logger_path = "logs/spike.copper";
    if let Some(parent) = Path::new(logger_path).parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).expect("Failed to create logs directory");
        }
    }

    let mut application = SpikeApplication::builder()
        .with_log_path(logger_path, SLAB_SIZE)
        .expect("Failed to setup logger.")
        .build()
        .expect("Failed to create application.");

    println!("[copper] starting clock: {}", application.clock().now());

    // Manual stepping API generated by #[copper_runtime] (non-sim mode):
    //   start_all_tasks() -> run_one_iteration() x N -> stop_all_tasks()
    application.start_all_tasks().expect("start_all_tasks failed");
    for i in 0..5 {
        println!("[copper] --- iteration {} ---", i);
        application.run_one_iteration().expect("run_one_iteration failed");
    }
    application.stop_all_tasks().expect("stop_all_tasks failed");

    println!("[copper] end clock: {}", application.clock().now());

    // --- statig ---
    println!("--- statig demo ---");
    run_statig();
}
