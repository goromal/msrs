# copper-rs (cu29 1.0.0-rc2) + statig 0.4 — Verified Integration Signatures

Source of truth: the throwaway crate in `spike/`, which **compiles and runs** under
cargo/rustc 1.95 (`/tmp/xcargo`). Every signature below is quoted from code that
actually built and ran. Captured run output is at the bottom.

> All copper APIs come in through a single glob import:
> ```rust
> use cu29::prelude::*;
> ```
> `Reflect`, `Freezable`, `CuSrcTask`, `CuSinkTask`, `CuTask`, `CuContext`,
> `CuResult`, `ComponentConfig`, `Tov`, `CuMsg`, and the `input_msg!`/`output_msg!`
> macros are all re-exported from the prelude.

---

## 1. Crate setup (Cargo.toml + build.rs)

`spike/Cargo.toml`:
```toml
[dependencies]
cu29 = "1.0.0-rc2"     # default features are enough; Reflect is in the prelude
statig = "0.4"
```

**A `build.rs` is mandatory** for any crate using `#[copper_runtime]`. Without it
the proc-macro panics at compile time:

> `message: no LOG_INDEX_DIR system variable set, be sure build.rs sets it`

`spike/build.rs` (verbatim, copied from the copper project template):
```rust
fn main() {
    println!(
        "cargo:rustc-env=LOG_INDEX_DIR={}",
        std::env::var("OUT_DIR").unwrap()
    );
}
```

---

## 2. `#[copper_runtime]` app struct + `main()` boilerplate

The macro is attached to an (empty) struct. The RON path is relative to the crate root.
```rust
#[copper_runtime(config = "copperconfig.ron")]
struct SpikeApplication {}

const SLAB_SIZE: Option<usize> = Some(1024 * 1024 * 10);
```

### Logger setup + manual stepping (the part that matters)

In **non-sim mode**, `#[copper_runtime]` generates these inherent methods on the app
struct: `builder()`, `clock()`, `start_all_tasks()`, `run_one_iteration()`,
`stop_all_tasks()`, and `run()` (the last loops forever). For a fixed number of
cycles, drive the runtime manually with `start_all_tasks → run_one_iteration × N →
stop_all_tasks`:

```rust
fn main() {
    // 1. Logger setup. with_log_path takes (path, Option<preallocated_slab_bytes>).
    let logger_path = "logs/spike.copper";
    if let Some(parent) = std::path::Path::new(logger_path).parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).expect("Failed to create logs directory");
        }
    }

    let mut application = SpikeApplication::builder()
        .with_log_path(logger_path, SLAB_SIZE)   // -> Result<builder, _>
        .expect("Failed to setup logger.")
        .build()                                  // -> Result<SpikeApplication, _>
        .expect("Failed to create application.");

    // 2. Clock accessor on the APP is `application.clock()` -> RobotClock.
    println!("[copper] starting clock: {}", application.clock().now());

    // 3. Manual stepping API (non-sim).
    application.start_all_tasks().expect("start_all_tasks failed");
    for i in 0..5 {
        application.run_one_iteration().expect("run_one_iteration failed");
    }
    application.stop_all_tasks().expect("stop_all_tasks failed");

    println!("[copper] end clock: {}", application.clock().now());
}
```

Notes:
- `application.clock()` returns a `cu29::clock::RobotClock`; `.now()` on it gives a
  `CuTime` that `Display`s as e.g. `530.524 µs`.
- `run()` exists but blocks forever (the template uses it); use `run_one_iteration()`
  for bounded/test stepping. All four stepping methods return `CuResult<()>`.

---

## 3. RON config (`copperconfig.ron`)

Type names in `type:` are resolved relative to the crate (tasks live at crate root,
so bare `Counter` / `Printer` work — no `crate::` prefix needed here). The `msg:` on a
connection is the **payload type**, here a bare `i32`:

```ron
(
    tasks: [
        ( id: "src",  type: "Counter" ),
        ( id: "sink", type: "Printer" ),
    ],
    cnx: [
        ( src: "src", dst: "sink", msg: "i32" ),
    ],
)
```

---

## 4. `CuSrcTask` — exact impl that compiled

```rust
#[derive(Default, Reflect)]
pub struct Counter {
    count: i32,
}

impl CuSrcTask for Counter {
    type Resources<'r> = ();
    type Output<'m> = output_msg!(i32);   // expands to CuMsg<i32>

    fn new(_config: Option<&ComponentConfig>, _resources: Self::Resources<'_>) -> CuResult<Self>
    where
        Self: Sized,
    {
        Ok(Self::default())
    }

    // NOTE: process takes ONE elided lifetime via `Self::Output<'_>`, NOT the
    // explicit `<'o>` shown in the master cutask.rs trait declaration. The
    // trait declares `fn process<'o>(&mut self, ctx, &mut Self::Output<'o>)`,
    // but impls elide it as below and compile fine in rc2.
    fn process(&mut self, ctx: &CuContext, output: &mut Self::Output<'_>) -> CuResult<()> {
        self.count += 1;
        output.set_payload(self.count);        // write payload
        output.tov = Tov::Time(ctx.now());     // timestamp; see §7
        Ok(())
    }
}
```

- `type Resources<'r>` **exists on all three task traits** in rc2 (Src, Task, Sink).
  Use `()` when the task needs no injected resources.
- `new` second arg is `_resources: Self::Resources<'_>` (a value, not `&`).

---

## 5. `CuSinkTask` — exact impl that compiled

```rust
#[derive(Default, Reflect)]
pub struct Printer;

impl CuSinkTask for Printer {
    type Resources<'r> = ();
    type Input<'m> = input_msg!(i32);   // expands to CuMsg<i32>

    fn new(_config: Option<&ComponentConfig>, _resources: Self::Resources<'_>) -> CuResult<Self>
    where
        Self: Sized,
    {
        Ok(Self)
    }

    fn process(&mut self, _ctx: &CuContext, input: &Self::Input<'_>) -> CuResult<()> {
        if let Some(value) = input.payload() {       // read payload (see §6)
            println!("[copper sink] received i32 = {}", value);
        }
        Ok(())
    }
}
```

(For reference, `CuTask` — the middle/transform variant, not used in the spike but
verified from the caterpillar example — has BOTH `type Input<'m>: CuMsgPack` and
`type Output<'m>: CuMsgPayload`, plus `type Resources<'r>`, and
`fn process(&mut self, ctx, input: &Self::Input<'_>, output: &mut Self::Output<'_>)`.)

---

## 6. `CuMsg<T>` payload read / write (real method names)

From `cu29_runtime::cutask` (`impl<T> CuMsg<T>`):
```rust
pub fn new(payload: Option<T>) -> Self
pub fn payload(&self) -> Option<&T>          // READ  -> used in the sink
pub fn set_payload(&mut self, payload: T)    // WRITE -> used in the source
pub fn clear_payload(&mut self)
pub fn payload_mut(&mut self) -> &mut Option<T>
```
- Read: `input.payload()` returns `Option<&T>` (the message may be empty).
- Write: `output.set_payload(value)` takes `T` by value.
- `CuMsg` also has a public `tov: Tov` field (time-of-validity) and a
  `metadata` field (`output.metadata.set_status(...)` exists; optional).

---

## 7. `CuContext` clock accessor (inside a task)

`CuContext` holds a public `clock: RobotClock` field and **derefs to `RobotClock`**,
so inside `process` either of these gives the current timestamp:
```rust
ctx.now()         // via Deref<Target = RobotClock>  <-- used in the spike
ctx.clock.now()   // explicit field access
```
Set a message's time-of-validity with:
```rust
output.tov = Tov::Time(ctx.now());
```
(`Tov` is an enum re-exported from the prelude; `Tov::Time(CuTime)` is the timestamped
variant.)

---

## 8. `Freezable` / `Reflect` — what they required in practice

- **`Reflect`**: every task struct must `#[derive(Reflect)]`. It is a supertrait bound
  on all three task traits (`CuSrcTask: Freezable + Reflect`, etc.). The derive is
  re-exported from `cu29::prelude`. Worked on a unit struct (`Printer`) and a
  fielded struct (`Counter`) with no extra attributes.

- **`Freezable`**: also a supertrait on all task traits. Two valid forms:
  - **No-op (stateless tasks):** an empty impl uses the trait's default
    freeze/thaw:
    ```rust
    impl Freezable for Printer {}
    ```
  - **Stateful (persist fields for keyframe/replay):** implement freeze/thaw with
    bincode. The bincode types live under `cu29::bincode`:
    ```rust
    impl Freezable for Counter {
        fn freeze<E: cu29::bincode::enc::Encoder>(&self, encoder: &mut E)
            -> Result<(), cu29::bincode::error::EncodeError> {
            cu29::bincode::Encode::encode(&self.count, encoder)
        }
        fn thaw<D: cu29::bincode::de::Decoder>(&mut self, decoder: &mut D)
            -> Result<(), cu29::bincode::error::DecodeError> {
            self.count = cu29::bincode::Decode::decode(decoder)?;
            Ok(())
        }
    }
    ```
  (cu29 re-exports `bincode` — it's the `cu-bincode` fork at 2.x.)

---

## 9. statig 0.4 — handler signature + dispatch with context

```rust
use statig::prelude::*;

#[derive(Debug, Default)]
pub struct Ctx { pub transitions: u32 }   // external mutable context

pub struct Toggle;                         // the event

#[derive(Default)]
pub struct Gate;                           // machine storage (the `&mut self`)

#[state_machine(initial = "State::closed()", state(derive(Debug)))]
impl Gate {
    // EXACT handler signature that compiled: statig injects `context: &mut Ctx`
    // (the external context, by parameter NAME `context`) and `event: &Toggle`.
    // `&mut self` is optional and refers to the machine storage. Returns
    // `Outcome<State>`. The macro generates the `State`/`Superstate` enums.
    #[state]
    fn closed(context: &mut Ctx, event: &Toggle) -> Outcome<State> {
        context.transitions += 1;
        Transition(State::open())          // or `Handled` / `Super`
    }

    #[state]
    fn open(context: &mut Ctx, event: &Toggle) -> Outcome<State> {
        context.transitions += 1;
        Transition(State::closed())
    }
}
```

Building, **initializing with context**, and dispatching:
```rust
let mut ctx = Ctx::default();

// Because handlers take a context, init also needs it:
let mut machine = Gate::default()
    .uninitialized_state_machine()
    .init_with_context(&mut ctx);          // NOT plain .init()

machine.state();                            // -> &State (Debug-printable)

// Dispatch an event WITH the external context:
machine.handle_with_context(&Toggle, &mut ctx);
```

Key facts:
- Context is matched by the **parameter name `context`** in handlers (statig 0.4
  convention). `&mut self` (machine storage) and `event: &T` are also injected by name.
- Handlers return `Outcome<State>`: variants used are `Transition(State::x())`,
  `Handled`, `Super` — all re-exported from `statig::prelude`.
- When a context type is used, the lifecycle is
  `.uninitialized_state_machine().init_with_context(&mut ctx)` then
  `.handle_with_context(&event, &mut ctx)`. (Plain `.init()` / `.handle()` are for
  context-free machines.)
- `machine.state()` returns the current `State` for inspection.

---

## 10. rc2 vs master trait-shape deltas observed

- The `process` methods are declared in the master trait with explicit lifetimes
  (`fn process<'o>(...)`, `fn process<'i,'o>(...)`), but **impls elide them** using
  `Self::Output<'_>` / `Self::Input<'_>` and compile cleanly in rc2 (see §4/§5).
- `type Resources<'r>` is present on **all three** task traits in rc2 (the task
  prompt's verified shapes omitted it for `CuTask`/`CuSrcTask`/`CuSinkTask`); set it to
  `()` when unused, and `new`'s signature is
  `fn new(config: Option<&ComponentConfig>, resources: Self::Resources<'_>)`.
- A `build.rs` exporting `LOG_INDEX_DIR=$OUT_DIR` is a hard requirement for
  `#[copper_runtime]` (compile-time panic otherwise) — easy to miss.

---

## Captured run output (`/tmp/xcargo run` in `spike/`)

```
[copper] starting clock: 530.524 µs
[copper] --- iteration 0 ---
[copper sink] received i32 = 1
[copper] --- iteration 1 ---
[copper sink] received i32 = 2
[copper] --- iteration 2 ---
[copper sink] received i32 = 3
[copper] --- iteration 3 ---
[copper sink] received i32 = 4
[copper] --- iteration 4 ---
[copper sink] received i32 = 5
[copper] end clock: 567.806 µs
--- statig demo ---
[statig] initial state = Closed
[statig] closed -> open (transition #1)
[statig] now in state = Open
[statig] open -> closed (transition #2)
[statig] now in state = Closed
[statig] closed -> open (transition #3)
[statig] now in state = Open
[statig] total transitions recorded in context = 3
Flushing the unified Logger ...
Unified Logger flushed.
```
```
$ /tmp/xcargo build   # at repo root -> builds only msrs-core, msrs-transport, echo
   Compiling msrs-transport v0.0.0 (.../crates/msrs-transport)
   Compiling echo v0.0.0 (.../examples/echo)
    Finished `dev` profile [unoptimized + debuginfo] target(s)
# (spike NOT built; `cargo metadata` members = msrs-core, msrs-transport, echo)
```

---

## 11. Payload-type constraints discovered in Task 9 (echo example)

- **Plain `String` cannot be a copper payload** under cu29 rc2's default
  (reflect-off) build: payloads must implement `TypePath`, and only primitives
  get it. Fix: a newtype deriving the full `CuMsgPayload` set —
  `#[derive(Default, Debug, Clone, Encode, Decode, Serialize, Deserialize, Reflect)]
  pub struct EchoMsg(pub String);` (the stub `Reflect` derive supplies `TypePath`).
- **Do not depend on `cu29-export` for log reading** — it force-enables
  `cu29/reflect`, flipping the whole build to `bevy_reflect` and breaking
  `#[derive(Reflect)]` on structs with non-reflectable fields (e.g. crossbeam
  channel ends in `IngressTask`/`EgressTask`). Instead read logs via the prelude:
  `gen_cumsgs!("copperconfig.ron")` + `UnifiedLoggerBuilder` → `UnifiedLogger::Read`
  → `UnifiedLoggerIOReader::new(read, UnifiedLogType::CopperList)` → loop
  `cu29::bincode::decode_from_std_read::<CopperList<CuMsgs>, _, _>(&mut reader, standard())`.
  Copperlist slot order matches `<CuMsgs as MatchingTasks>::get_all_task_ids()`.
- **RON `type:` strings**: crate-root type aliases (`pub type EchoFsm =
  msrs_core::FsmTask<EchoMachine>;` referenced as `"EchoFsm"`) side-step
  generic-path parsing; `msg:` needs a fully qualified path (`"crate::EchoMsg"`)
  because `gen_cumsgs!` codegen lands in a nested module.
- **Replay + channel injection**: `IngressTask::install`/`EgressTask::install`
  slots are consumed by `new()`, so a second runtime build in the same process
  (replay) just requires calling `install()` again with fresh ends.
