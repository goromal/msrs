# msrs — Session Handoff

**Last updated:** 2026-07-01
**Repo:** `/data/andrew/dev/orch/sources/msrs` (standalone git repo)
**Active branch:** `dev/msrs-mvp` (feature branch off `main`; do NOT work on `main`)
**Progress:** 7 of 10 implementation tasks complete (Tasks 0–6). Tasks 7, 8, 9 remain.

---

## What msrs is

A Rust conventions/glue framework over **copper-rs** (`cu29`) that packages a pure `Store` + a
`statig` FSM as a deterministic copper-rs task, with a `Transport` plug-in for IPC and an `RtConfig`
for real-time thread controls. It is the Rust successor to **mscpp**. Full design rationale is in
[README.md](README.md) (the approved design spec). Implementation plan is in
[docs/superpowers/plans/2026-06-29-msrs-implementation.md](docs/superpowers/plans/2026-06-29-msrs-implementation.md).

---

## ⚠️ Environment: the toolchain is NOT on PATH

There is **no `cargo`/`rustc` on PATH**. This machine is NixOS. Prior sessions used a wrapper at
`/tmp/xcargo` — **that temp file will NOT exist in your new session; recreate it first:**

```bash
cat > /tmp/xcargo <<'EOF'
#!/usr/bin/env bash
exec nix shell nixpkgs#cargo nixpkgs#rustc nixpkgs#pkg-config --command cargo "$@"
EOF
chmod +x /tmp/xcargo
/tmp/xcargo --version   # cargo 1.95.0
```

Use `/tmp/xcargo <args>` for every cargo command (build, test, run). The first build of `cu29`'s
tree is already cached; incremental builds are fast.

`gh` CLI is authenticated (as `goromal`) and was used to read copper-rs source/templates.

---

## How to resume (subagent-driven execution)

This work is being executed via the **superpowers-extended-cc:subagent-driven-development** skill
(coordinator stays in-session, dispatches one implementer subagent per task, then reviews). To
resume the same way, or to use the parallel-session variant:

```
/superpowers-extended-cc:executing-plans docs/superpowers/plans/2026-06-29-msrs-implementation.md
```

The task-persistence file
[docs/superpowers/plans/2026-06-29-msrs-implementation.md.tasks.json](docs/superpowers/plans/2026-06-29-msrs-implementation.md.tasks.json)
is the source of truth for task status (Tasks 0–6 = `completed`, Tasks 7–9 = `pending`). Native task
IDs #14/#15/#16 map to plan Tasks 7/8/9.

**Per-task loop:** dispatch implementer with the task's full text + the ground-truth notes (below) →
spec review → code-quality review → mark complete → sync `.tasks.json`. Serialize implementers
(Tasks 7 & 8 both touch `crates/msrs-transport/src/lib.rs`).

---

## 🔑 Ground truth for all remaining copper-rs/statig work

**READ FIRST:** [docs/superpowers/notes/copper-statig-signatures.md](docs/superpowers/notes/copper-statig-signatures.md)
— compiler-verified cu29 1.0.0-rc2 + statig 0.4 signatures captured by the Task 5 spike.
**Working reference code:** [spike/src/main.rs](spike/src/main.rs) — a runnable source+sink copper app
+ statig machine. (The `spike/` crate is excluded from the workspace via `exclude = ["spike"]` in the
root `Cargo.toml`; run it with `cd spike && /tmp/xcargo run`.)

Key facts any implementer MUST honor:
- Every copper task struct needs `#[derive(Reflect)]` (from `cu29::prelude`) + an `impl Freezable`
  (empty `impl Freezable for T {}` is fine for stateless tasks).
- `#[copper_runtime(config="…ron")]` **requires a `build.rs`** exporting `LOG_INDEX_DIR=$OUT_DIR`
  (see `spike/build.rs`) — omitting it is a compile-time panic.
- Runtime stepping (non-sim): `App::builder().with_log_path(path, Option<slab_bytes>).build()`, then
  `start_all_tasks()` → `run_one_iteration()` ×N → `stop_all_tasks()`. `application.clock().now()` for
  app-level time. (`run()` blocks forever — use `run_one_iteration()` for bounded/test runs.)
- `CuSrcTask`/`CuTask`/`CuSinkTask`: `type Resources<'r> = ()`;
  `fn new(_config: Option<&ComponentConfig>, _res: Self::Resources<'_>) -> CuResult<Self>`;
  `fn process(&mut self, ctx: &CuContext, …) -> CuResult<()>`.
- `input_msg!(T)` / `output_msg!(T)` expand to **`CuMsg<T>`** — so generic tasks can name `CuMsg<T>`
  directly (this is how `FsmTask<M>` uses `type Input<'m> = CuMsg<M::In>`).
- `CuMsg` payload: read `input.payload() -> Option<&T>`; write `output.set_payload(v)`;
  `output.clear_payload()`; timestamp via `output.tov = Tov::Time(ctx.now())`.
- `CuContext` derefs to `RobotClock`: `ctx.now()` returns `CuTime(u64)` with `.as_nanos() -> u64`.
- `statig` 0.4: handler `fn state(context: &mut Ctx, event: &Ev) -> Outcome<State>` (context injected
  by **parameter name** `context`); init via
  `Machine::default().uninitialized_state_machine().init_with_context(&mut ctx)`; dispatch
  `machine.handle_with_context(&Ev, &mut ctx)`; inspect via `machine.state()`.

---

## What's done (Tasks 0–6, all committed on `dev/msrs-mvp`)

| Task | Commit | What |
|------|--------|------|
| 0 Scaffold | `cb31df7`/`cce2166` | Cargo workspace: `crates/msrs-core`, `crates/msrs-transport`, `examples/echo`; deps cu29=1.0.0-rc2, statig=0.4, thread-priority=3, core_affinity=0.8, crossbeam-channel=0.5; Cargo.lock committed. |
| 1 `Trigger` | `5f195ca` | `Trigger<'a,In>::{Tick(u64), Message(&In)}` + `message()`/`is_tick()`. `crates/msrs-core/src/trigger.rs`. |
| 2 `Effects` | `58ea638` | `Effects<Out>::{emit, is_empty, take, take_one}` — write-only capability. `.../effects.rs`. |
| 3 `Store` | `8c075c0` | `Store` marker trait + purity doc. `.../store.rs`. |
| 4 `RtConfig` | `ddb3885` | `RtConfig{scheduler,affinity}`, `SchedPolicy::{Normal,Fifo(u8)}`, best-effort `apply()` (thread-priority 3.1.1: `ThreadPriorityValue::try_from`). `.../rt.rs`. |
| 5 Spike | `3f34d0d` | Runnable copper source+sink + statig machine; signatures captured in the notes file. `spike/`. |
| 6 `FsmTask` | `d96a2ae` | Generic `FsmTask<M>` impls `CuTask` (Input `CuMsg<M::In>`, Output `CuMsg<M::Out>`); `FsmSpec` trait + copper-agnostic `run_step`. `.../fsm_task.rs`. |

**State:** `msrs-core` builds and passes **9/9 tests** (`/tmp/xcargo test -p msrs-core`). `msrs-transport`
is still an empty skeleton (`lib.rs` doc comment only). `examples/echo` is a placeholder `main`.

There is one uncommitted change in the working tree: the `.tasks.json` status sync for Task 6 (being
committed alongside this handoff).

---

## What remains

### Task 7 (#14): `Transport` trait + `TransportDriver`  — blockedBy Task 4 (done) ⇒ ready
Files: `crates/msrs-transport/src/{transport.rs,driver.rs,lib.rs}`. Full code is in the plan
(§Task 7) — it's copper-agnostic (crossbeam channels + a driver thread that applies `RtConfig`),
with an in-memory `Loopback` test. Should be low-risk. `Verify: /tmp/xcargo test -p msrs-transport`.

### Task 8 (#15): `IngressTask`/`EgressTask` copper bridge tasks — blockedBy Tasks 5, 7
Files: `crates/msrs-transport/src/tasks.rs` (+ lib). Copper-agnostic `ingress_step`/`egress_step`
helpers (full code in plan §Task 8) with tests, THEN `IngressTask<T>: CuSrcTask` /
`EgressTask<T>: CuSinkTask` holding `Receiver<T>`/`Sender<T>`, using the spike signatures. Channels
are injected (not from RON) — confirm the injection mechanism (constructor vs `Resources`) against
the spike. `Verify: /tmp/xcargo test -p msrs-transport tasks`.

### Task 9 (#16): Echo example end-to-end — blockedBy Tasks 6, 8 — **USER-GATE**
Files: `examples/echo/src/{echo.rs,main.rs}`, `examples/echo/copperconfig.ron`, needs a `build.rs`
(per spike). `EchoStore` (pure) + `EchoMachine` (statig, impls `FsmSpec<In=String,Out=String>`) wired
`ingress → FsmTask<EchoMachine> → egress` via RON, fed by an in-process transport. Must run and echo,
and demonstrate unified-log **replay determinism** (bit-for-bit). This task is a user-ordered
verification gate: close only with captured per-criterion evidence (`AC: … — PROVEN BY …`); the
`post-task-complete-revalidate.sh` hook enforces this.

**Likely friction points for Task 9:** (a) wiring `statig`'s `StateMachine` wrapper inside
`EchoMachine::step` (FsmTask deliberately left statig to the `FsmSpec` implementor — the echo machine
is where statig actually gets exercised); (b) the RON task-type names + edges; (c) getting a bounded
run + replay via `run_one_iteration()`. Lean on `spike/src/main.rs`.

---

## Guardrails / gotchas encountered

- **Superpowers hooks are active.** A `blockedBy` hook blocks moving a task to `in_progress` until its
  blockers are `completed` (mark the blocker complete first; beware races if you batch TaskUpdates).
  A user-gate revalidation hook fires on closing Tasks 5 & 9 and demands `AC: … — PROVEN BY …`
  evidence in the same turn — run the verification yourself and post per-criterion lines.
- **Two user-gate tasks:** Task 5 (done, evidence posted) and Task 9 (pending).
- `spike/` writes ~10 MB of log slabs per `run_one_iteration()` into `spike/logs/` (gitignored).
- Determinism-of-FSM-state across copper freeze/thaw is currently a no-op in `FsmTask` (documented in
  code) — out of scope for the MVP; revisit if keyframe replay of FSM internals is needed.
- Per-middleware transport crates (`msrs-transport-r2r`, `-iceoryx2`, `-tonic`) are **explicitly
  deferred** — not in this plan. Task 9 uses an in-process transport only.

## When all tasks are done
Dispatch a final full-implementation code review, then use
**superpowers-extended-cc:finishing-a-development-branch** to integrate `dev/msrs-mvp`.
