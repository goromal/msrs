# msrs (microservice-rs) — Design Spec

**Date:** 2026-06-29
**Status:** Approved design, pre-implementation
**Replaces:** [mscpp](https://github.com/goromal/mscpp) (deterministic, FSM-driven reactor framework for C++)

---

## 1. Purpose

msrs is the Rust successor to mscpp: a framework for writing **deterministic, testable, FSM-driven
microservices and daemons** — the kind used in robotics, where reproducible behavior and clean
separation between business logic and I/O matter more than raw concurrency.

mscpp achieved determinism by hand-rolling a single-threaded, logical-tag reactor and enforcing a
three-layer architecture (Store / FSM States / Reactor) through C++ `final` methods and `private`
members. msrs achieves the **same architectural guarantees** while delegating the runtime,
scheduling, message passing, and logging/replay to [copper-rs](https://github.com/copper-project/copper-rs)
(crate `cu29`), a mature Rust robotics runtime.

**msrs is therefore a conventions + glue layer, not a runtime.** It is a small set of crates that
defines:

1. How a Store + FSM is packaged as a copper-rs task.
2. A `Transport` trait and bridging pattern for plugging in IPC middleware (r2r/ROS 2, iceoryx2, gRPC).
3. A real-time configuration helper (`RtConfig`) built on `thread-priority` and `core_affinity`.

### Non-goals

- msrs does **not** implement its own scheduler, clock, or message bus — copper-rs owns these.
- msrs does **not** reimplement mscpp's logical-tag/microstep machinery; copper-rs's compile-time
  DAG and cycle clock provide the equivalent ordering guarantees.
- msrs does **not** wrap every middleware itself; it ships a few reference `Transport` impls and a
  contract for others.

---

## 2. Design tenets (carried over from mscpp)

1. **Determinism** — single-threaded copper-rs scheduling by default; all non-determinism is
   quarantined to transport driver threads and sampled at cycle boundaries.
2. **Testability** — all business logic lives in pure Store functions, unit-testable in isolation.
3. **Enforced separation** — the type system prevents business logic from touching I/O, the clock,
   or the runtime.
4. **Clean async I/O boundary** — middleware runs on its own thread and communicates with the
   deterministic DAG only through lock-free channels.
5. **Replayability** — every task's inputs and outputs are recorded by copper-rs's unified log,
   enabling bit-for-bit replay debugging.

---

## 3. Concept mapping: mscpp → msrs

| mscpp concept | msrs equivalent | Mechanism |
|---|---|---|
| Store (pure business logic) | `Store` struct | Plain Rust, private fields, pure `&self`/`&mut self` methods, no dependencies. |
| FSM States (coordination) | `statig` state machine | `#[state]` handlers read input messages → call Store → write effects. |
| Reactor (enforcement shell) | `FsmTask<S, M>` | A copper-rs `CuTask` adapter; the only component the runtime sees. |
| Ports (`InputPort`/`OutputPort`) | typed CopperList messages: inputs via `Trigger::Message`, outputs via `Outbox<T>` | copper-rs message graph. |
| `StepTrigger` (HEARTBEAT / LOGICAL_ACTION) | `Trigger { Tick(CuTime), Message(M::In) }` | Cycle tick vs. message arrival. |
| `LogicalTag` + microsteps | `CuTime` + intra-cycle DAG ordering | Inherited from copper-rs. |
| I/O Adapters (gRPC, ROS2) | `Transport` trait + driver thread + Ingress/Egress tasks | See §6. |
| `ReactorScheduler` | copper-rs runtime (RON-configured DAG) | Inherited. |
| spdlog + custom logging | `cu29-unifiedlog` (structured, replayable) | Inherited. |
| *(new in msrs)* | `RtConfig` | `thread-priority` + `core_affinity` applied to runtime/driver threads. |

---

## 4. Architecture overview

```
┌──────────────────────────────────────────────────────────────────┐
│  Transport driver threads (one per middleware, NON-deterministic)  │
│   r2r executor   │   iceoryx2 poll   │   tonic/gRPC server         │
└─────────┬──────────────────┬───────────────────┬──────────────────┘
          │ lock-free channels (the determinism boundary)            │
          ▼                                        ▲
┌──────────────────────────────────────────────────────────────────┐
│  copper-rs DAG (single-threaded, deterministic, replay-logged)     │
│                                                                    │
│   IngressTask ──▶ FsmTask<S,M> ──▶ FsmTask<S,M> ──▶ EgressTask     │
│   (source)         │                                  (sink)       │
│                    ├─ Trigger ─▶ statig StateMachine               │
│                    │                 └─ &mut Store (pure logic)    │
│                    └─ Outbox<T> ─▶ CopperList                      │
└──────────────────────────────────────────────────────────────────┘
```

Three responsibilities, three layers — identical in spirit to mscpp:

- **Store** = what the service *knows and computes* (pure).
- **statig FSM** = what is *allowed to happen when* (coordination).
- **FsmTask + copper-rs** = *enforcement and execution* (runtime).

---

## 5. Components

### Crate layout

```
msrs/
  msrs-core/                FsmTask, Trigger, Effects/Outbox, Store conventions, RtConfig
  msrs-transport/           Transport trait, TransportDriver, IngressTask, EgressTask
  msrs-transport-r2r/       ROS 2 Transport impl (driver owns the r2r executor)
  msrs-transport-iceoryx2/  Zero-copy IPC Transport impl (poll-based)
  msrs-transport-tonic/     gRPC Transport impl (request/response via correlation IDs)
  examples/echo/            RON DAG + Store + statig FSM + transport wiring
```

Splitting each middleware into its own crate keeps heavy/optional dependencies (ROS 2, iceoryx2,
tonic) out of the core and lets a service depend on only the transports it uses.

### 5.1 `msrs-core`

**`Store` convention.** Not a heavyweight trait — a Store is any struct whose fields are
module-private and whose methods are pure (query: `&self`; mutation: `&mut self`, minimal). This
mirrors mscpp's "all business logic in Store" rule. The discipline is enforced by visibility, not by
inheritance.

**`Trigger<In>`.** The Rust analog of mscpp's `StepTrigger`:

```rust
pub enum Trigger<'a, In> {
    /// Periodic execution — copper-rs cycle tick. (mscpp HEARTBEAT)
    Tick(CuTime),
    /// A message arrived this cycle. (mscpp LOGICAL_ACTION + action_name)
    Message(&'a In),
}
```

**`Effects` / `Outbox<T>`.** The write-side capability handed to FSM handlers. It can enqueue typed
output messages but exposes nothing else — no clock, no transports, no runtime. This is what makes
"business logic cannot do I/O" a compile-time fact rather than a convention.

**`FsmTask<S, M>`.** The enforcement shell and the bridge to copper-rs. It implements copper-rs's
`CuTask`, owns an `S: Store` and an `M` statig `StateMachine`, and on each `process()`:

1. Builds a `Trigger` (`Message` if an input is present this cycle, else `Tick`).
2. Calls the statig machine's handler, passing `&mut Store`, the `Trigger`, and a fresh `Effects`.
3. Flushes `Effects` outputs onto the task's copper-rs `Outbox`.

Users never implement `CuTask` directly; they implement a Store and a statig machine and parameterize
`FsmTask`. The trait is sealed so the plumbing cannot be bypassed (the Rust equivalent of mscpp's
`final` `doHeartbeat`/`executeLogicalAction`).

**`RtConfig`.** Declarative real-time knobs applied at startup:

```rust
pub struct RtConfig {
    pub scheduler: SchedPolicy,   // e.g. Fifo(priority) | Other  (via thread-priority)
    pub affinity:  Vec<CoreId>,   // pin runtime thread(s) (via core_affinity)
}
```

Applied to the copper-rs runtime thread before the loop starts. Driver threads receive a separate,
typically lower-priority `RtConfig` so middleware cannot starve the deterministic loop.

### 5.2 `msrs-transport`

See §6.

---

## 6. The IPC plug-in pattern

This is the central extension point and the area most specific to robotics deployment. The wrinkle:
ROS 2 (via r2r) insists on owning an executor thread, iceoryx2 is poll-friendly, gRPC needs a
request/response correlation — yet copper-rs wants determinism at cycle boundaries. msrs reconciles
these with **a driver thread per transport, bridged to channel-backed source/sink tasks.**

### 6.1 The `Transport` trait

```rust
pub trait Transport: Send + 'static {
    /// Messages flowing middleware → DAG.
    type Inbound:  CuMsgPayload;
    /// Messages flowing DAG → middleware.
    type Outbound: CuMsgPayload;

    /// Runs on the dedicated driver thread; owns the middleware executor/loop.
    /// Reads outbound messages from the DAG and publishes them; receives external
    /// messages and forwards them to the DAG. Returns when shut down.
    fn run(
        self,
        rx_out: Receiver<Self::Outbound>,
        tx_in:  Sender<Self::Inbound>,
    ) -> CuResult<()>;
}
```

A transport is fully described by: its two payload types and a `run` loop that owns the middleware.
It never touches copper-rs internals.

### 6.2 The bridge

`TransportDriver::spawn(transport, rt_config)`:

1. Creates a paired set of lock-free channels (inbound, outbound).
2. Spawns the driver thread running `transport.run(rx_out, tx_in)`, with `rt_config` applied.
3. Returns an **`IngressTask`** (copper-rs *source*; drains the inbound channel each cycle and emits
   onto the CopperList) and an **`EgressTask`** (copper-rs *sink*; takes DAG outputs and pushes them
   to the outbound channel).

The IngressTask/EgressTask are ordinary copper-rs tasks wired into the RON DAG like any other.

### 6.3 Determinism boundary

All non-determinism — *when* a network/topic message arrives — lives in the driver thread. The DAG
samples the inbound channel **only at the start of a cycle**, and copper-rs's unified log records
exactly what was sampled. Therefore replay is bit-for-bit: re-feeding the logged inbound messages
reproduces identical DAG behavior, regardless of original arrival timing.

### 6.4 Request/response ("logical actions")

- **Intra-process, same cycle:** order the request-handling `FsmTask` before the response-consuming
  task in the DAG. The response is produced and consumed within one cycle — the zero-latency
  request/response that mscpp implemented with logical-action microsteps.
- **Cross-process RPC (e.g. gRPC):** the driver thread holds a correlation-ID → pending-call map. The
  outbound message carries the ID; when the matching response returns through the DAG, the driver
  completes the waiting call. This lives off the deterministic path, in the transport.

### 6.5 Reference implementations

| Crate | Middleware | Driver `run` strategy |
|---|---|---|
| `msrs-transport-r2r` | ROS 2 | Owns the r2r node + executor; subscriptions push to `tx_in`, `rx_out` drives publishers. |
| `msrs-transport-iceoryx2` | iceoryx2 | Poll loop: sample subscribers → `tx_in`; drain `rx_out` → publishers (zero-copy). |
| `msrs-transport-tonic` | gRPC | tonic server; RPC handler enqueues to `tx_in` with a correlation ID, awaits matching `rx_out`. |

---

## 7. Error handling

- **Store** pure functions return `Result<_, _>` or typed result structs (the Rust form of mscpp's
  result objects), e.g. `EnqueueResult { success, job_id, error }`.
- **FSM handlers** translate failures into coordination: emit an error message to an error `Outbox`
  and/or transition the statig machine to an error state.
- **Transport failures** are surfaced as an inbound message variant
  (`TransportEvent::Error { .. }`) so they enter the replay graph and the unified log — not swallowed
  on the driver thread.
- **Task-level faults** use copper-rs's `CuError`/`CuResult`.

No silent failure: every error either becomes a logged message in the graph or propagates as a
`CuError`.

---

## 8. Testing strategy

| Layer | How it is tested | Parallel to mscpp |
|---|---|---|
| Store | Plain unit tests on pure functions (no runtime, no I/O). | Identical — "100% testable Store." |
| FSM | Drive the statig machine with synthetic `Trigger`s; assert next-state and the messages captured by a fake `Effects`/`Outbox`. | mscpp FSM-state unit tests. |
| Transport | Mock the trait; exercise IngressTask/EgressTask over an in-memory channel pair. | mscpp IOAdapter lifecycle tests. |
| Integration | Run the DAG; record a unified log; replay it and assert bit-for-bit equality. | Stronger than mscpp — replay is built in. |

---

## 9. End-to-end example (echo service)

The `examples/echo/` crate demonstrates the full stack:

1. **Store** — `EchoStore { count }` with a pure `fn echo(&self, msg: &str) -> String`.
2. **FSM** — a one-state statig machine whose handler, on `Trigger::Message(req)`, calls
   `store.echo(...)` and writes the reply to its `Outbox`.
3. **Task** — `type EchoTask = FsmTask<EchoStore, EchoMachine>;`.
4. **Transport** — `msrs-transport-tonic` exposes the service over gRPC (or `-r2r` over a ROS 2 topic).
5. **Wiring** — a RON file declares the DAG: `IngressTask → EchoTask → EgressTask`. `main` builds the
   copper-rs runtime, applies `RtConfig`, spawns the transport driver, and runs the loop.

This is the msrs analog of mscpp's `grpc_echo_example.cpp` / `ros2_echo_example.cpp`.

---

## 10. Dependency summary

| Crate | Role |
|---|---|
| `cu29` (copper-rs) | Runtime: DAG scheduler, CopperList messaging, unified log + replay. |
| `statig` | Hierarchical state machines — the FSM coordination layer. |
| `thread-priority` | SCHED_FIFO/RR priority for the runtime and driver threads. |
| `core_affinity` | CPU pinning to reduce cache jitter on the deterministic loop. |
| `r2r` / `iceoryx2` / `tonic` | Optional, per-transport middleware (each in its own crate). |

---

## 11. Open questions for implementation

1. **Channel choice** for the transport bridge — copper-rs's own primitives vs. `crossbeam`/`thingbuf`.
   Prefer a bounded, lock-free, allocation-free queue to preserve the no-alloc hot path.
2. **statig ergonomics** — confirm statig's handler signature can carry `&mut Store` + `&mut Effects`
   cleanly, or whether a thin context struct is needed.
3. **Multi-task services** — whether one logical "microservice" maps to one `FsmTask` or a small
   sub-DAG of cooperating `FsmTask`s sharing a Store (mscpp was one Store per reactor).
4. **`parallel-rt`** — keep single-threaded determinism as the default; revisit copper-rs's
   experimental parallel runtime only if a service is provably CPU-bound.
