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
//! Route (B): a process-wide, `TypeId`-keyed registry stores the channel ends
//! before the runtime is constructed.  The application (Task 9: echo example)
//! calls `IngressTask::install(rx)` and `EgressTask::install(tx)` before
//! building the copper runtime.  When the generated runtime code calls
//! `IngressTask::new(...)` / `EgressTask::new(...)`, `new()` pops the matching
//! channel end out of the registry.  Type safety is enforced at runtime via
//! `TypeId` and `Any::downcast`; a clear `CuError` is returned if the channel
//! was not installed or the type does not match.

use crossbeam_channel::{Receiver, Sender};

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

// ---------------------------------------------------------------------------
// copper-rs CuSrcTask / CuSinkTask impls
// ---------------------------------------------------------------------------

use cu29::prelude::*;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

// Per-type channel-end registries.  Each entry is consumed exactly once by
// `new()`, so a type error or double-construction is caught immediately.

static INGRESS_SLOTS: OnceLock<Mutex<HashMap<TypeId, Box<dyn Any + Send>>>> = OnceLock::new();
static EGRESS_SLOTS: OnceLock<Mutex<HashMap<TypeId, Box<dyn Any + Send>>>> = OnceLock::new();

fn ingress_slots() -> &'static Mutex<HashMap<TypeId, Box<dyn Any + Send>>> {
    INGRESS_SLOTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn egress_slots() -> &'static Mutex<HashMap<TypeId, Box<dyn Any + Send>>> {
    EGRESS_SLOTS.get_or_init(|| Mutex::new(HashMap::new()))
}

// ---------------------------------------------------------------------------
// IngressTask<T>
// ---------------------------------------------------------------------------

/// A copper source task that drains inbound messages from the transport channel.
///
/// Each `process()` cycle calls [`ingress_step`]: if a message is ready, it is
/// set as the output payload (with the current context clock); otherwise the
/// output is cleared (empty slot on the CopperList).
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
        let mut guard = ingress_slots().lock().expect("IngressTask registry poisoned");
        let key = TypeId::of::<T>();
        if guard.contains_key(&key) {
            panic!("IngressTask<{}> channel already installed", std::any::type_name::<T>());
        }
        guard.insert(key, Box::new(rx));
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
        let key = TypeId::of::<T>();
        let boxed = ingress_slots()
            .lock()
            .map_err(|_| CuError::from("IngressTask registry poisoned"))?
            .remove(&key)
            .ok_or_else(|| {
                CuError::from(format!(
                    "IngressTask<{}> channel not installed; call IngressTask::install(rx) \
                     before building the copper runtime",
                    std::any::type_name::<T>()
                ))
            })?;
        let rx = *boxed
            .downcast::<Receiver<T>>()
            .map_err(|_| CuError::from("IngressTask channel type mismatch (downcast failed)"))?;
        Ok(Self { rx })
    }

    fn process<'o>(&mut self, ctx: &CuContext, new_msg: &mut Self::Output<'o>) -> CuResult<()> {
        match ingress_step(&self.rx) {
            Some(v) => {
                new_msg.set_payload(v);
                new_msg.tov = Tov::Time(ctx.now());
            }
            None => new_msg.clear_payload(),
        }
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
        let mut guard = egress_slots().lock().expect("EgressTask registry poisoned");
        let key = TypeId::of::<T>();
        if guard.contains_key(&key) {
            panic!("EgressTask<{}> channel already installed", std::any::type_name::<T>());
        }
        guard.insert(key, Box::new(tx));
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
        let key = TypeId::of::<T>();
        let boxed = egress_slots()
            .lock()
            .map_err(|_| CuError::from("EgressTask registry poisoned"))?
            .remove(&key)
            .ok_or_else(|| {
                CuError::from(format!(
                    "EgressTask<{}> channel not installed; call EgressTask::install(tx) \
                     before building the copper runtime",
                    std::any::type_name::<T>()
                ))
            })?;
        let tx = *boxed
            .downcast::<Sender<T>>()
            .map_err(|_| CuError::from("EgressTask channel type mismatch (downcast failed)"))?;
        Ok(Self { tx })
    }

    fn process<'i>(&mut self, _ctx: &CuContext, input: &Self::Input<'i>) -> CuResult<()> {
        egress_step(&self.tx, input.payload().cloned()).map_err(CuError::from)
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
}
