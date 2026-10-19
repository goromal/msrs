//! Process-wide channel-injection registry for copper tasks.
//!
//! copper constructs tasks through `new()` with only the RON config, so channel
//! ends (or any other runtime-created value) must be parked here before the
//! runtime is built and taken back out inside `new()`. The built-in ingress and
//! egress tasks use it; service-owned tasks (for example a custom `CuSrcTask`)
//! can too:
//!
//! ```ignore
//! // before building the runtime
//! msrs_transport::registry::install::<MySource, _>(rx);
//! // inside MySource::new()
//! let rx: Receiver<MyMsg> = msrs_transport::registry::take::<MySource, _>()?;
//! ```
//!
//! Slots are keyed by the consuming task type `K`, so two task types sharing a
//! payload type never collide. Each slot is consumed by `take`, so a second
//! runtime build (for example for replay) needs a fresh `install`.

use cu29::prelude::CuError;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

type Slots = Mutex<HashMap<TypeId, Box<dyn Any + Send>>>;

static SLOTS: OnceLock<Slots> = OnceLock::new();

/// Lock the registry. The map is never left half-updated, so a poisoned lock
/// (a panic elsewhere while it was held) is safe to recover.
fn slots() -> MutexGuard<'static, HashMap<TypeId, Box<dyn Any + Send>>> {
    SLOTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Park `value` for the task type `K`.
///
/// # Panics
///
/// Panics if a value for `K` is already installed and not yet taken.
pub fn install<K: 'static, V: Send + 'static>(value: V) {
    let mut guard = slots();
    let key = TypeId::of::<K>();
    if guard.contains_key(&key) {
        // Release the lock first so a double install never poisons the
        // registry for other tasks; the first install stays in place.
        drop(guard);
        panic!("{} channel already installed", std::any::type_name::<K>());
    }
    guard.insert(key, Box::new(value));
}

/// Take the value installed for the task type `K`.
///
/// Returns a `CuError` if nothing was installed for `K` or the installed value
/// is not a `V`.
pub fn take<K: 'static, V: 'static>() -> Result<V, CuError> {
    let boxed = slots().remove(&TypeId::of::<K>()).ok_or_else(|| {
        CuError::from(format!(
            "{} channel not installed; call install() before building the copper runtime",
            std::any::type_name::<K>()
        ))
    })?;
    boxed.downcast::<V>().map(|v| *v).map_err(|_| {
        CuError::from(format!(
            "{} registry slot holds a different type than {}",
            std::any::type_name::<K>(),
            std::any::type_name::<V>()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct KeyA;
    struct KeyB;
    struct KeyC;

    #[test]
    fn install_then_take_round_trips_once() {
        install::<KeyA, _>(5u8);
        assert_eq!(take::<KeyA, u8>().unwrap(), 5);
        assert!(take::<KeyA, u8>().is_err());
    }

    #[test]
    fn take_with_wrong_type_errors() {
        install::<KeyB, _>(5u8);
        assert!(take::<KeyB, u16>().is_err());
    }

    #[test]
    #[should_panic(expected = "already installed")]
    fn double_install_panics() {
        install::<KeyC, _>(1u8);
        install::<KeyC, _>(2u8);
    }
}
