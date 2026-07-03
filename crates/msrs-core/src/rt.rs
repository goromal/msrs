//! Real-time scheduling controls for the runtime and driver threads.

/// Thread scheduler policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedPolicy {
    /// Default time-sharing scheduler (`SCHED_OTHER`).
    Normal,
    /// `SCHED_FIFO` real-time policy at the given priority (1..=99 on Linux).
    /// Requires `CAP_SYS_NICE`; on failure `apply()` reports an error rather
    /// than panicking.
    Fifo(u8),
}

/// Declarative real-time configuration applied to the *current* thread.
#[derive(Debug, Clone)]
pub struct RtConfig {
    pub scheduler: SchedPolicy,
    /// Core ids to pin this thread to. Empty = no pinning.
    pub affinity: Vec<usize>,
}

impl RtConfig {
    /// Non-real-time default: normal scheduling, no pinning.
    pub fn normal() -> Self {
        Self {
            scheduler: SchedPolicy::Normal,
            affinity: Vec::new(),
        }
    }

    /// Apply affinity then scheduler policy to the current thread.
    ///
    /// Best-effort and non-panicking: returns `Err(msg)` describing the first
    /// failure (e.g. missing privileges for `Fifo`) but always attempts both.
    pub fn apply(&self) -> Result<(), String> {
        let mut first_err: Option<String> = None;

        if !self.affinity.is_empty() {
            match core_affinity::get_core_ids() {
                Some(ids) => {
                    for want in &self.affinity {
                        match ids.iter().find(|c| c.id == *want) {
                            Some(core) => {
                                if !core_affinity::set_for_current(*core) {
                                    first_err
                                        .get_or_insert(format!("failed to pin to core {want}"));
                                }
                            }
                            None => {
                                first_err.get_or_insert(format!("core {want} not available"));
                            }
                        }
                    }
                }
                None => {
                    first_err.get_or_insert("core ids unavailable".into());
                }
            }
        }

        if let SchedPolicy::Fifo(prio) = self.scheduler {
            if let Err(e) = set_fifo(prio) {
                first_err.get_or_insert(e);
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

#[cfg(target_os = "linux")]
fn set_fifo(prio: u8) -> Result<(), String> {
    use thread_priority::{
        set_thread_priority_and_policy, thread_native_id, RealtimeThreadSchedulePolicy,
        ThreadPriority, ThreadPriorityValue, ThreadSchedulePolicy,
    };
    let policy = ThreadSchedulePolicy::Realtime(RealtimeThreadSchedulePolicy::Fifo);
    let priority = ThreadPriority::Crossplatform(
        ThreadPriorityValue::try_from(prio).map_err(|e| format!("invalid priority: {e}"))?,
    );
    set_thread_priority_and_policy(thread_native_id(), priority, policy)
        .map_err(|e| format!("set SCHED_FIFO failed: {e:?}"))
}

#[cfg(not(target_os = "linux"))]
fn set_fifo(_prio: u8) -> Result<(), String> {
    Err("SCHED_FIFO only supported on Linux".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_applies_cleanly() {
        assert_eq!(RtConfig::normal().apply(), Ok(()));
    }

    #[test]
    fn pinning_to_available_core_does_not_panic() {
        if let Some(ids) = core_affinity::get_core_ids() {
            if let Some(first) = ids.first() {
                let cfg = RtConfig {
                    scheduler: SchedPolicy::Normal,
                    affinity: vec![first.id],
                };
                // Should succeed or report a benign error, never panic.
                let _ = cfg.apply();
            }
        }
    }
}
