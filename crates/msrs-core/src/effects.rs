//! The write capability handed to FSM handlers.

/// Buffers output messages produced during a single FSM step.
///
/// Deliberately minimal: a handler can only `emit`. It cannot read the clock,
/// reach a transport, or touch the runtime — so business logic cannot perform
/// I/O. The owning [`crate::fsm_task::FsmTask`] drains the buffer after the step
/// and forwards it to the copper-rs output message.
#[derive(Debug)]
pub struct Effects<Out> {
    outbox: Vec<Out>,
}

impl<Out> Default for Effects<Out> {
    fn default() -> Self {
        Self { outbox: Vec::new() }
    }
}

impl<Out> Effects<Out> {
    /// Queue an output message to be emitted onto the task's output port.
    pub fn emit(&mut self, value: Out) {
        self.outbox.push(value);
    }

    /// True if nothing was emitted this step.
    pub fn is_empty(&self) -> bool {
        self.outbox.is_empty()
    }

    /// Drain all buffered outputs in emission order (fan-out).
    pub fn take(&mut self) -> Vec<Out> {
        std::mem::take(&mut self.outbox)
    }

    /// Drain a single output (the common 0-or-1 case).
    ///
    /// Returns the emitted value (or `None`) and clears the buffer. If the step
    /// emitted more than once, nothing is silently dropped: the buffer is
    /// cleared and [`ExtraEmits`] reports how many values were emitted, so the
    /// caller can fail loudly.
    pub fn take_one(&mut self) -> Result<Option<Out>, ExtraEmits> {
        let mut v = std::mem::take(&mut self.outbox);
        match v.len() {
            0 => Ok(None),
            1 => Ok(v.pop()),
            emitted => Err(ExtraEmits { emitted }),
        }
    }
}

/// A single-output consumer found more than one emission in one step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtraEmits {
    /// Number of values emitted during the step (always > 1).
    pub emitted: usize,
}

impl std::fmt::Display for ExtraEmits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FSM step emitted {} outputs but this adapter forwards at most one; \
             emit once per step or use a batch output (FsmBatchTask / run_step_all)",
            self.emitted
        )
    }
}

impl std::error::Error for ExtraEmits {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emit_then_take_preserves_order() {
        let mut e: Effects<i32> = Effects::default();
        assert!(e.is_empty());
        e.emit(1);
        e.emit(2);
        assert!(!e.is_empty());
        assert_eq!(e.take(), vec![1, 2]);
        assert!(e.is_empty());
    }

    #[test]
    fn take_one_returns_first_and_clears() {
        let mut e: Effects<&str> = Effects::default();
        assert_eq!(e.take_one(), Ok(None));
        e.emit("a");
        assert_eq!(e.take_one(), Ok(Some("a")));
        assert!(e.is_empty());
    }

    #[test]
    fn take_one_reports_extra_emits_instead_of_dropping() {
        let mut e: Effects<&str> = Effects::default();
        e.emit("a");
        e.emit("b");
        assert_eq!(e.take_one(), Err(ExtraEmits { emitted: 2 }));
        assert!(e.is_empty());
    }
}
