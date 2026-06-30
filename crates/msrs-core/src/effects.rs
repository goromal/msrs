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

    /// Drain a single output (the common 0-or-1 case). Returns the first
    /// emitted value and clears the buffer; extra emits are dropped with the
    /// expectation that single-output tasks emit at most once.
    pub fn take_one(&mut self) -> Option<Out> {
        let mut v = std::mem::take(&mut self.outbox);
        if v.is_empty() {
            None
        } else {
            Some(v.swap_remove(0))
        }
    }
}

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
        assert_eq!(e.take_one(), None);
        e.emit("a");
        assert_eq!(e.take_one(), Some("a"));
        assert!(e.is_empty());
    }
}
