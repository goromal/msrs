//! Execution-context discriminator for FSM handlers.

/// What caused this FSM step: a periodic tick or an inbound message.
#[derive(Debug)]
pub enum Trigger<'a, In> {
    /// Periodic execution at the given monotonic nanosecond timestamp.
    Tick(u64),
    /// A message arrived this cycle.
    Message(&'a In),
}

impl<'a, In> Trigger<'a, In> {
    /// The inbound payload, if this is a `Message` trigger.
    pub fn message(&self) -> Option<&In> {
        match self {
            Trigger::Message(m) => Some(m),
            Trigger::Tick(_) => None,
        }
    }

    /// True if this is a periodic tick.
    pub fn is_tick(&self) -> bool {
        matches!(self, Trigger::Tick(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_has_no_message() {
        let t: Trigger<'_, u32> = Trigger::Tick(42);
        assert!(t.is_tick());
        assert_eq!(t.message(), None);
    }

    #[test]
    fn message_exposes_payload() {
        let payload = 7u32;
        let t = Trigger::Message(&payload);
        assert!(!t.is_tick());
        assert_eq!(t.message(), Some(&7));
    }
}
