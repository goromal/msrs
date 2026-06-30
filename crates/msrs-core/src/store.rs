//! The pure business-logic layer.

/// Marker for a service's pure business-logic store.
///
/// A `Store` holds service state and exposes **pure** methods:
/// queries take `&self`, mutations take `&mut self` and stay minimal. A `Store`
/// must not depend on ports, transports, the clock, or the runtime — which makes
/// it fully unit-testable in isolation. FSM handlers call into the store; the
/// store never calls out.
pub trait Store {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Counter {
        count: i64,
    }

    impl Counter {
        fn next(&self, amount: i64) -> i64 {
            self.count + amount
        }
        fn apply(&mut self, new_value: i64) {
            self.count = new_value;
        }
    }

    impl Store for Counter {}

    #[test]
    fn store_logic_is_pure_and_testable() {
        let mut c = Counter::default();
        let n = c.next(5); // pure query
        assert_eq!(n, 5);
        c.apply(n); // minimal mutation
        assert_eq!(c.next(0), 5);
    }
}
