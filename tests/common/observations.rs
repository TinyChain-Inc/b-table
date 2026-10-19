// Test-only observation counters, excluded from non-test builds. Each test
// scopes its own task-local values so concurrent tests do not share state.
tokio::task_local! {
    pub(crate) static INDEXED_CLONES: std::cell::Cell<usize>;
}

use super::adapters::Value;

impl Clone for Value {
    fn clone(&self) -> Self {
        match self {
            Self::Number(value) => Self::Number(*value),
            Self::String(value) => {
                if value == "indexed" {
                    let _ = INDEXED_CLONES.try_with(|count| count.set(count.get() + 1));
                }

                Self::String(value.clone())
            }
        }
    }
}
