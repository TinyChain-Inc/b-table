use super::adapters::Value;

// Test-only observation counters, excluded from non-test builds. Each test
// scopes its own task-local values so concurrent tests do not share state.
tokio::task_local! {
    pub(crate) static INDEXED_CLONES: std::cell::Cell<usize>;
    pub(crate) static VALUE_CLONES: std::cell::Cell<usize>;
}

impl Clone for Value {
    fn clone(&self) -> Self {
        let _ = VALUE_CLONES.try_with(|count| count.set(count.get() + 1));
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
