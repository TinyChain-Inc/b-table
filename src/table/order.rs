use std::cmp::Ordering;

use b_tree::collate::Collate;

use crate::Row;

/// Comparison of full primary rows in a selected native index's storage order.
#[derive(Clone, Eq, PartialEq)]
pub struct RowCollator<C, O = C> {
    indices: Vec<usize>,
    ordering: b_tree::IndexOrder<C, O>,
    reverse: bool,
}

impl<C, O> RowCollator<C, O> {
    pub(super) fn new(
        indices: Vec<usize>,
        ordering: b_tree::IndexOrder<C, O>,
        reverse: bool,
    ) -> Self {
        Self {
            indices,
            ordering,
            reverse,
        }
    }
}

impl<C, O> Collate for RowCollator<C, O>
where
    C: Collate,
    O: Collate<Value = C::Value>,
{
    type Value = Row<C::Value>;

    fn cmp(&self, left: &Self::Value, right: &Self::Value) -> Ordering {
        let order = self.ordering.key_collator().cmp_iter(
            self.indices.iter().map(|&i| &left[i]),
            self.indices.iter().map(|&i| &right[i]),
        );

        if self.reverse { order.reverse() } else { order }
    }
}
