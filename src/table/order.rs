use std::cmp::Ordering;

use b_tree::collate::Collate;

use crate::Row;

/// Comparison of full primary rows in a selected native index's storage order.
#[derive(Clone, Eq, PartialEq)]
pub struct RowCollator<C> {
    indices: Vec<usize>,
    collator: b_tree::RangeCollator<C>,
    reverse: bool,
}

impl<C> RowCollator<C> {
    pub(super) fn new(
        indices: Vec<usize>,
        collator: b_tree::RangeCollator<C>,
        reverse: bool,
    ) -> Self {
        Self {
            indices,
            collator,
            reverse,
        }
    }
}

impl<C> Collate for RowCollator<C>
where
    C: Collate,
{
    type Value = Row<C::Value>;

    fn cmp(&self, left: &Self::Value, right: &Self::Value) -> Ordering {
        let order = self.collator.cmp_key_iter(
            self.indices.iter().map(|&i| &left[i]),
            self.indices.iter().map(|&i| &right[i]),
        );

        if self.reverse { order.reverse() } else { order }
    }
}
