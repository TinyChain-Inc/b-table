use std::collections::HashMap;
use std::hash::Hash;
use std::{fmt, io, mem};

use b_tree::Key;
use smallvec::smallvec;

use crate::IndexStack;
use crate::plan::IndexQuery;
use crate::schema::{ColumnRange, IndexSchema, Schema};

#[inline]
pub(super) fn borrow_columns<'a, K, V>(
    row: &'a [V],
    columns_in: &[K],
    columns_out: &[K],
) -> Key<&'a V>
where
    K: Eq,
{
    assert_eq!(row.len(), columns_in.len());

    debug_assert!(
        columns_out
            .iter()
            .all(|col_name| columns_in.contains(col_name))
    );

    columns_out
        .iter()
        .filter_map(|col_name| columns_in.iter().position(|c| c == col_name))
        .map(|i| &row[i])
        .collect()
}

#[inline]
pub(super) fn clone_columns<K, V>(row: &[V], columns_in: &[K], columns_out: &[K]) -> Vec<V>
where
    K: Eq,
    V: Clone,
{
    assert_eq!(row.len(), columns_in.len());

    debug_assert!(
        columns_out
            .iter()
            .all(|col_name| columns_in.contains(col_name))
    );

    columns_out
        .iter()
        .filter_map(|col_name| columns_in.iter().position(|c| c == col_name))
        .map(|i| row[i].clone())
        .collect()
}

#[inline]
pub(super) fn extract_columns<K, V>(mut row: Key<V>, columns_in: &[K], columns_out: &[K]) -> Key<V>
where
    K: Eq + fmt::Debug,
    V: Default + Clone + fmt::Debug,
{
    assert_eq!(
        row.len(),
        columns_in.len(),
        "row {row:?} does not match column schema {columns_in:?}"
    );

    debug_assert!(
        columns_out
            .iter()
            .all(|col_name| columns_in.contains(col_name)),
        "input columns {columns_in:?} are missing some output columns {columns_out:?}"
    );

    let mut selection = smallvec![V::default(); columns_out.len()];

    for (i_to, name_out) in columns_out.iter().enumerate() {
        let i_from = columns_in
            .iter()
            .position(|name_in| name_in == name_out)
            .expect("column index");

        mem::swap(&mut row[i_from], &mut selection[i_to]);
    }

    selection
}

#[inline]
pub(super) fn bad_key<V: fmt::Debug>(key: &[V], key_len: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid key: {key:?}, expected exactly {key_len} column(s)",),
    )
}

#[inline]
pub(super) fn valid_schema<S: Schema>(schema: &S) -> Result<(), io::Error> {
    if schema.primary().columns().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{schema:?} contains no columns"),
        ));
    }

    for (index_name, index) in schema.auxiliary() {
        if index.columns().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("index {index_name} is empty"),
            ));
        }

        for col_name in index.columns() {
            if !schema.primary().columns().contains(col_name) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("index {index_name} refers to unknown column {col_name}"),
                ));
            }
        }

        // note: it's inefficient to remove this requirement
        // because it would break the assumption
        // that every key constructed by merging two indices exists in the primary index
        for col_name in schema.key() {
            if !index.columns().contains(col_name) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("index {index_name} is missing primary key column {col_name}"),
                ));
            }
        }
    }

    Ok(())
}

#[inline]
pub(super) fn index_range_borrow<'a, K: Eq + Hash, V>(
    columns: &[K],
    range: &'a HashMap<K, ColumnRange<V>>,
) -> b_tree::Range<&'a V> {
    let mut prefix = Key::with_capacity(range.len());

    for col_name in columns {
        if let Some(col_range) = range.get(col_name) {
            match col_range {
                ColumnRange::Eq(value) => {
                    prefix.push(value);
                }
                ColumnRange::In((start, end)) => {
                    return b_tree::Range::with_bounds(prefix, (start.as_ref(), end.as_ref()));
                }
            }
        } else {
            break;
        }
    }

    b_tree::Range::from_prefix(prefix)
}

#[inline]
pub(super) fn index_range_for<'a, K: Eq + Hash, V>(
    columns: &[K],
    range: &mut HashMap<K, ColumnRange<V>>,
) -> b_tree::Range<V> {
    let mut prefix = Key::with_capacity(range.len());

    for col_name in columns {
        if let Some(col_range) = range.remove(col_name) {
            match col_range {
                ColumnRange::Eq(value) => {
                    prefix.push(value);
                }
                ColumnRange::In(bounds) => {
                    return b_tree::Range::with_bounds(prefix, bounds);
                }
            }
        } else {
            break;
        }
    }

    b_tree::Range::from_prefix(prefix)
}

#[inline]
pub(super) fn inner_range_for<'a, K, V>(
    query: &IndexQuery<'a, K>,
    range: &HashMap<K, ColumnRange<V>>,
) -> b_tree::Range<V>
where
    K: Eq + Hash + fmt::Debug,
    V: Clone,
{
    let mut inner_range = Key::with_capacity(query.range().len());
    let mut range_columns = query.range().into_iter();

    let inner_range = loop {
        if let Some(col_name) = range_columns.next() {
            match range.get(col_name).cloned().expect("column range") {
                ColumnRange::Eq(value) => inner_range.push(value),
                ColumnRange::In(bounds) => break b_tree::Range::with_bounds(inner_range, bounds),
            }
        } else {
            break b_tree::Range::from(inner_range);
        }
    };

    assert!(range_columns.next().is_none());

    inner_range
}

pub(super) fn prefix_extractor<K, V>(
    columns_in: &[K],
    columns_out: &[K],
) -> impl Fn(Key<V>) -> Key<V> + Send + 'static
where
    K: PartialEq + fmt::Debug,
    V: Default + Clone,
{
    debug_assert!(columns_out.len() <= columns_in.len());
    debug_assert!(!columns_out.is_empty());
    debug_assert!(
        columns_out.iter().all(|id| columns_in.contains(&id)),
        "{columns_out:?} is not a subset of {columns_in:?}"
    );

    #[cfg(feature = "logging")]
    log::trace!("extract columns {columns_out:?} from {columns_in:?}");

    let indices = columns_out
        .iter()
        .map(|name_out| {
            columns_in
                .iter()
                .position(|name_in| name_in == name_out)
                .expect("column index")
        })
        .collect::<IndexStack<_>>();

    move |mut key| {
        let mut prefix = smallvec![V::default(); indices.len()];

        for (i_to, i_from) in indices.iter().copied().enumerate() {
            mem::swap(&mut key[i_from], &mut prefix[i_to]);
        }

        prefix
    }
}
