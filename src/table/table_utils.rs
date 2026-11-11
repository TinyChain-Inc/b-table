use std::{
    collections::{HashMap, HashSet},
    fmt,
    hash::Hash,
    io, mem,
};

use b_tree::Key;

use super::PRIMARY;
use crate::IndexStack;
use crate::schema::{ColumnRange, IndexSchema, Schema};

#[inline]
fn selected_columns<'a, 'b, K, V>(
    row: &'a [V],
    columns_in: &'b [K],
    columns_out: &'b [K],
) -> impl Iterator<Item = &'a V> + 'b
where
    'a: 'b,
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
        .filter_map(move |col_name| columns_in.iter().position(|c| c == col_name))
        .map(move |i| &row[i])
}

#[inline]
pub(super) fn borrow_columns<'a, K, V>(
    row: &'a [V],
    columns_in: &[K],
    columns_out: &[K],
) -> Key<&'a V>
where
    K: Eq,
{
    selected_columns(row, columns_in, columns_out).collect()
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

    let mut names = HashSet::with_capacity(schema.auxiliary().len());
    for (index_name, index) in schema.auxiliary() {
        if index_name == PRIMARY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("reserved Table index name: {index_name}"),
            ));
        }
        if !names.insert(index_name.as_str()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("duplicate Table index name: {index_name}"),
            ));
        }

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

        // Noncovering queries need every primary-key column for exact lookup.
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
pub(super) fn index_range_for<K: Eq + Hash, V>(
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

pub(super) fn prefix_extractor<K, V>(
    columns_in: &[K],
    columns_out: &[K],
) -> impl Fn(Key<V>) -> Key<V> + Send + 'static
where
    K: PartialEq + fmt::Debug,
    V: Default + Clone,
{
    debug_assert!(!columns_out.is_empty());
    debug_assert!(
        columns_out.iter().all(|id| columns_in.contains(id)),
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

    let mut remaining = vec![0_usize; columns_in.len()];
    for &index in &indices {
        remaining[index] += 1;
    }

    let indices: IndexStack<_> = indices
        .into_iter()
        .map(|index| {
            remaining[index] -= 1;
            (index, remaining[index] == 0)
        })
        .collect();

    move |mut key| {
        indices
            .iter()
            .map(|&(index, last)| {
                if last {
                    mem::take(&mut key[index])
                } else {
                    key[index].clone()
                }
            })
            .collect()
    }
}
