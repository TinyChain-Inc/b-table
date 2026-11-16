use std::{fmt, io, pin::Pin, sync::Arc};

use b_tree::collate::Collate;
use freqfs::{DirDeref, DirReadGuardOwned, DirWriteGuardOwned, FileLoad};
use futures::future::TryFutureExt;
use futures::stream::Stream;
use safecast::AsType;
use smallvec::SmallVec;

use self::table_state::TableState;
use self::table_utils::bad_key;
use super::Node;
use super::schema::{IndexSchema, Range, Schema, TableSchema};

mod order;
mod table_lock;
mod table_state;
pub(crate) mod table_utils;

pub use order::RowCollator;
pub use table_lock::TableLock;

const PRIMARY: &str = "primary";

/// The maximum number of values in a stack-allocated [`Row`]
pub const ROW_STACK_SIZE: usize = 32;

/// A read guard acquired on a [`TableLock`]
pub type TableReadGuard<S, IS, C, FE, O = C> = Table<S, IS, C, Arc<DirReadGuardOwned<FE>>, O>;

/// A write guard acquired on a [`TableLock`]
pub type TableWriteGuard<S, IS, C, FE, O = C> = Table<S, IS, C, DirWriteGuardOwned<FE>, O>;

/// The type of row returned in a [`Stream`] of [`Rows`]
pub type Row<V> = SmallVec<[V; ROW_STACK_SIZE]>;

/// A stream of table rows
pub type Rows<V> = Pin<Box<dyn Stream<Item = Result<Row<V>, io::Error>> + Send>>;

/// A database table with support for multiple indices
pub struct Table<S, IS, C, G, O = C> {
    schema: Arc<TableSchema<S>>,
    state: TableState<IS, C, G, O>,
}

impl<S, IS, C, G, O> Clone for Table<S, IS, C, G, O>
where
    C: Clone,
    O: Clone,
    G: Clone,
{
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            state: self.state.clone(),
        }
    }
}

impl<S, C, FE, G, O> Table<S, S::Index, C, G, O>
where
    S: Schema,
    C: Collate<Value = S::Value> + 'static,
    O: Collate<Value = S::Value> + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static + FileLoad,
    G: DirDeref<Entry = FE> + 'static,
    Range<S::Id, S::Value>: fmt::Debug,
{
    /// Return `true` if the given `key` is present in this [`Table`].
    pub async fn contains(&self, key: &[S::Value]) -> Result<bool, io::Error> {
        let key_len = self.schema.key().len();

        if key.len() == key_len {
            self.state.contains(key).await
        } else {
            Err(bad_key(key, key_len))
        }
    }

    /// Return the first row in the given `range` using the given `order`.
    pub async fn first(
        &self,
        range: Range<S::Id, S::Value>,
        order: &[S::Id],
        select: Option<&[S::Id]>,
    ) -> Result<Option<Row<S::Value>>, io::Error>
    where
        C: Clone + Send + Sync,
        O: Clone + Send + Sync,
        G: Sync,
    {
        let range = range.into_inner();
        let select = select.unwrap_or(self.schema.key());
        let index = self.schema.query_index(&range, order)?;
        if select.is_empty()
            || select
                .iter()
                .any(|column| !self.schema.primary().columns().contains(column))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid column selection: {select:?}"),
            ));
        }

        self.state
            .first(index, &range, select, self.schema.key())
            .await
    }

    /// Look up a row by its `key`.
    pub async fn get_row(&self, key: &[S::Value]) -> Result<Option<Row<S::Value>>, io::Error> {
        let key_len = self.schema.key().len();

        if key.len() == key_len {
            self.state.get_row(key).await
        } else {
            Err(bad_key(key, key_len))
        }
    }

    /// Look up a value by its `key`.
    pub async fn get_value(&self, key: &[S::Value]) -> Result<Option<Row<S::Value>>, io::Error> {
        let key_len = self.schema.key().len();

        self.get_row(key)
            .map_ok(move |maybe_row| maybe_row.map(move |mut row| row.drain(key_len..).collect()))
            .await
    }
}

impl<S, C, FE, G, O> Table<S, S::Index, C, G, O>
where
    S: Schema,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    O: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static + FileLoad,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    Range<S::Id, S::Value>: fmt::Debug,
{
    /// Count how many rows in this [`Table`] lie within the given `range`.
    pub async fn count(&self, range: Range<S::Id, S::Value>) -> Result<u64, io::Error> {
        let range = range.into_inner();
        let index = self.schema.query_index(&range, &[])?;
        self.state.count(index, range, self.schema.key()).await
    }

    /// Return `true` if the given [`Range`] of this [`Table`] does not contain any rows.
    pub async fn is_empty(&self, range: Range<S::Id, S::Value>) -> Result<bool, io::Error> {
        let range = range.into_inner();
        let index = self.schema.query_index(&range, &[])?;
        self.state.is_empty(index, range, self.schema.key()).await
    }

    /// Return full primary rows together with their storage-order comparator.
    /// One index selection determines both the stream and its comparator,
    /// including reverse traversal. The comparator accepts complete schema-width
    /// rows and owns its metadata independently of this Table guard.
    pub async fn rows(
        &self,
        range: Range<S::Id, S::Value>,
        order: &[S::Id],
        reverse: bool,
    ) -> Result<(Rows<S::Value>, RowCollator<C, O>), io::Error> {
        let range = range.into_inner();
        let index = self.schema.query_index(&range, order)?;
        self.state
            .rows(index, range, reverse, self.schema.key())
            .await
    }

    /// Construct a [`Stream`] of the requested columns of the [`Rows`] within the given `range`.
    ///
    /// One index must support every constraint and the literal ordering prefix.
    /// Constraints are leading equalities followed by at most one range.
    /// Unsupported combinations return [`io::ErrorKind::Unsupported`] before traversal.
    pub async fn select(
        &self,
        range: Range<S::Id, S::Value>,
        order: &[S::Id],
        reverse: bool,
        columns: &[S::Id],
    ) -> Result<Rows<S::Value>, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("Table::select with order {order:?}");

        let range = range.into_inner();
        let index = self.schema.query_index(&range, order)?;
        if columns.is_empty()
            || columns
                .iter()
                .any(|column| !self.schema.primary().columns().contains(column))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid column selection: {columns:?}"),
            ));
        }

        self.state
            .select(index, range, reverse, columns, self.schema.key())
            .await
    }
}

impl<S: fmt::Debug, IS, C, G, O> fmt::Debug for Table<S, IS, C, G, O> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "table with schema {:?}", self.schema.inner())
    }
}

impl<S, IS, C, FE, O> Table<S, IS, C, DirWriteGuardOwned<FE>, O> {
    /// Downgrade this write lock to a read lock.
    pub fn downgrade(self) -> Table<S, IS, C, Arc<DirReadGuardOwned<FE>>, O> {
        Table {
            schema: self.schema,
            state: self.state.downgrade(),
        }
    }
}

impl<S, C, FE, O> Table<S, S::Index, C, DirWriteGuardOwned<FE>, O>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    O: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static + FileLoad,
    <S as Schema>::Index: Send + Sync,
{
    /// Delete a row from this [`Table`] by its `key`.
    /// Returns `true` if the given `key` was present.
    pub async fn delete_row(&mut self, key: &[S::Value]) -> Result<bool, io::Error> {
        let key_len = self.schema.key().len();

        if key.len() == key_len {
            self.state.delete_row(key).await
        } else {
            Err(bad_key(key, key_len))
        }
    }

    /// Delete all rows in the given `range` from this [`Table`].
    pub async fn delete_range(
        &mut self,
        range: Range<S::Id, S::Value>,
    ) -> Result<usize, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("Table::delete_range {range:?}");

        let range = range.into_inner();
        let index = self.schema.query_index(&range, &[])?;

        self.state
            .delete_range(index, range, self.schema.key())
            .await
    }

    /// Insert or update a row in this [`Table`].
    ///
    /// Returns `true` for a new primary key, `false` for replacement.
    /// Semantic equality never suppresses publication of the supplied values.
    ///
    /// Multiple semantic matches for a primary key are rejected before mutation.
    /// Replacement preserves primary-index order. Auxiliary changes precede primary
    /// replacement, so ordering errors may retain partial native mutations;
    /// callers own recovery.
    pub async fn upsert(
        &mut self,
        key: Vec<S::Value>,
        values: Vec<S::Value>,
    ) -> Result<bool, S::Error> {
        let mut row = self.schema.validate_key(key)?;
        let values = self.schema.validate_values(values)?;

        row.reserve_exact(values.len());
        row.extend(values);

        let previous = self.state.get_row(&row[..self.schema.key().len()]).await?;
        self.state
            .upsert(row, previous.as_deref())
            .map_err(S::Error::from)
            .await
    }

    /// Delete all rows from this [`Table`].
    pub async fn truncate(&mut self) -> Result<(), io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("Table::truncate");

        self.state.truncate().await
    }
}
