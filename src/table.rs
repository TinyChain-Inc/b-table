mod table_lock;
mod table_state;
mod table_utils;

use std::pin::Pin;
use std::sync::Arc;
use std::{fmt, io};

use b_tree::collate::Collate;
use freqfs::{DirDeref, DirReadGuardOwned, DirWriteGuardOwned, FileLoad};
use futures::future::TryFutureExt;
use futures::stream::Stream;
use safecast::AsType;
use smallvec::SmallVec;

use super::schema::{IndexSchema, Range, Schema, TableSchema};
use super::Node;

use table_state::TableState;
use table_utils::bad_key;

pub use table_lock::TableLock;

const PRIMARY: &str = "primary";

/// The maximum number of values in a stack-allocated [`Row`]
pub const ROW_STACK_SIZE: usize = 32;

/// A read guard acquired on a [`TableLock`]
pub type TableReadGuard<S, IS, C, FE> = Table<S, IS, C, Arc<DirReadGuardOwned<FE>>>;

/// A write guard acquired on a [`TableLock`]
pub type TableWriteGuard<S, IS, C, FE> = Table<S, IS, C, DirWriteGuardOwned<FE>>;

/// The type of row returned in a [`Stream`] of [`Rows`]
pub type Row<V> = SmallVec<[V; ROW_STACK_SIZE]>;

/// A stream of table rows
pub type Rows<V> = Pin<Box<dyn Stream<Item = Result<Row<V>, io::Error>> + Send>>;

/// A database table with support for multiple indices
pub struct Table<S, IS, C, G> {
    schema: Arc<TableSchema<S>>,
    state: TableState<IS, C, G>,
}

impl<S, IS, C, G> Clone for Table<S, IS, C, G>
where
    C: Clone,
    G: Clone,
{
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            state: self.state.clone(),
        }
    }
}

impl<S, C, FE, G> Table<S, S::Index, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value> + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    G: DirDeref<Entry = FE> + 'static,
    Node<S::Value>: FileLoad,
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
    ) -> Result<Option<Row<S::Value>>, io::Error> {
        let range = range.into_inner();
        let select = select.unwrap_or(self.schema.key());
        let plan = self.schema.plan_query(&range, order, self.schema.key())?;

        self.state
            .first(&plan, &range, select, self.schema.key())
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

impl<S, C, FE, G> Table<S, S::Index, C, G>
where
    S: Schema,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    G: DirDeref<Entry = FE> + Clone + Send + Sync + 'static,
    Node<S::Value>: FileLoad,
    Range<S::Id, S::Value>: fmt::Debug,
{
    /// Count how many rows in this [`Table`] lie within the given `range`.
    pub async fn count(&self, range: Range<S::Id, S::Value>) -> Result<u64, io::Error> {
        let range = range.into_inner();
        let plan = self.schema.plan_query(&range, &[], self.schema.key())?;
        self.state.count(plan, range, self.schema.key()).await
    }

    /// Return `true` if the given [`Range`] of this [`Table`] does not contain any rows.
    pub async fn is_empty(&self, range: Range<S::Id, S::Value>) -> Result<bool, io::Error> {
        let range = range.into_inner();
        let plan = self.schema.plan_query(&range, &[], Default::default())?;
        self.state.is_empty(plan, range, self.schema.key()).await
    }

    /// Construct a [`Stream`] of the `select`ed columns of the [`Rows`] within the given `range`.
    pub async fn rows<'a>(
        &'a self,
        range: Range<S::Id, S::Value>,
        order: &'a [S::Id],
        reverse: bool,
        select: Option<&'a [S::Id]>,
    ) -> Result<Rows<S::Value>, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("Table::rows with order {order:?}");

        let range = range.into_inner();
        let select = select.unwrap_or(self.schema.primary().columns());
        let plan = self.schema.plan_query(&range, order, self.schema.key())?;

        self.state
            .rows(plan, range, reverse, select, self.schema.key())
            .await
    }

    /// Consume this [`TableReadGuard`] to construct a [`Stream`] of all the rows in the [`Table`].
    pub async fn into_rows(self) -> Result<Rows<S::Value>, io::Error> {
        let rows = self.rows(Range::default(), &[], false, None).await?;
        Ok(Box::pin(rows))
    }
}

impl<S: fmt::Debug, IS, C, G> fmt::Debug for Table<S, IS, C, G> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "table with schema {:?}", self.schema.inner())
    }
}

impl<S, IS, C, FE> Table<S, IS, C, DirWriteGuardOwned<FE>> {
    /// Downgrade this write lock to a read lock.
    pub fn downgrade(self) -> Table<S, IS, C, Arc<DirReadGuardOwned<FE>>> {
        Table {
            schema: self.schema,
            state: self.state.downgrade(),
        }
    }
}

impl<S, C, FE> Table<S, S::Index, C, DirWriteGuardOwned<FE>>
where
    S: Schema + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static,
    <S as Schema>::Index: Send + Sync,
    Node<S::Value>: FileLoad,
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
        let plan = self.schema.plan_query(&range, &[], self.schema.key())?;

        self.state
            .delete_range(plan, range, self.schema.key())
            .await
    }

    /// Delete all rows from the `other` table from this one.
    /// The `other` table **must** have an identical schema and collation.
    pub async fn delete_all(
        &mut self,
        other: TableReadGuard<S, S::Index, C, FE>,
    ) -> Result<(), io::Error> {
        // no need to check the collator for equality, that will be done in the index operations

        // but do check that the indices to merge are the same
        if self.schema != other.schema {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot delete the contents of a table with schema {:?} from one with schema {:?}",
                    other.schema.inner(), self.schema.inner()
                ),
            ));
        }

        self.state.delete_all(other.state).await
    }

    /// Insert all rows from the `other` table into this one.
    /// The `other` table **must** have an identical schema and collation.
    pub async fn merge(
        &mut self,
        other: TableReadGuard<S, S::Index, C, FE>,
    ) -> Result<(), io::Error> {
        // no need to check the collator for equality, that will be done in the merge operations

        // but do check that the indices to merge are the same
        if self.schema != other.schema {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot merge a table with schema {:?} into one with schema {:?}",
                    other.schema.inner(),
                    self.schema.inner()
                ),
            ));
        }

        self.state.merge(other.state).await
    }

    /// Insert or update a row in this [`Table`].
    /// Returns `true` if a new row was inserted.
    pub async fn upsert(
        &mut self,
        key: Vec<S::Value>,
        values: Vec<S::Value>,
    ) -> Result<bool, S::Error> {
        let key = self.schema.validate_key(key)?;
        let values = self.schema.validate_values(values)?;

        let mut row = Vec::with_capacity(key.len() + values.len());
        row.extend(key);
        row.extend(values);

        self.state.upsert(row).map_err(S::Error::from).await
    }

    /// Delete all rows from this [`Table`].
    pub async fn truncate(&mut self) -> Result<(), io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("Table::truncate");

        self.state.truncate().await
    }
}
