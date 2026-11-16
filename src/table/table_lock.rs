use std::{
    collections::{BTreeMap, HashMap},
    io,
    ops::ControlFlow,
    sync::Arc,
};

use b_tree::BTreeLock;
use b_tree::collate::CollateRef;
use freqfs::{DirLock, FileLoad, FileSave};
use safecast::AsType;

use super::table_state::TableState;
use super::table_utils::bad_key;
use super::{PRIMARY, Table, TableReadGuard, TableWriteGuard};
use crate::schema::{Schema, TableSchema};
use crate::{IndexStack, Node, Row};

/// A futures-aware read-write lock on a [`Table`]
pub struct TableLock<S, IS, C, FE, O = C> {
    schema: Arc<TableSchema<S>>,
    dir: DirLock<FE>,
    primary: BTreeLock<IS, C, FE, O>,
    // use a BTreeMap to make sure index locks are always acquired in-order
    auxiliary: BTreeMap<Arc<str>, BTreeLock<IS, C, FE, O>>,
}

impl<S, IS, C, FE, O> Clone for TableLock<S, IS, C, FE, O>
where
    C: Clone,
    O: Clone,
{
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            dir: self.dir.clone(),
            primary: self.primary.clone(),
            auxiliary: self.auxiliary.clone(),
        }
    }
}

impl<S, IS, C, FE, O> TableLock<S, IS, C, FE, O> {
    /// Borrow the [`Schema`] of this [`Table`].
    pub fn schema(&self) -> &S {
        self.schema.inner()
    }

    /// Borrow the ordering for this [`Table`].
    pub fn ordering(&self) -> &b_tree::IndexOrder<C, O> {
        self.primary.ordering()
    }
}

impl<S, C, FE, O> TableLock<S, S::Index, C, FE, O>
where
    S: Schema,
    C: Clone,
    O: Clone,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
{
    /// Create a new [`Table`], admitting each index root in order.
    pub async fn create(
        schema: S,
        ordering: b_tree::IndexOrder<C, O>,
        dir: DirLock<FE>,
    ) -> Result<Self, io::Error> {
        schema.validate()?;

        let mut dir_contents = dir.try_write()?;

        if !dir_contents.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "creating a Table requires empty storage",
            ));
        }

        let primary = {
            let dir = dir_contents.create_dir(PRIMARY.to_string())?;
            BTreeLock::create(schema.primary().clone(), ordering.clone(), dir).await
        }?;

        let mut auxiliary = BTreeMap::new();
        for (name, schema) in schema.auxiliary() {
            let index = {
                let dir = dir_contents.create_dir(name.to_string())?;
                BTreeLock::create(schema.clone(), ordering.clone(), dir).await
            }?;

            auxiliary.insert(name.clone().into(), index);
        }

        std::mem::drop(dir_contents);

        Ok(Self {
            schema: Arc::new(schema.into()),
            primary,
            auxiliary,
            dir,
        })
    }

    /// Load an existing [`Table`] with the given `schema` from the given `dir`
    pub fn load(
        schema: S,
        ordering: b_tree::IndexOrder<C, O>,
        dir: DirLock<FE>,
    ) -> Result<Self, io::Error> {
        schema.validate()?;

        let dir_contents = dir.try_read()?;

        let primary = {
            let dir = dir_contents.get_dir(PRIMARY).ok_or_else(|| {
                freqfs::IntegrityError::invalid_data("missing Table primary index")
            })?;
            BTreeLock::load(schema.primary().clone(), ordering.clone(), dir.clone())
        }?;

        let mut auxiliary = BTreeMap::new();
        for (name, schema) in schema.auxiliary() {
            let index = {
                let dir = dir_contents.get_dir(name).ok_or_else(|| {
                    freqfs::IntegrityError::invalid_data("missing Table auxiliary index")
                })?;
                BTreeLock::load(schema.clone(), ordering.clone(), dir.clone())
            }?;

            auxiliary.insert(name.clone().into(), index);
        }

        std::mem::drop(dir_contents);

        Ok(Self {
            schema: Arc::new(schema.into()),
            primary,
            auxiliary,
            dir,
        })
    }

    pub async fn sync(&self) -> Result<(), io::Error>
    where
        FE: FileSave + Clone,
    {
        self.dir.sync().await
    }

    /// Explicitly synchronize this table's backing storage durably.
    pub async fn sync_all(&self) -> Result<(), io::Error>
    where
        FE: FileSave + Clone,
    {
        self.dir.sync_all().await
    }
}

impl<S, C, FE, O> TableLock<S, S::Index, C, FE, O>
where
    S: Schema + Send + Sync,
    S::Index: Send + Sync,
    C: b_tree::collate::Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    O: b_tree::collate::Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + FileLoad,
{
    /// Resolve one semantic primary key without acquiring auxiliary indexes.
    pub async fn get_row(&self, key: &[S::Value]) -> io::Result<Option<Row<S::Value>>> {
        if key.len() != self.schema.key().len() {
            return Err(bad_key(key, self.schema.key().len()));
        }
        self.primary
            .read()
            .await
            .get(key)
            .await
            .map_err(freqfs::IntegrityError::from_read)
    }

    /// Check unique primary-key presence without copying the row's values.
    pub async fn contains(&self, key: &[S::Value]) -> io::Result<bool> {
        if key.len() != self.schema.key().len() {
            return Err(bad_key(key, self.schema.key().len()));
        }
        self.primary
            .read()
            .await
            .get_with(key, |_| Ok::<_, io::Error>(()))
            .await
            .map(|row| row.is_some())
            .map_err(freqfs::IntegrityError::from_read)
    }

    /// Validate native indexes and their consistency before accepting storage.
    pub async fn validate(&self) -> Result<(), io::Error>
    where
        S::Value: Default,
    {
        self.primary.validate().await?;
        for index in self.auxiliary.values() {
            index.validate().await?;
        }

        self.validate_indexes().await
    }

    async fn validate_indexes(&self) -> Result<(), io::Error>
    where
        S::Value: Default,
    {
        use futures::TryStreamExt;

        use crate::IndexSchema;

        let invalid = |message| freqfs::IntegrityError::invalid_data(message);

        {
            let dir = self.dir.read().await;
            if dir.len() != self.auxiliary.len() + 1
                || dir.iter().any(|(name, entry)| {
                    !entry.is_dir()
                        || (name != PRIMARY && !self.auxiliary.contains_key(name.as_str()))
                })
            {
                return Err(invalid("invalid Table index directories"));
            }
        }

        let columns = self.primary.schema().columns();
        let projections: Vec<_> = self
            .auxiliary
            .values()
            .map(|index| {
                let positions: IndexStack<_> = index
                    .schema()
                    .columns()
                    .iter()
                    .map(|column| {
                        columns
                            .iter()
                            .position(|stored| stored == column)
                            .expect("column")
                    })
                    .collect();
                (index, positions)
            })
            .collect();

        let primary = self.primary.read().await;
        let mut rows = primary
            .clone()
            .keys(b_tree::Range::<S::Value>::default())
            .await?;
        let key_len = self.schema.key().len();
        let mut previous: Option<Row<S::Value>> = None;
        let mut count = 0;
        while let Some(mut row) = rows.try_next().await? {
            let key = row
                .get(..key_len)
                .ok_or_else(|| invalid("invalid Table primary key width"))?;
            if let Some(previous) = &previous {
                if previous.as_slice() == key {
                    return Err(invalid("ambiguous Table primary key"));
                }

                // Distinct semantic keys can share a collation group. Resolve
                // only those ties, using the capability already held by the scan.
                if self
                    .primary
                    .ordering()
                    .range_collator()
                    .cmp_ref(previous.as_slice(), key)
                    .is_eq()
                {
                    primary
                        .get_with(key, |_| Ok::<_, io::Error>(()))
                        .await
                        .map_err(freqfs::IntegrityError::from_read)?
                        .ok_or_else(|| invalid("missing Table primary row"))?;
                }
            }

            count += 1;
            for (index, positions) in &projections {
                let key: Row<_> = positions
                    .iter()
                    .map(|&position| row[position].clone())
                    .collect();
                if !index.read().await.contains(&key).await? {
                    return Err(invalid("inconsistent Table index"));
                }
            }

            row.truncate(key_len);
            previous = Some(row);
        }

        for index in self.auxiliary.values() {
            let mut actual = 0_u64;
            index
                .read()
                .await
                .visit(b_tree::Range::<S::Value>::default(), false, |_| {
                    actual += 1;
                    Ok::<_, io::Error>(ControlFlow::Continue(()))
                })
                .await?;
            if actual != count {
                return Err(invalid("inconsistent Table index count"));
            }
        }

        Ok(())
    }

    /// Copy reachable index nodes into empty delegated storage.
    pub async fn copy_into(&self, dir: DirLock<FE>) -> Result<Self, io::Error>
    where
        FE: Clone,
    {
        let (primary, targets) = {
            let mut contents = dir.write().await;
            if !contents.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "copy requires empty storage",
                ));
            }

            let primary = contents.create_dir(PRIMARY.to_string())?;
            let targets = self
                .auxiliary
                .keys()
                .map(|name| {
                    contents
                        .create_dir(name.to_string())
                        .map(|dir| (name.clone(), dir))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            (primary, targets)
        };

        let primary = self.primary.copy_into(primary).await?;
        let mut auxiliary = BTreeMap::new();
        for (name, target) in targets {
            auxiliary.insert(name.clone(), self.auxiliary[&name].copy_into(target).await?);
        }

        Ok(Self {
            schema: self.schema.clone(),
            dir,
            primary,
            auxiliary,
        })
    }
}

impl<S, C, FE, O> TableLock<S, S::Index, C, FE, O>
where
    S: Schema,
    C: Clone,
    O: Clone,
    FE: Send + Sync + FileLoad,
{
    /// Lock this [`Table`] for reading.
    pub async fn read(&self) -> TableReadGuard<S, S::Index, C, FE, O> {
        #[cfg(feature = "logging")]
        log::debug!("locking table for reading...");

        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.read().await;

        #[cfg(feature = "logging")]
        log::trace!("locked primary index for reading");

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in &self.auxiliary {
            let index = index.read().await;
            auxiliary.insert(name.clone(), index);

            #[cfg(feature = "logging")]
            log::trace!("locked index {name} for reading");
        }

        Table {
            schema,
            state: TableState { auxiliary, primary },
        }
    }

    /// Lock this [`Table`] for reading, without borrowing.
    pub async fn into_read(self) -> TableReadGuard<S, S::Index, C, FE, O> {
        #[cfg(feature = "logging")]
        log::debug!("locking table for reading...");

        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.into_read().await;

        #[cfg(feature = "logging")]
        log::trace!("locked primary index for reading");

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in self.auxiliary {
            let index = index.into_read().await;

            #[cfg(feature = "logging")]
            log::trace!("locked index {name} for reading");

            auxiliary.insert(name, index);
        }

        Table {
            schema,
            state: TableState { auxiliary, primary },
        }
    }

    /// Lock this [`Table`] for reading synchronously, if possible.
    pub fn try_read(&self) -> Result<TableReadGuard<S, S::Index, C, FE, O>, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("locking table for reading...");

        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.try_read()?;

        #[cfg(feature = "logging")]
        log::trace!("locked primary index for reading");

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in self.auxiliary.iter() {
            let index = index.try_read()?;
            auxiliary.insert(name.clone(), index);

            #[cfg(feature = "logging")]
            log::trace!("locked index {name} for reading");
        }

        Ok(Table {
            schema,
            state: TableState { auxiliary, primary },
        })
    }

    /// Lock this [`Table`] for writing.
    pub async fn write(&self) -> TableWriteGuard<S, S::Index, C, FE, O> {
        #[cfg(feature = "logging")]
        log::debug!("locking table for writing...");

        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.write().await;

        #[cfg(feature = "logging")]
        log::trace!("locked primary index for writing");

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in self.auxiliary.iter() {
            let index = index.write().await;
            auxiliary.insert(name.clone(), index);

            #[cfg(feature = "logging")]
            log::trace!("locked index {name} for writing");
        }

        Table {
            schema,
            state: TableState { auxiliary, primary },
        }
    }

    /// Lock this [`Table`] for writing, without borrowing.
    pub async fn into_write(self) -> TableWriteGuard<S, S::Index, C, FE, O> {
        #[cfg(feature = "logging")]
        log::debug!("locking table for reading...");

        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.into_write().await;

        #[cfg(feature = "logging")]
        log::trace!("locked primary index for writing");

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in self.auxiliary.into_iter() {
            let index = index.into_write().await;

            #[cfg(feature = "logging")]
            log::trace!("locked index {name} for writing");

            auxiliary.insert(name, index);
        }

        Table {
            schema,
            state: TableState { auxiliary, primary },
        }
    }

    /// Lock this [`Table`] for writing synchronously, if possible.
    pub fn try_write(&self) -> Result<TableWriteGuard<S, S::Index, C, FE, O>, io::Error> {
        #[cfg(feature = "logging")]
        log::debug!("locking table for writing...");

        let schema = self.schema.clone();

        // lock the primary key first, separately from the indices, to avoid a deadlock
        let primary = self.primary.try_write()?;

        #[cfg(feature = "logging")]
        log::trace!("locked primary index for writing");

        // then lock each index in-order
        let mut auxiliary = HashMap::with_capacity(self.auxiliary.len());
        for (name, index) in self.auxiliary.iter() {
            let index = index.try_write()?;
            auxiliary.insert(name.clone(), index);

            #[cfg(feature = "logging")]
            log::trace!("locked index {name} for writing");
        }

        Ok(Table {
            schema,
            state: TableState { auxiliary, primary },
        })
    }
}
