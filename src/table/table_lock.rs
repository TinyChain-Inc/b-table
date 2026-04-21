use std::sync::Arc;
use std::collections::{BTreeMap, HashMap};
use std::io;

use freqfs::{DirLock, FileLoad, FileSave};
use b_tree::BTreeLock;
use safecast::AsType;

use crate::Node;
use crate::schema::{TableSchema, Schema};

use super::{PRIMARY, TableWriteGuard, TableReadGuard, Table};
use super::table_utils::valid_schema;
use super::table_state::TableState;


/// A futures-aware read-write lock on a [`Table`]
pub struct TableLock<S, IS, C, FE> {
    schema: Arc<TableSchema<S>>,
    dir: DirLock<FE>,
    primary: BTreeLock<IS, C, FE>,
    // use a BTreeMap to make sure index locks are always acquired in-order
    auxiliary: BTreeMap<Arc<str>, BTreeLock<IS, C, FE>>,
}

impl<S, IS, C, FE> Clone for TableLock<S, IS, C, FE>
where
    C: Clone,
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

impl<S, IS, C, FE> TableLock<S, IS, C, FE> {
    /// Borrow the [`Schema`] of this [`Table`].
    pub fn schema(&self) -> &S {
        self.schema.inner()
    }

    /// Borrow the collator for this [`Table`].
    pub fn collator(&self) -> &b_tree::Collator<C> {
        self.primary.collator()
    }
}

impl<S, C, FE> TableLock<S, S::Index, C, FE>
where
    S: Schema,
    C: Clone,
    FE: AsType<Node<S::Value>> + Send + Sync,
    Node<S::Value>: FileLoad,
{
    /// Create a new [`Table`]
    pub fn create(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        valid_schema(&schema)?;

        let mut dir_contents = dir.try_write()?;

        let primary = {
            let dir = dir_contents.create_dir(PRIMARY.to_string())?;
            BTreeLock::create(schema.primary().clone(), collator.clone(), dir)
        }?;

        let mut auxiliary = BTreeMap::new();
        for (name, schema) in schema.auxiliary() {
            let index = {
                let dir = dir_contents.create_dir(name.to_string())?;
                BTreeLock::create(schema.clone(), collator.clone(), dir)
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
    pub fn load(schema: S, collator: C, dir: DirLock<FE>) -> Result<Self, io::Error> {
        valid_schema(&schema)?;

        let mut dir_contents = dir.try_write()?;

        let primary = {
            let dir = dir_contents.get_or_create_dir(PRIMARY.to_string())?;
            BTreeLock::load(schema.primary().clone(), collator.clone(), dir.clone())
        }?;

        let mut auxiliary = BTreeMap::new();
        for (name, schema) in schema.auxiliary() {
            let index = {
                let dir = dir_contents.get_or_create_dir(name.clone())?;
                BTreeLock::load(schema.clone(), collator.clone(), dir.clone())
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
        FE: for<'a> FileSave + Clone,
    {
        self.dir.sync().await
    }
}

impl<S, C, FE> TableLock<S, S::Index, C, FE>
where
    S: Schema,
    C: Clone,
    FE: Send + Sync,
    Node<S::Value>: FileLoad,
{
    /// Lock this [`Table`] for reading.
    pub async fn read(&self) -> TableReadGuard<S, S::Index, C, FE> {
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
    pub async fn into_read(self) -> TableReadGuard<S, S::Index, C, FE> {
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
    pub fn try_read(&self) -> Result<TableReadGuard<S, S::Index, C, FE>, io::Error> {
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
    pub async fn write(&self) -> TableWriteGuard<S, S::Index, C, FE> {
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
    pub async fn into_write(self) -> TableWriteGuard<S, S::Index, C, FE> {
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
    pub fn try_write(&self) -> Result<TableWriteGuard<S, S::Index, C, FE>, io::Error> {
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