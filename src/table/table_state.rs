use std::{collections::HashMap, fmt, io, sync::Arc};

use b_tree::BTree;
use b_tree::collate::Collate;
use freqfs::{DirDeref, DirReadGuardOwned, DirWriteGuardOwned, FileLoad};
use futures::future;
use futures::stream::TryStreamExt;
use safecast::AsType;

use super::table_utils::{borrow_columns, index_range_for, prefix_extractor};
use crate::schema::{IndexId, IndexSchema, Range};
use crate::{ColumnRange, IndexStack, Node, Row};

type ScopedRows<'a, V> =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<Row<V>, io::Error>> + Send + 'a>>;

pub(super) struct TableState<IS, C, G> {
    // IMPORTANT! the auxiliary field must go before primary so that it will be dropped first
    pub(super) auxiliary: HashMap<Arc<str>, BTree<IS, C, G>>,
    pub(super) primary: BTree<IS, C, G>,
}

impl<IS, C: Clone, G: Clone> Clone for TableState<IS, C, G> {
    fn clone(&self) -> Self {
        Self {
            primary: self.primary.clone(),
            auxiliary: self.auxiliary.clone(),
        }
    }
}

impl<IS, C, G> TableState<IS, C, G> {
    #[inline]
    fn get_index(&self, index_id: IndexId<'_>) -> Option<&BTree<IS, C, G>> {
        match index_id {
            IndexId::Primary => Some(&self.primary),
            IndexId::Auxiliary(index_id) => self.auxiliary.get(index_id),
        }
    }
}

impl<IS, C, FE, G> TableState<IS, C, G>
where
    IS: IndexSchema,
    C: Collate<Value = IS::Value> + 'static,
    FE: AsType<Node<IS::Value>> + Send + Sync + 'static + FileLoad,
    G: DirDeref<Entry = FE> + 'static,
    Range<IS::Id, IS::Value>: fmt::Debug,
{
    pub(super) async fn contains(&self, prefix: &[IS::Value]) -> Result<bool, io::Error> {
        Ok(self.get_row(prefix).await?.is_some())
    }

    pub(super) async fn get_row(
        &self,
        key: &[IS::Value],
    ) -> Result<Option<Row<IS::Value>>, io::Error> {
        // Unlike a general tree prefix, a Table primary key must be unique.
        self.primary
            .get(key)
            .await
            .map_err(freqfs::IntegrityError::from_read)
    }

    pub(super) async fn first(
        &self,
        index: IndexId<'_>,
        range: &HashMap<IS::Id, ColumnRange<IS::Value>>,
        select: &[IS::Id],
        key_columns: &[IS::Id],
    ) -> Result<Option<Row<IS::Value>>, io::Error>
    where
        C: Clone + Send + Sync,
        G: Sync,
    {
        let primary = self.primary.as_ref();
        let index = self
            .get_index(index)
            .expect("selected schema index")
            .as_ref();
        let mut rows =
            TableState::index_rows(&primary, index, range.clone(), false, select, key_columns)
                .await?;
        rows.try_next().await
    }
}

impl<IS, C, FE, G> TableState<IS, C, G>
where
    IS: IndexSchema,
    C: Collate<Value = IS::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<IS::Value>> + Send + Sync + 'static + FileLoad,
    G: DirDeref<Entry = FE> + Clone + Send + Sync,
    Range<IS::Id, IS::Value>: fmt::Debug,
{
    pub(super) async fn count<'a>(
        &'a self,
        index: IndexId<'_>,
        range: HashMap<IS::Id, ColumnRange<IS::Value>>,
        key_columns: &'a [IS::Id],
    ) -> Result<u64, io::Error> {
        // Zero-field projection retains scan ordering and validation without
        // cloning values that this predicate would immediately discard.
        let mut rows = self.rows(index, range, false, &[], key_columns).await?;

        let mut count = 0;
        while let Some(_row) = rows.try_next().await? {
            count += 1;
        }

        Ok(count)
    }

    pub(super) async fn is_empty<'a>(
        &'a self,
        index: IndexId<'_>,
        range: HashMap<IS::Id, ColumnRange<IS::Value>>,
        key_columns: &'a [IS::Id],
    ) -> Result<bool, io::Error> {
        let mut rows = self.rows(index, range, false, &[], key_columns).await?;
        rows.try_next().await.map(|row| row.is_none())
    }

    pub(super) async fn rows<'s>(
        &self,
        index: IndexId<'_>,
        range: HashMap<IS::Id, ColumnRange<IS::Value>>,
        reverse: bool,
        select: &[IS::Id],
        key_columns: &[IS::Id],
    ) -> Result<ScopedRows<'s, IS::Value>, io::Error>
    where
        G: 's,
    {
        let index = self.get_index(index).expect("selected schema index");
        Self::index_rows(
            &self.primary,
            index.clone(),
            range,
            reverse,
            select,
            key_columns,
        )
        .await
    }

    async fn index_rows<'s>(
        primary: &BTree<IS, C, G>,
        index: BTree<IS, C, G>,
        mut range: HashMap<IS::Id, ColumnRange<IS::Value>>,
        reverse: bool,
        select: &[IS::Id],
        key_columns: &[IS::Id],
    ) -> Result<ScopedRows<'s, IS::Value>, io::Error>
    where
        G: 's,
    {
        let columns = index.schema().columns();
        let index_range = index_range_for(columns, &mut range);
        assert!(range.is_empty(), "selected index supports every constraint");

        let covering = select.iter().all(|column| columns.contains(column));
        let required = if covering { select } else { key_columns };
        let positions: Vec<_> = columns
            .iter()
            .enumerate()
            .filter_map(|(position, column)| required.contains(column).then_some(position))
            .collect();
        let requested: IndexStack<_> = required
            .iter()
            .map(|column| {
                columns
                    .iter()
                    .position(|stored| stored == column)
                    .expect("column")
            })
            .collect();
        let extract = (positions.as_slice() != requested.as_slice())
            .then(|| prefix_extractor(&positions, &requested));

        // Only physical positions cross the owned stream boundary. No column names
        // are copied; final extraction retains requested order and repeated columns.
        let keys: ScopedRows<'s, IS::Value> = index.select(index_range, positions, reverse).await?;
        let keys: ScopedRows<'s, IS::Value> = if let Some(extract) = extract {
            Box::pin(keys.map_ok(extract))
        } else {
            keys
        };

        // Public selection rejects empty columns; internal predicates use them
        // to consume row presence without copying stored fields.
        if covering {
            Ok(keys)
        } else {
            let index = primary.clone();
            let rows = keys
                .map_ok(move |key| {
                    let index = index.clone();
                    async move {
                        index
                            .get(&key)
                            .await
                            .map_err(freqfs::IntegrityError::from_read)
                    }
                })
                .try_buffered(num_cpus::get())
                .and_then(|row| {
                    future::ready(
                        row.ok_or_else(|| {
                            freqfs::IntegrityError::invalid_data("missing primary row")
                        }),
                    )
                });

            let columns = primary.schema().columns();
            if columns == select {
                Ok(Box::pin(rows))
            } else {
                let extract = prefix_extractor(columns, select);
                Ok(Box::pin(rows.map_ok(extract)))
            }
        }
    }
}

impl<IS, C, FE> TableState<IS, C, DirWriteGuardOwned<FE>>
where
    IS: IndexSchema + Send + Sync,
    C: Collate<Value = IS::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<IS::Value>> + Send + Sync + 'static + FileLoad,
    DirWriteGuardOwned<FE>: DirDeref<Entry = FE>,
{
    pub(super) async fn delete_row(&mut self, key: &[IS::Value]) -> Result<bool, io::Error> {
        let row = if let Some(row) = self.get_row(key).await? {
            row
        } else {
            return Ok(false);
        };

        self.remove_row(&row).await?;

        Ok(true)
    }

    // The caller has already resolved the complete primary row.
    pub(super) async fn remove_row(&mut self, row: &[IS::Value]) -> Result<(), io::Error> {
        let columns = self.primary.schema().columns();
        let deletes = self.auxiliary.values_mut().map(|index| async move {
            let key = borrow_columns(row, columns, index.schema().columns());

            if !index.delete(&key).await? {
                return Err(freqfs::IntegrityError::invalid_data(
                    "table index is out of sync",
                ));
            }

            Ok(())
        });
        future::try_join_all(deletes).await?;

        if !self.primary.delete(row).await? {
            return Err(freqfs::IntegrityError::invalid_data(
                "missing table primary row",
            ));
        }

        Ok(())
    }

    pub(super) async fn delete_range(
        &mut self,
        index: IndexId<'_>,
        range: HashMap<IS::Id, ColumnRange<IS::Value>>,
        key_columns: &[IS::Id],
    ) -> Result<usize, io::Error> {
        let mut deleted = 0;

        while let Some(row) = self
            .first(index, &range, self.primary.schema().columns(), key_columns)
            .await?
        {
            self.remove_row(&row).await?;
            deleted += 1;
        }

        Ok(deleted)
    }

    pub(super) async fn upsert(
        &mut self,
        row: Vec<IS::Value>,
        previous: Option<&[IS::Value]>,
    ) -> Result<bool, io::Error> {
        let columns = self.primary.schema().columns();
        let updates = self.auxiliary.values_mut().map(|index| {
            let row = &row;

            async move {
                let key = borrow_columns(row, columns, index.schema().columns());

                if let Some(old) = previous {
                    let old_key = borrow_columns(old, columns, index.schema().columns());

                    if key == old_key {
                        if !index
                            .replace(
                                &old_key.into_iter().cloned().collect::<Vec<_>>(),
                                key.into_iter().cloned().collect(),
                            )
                            .await?
                        {
                            return Err(freqfs::IntegrityError::invalid_data(
                                "table index is out of sync",
                            ));
                        }

                        return Ok(());
                    }

                    if !index.delete(&old_key).await? {
                        return Err(freqfs::IntegrityError::invalid_data(
                            "table index is out of sync",
                        ));
                    }
                }

                index.insert(key.into_iter().cloned().collect()).await?;
                Ok(())
            }
        });

        future::try_join_all(updates).await?;

        if let Some(old) = previous {
            if !self.primary.replace(old, row).await? {
                return Err(freqfs::IntegrityError::invalid_data(
                    "missing table primary row",
                ));
            }
        } else {
            self.primary.insert(row).await?;
        }

        Ok(previous.is_none())
    }

    pub(super) async fn truncate(&mut self) -> Result<(), io::Error> {
        let mut truncates = IndexStack::with_capacity(self.auxiliary.len() + 1);
        truncates.push(self.primary.truncate());

        for index in self.auxiliary.values_mut() {
            truncates.push(index.truncate());
        }

        future::try_join_all(truncates).await?;

        Ok(())
    }
}

impl<IS, C, FE> TableState<IS, C, DirWriteGuardOwned<FE>> {
    pub(super) fn downgrade(self) -> TableState<IS, C, Arc<DirReadGuardOwned<FE>>> {
        TableState {
            primary: self.primary.downgrade(),
            auxiliary: self
                .auxiliary
                .into_iter()
                .map(|(name, index)| (name, index.downgrade()))
                .collect(),
        }
    }
}
