use std::fmt;

use b_tree::Key;
use b_tree::collate::Collate;
use destream::de;
use freqfs::{DirLock, FileLoad};
use futures::TryFutureExt;
use safecast::AsType;

use super::{IndexSchema, Node, Range, Schema, TableLock};

struct TableVisitor<S, IS, C, FE, O> {
    table: TableLock<S, IS, C, FE, O>,
}

impl<S, IS, C, FE, O> de::Visitor for TableVisitor<S, IS, C, FE, O>
where
    S: Schema<Index = IS> + Send + Sync + fmt::Debug,
    IS: IndexSchema<Id = S::Id> + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    O: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static + FileLoad,
    IS::Value: de::FromStream<Context = ()>,
    S::Value: de::FromStream<Context = ()>,
    Node<S::Value>: fmt::Debug,
    Range<S::Id, S::Value>: fmt::Debug,
    IS::Error: Send + Sync,
{
    type Value = TableLock<S, IS, C, FE, O>;

    fn expecting() -> &'static str {
        "a Table"
    }

    async fn visit_seq<A: de::SeqAccess>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let key_len = self.table.schema().key().len();

        let mut table = self.table.write().await;

        while let Some(mut row) = seq.next_element::<Key<S::Value>>(()).await? {
            if row.len() == self.table.schema().primary().len() {
                let values = row.drain(key_len..).collect::<Vec<_>>();
                let key = row.into_vec();

                table.upsert(key, values).map_err(de::Error::custom).await?;
            } else {
                return Err(de::Error::invalid_length(
                    row.len(),
                    format!("a row of a table with schema {:?}", self.table.schema()),
                ));
            }
        }

        Ok(self.table)
    }
}

impl<S, IS, C, FE, O> de::FromStream for TableLock<S, IS, C, FE, O>
where
    S: Schema<Index = IS> + Send + Sync + fmt::Debug,
    IS: IndexSchema<Id = S::Id> + Send + Sync,
    C: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    O: Collate<Value = S::Value> + Clone + Send + Sync + 'static,
    FE: AsType<Node<S::Value>> + Send + Sync + 'static + FileLoad,
    IS::Value: de::FromStream<Context = ()>,
    S::Value: de::FromStream<Context = ()>,
    Node<S::Value>: fmt::Debug,
    Range<S::Id, S::Value>: fmt::Debug,
    IS::Error: Send + Sync,
{
    type Context = (S, b_tree::IndexOrder<C, O>, DirLock<FE>);

    async fn from_stream<D: de::Decoder>(
        context: Self::Context,
        decoder: &mut D,
    ) -> Result<Self, D::Error> {
        let (schema, ordering, dir) = context;
        let table = TableLock::create(schema, ordering, dir)
            .await
            .map_err(de::Error::custom)?;
        decoder.decode_seq(TableVisitor { table }).await
    }
}
