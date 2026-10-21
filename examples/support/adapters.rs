use std::cmp::Ordering;
use std::io;

use destream::{de, en};
use futures::TryStreamExt;
use get_size::GetSize;
use number_general::NumberCollator;
use safecast::as_type;

use b_table::collate::{self, Collate};
use b_table::{BTreeSchema, IndexSchema as IndexSchemaInstance, Node};

pub(super) const BLOCK_SIZE: usize = 4_096;

#[derive(Copy, Clone, Eq, PartialEq)]
pub(super) struct Collator {
    string: collate::Collator<String>,
    number: NumberCollator,
}

impl Collator {
    pub(super) fn new() -> Self {
        Self {
            string: collate::Collator::default(),
            number: NumberCollator::default(),
        }
    }
}

impl Collate for Collator {
    type Value = Value;

    fn cmp(&self, left: &Self::Value, right: &Self::Value) -> Ordering {
        match (left, right) {
            (Value::String(l), Value::String(r)) => self.string.cmp(l, r),
            (Value::Number(l), Value::Number(r)) => self.number.cmp(l, r),
            (l, r) => panic!("tried to compare un-like types: {:?} vs {:?}", l, r),
        }
    }
}

// This example stores only scalar numbers and strings.
#[derive(Debug, Eq, PartialEq)]
#[cfg_attr(not(test), derive(Clone))]
pub(super) enum Value {
    Number(number_general::Number),
    String(String),
}

impl Default for Value {
    fn default() -> Self {
        Self::Number(0.into())
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::Number(value.into())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl GetSize for Value {
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Number(_) => 0,
            Self::String(value) => value.capacity(),
        }
    }
}

impl de::FromStream for Value {
    type Context = ();

    async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        struct Scalar;

        impl de::Visitor for Scalar {
            type Value = Value;

            fn expecting() -> &'static str {
                "a number or string"
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
                Ok(Value::Number(value.into()))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
                Ok(Value::Number(value.into()))
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
                Ok(Value::Number(value.into()))
            }

            fn visit_string<E: de::Error>(self, value: String) -> Result<Value, E> {
                Ok(Value::String(value))
            }
        }
        decoder.decode_any(Scalar).await
    }
}

impl<'en> en::ToStream<'en> for Value {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Number(value) => value.to_stream(encoder),
            Self::String(value) => value.to_stream(encoder),
        }
    }
}

#[derive(Clone)]
pub(super) enum File {
    Node(Node<Value>),
}

impl de::FromStream for File {
    type Context = ();

    async fn from_stream<D: de::Decoder>(cxt: (), decoder: &mut D) -> Result<Self, D::Error> {
        Node::from_stream(cxt, decoder).await.map(Self::Node)
    }
}

impl<'en> en::ToStream<'en> for File {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Node(node) => node.to_stream(encoder),
        }
    }
}

as_type!(File, Node, Node<Value>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct IndexSchema {
    pub(super) columns: Vec<String>,
}

impl IndexSchema {
    pub(super) fn new<C: IntoIterator<Item = &'static str>>(columns: C) -> Self {
        Self {
            columns: columns.into_iter().map(String::from).collect(),
        }
    }
}

impl BTreeSchema for IndexSchema {
    type Error = io::Error;
    type Value = Value;

    fn block_size(&self) -> usize {
        BLOCK_SIZE
    }

    fn len(&self) -> usize {
        self.columns.len()
    }

    fn order(&self) -> usize {
        8
    }

    fn validate_prefix(&self, prefix: &[Self::Value]) -> Result<(), Self::Error> {
        if prefix.is_empty() || prefix.len() > self.len() {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid separator width",
            ))
        } else {
            Ok(())
        }
    }

    fn validate_key(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error> {
        if key.len() == self.len() {
            Ok(key)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "wrong number of values",
            ))
        }
    }
}

impl IndexSchemaInstance for IndexSchema {
    type Id = String;

    fn columns(&self) -> &[Self::Id] {
        &self.columns
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct TableSchema {
    pub(super) primary: IndexSchema,
    pub(super) auxiliary: Vec<(String, IndexSchema)>,
}

impl TableSchema {
    pub(super) fn new<C, I>(columns: C, indices: I) -> Self
    where
        C: IntoIterator<Item = &'static str>,
        I: IntoIterator<Item = (String, C)>,
    {
        Self {
            primary: IndexSchema::new(columns),
            auxiliary: indices
                .into_iter()
                .map(|(name, columns)| (name, IndexSchema::new(columns)))
                .collect(),
        }
    }
}

impl b_table::Schema for TableSchema {
    type Id = String;
    type Error = io::Error;
    type Value = Value;
    type Index = IndexSchema;

    fn key(&self) -> &[Self::Id] {
        &self.primary.columns[..1]
    }

    fn values(&self) -> &[Self::Id] {
        &self.primary.columns[1..]
    }

    fn primary(&self) -> &Self::Index {
        &self.primary
    }

    fn auxiliary(&self) -> &[(String, IndexSchema)] {
        &self.auxiliary
    }

    fn validate_key(&self, key: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error> {
        if key.len() == 1 {
            Ok(key)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid key: {:?}", key),
            ))
        }
    }

    fn validate_values(&self, values: Vec<Self::Value>) -> Result<Vec<Self::Value>, Self::Error> {
        if values.len() == 3 {
            Ok(values)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid values: {:?}", values),
            ))
        }
    }
}

impl get_size::GetSize for File {
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Node(node) => node.get_heap_size(),
        }
    }
}

impl freqfs::FileLoad for File {
    async fn load(
        _: &std::path::Path,
        file: tokio::fs::File,
        _: std::fs::Metadata,
    ) -> std::io::Result<Self> {
        tbon::de::read_from((), file)
            .await
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }
}

impl freqfs::FileSave for File {
    async fn save(&self, file: &mut tokio::fs::File) -> std::io::Result<u64> {
        use tokio::io::AsyncWriteExt;

        let mut stream = tbon::en::encode(self).map_err(std::io::Error::other)?;
        let mut size = 0;

        while let Some(chunk) = stream.try_next().await.map_err(std::io::Error::other)? {
            file.write_all(&chunk).await?;
            size += chunk.len() as u64;
        }

        Ok(size)
    }
}
