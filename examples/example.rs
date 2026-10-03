use std::cmp::Ordering;
use std::io;
use std::ops::Bound;
use std::path::PathBuf;

use b_table::collate::{self, Collate};
use b_table::{BTreeSchema, IndexSchema as IndexSchemaInstance, Node, Range, TableLock};
use destream::{de, en};
use freqfs::Cache;
#[cfg(test)]
use futures::FutureExt;
use futures::TryStreamExt;
use get_size::GetSize;
use number_general::NumberCollator;
use rand::RngExt;
use safecast::as_type;
use tokio::fs;

#[tokio::test]
async fn variable_width_separators_rebalance_and_reopen() -> Result<(), io::Error> {
    let path = setup_tmp_dir().await?;
    let key = |index: usize| {
        Value::String(format!(
            "{index:04}-{}",
            "x".repeat(if index % 2 == 0 { 8 } else { 512 })
        ))
    };
    let cache = Cache::<File>::new(4 * 1024 * 1024, None, 0, std::time::Duration::from_secs(3));
    let tree = b_tree::BTreeLock::create(
        IndexSchema::new(["key"]),
        Collator::new(),
        cache.load(path.clone())?,
    )
    .await?;
    {
        let mut tree = tree.write().await;
        for index in 0..128 {
            assert!(tree.insert(vec![key(index)]).await?);
        }
        // Short lower bounds are replaced by long keys without changing index width.
        for index in (0..128).step_by(2) {
            assert!(tree.delete(&[key(index)]).await?);
        }
        // Deleting from both ends exercises sibling borrowing and node retirement.
        for index in (1..64).step_by(2).rev() {
            assert!(tree.delete(&[key(index)]).await?);
        }
    }
    tree.validate().await?;
    tree.sync_all().await?;
    drop(tree);
    let cache = Cache::<File>::new(4 * 1024 * 1024, None, 0, std::time::Duration::from_secs(3));
    let tree = b_tree::BTreeLock::load(
        IndexSchema::new(["key"]),
        Collator::new(),
        cache.load(path.clone())?,
    )?;
    tree.validate().await?;
    let actual: Vec<_> = tree
        .read()
        .await
        .keys(b_tree::Range::<Value>::default())
        .await?
        .map_ok(|row| row[0].clone())
        .try_collect()
        .await?;
    assert_eq!(actual, (65..128).step_by(2).map(key).collect::<Vec<_>>());
    drop(tree);
    fs::remove_dir_all(path).await
}

const BLOCK_SIZE: usize = 4_096;

#[tokio::test]
async fn cancelled_index_construction_is_not_loadable() -> io::Result<()> {
    let schema = || {
        TableSchema::new(
            vec!["up", "up_name", "down", "down_name"],
            [("down".into(), vec!["down", "up"])],
        )
    };
    let path = setup_tmp_dir().await?;
    let empty_size = File::Node(Node::Leaf(vec![])).get_size();
    let cache = Cache::<File>::new(
        2 * empty_size,
        Some(1),
        0,
        std::time::Duration::from_secs(1),
    );
    let root = cache.load(path.clone())?;
    let filler = root
        .write()
        .await
        .create_empty_file("pinned".into(), File::Node(Node::Leaf(vec![])))
        .await?;
    let pinned = filler.read::<Node<Value>>().await?;
    let dir = root.write().await.create_dir("table".into())?;
    {
        let construction = TableLock::create(schema(), Collator::new(), dir.clone());
        futures::pin_mut!(construction);
        assert!(futures::poll!(&mut construction).is_pending());
    }
    // The primary root was admitted, but eviction cannot acquire a handle
    // while the filler is pinned. Cancelling leaves the later index incomplete.
    let primary = dir.read().await.get_dir("primary").unwrap().clone();
    assert!(!primary.read().await.is_empty());
    let auxiliary = dir.read().await.get_dir("down").unwrap().clone();
    assert!(auxiliary.read().await.is_empty());
    assert!(TableLock::load(schema(), Collator::new(), dir).is_err());
    drop(pinned);
    root.write().await.truncate_and_sync().await?;
    Ok(())
}

#[tokio::test]
async fn load_requires_all_indexes() -> Result<(), io::Error> {
    let schema = || {
        TableSchema::new(
            vec!["up", "up_name", "down", "down_name"],
            [("down".into(), vec!["down", "up"])],
        )
    };
    let path = setup_tmp_dir().await?;
    let cache = Cache::<File>::new(64 * BLOCK_SIZE, None, 0, std::time::Duration::from_secs(3));
    let root = cache.load(path)?;
    assert!(TableLock::load(schema(), Collator::new(), root.clone()).is_err());
    assert!(root.read().await.is_empty());
    let table = TableLock::create(schema(), Collator::new(), root.clone()).await?;
    table.sync().await?;
    assert!(TableLock::load(schema(), Collator::new(), root.clone()).is_ok());
    drop(table);
    root.write().await.delete("down").await;
    assert!(TableLock::load(schema(), Collator::new(), root.clone()).is_err());
    assert!(!root.read().await.contains("down"));
    Ok(())
}

#[test]
fn in_place_indexes_reopen_and_reject_inconsistency() {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap();
            let (send, receive) = std::sync::mpsc::sync_channel(0);
            runtime.spawn(async move {
                let result = std::panic::AssertUnwindSafe(async {
                    let schema = || {
                        TableSchema::new(
                            vec!["up", "up_name", "down", "down_name"],
                            [
                                ("down".into(), vec!["down", "up"]),
                                ("name".into(), vec!["up_name", "up"]),
                            ],
                        )
                    };
                    let path = setup_tmp_dir().await?;
                    let cache = Cache::<File>::new(
                        1024 * BLOCK_SIZE,
                        None,
                        0,
                        std::time::Duration::from_secs(3),
                    );
                    let dir = cache.load(path.clone())?;
                    let table = TableLock::create(schema(), Collator::new(), dir.clone()).await?;
                    let rows = futures::stream::iter((0..40).flat_map(|key| {
                        let row = (
                            vec![key.into()],
                            vec![
                                "up".to_string().into(),
                                key.into(),
                                "down".to_string().into(),
                            ],
                        );
                        [Ok(row.clone()), Ok(row)]
                    }));
                    assert_eq!(table.write().await.upsert_sorted(rows).await?, 40);
                    let rows = futures::stream::iter([
                        Ok((
                            vec![40.into()],
                            vec![
                                "up".to_string().into(),
                                40.into(),
                                "down".to_string().into(),
                            ],
                        )),
                        Err(io::Error::other("injected source error")),
                    ]);
                    assert!(table.write().await.upsert_sorted(rows).await.is_err());
                    assert!(table.read().await.get_row(&[40.into()]).await?.is_some());

                    // Replacement preserves one row per primary key and removes stale indexes.
                    for key in 0..40 {
                        let mut values = vec![
                            "changed".to_string().into(),
                            (key + 100).into(),
                            "down".to_string().into(),
                        ];
                        let mut guard = table.write().await;
                        assert!(
                            !guard
                                .upsert(vec![key.into()], values.clone())
                                .boxed()
                                .await?
                        );
                        assert!(
                            !guard
                                .upsert(vec![key.into()], values.clone())
                                .boxed()
                                .await?
                        );
                        values[2] = "annotation only".to_string().into();
                        assert!(
                            !guard
                                .upsert(vec![key.into()], values.clone())
                                .boxed()
                                .await?
                        );
                        assert_eq!(
                            guard.get_row(&[key.into()]).await?.unwrap().as_slice(),
                            &[
                                key.into(),
                                values[0].clone(),
                                values[1].clone(),
                                values[2].clone()
                            ]
                        );
                        assert!(guard.upsert(vec![], values).boxed().await.is_err());
                    }
                    table.validate().await?;
                    assert_eq!(table.read().await.count(Range::default()).await?, 41);
                    for key in 0..30 {
                        table
                            .write()
                            .await
                            .delete_row(&[key.into()])
                            .boxed()
                            .await?;
                    }
                    table.validate().await?;
                    table.sync_all().await?;
                    drop(table);
                    let reopened = Cache::<File>::new(
                        1024 * BLOCK_SIZE,
                        None,
                        0,
                        std::time::Duration::from_secs(3),
                    )
                    .load(path.clone())?;
                    let table = TableLock::load(schema(), Collator::new(), reopened.clone())?;
                    table.validate().await?;
                    assert_eq!(table.read().await.count(Range::default()).await?, 11);
                    let auxiliary = reopened.read().await.get_dir("down").unwrap().clone();
                    let name = auxiliary.read().await.iter().next().unwrap().0.clone();
                    *auxiliary
                        .write()
                        .await
                        .write_file::<_, Node<Value>>(&name, 0)
                        .await? = Node::Leaf(vec![]);
                    assert!(table.validate().await.is_err());
                    *auxiliary
                        .write()
                        .await
                        .write_file::<_, Node<Value>>("00000000-0000-0000-0000-000000000000", 0)
                        .await? = Node::Leaf(vec![]);
                    // Native mutation fails structurally on a missing old auxiliary entry.
                    assert!(
                        table
                            .write()
                            .await
                            .upsert(
                                vec![39.into()],
                                vec![
                                    "again".to_string().into(),
                                    999.into(),
                                    "down".to_string().into()
                                ]
                            )
                            .boxed()
                            .await
                            .is_err()
                    );
                    fs::remove_dir_all(path).await?;
                    Ok::<_, io::Error>(())
                })
                .catch_unwind()
                .await;
                send.send(result).unwrap();
            });
            match receive.recv().unwrap() {
                Ok(result) => result.unwrap(),
                Err(panic) => std::panic::resume_unwind(panic),
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

#[derive(Copy, Clone, Eq, PartialEq)]
struct Collator {
    string: collate::Collator<String>,
    number: NumberCollator,
}

impl Collator {
    fn new() -> Self {
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
#[derive(Clone, Debug, Eq, PartialEq)]
enum Value {
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
enum File {
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
struct IndexSchema {
    columns: Vec<String>,
}

impl IndexSchema {
    fn new<C: IntoIterator<Item = &'static str>>(columns: C) -> Self {
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
struct TableSchema {
    primary: IndexSchema,
    auxiliary: Vec<(String, IndexSchema)>,
}

impl TableSchema {
    fn new<C, I>(columns: C, indices: I) -> Self
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

async fn setup_tmp_dir() -> Result<PathBuf, io::Error> {
    loop {
        let rand: u32 = rand::rng().random();
        let path = PathBuf::from(format!("/tmp/test_table_{}", rand));
        if !path.exists() {
            fs::create_dir(&path).await?;
            break Ok(path);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    // set up the test directory
    let path = setup_tmp_dir().await?;

    // initialize the cache
    let cache = Cache::<File>::new(BLOCK_SIZE, None, 0, std::time::Duration::from_secs(3));

    // load the directory and file paths into memory (not file contents, yet)
    let dir = cache.load(path.clone())?;

    // construct the schema
    let schema = TableSchema::new(
        vec!["up", "up_name", "down", "down_name"],
        [
            ("up_name".into(), vec!["up_name", "up"]),
            ("down".into(), vec!["down", "up"]),
        ],
    );

    let row1 = [
        1.into(),
        "one".to_string().into(),
        9.into(),
        "nine".to_string().into(),
    ];

    let row2 = [
        2.into(),
        "two".to_string().into(),
        8.into(),
        "eight".to_string().into(),
    ];

    // create the table
    let table = TableLock::create(schema, Collator::new(), dir).await?;

    // test reading from an empty table
    {
        let guard = table.read().await;
        let range = Range::default();
        assert_eq!(guard.count(range.clone()).await?, 0);

        assert!(guard.is_empty(range).await?);

        let range = Range::from_iter([("up".to_string(), Value::Number(1.into()))]);
        assert_eq!(guard.count(range.clone()).await?, 0);

        assert!(guard.is_empty(range).await?);
    }

    {
        // test inserting a row
        {
            let mut guard = table.write().await;

            let key = row1[..1].to_vec();
            let values = row1[1..].to_vec();

            assert!(guard.upsert(key.clone(), values.clone()).await?);
            assert!(!guard.upsert(key, values).await?);
        }

        // test reading a row
        {
            let guard = table.read().await;
            assert_eq!(guard.count(Default::default()).await?, 1);
            assert!(!guard.is_empty(Default::default()).await?);

            let range = Range::from_iter([("up".to_string(), Value::Number(1.into()))]);
            assert_eq!(guard.count(range.clone()).await?, 1);

            assert!(!guard.is_empty(range).await?);

            let range =
                Range::from_iter([("up_name".to_string(), Value::String("one".to_string()))]);

            assert_eq!(guard.count(range.clone()).await?, 1);
            assert!(!guard.is_empty(range).await?);

            let range = Range::from_iter([("up".to_string(), Value::Number(2.into()))]);
            assert_eq!(guard.count(range.clone()).await?, 0);

            assert!(guard.is_empty(range).await?);
        }

        let key = row2[..1].to_vec();
        let values = row2[1..].to_vec();

        // test inserting a second row
        {
            let mut guard = table.write().await;
            assert!(guard.upsert(key.clone(), values.clone()).await?);
            assert!(!guard.upsert(key, values).await?);
        }

        // test reading a range
        {
            let guard = table.read().await;

            assert_eq!(guard.count(Default::default()).await?, 2);

            let range = Range::from_iter([("up".to_string(), Value::Number(2.into()))]);
            assert_eq!(guard.count(range.clone()).await?, 1);
            assert!(!guard.is_empty(range).await?);

            let range =
                Range::from_iter([("up_name".to_string(), Value::String("two".to_string()))]);

            assert_eq!(guard.count(range.clone()).await?, 1);
            assert!(!guard.is_empty(range).await?);

            let range = Range::from_iter([("up".to_string(), Value::Number(2.into()))]);
            assert_eq!(guard.count(range.clone()).await?, 1);
            assert!(!guard.is_empty(range).await?);

            let range = Range::from_iter([(
                "up".to_string(),
                (Bound::Included(1.into()), Bound::Excluded(5.into())),
            )]);

            assert_eq!(guard.count(range.clone()).await?, 2);
            assert!(!guard.is_empty(range).await?);
        }
    }

    // test reading a stream of all rows
    {
        let guard = table.read().await;
        let mut stream = guard.rows(Range::default(), &[], false, None).await?;

        assert_eq!(
            stream.try_next().await?,
            Some(row1.iter().cloned().collect())
        );
        assert_eq!(
            stream.try_next().await?,
            Some(row2.iter().cloned().collect())
        );
        assert_eq!(stream.try_next().await?, None);

        let range = Range::from_iter([(
            "down".to_string(),
            (Bound::Unbounded, Bound::Excluded(10.into())),
        )]);

        let mut stream = guard.rows(range, &[], true, None).await?;

        assert_eq!(
            stream.try_next().await?,
            Some(row1.iter().cloned().collect())
        );

        assert_eq!(
            stream.try_next().await?,
            Some(row2.iter().cloned().collect())
        );

        assert_eq!(stream.try_next().await?, None);
    }

    // test deleting a row
    {
        table.write().await.delete_row(&[1.into()]).await?;

        let guard = table.read().await;
        assert_eq!(guard.count(Default::default()).await?, 1);

        let range = Range::from_iter([("up".to_string(), Value::Number(1.into()))]);
        assert_eq!(guard.count(range.clone()).await?, 0);
        assert!(guard.is_empty(range).await?);
    }

    // test deleting the last row
    {
        table.write().await.delete_row(&[2.into()]).await?;

        let guard = table.read().await;
        assert_eq!(guard.count(Default::default()).await?, 0);
        assert!(guard.is_empty(Default::default()).await?);

        let range = Range::from_iter([("up".to_string(), Value::Number(2.into()))]);
        assert_eq!(guard.count(range.clone()).await?, 0);
        assert!(guard.is_empty(range).await?);
    }

    // clean up
    fs::remove_dir_all(path).await
}

impl get_size::GetSize for File {
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Node(node) => node.get_heap_size(),
        }
    }
}

impl freqfs::FileLoad for File {
    async fn load_size(
        _: &std::path::Path,
        _: &mut tokio::fs::File,
        metadata: &std::fs::Metadata,
    ) -> io::Result<usize> {
        // TBON sequences have no allocation-sized length header. Every retained
        // row/cell needs encoded input; cover geometric Vec growth and scalar
        // payload bytes before decoding without retaining any parsed payload.
        let encoded = usize::try_from(metadata.len()).map_err(io::Error::other)?;
        let per_byte =
            2 * std::mem::size_of::<Vec<Value>>() + 8 * std::mem::size_of::<Value>() + 32;
        encoded
            .checked_mul(per_byte)
            .and_then(|size| size.checked_add(std::mem::size_of::<Self>()))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "node too large"))
    }

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
        use futures::TryStreamExt;
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
