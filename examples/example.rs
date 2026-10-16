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
        for index in 0..128 {
            let grown = Value::String(format!("{index:04}-{}", "y".repeat(1024)));
            assert!(tree.replace(&[key(index)], vec![grown.clone()]).await?);
            assert!(tree.replace(&[grown], vec![key(index)]).await?);
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
                    let invalid = futures::stream::iter([Ok((vec![41.into()], vec![]))]);
                    assert_eq!(
                        table
                            .write()
                            .await
                            .upsert_sorted(invalid)
                            .await
                            .unwrap_err()
                            .kind(),
                        io::ErrorKind::InvalidInput
                    );
                    assert!(table.read().await.get_row(&[41.into()]).await?.is_none());

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
                        assert!(
                            guard
                                .upsert(vec![key.into()], vec![])
                                .boxed()
                                .await
                                .is_err()
                        );
                        assert_eq!(
                            guard.get_row(&[key.into()]).await?.unwrap()[2],
                            (key + 100).into()
                        );
                    }
                    table.validate().await?;
                    assert_eq!(table.read().await.count(Range::default()).await?, 41);
                    assert!(table.write().await.delete_row(&[0.into()]).boxed().await?);
                    for (column, start, end) in [("up", 1, 10), ("down", 110, 130)] {
                        let range = Range::from_iter([(
                            column.to_string(),
                            (Bound::Included(start.into()), Bound::Excluded(end.into())),
                        )]);
                        assert_eq!(
                            table
                                .write()
                                .await
                                .delete_range(range.clone())
                                .boxed()
                                .await?,
                            (end - start) as usize
                        );
                        assert_eq!(table.write().await.delete_range(range).boxed().await?, 0);
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

#[tokio::test]
async fn unchanged_auxiliary_entries_do_not_clone_values() -> io::Result<()> {
    let path = setup_tmp_dir().await?;
    let schema = TableSchema::new(
        vec!["up", "up_name", "down", "down_name"],
        [("name".into(), vec!["up_name", "up"])],
    );
    let cache = Cache::<File>::new(
        1024 * BLOCK_SIZE,
        None,
        0,
        std::time::Duration::from_secs(3),
    );
    let table = TableLock::create(schema, Collator::new(), cache.load(path.clone())?).await?;
    table
        .write()
        .await
        .upsert(
            vec![1.into()],
            vec![
                "indexed".to_string().into(),
                2.into(),
                "old".to_string().into(),
            ],
        )
        .boxed()
        .await?;

    INDEXED_CLONES
        .scope(std::cell::Cell::new(0), async {
            assert!(
                !table
                    .write()
                    .await
                    .upsert(
                        vec![1.into()],
                        vec![
                            "indexed".to_string().into(),
                            2.into(),
                            "new".to_string().into()
                        ],
                    )
                    .boxed()
                    .await?
            );
            // Resolving the old primary row clones the value once. The unchanged
            // auxiliary entry requires neither another clone nor an insertion.
            assert_eq!(INDEXED_CLONES.with(std::cell::Cell::get), 1);
            INDEXED_CLONES.with(|count| count.set(0));
            let mut calls = 0;
            table
                .write()
                .await
                .try_update(vec![1.into()], |old| {
                    calls += 1;
                    assert_eq!(old.unwrap()[3], "new".to_string().into());
                    Ok::<_, io::Error>(Some(vec![
                        "indexed".to_string().into(),
                        2.into(),
                        "updated".to_string().into(),
                    ]))
                })
                .await?;
            assert_eq!(calls, 1);
            assert_eq!(INDEXED_CLONES.with(std::cell::Cell::get), 1);
            Ok::<_, io::Error>(())
        })
        .await?;

    table.validate().await?;
    drop(table);
    fs::remove_dir_all(path).await
}

#[tokio::test]
async fn callback_update_validates_before_mutation_and_reopens() -> io::Result<()> {
    let path = setup_tmp_dir().await?;
    let schema = || {
        TableSchema::new(
            vec!["up", "up_name", "down", "down_name"],
            [("name".into(), vec!["up_name", "up"])],
        )
    };
    let cache = Cache::<File>::new(
        1024 * BLOCK_SIZE,
        None,
        0,
        std::time::Duration::from_secs(3),
    );
    let dir = cache.load(path.clone())?;
    let table = TableLock::create(schema(), Collator::new(), dir.clone()).await?;
    let values = || {
        vec![
            "name".to_string().into(),
            2.into(),
            "value".to_string().into(),
        ]
    };
    {
        let mut table = table.write().await;
        let error = table
            .try_update::<io::Error>(vec![], |_| panic!("invalid key reached callback"))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        table
            .try_update(vec![1.into()], |old| {
                assert!(old.is_none());
                Ok::<_, io::Error>(None)
            })
            .await?;
        assert!(table.get_row(&[1.into()]).await?.is_none());
        table
            .try_update(vec![1.into()], |old| {
                assert!(old.is_none());
                Ok::<_, io::Error>(Some(values()))
            })
            .await?;
        let original = table.get_row(&[1.into()]).await?.unwrap();
        let error = table
            .try_update::<io::Error>(vec![1.into()], |old| {
                assert_eq!(old.unwrap(), original.as_slice());
                Err(io::Error::other("callback failed"))
            })
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "callback failed");
        let error = table
            .try_update(vec![1.into()], |_| Ok::<_, io::Error>(Some(vec![])))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(table.get_row(&[1.into()]).await?.unwrap(), original);
        table
            .try_update(vec![1.into()], |_| Ok::<_, io::Error>(Some(values())))
            .await?;
        assert_eq!(table.get_row(&[1.into()]).await?.unwrap(), original);
        table
            .try_update(vec![1.into()], |old| {
                assert_eq!(old.unwrap(), original.as_slice());
                Ok::<_, io::Error>(None)
            })
            .await?;
        assert!(table.get_row(&[1.into()]).await?.is_none());
        table
            .try_update(vec![2.into()], |_| Ok::<_, io::Error>(Some(values())))
            .await?;
    }
    table.validate().await?;
    table.sync().await?;
    drop(table);
    drop(dir);
    let cache = Cache::<File>::new(
        1024 * BLOCK_SIZE,
        None,
        0,
        std::time::Duration::from_secs(3),
    );
    let table = TableLock::load(schema(), Collator::new(), cache.load(path.clone())?)?;
    table.validate().await?;
    assert!(table.read().await.get_row(&[1.into()]).await?.is_none());
    assert_eq!(
        table
            .read()
            .await
            .get_value(&[2.into()])
            .await?
            .unwrap()
            .as_slice(),
        values().as_slice()
    );
    drop(table);
    fs::remove_dir_all(path).await
}

#[tokio::test]
async fn range_deletion_uses_reordered_covering_rows() -> io::Result<()> {
    let schema = || {
        TableSchema::new(
            vec!["up", "up_name", "down", "down_name"],
            [("cover".into(), vec!["down", "down_name", "up_name", "up"])],
        )
    };
    let path = setup_tmp_dir().await?;
    let cache = Cache::<File>::new(
        1024 * BLOCK_SIZE,
        None,
        0,
        std::time::Duration::from_secs(3),
    );
    let table = TableLock::create(schema(), Collator::new(), cache.load(path.clone())?).await?;
    let rows = futures::stream::iter((0..40).map(|key| {
        Ok((
            vec![key.into()],
            vec![
                "name".to_string().into(),
                (key + 100).into(),
                "note".to_string().into(),
            ],
        ))
    }));
    assert_eq!(table.write().await.upsert_sorted(rows).await?, 40);

    let range = Range::from_iter([(
        "down".to_string(),
        (Bound::Included(110.into()), Bound::Excluded(130.into())),
    )]);
    assert_eq!(
        table
            .write()
            .await
            .delete_range(range.clone())
            .boxed()
            .await?,
        20
    );
    assert_eq!(table.write().await.delete_range(range).boxed().await?, 0);
    table.validate().await?;
    table.sync_all().await?;
    drop(table);

    let cache = Cache::<File>::new(
        1024 * BLOCK_SIZE,
        None,
        0,
        std::time::Duration::from_secs(3),
    );
    let table = TableLock::load(schema(), Collator::new(), cache.load(path.clone())?)?;
    table.validate().await?;
    let mut rows = table.read().await.into_rows().await?;
    assert!(table.try_write().is_err());
    let mut actual = Vec::new();
    while let Some(row) = rows.try_next().await? {
        actual.push(row[0].clone());
    }
    assert_eq!(
        actual,
        (0..10).chain(30..40).map(Value::from).collect::<Vec<_>>()
    );
    drop(rows);
    drop(table.try_write()?);
    drop(table);
    fs::remove_dir_all(path).await
}

#[tokio::test]
async fn replacement_rejects_primary_relocation_after_auxiliary_changes() -> io::Result<()> {
    let path = setup_tmp_dir().await?;
    let schema = TableSchema::new(
        vec!["up", "up_name", "down", "down_name"],
        [("name".into(), vec!["up_name", "up"])],
    );
    let dir = Cache::<File>::new(
        1024 * BLOCK_SIZE,
        None,
        0,
        std::time::Duration::from_secs(1),
    )
    .load(path.clone())?;
    let table = TableLock::create(schema, Collator::new(), dir.clone()).await?;
    let values = |name: &str| vec![name.to_string().into(), 2.into(), "note".to_string().into()];
    assert_eq!(
        table
            .write()
            .await
            .upsert_sorted(futures::stream::iter(
                ["a", "c"].map(|name| Ok((vec![1.into()], values(name)))),
            ))
            .await?,
        2
    );
    assert!(
        !table
            .write()
            .await
            .upsert(vec![1.into()], values("b"))
            .boxed()
            .await?
    );
    assert!(
        !table
            .write()
            .await
            .upsert(vec![1.into()], values("b"))
            .boxed()
            .await?
    );
    table.validate().await?;

    let error = table
        .write()
        .await
        .upsert(vec![1.into()], values("z"))
        .boxed()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        table
            .read()
            .await
            .get_value(&[1.into()])
            .await?
            .unwrap()
            .as_slice(),
        values("b").as_slice()
    );
    assert_eq!(table.read().await.count(Range::default()).await?, 2);

    // Auxiliary updates precede replacement admission. Inspect native contents
    // to demonstrate the documented partial failure without asking a query to
    // resolve a deliberately inconsistent index back through the primary row.
    let auxiliary = dir.read().await.get_dir("name").unwrap().clone();
    let root = auxiliary
        .read()
        .await
        .read_file::<_, Node<Value>>("00000000-0000-0000-0000-000000000000")
        .await?
        .clone();
    let Node::Leaf(rows) = root else {
        panic!("small auxiliary leaf")
    };
    assert_eq!(
        rows,
        [
            vec!["c".to_string().into(), 1.into()],
            vec!["z".to_string().into(), 1.into()]
        ]
    );
    assert!(table.validate().await.is_err());
    drop(table);
    drop(auxiliary);
    drop(dir);
    fs::remove_dir_all(path).await
}

#[tokio::test]
async fn auxiliary_mutations_overlap_and_release_guards() -> io::Result<()> {
    const ROOT: &str = "00000000-0000-0000-0000-000000000000";
    for operation in ["insert", "update", "delete", "ordered"] {
        for cancel in [false, true] {
            let path = setup_tmp_dir().await?;
            let schema = TableSchema::new(
                vec!["up", "up_name", "down", "down_name"],
                [
                    ("left".into(), vec!["up_name", "up"]),
                    ("right".into(), vec!["down", "up"]),
                ],
            );
            let dir = Cache::<File>::new(
                1024 * BLOCK_SIZE,
                None,
                0,
                std::time::Duration::from_secs(3),
            )
            .load(path.clone())?;
            let table = TableLock::create(schema, Collator::new(), dir.clone()).await?;
            let values = || {
                vec![
                    "new".to_string().into(),
                    3.into(),
                    "note".to_string().into(),
                ]
            };
            table
                .write()
                .await
                .upsert(
                    vec![1.into()],
                    vec![
                        "old".to_string().into(),
                        2.into(),
                        "note".to_string().into(),
                    ],
                )
                .await?;
            let mut roots = Vec::new();
            for name in ["primary", "left", "right"] {
                let index = dir.read().await.get_dir(name).unwrap().clone();
                roots.push(index.read().await.get_file(ROOT).unwrap().clone());
            }
            let before = match &*roots[0].read::<Node<Value>>().await? {
                Node::Leaf(rows) => rows.clone(),
                _ => panic!("small primary leaf"),
            };
            let mut blocked = Vec::new();
            for root in &roots[1..] {
                blocked.push(root.read::<Node<Value>>().await?);
            }
            let mut owner = table.write().await;
            let mut mutation = Box::pin(async {
                match operation {
                    "insert" => {
                        assert!(owner.upsert(vec![2.into()], values()).await?);
                    }
                    "update" => {
                        owner
                            .try_update(vec![1.into()], |_| Ok::<_, io::Error>(Some(values())))
                            .await?;
                    }
                    "delete" => {
                        assert!(owner.delete_row(&[1.into()]).await?);
                    }
                    "ordered" => {
                        assert_eq!(
                            owner
                                .upsert_sorted(futures::stream::iter([Ok((
                                    vec![2.into()],
                                    values()
                                ))]))
                                .await?,
                            1
                        );
                    }
                    _ => unreachable!(),
                }
                Ok::<_, io::Error>(())
            });
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    assert!(futures::poll!(&mut mutation).is_pending(), "{operation}");
                    if roots[1..]
                        .iter()
                        .all(|root| root.try_read::<Node<Value>>().is_err())
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("both auxiliary writes must start before either is released");
            match &*roots[0].try_read::<Node<Value>>()? {
                Node::Leaf(rows) => assert_eq!(rows, &before),
                _ => panic!("small primary leaf"),
            }
            if cancel {
                drop(mutation);
                for root in &roots[1..] {
                    drop(root.try_read::<Node<Value>>()?);
                }
                match &*roots[0].try_read::<Node<Value>>()? {
                    Node::Leaf(rows) => assert_eq!(rows, &before),
                    _ => panic!("small primary leaf"),
                }
                drop(blocked);
            } else {
                drop(blocked);
                tokio::time::timeout(std::time::Duration::from_secs(2), mutation)
                    .await
                    .expect("mutation must finish after release")?;
            }
            drop(owner);
            drop(table.try_write()?);
            table.validate().await?;
            drop(roots);
            drop(table);
            drop(dir);
            fs::remove_dir_all(path).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn auxiliary_error_releases_pending_sibling_before_primary() -> io::Result<()> {
    const ROOT: &str = "00000000-0000-0000-0000-000000000000";
    let path = setup_tmp_dir().await?;
    let schema = TableSchema::new(
        vec!["up", "up_name", "down", "down_name"],
        [
            ("left".into(), vec!["up_name", "up"]),
            ("right".into(), vec!["down", "up"]),
        ],
    );
    let dir = Cache::<File>::new(
        1024 * BLOCK_SIZE,
        None,
        0,
        std::time::Duration::from_secs(3),
    )
    .load(path.clone())?;
    let table = TableLock::create(schema, Collator::new(), dir.clone()).await?;
    table
        .write()
        .await
        .upsert(
            vec![1.into()],
            vec![
                "old".to_string().into(),
                2.into(),
                "note".to_string().into(),
            ],
        )
        .await?;
    let left = dir.read().await.get_dir("left").unwrap().clone();
    let right = dir.read().await.get_dir("right").unwrap().clone();
    let left = left.read().await.get_file(ROOT).unwrap().clone();
    let right = right.read().await.get_file(ROOT).unwrap().clone();
    let blocked = left.read::<Node<Value>>().await?;
    let mut broken = right.write::<Node<Value>>(0).await?;
    *broken = Node::Leaf(Vec::new());
    let mut owner = table.write().await;
    let key = [1.into()];
    let mut deletion = Box::pin(owner.delete_row(&key));
    assert!(futures::poll!(&mut deletion).is_pending());
    drop(broken);
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), deletion)
        .await
        .expect("index error must cancel blocked siblings")
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    drop(left.try_read::<Node<Value>>()?);
    assert!(owner.get_row(&[1.into()]).await?.is_some());
    drop(blocked);
    drop(owner);
    drop(table.try_write()?);
    drop(left);
    drop(right);
    drop(table);
    drop(dir);
    fs::remove_dir_all(path).await
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
#[derive(Debug, Eq, PartialEq)]
#[cfg_attr(not(test), derive(Clone))]
enum Value {
    Number(number_general::Number),
    String(String),
}

#[cfg(test)]
tokio::task_local! {
    static INDEXED_CLONES: std::cell::Cell<usize>;
}

#[cfg(test)]
impl Clone for Value {
    fn clone(&self) -> Self {
        match self {
            Self::Number(value) => Self::Number(*value),
            Self::String(value) => {
                if value == "indexed" {
                    let _ = INDEXED_CLONES.try_with(|count| count.set(count.get() + 1));
                }

                Self::String(value.clone())
            }
        }
    }
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

// Timing entrypoint; use the shared fensor comparison runner, separately from correctness tests.
#[test]
#[ignore = "manual multi-index performance measurement"]
fn benchmark_index_mutations() -> io::Result<()> {
    let workers = std::env::var("RAYON_NUM_THREADS")
        .unwrap_or_else(|_| "1".into())
        .parse::<usize>()
        .unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?
        .block_on(async {
            for indexes in [0, 1, 4, 8] {
                let schema = || {
                    TableSchema::new(
                        vec!["up", "up_name", "down", "down_name"],
                        (0..indexes).map(|i| (format!("index{i}"), vec!["up_name", "up"])),
                    )
                };
                let values = |name: &str| {
                    vec![
                        name.to_string().into(),
                        2.into(),
                        "payload".to_string().into(),
                    ]
                };
                let path = setup_tmp_dir().await?;
                let cache = || {
                    Cache::<File>::new(64 * 1024 * 1024, None, 0, std::time::Duration::from_secs(3))
                };
                let table =
                    TableLock::create(schema(), Collator::new(), cache().load(path.clone())?)
                        .await?;
                let emit = |operation, temperature, elapsed: std::time::Duration| {
                    println!(
                        "TABLE,indexes_{indexes},{operation},{temperature},wall,ns,{}",
                        elapsed.as_nanos()
                    );
                };

                let start = std::time::Instant::now();
                assert_eq!(
                    table
                        .write()
                        .await
                        .upsert_sorted(futures::stream::iter(
                            (0..128).map(|key| Ok((vec![key.into()], values("initial")))),
                        ))
                        .await?,
                    128
                );
                emit("ordered_insert", "new", start.elapsed());
                table.validate().await?;

                let start = std::time::Instant::now();
                {
                    let mut table = table.write().await;
                    for key in 0..128 {
                        assert!(!table.upsert(vec![key.into()], values("warm")).await?);
                    }
                }
                emit("replace", "resident", start.elapsed());
                table.validate().await?;
                table.sync_all().await?;
                drop(table);

                let table =
                    TableLock::load(schema(), Collator::new(), cache().load(path.clone())?)?;
                let start = std::time::Instant::now();
                {
                    let mut table = table.write().await;
                    for key in 0..128 {
                        assert!(!table.upsert(vec![key.into()], values("cold")).await?);
                    }
                }
                emit("replace", "cold_start", start.elapsed());
                table.validate().await?;

                let start = std::time::Instant::now();
                {
                    let mut table = table.write().await;
                    for key in 0..128 {
                        assert!(table.delete_row(&[key.into()]).await?);
                    }
                }
                emit("delete", "resident", start.elapsed());
                table.validate().await?;
                drop(table);
                fs::remove_dir_all(path).await?;
            }
            Ok(())
        })
}
