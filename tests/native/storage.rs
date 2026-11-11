use super::*;

#[tokio::test]
async fn index_names_reject_before_storage_access() -> io::Result<()> {
    fn schema(case: usize) -> TableSchema {
        let indices = match case {
            0 => vec![("aux".into(), vec!["down", "up"])],
            1 => vec![("primary".into(), vec!["up", "up_name", "down", "down_name"])],
            2 => vec![
                ("aux".into(), vec!["down", "up"]),
                ("aux".into(), vec!["down", "up"]),
            ],
            3 => vec![
                ("aux".into(), vec!["down", "up"]),
                ("aux".into(), vec!["up", "down"]),
            ],
            _ => unreachable!(),
        };
        TableSchema::new(vec!["up", "up_name", "down", "down_name"], indices)
    }

    for case in 1..=3 {
        let fixture = TestDir::new("invalid_index_names")?;
        {
            let cache = Cache::<File>::new(
                1024 * BLOCK_SIZE,
                Some(1),
                0,
                std::time::Duration::from_secs(3),
            );
            let dir = cache.load(fixture.path().to_owned())?;
            {
                let held = dir.write().await;
                let error = TableLock::create(schema(case), Collator::new(), dir.clone())
                    .now_or_never()
                    .expect("invalid index names waited for storage")
                    .err()
                    .unwrap();
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                let error = TableLock::load(schema(case), Collator::new(), dir.clone())
                    .err()
                    .unwrap();
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                assert!(held.is_empty());
            }
            let table = TableLock::create(schema(0), Collator::new(), dir.clone()).await?;
            table
                .write()
                .await
                .upsert(
                    vec![1.into()],
                    vec![
                        "name".to_string().into(),
                        2.into(),
                        "note".to_string().into(),
                    ],
                )
                .await?;
            table.sync_all().await?;
            {
                let held = dir.write().await;
                let names: Vec<_> = held.names().cloned().collect();
                let error = TableLock::load(schema(case), Collator::new(), dir.clone())
                    .err()
                    .unwrap();
                assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                assert_eq!(held.names().cloned().collect::<Vec<_>>(), names);
            }
            drop(table.try_write()?);
            table.validate().boxed().await?;
        }
        {
            let dir = Cache::<File>::new(
                1024 * BLOCK_SIZE,
                Some(1),
                0,
                std::time::Duration::from_secs(3),
            )
            .load(fixture.path().to_owned())?;
            let table = TableLock::load(schema(0), Collator::new(), dir)?;
            table.validate().boxed().await?;
            assert_eq!(
                table
                    .read()
                    .await
                    .get_row(&[1.into()])
                    .await?
                    .unwrap()
                    .as_slice(),
                &[
                    1.into(),
                    "name".to_string().into(),
                    2.into(),
                    "note".to_string().into()
                ]
            );
        }
        fixture.close()?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn strict_validation_checks_primary_keys_across_leaves_and_ties() -> io::Result<()> {
    #[derive(Clone, Eq, PartialEq)]
    struct TiedNumbers;

    impl Collate for TiedNumbers {
        type Value = Value;

        fn cmp(&self, left: &Value, right: &Value) -> Ordering {
            match (left, right) {
                (Value::Number(_), Value::Number(_)) => Ordering::Equal,
                _ => Collator::new().cmp(left, right),
            }
        }
    }

    fn schema() -> TableSchema {
        TableSchema::new(
            vec!["up", "up_name", "down", "down_name"],
            [("name".into(), vec!["up_name", "up"])],
        )
    }

    for count in [3, 40] {
        // Both corruption cases have distinct complete rows and consistent auxiliary data.
        for duplicate in [None, Some(1), Some(count - 1)] {
            let fixture = TestDir::new("primary_key_validation")?;
            {
                let dir = Cache::<File>::new(
                    1024 * BLOCK_SIZE,
                    None,
                    0,
                    std::time::Duration::from_secs(3),
                )
                .load(fixture.path().to_owned())?;
                let table = TableLock::create(schema(), TiedNumbers, dir.clone()).await?;
                let primary = dir.read().await.get_dir("primary").unwrap().clone();
                let primary = b_tree::BTreeLock::load(
                    table.schema().primary().clone(),
                    TiedNumbers,
                    primary,
                )?;
                let (name, index_schema) = &table.schema().auxiliary()[0];
                let auxiliary = dir.read().await.get_dir(name).unwrap().clone();
                let auxiliary =
                    b_tree::BTreeLock::load(index_schema.clone(), TiedNumbers, auxiliary)?;
                for position in 0..count {
                    let key = if duplicate == Some(position) {
                        0
                    } else {
                        position
                    };
                    let name = Value::from(format!("row-{position:04}"));
                    // Native index writes deliberately bypass Table's unique-key invariant.
                    primary
                        .write()
                        .await
                        .insert(vec![
                            key.into(),
                            name.clone(),
                            0.into(),
                            "note".to_string().into(),
                        ])
                        .await?;
                    auxiliary
                        .write()
                        .await
                        .insert(vec![name, key.into()])
                        .await?;
                }
                primary.validate().await?;
                auxiliary.validate().await?;
                table.sync_all().await?;
                if duplicate.is_some() {
                    let error = table.validate().boxed().await.unwrap_err();
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                    assert!(freqfs::IntegrityError::is_marked(&error));
                } else {
                    table.validate().boxed().await?;
                    assert_eq!(
                        table.read().await.count(Range::default()).await?,
                        count as u64
                    );
                }
                drop(table.try_write()?);
            }
            {
                let dir = Cache::<File>::new(
                    1024 * BLOCK_SIZE,
                    None,
                    0,
                    std::time::Duration::from_secs(3),
                )
                .load(fixture.path().to_owned())?;
                let table = TableLock::load(schema(), TiedNumbers, dir)?;
                if duplicate.is_some() {
                    let error = table.validate().boxed().await.unwrap_err();
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                    assert!(freqfs::IntegrityError::is_marked(&error));
                } else {
                    table.validate().boxed().await?;
                }
            }
            fixture.close()?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn index_validation_clones_only_required_fields() -> io::Result<()> {
    use super::observations::INDEXED_CLONES;

    for selected in [false, true] {
        let fixture = TestDir::new("validation_projection")?;
        {
            let schema = TableSchema::new(
                vec!["up", "up_name", "down", "down_name"],
                [(
                    "aux".into(),
                    if selected {
                        vec!["up_name", "up"]
                    } else {
                        vec!["down", "up"]
                    },
                )],
            );
            let cache = Cache::<File>::new(
                1024 * BLOCK_SIZE,
                None,
                0,
                std::time::Duration::from_secs(3),
            );
            let dir = cache.load(fixture.path().to_owned())?;
            let table = TableLock::create(schema, Collator::new(), dir.clone()).await?;
            for key in 0..20 {
                table
                    .write()
                    .await
                    .upsert(
                        vec![key.into()],
                        vec![
                            "indexed".to_string().into(),
                            (key + 100).into(),
                            "note".to_string().into(),
                        ],
                    )
                    .await?;
            }

            INDEXED_CLONES
                .scope(std::cell::Cell::new(0), async {
                    // Strict schema validation owns its input rows. Measure those
                    // required clones separately from the cross-index consistency pass.
                    let primary = dir.read().await.get_dir("primary").unwrap().clone();
                    b_tree::BTreeLock::load(
                        table.schema().primary().clone(),
                        Collator::new(),
                        primary,
                    )?
                    .validate()
                    .await?;
                    for (name, schema) in table.schema().auxiliary() {
                        let native = dir.read().await.get_dir(name).unwrap().clone();
                        b_tree::BTreeLock::load(schema.clone(), Collator::new(), native)?
                            .validate()
                            .await?;
                    }
                    let strict_clones = INDEXED_CLONES.with(std::cell::Cell::get);
                    INDEXED_CLONES.with(|count| count.set(0));

                    // One owned primary scan; only an index which selects the marker
                    // needs another copy. Auxiliary cardinality scans borrow their rows.
                    table.validate().boxed().await?;
                    assert_eq!(
                        INDEXED_CLONES.with(std::cell::Cell::get),
                        strict_clones + 20 * if selected { 2 } else { 1 }
                    );
                    Ok::<_, io::Error>(())
                })
                .await?;
            table.sync_all().await?;
        }
        fixture.close()?;
    }
    Ok(())
}

#[tokio::test]
async fn variable_width_separators_rebalance_and_reopen() -> Result<(), io::Error> {
    let fixture = TestDir::new("test_table")?;
    let path = fixture.path().to_owned();

    {
        let key = |index: usize| {
            Value::String(format!(
                "{index:04}-{}",
                "x".repeat(if index.is_multiple_of(2) { 8 } else { 512 })
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
            // Deleting alternating keys exercises variable-width separator boundaries.
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
    }

    fixture.close()
}

#[tokio::test]
async fn cancelled_index_construction_is_not_loadable() -> io::Result<()> {
    let schema = || {
        TableSchema::new(
            vec!["up", "up_name", "down", "down_name"],
            [("down".into(), vec!["down", "up"])],
        )
    };
    let fixture = TestDir::new("test_table")?;
    let path = fixture.path().to_owned();

    {
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
            .create_file("pinned".into(), File::Node(Node::Leaf(vec![])))
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
    }

    fixture.close()?;
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
    let fixture = TestDir::new("test_table")?;
    let path = fixture.path().to_owned();

    {
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
    }

    fixture.close()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn in_place_indexes_reopen_and_reject_inconsistency() -> io::Result<()> {
    fn schema() -> TableSchema {
        TableSchema::new(
            vec!["up", "up_name", "down", "down_name"],
            [
                ("down".into(), vec!["down", "up"]),
                ("name".into(), vec!["up_name", "up"]),
            ],
        )
    }

    async fn ingest(table: &TableLock<TableSchema, IndexSchema, Collator, File>) -> io::Result<()> {
        let mut writer = table.write().await;
        for key in 0..40 {
            let row = vec![
                "up".to_string().into(),
                key.into(),
                "down".to_string().into(),
            ];
            assert!(writer.upsert(vec![key.into()], row.clone()).await?);
            assert!(!writer.upsert(vec![key.into()], row).await?);
        }
        assert!(
            writer
                .upsert(
                    vec![40.into()],
                    vec![
                        "up".to_string().into(),
                        40.into(),
                        "down".to_string().into(),
                    ]
                )
                .await?
        );
        let error = writer.upsert(vec![41.into()], vec![]).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(writer.get_row(&[41.into()]).await?.is_none());
        Ok(())
    }

    async fn mutate(table: &TableLock<TableSchema, IndexSchema, Collator, File>) -> io::Result<()> {
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
        Ok(())
    }

    async fn reopen(path: &std::path::Path) -> io::Result<()> {
        let reopened = Cache::<File>::new(
            1024 * BLOCK_SIZE,
            None,
            0,
            std::time::Duration::from_secs(3),
        )
        .load(path.to_owned())?;
        let table = TableLock::load(schema(), Collator::new(), reopened.clone())?;
        table.validate().await?;
        assert_eq!(table.read().await.count(Range::default()).await?, 11);
        let auxiliary = reopened.read().await.get_dir("down").unwrap().clone();
        let name = auxiliary.read().await.iter().next().unwrap().0.clone();
        *auxiliary
            .write()
            .await
            .write_file::<Node<Value>>(&name)
            .await? = Node::Leaf(vec![]);
        assert!(table.validate().await.is_err());
        *auxiliary
            .write()
            .await
            .write_file::<Node<Value>>("00000000-0000-0000-0000-000000000000")
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
        Ok(())
    }

    let fixture = TestDir::new("test_table")?;
    let path = fixture.path().to_owned();
    // Exercise a normal worker stack, not the test entry thread's stack.
    tokio::spawn(async move {
        let cache = Cache::<File>::new(
            1024 * BLOCK_SIZE,
            None,
            0,
            std::time::Duration::from_secs(3),
        );
        let table = TableLock::create(schema(), Collator::new(), cache.load(path.clone())?).await?;
        Box::pin(ingest(&table)).await?;
        Box::pin(mutate(&table)).await?;
        table.sync_all().await?;
        drop(table);
        Box::pin(reopen(&path)).await?;
        Ok::<_, io::Error>(())
    })
    .await
    .expect("lifecycle worker")?;
    fixture.close()
}
