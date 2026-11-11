use super::*;

#[tokio::test]
async fn semantic_equal_auxiliary_entries_are_replaced() -> io::Result<()> {
    let fixture = TestDir::new("test_table")?;
    let path = fixture.path().to_owned();

    {
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
                // The old row and both auxiliary replacement arguments are owned.
                // Semantic equality must not suppress the supplied representation.
                assert_eq!(INDEXED_CLONES.with(std::cell::Cell::get), 3);
                INDEXED_CLONES.with(|count| count.set(0));
                assert!(
                    !table
                        .write()
                        .await
                        .upsert(
                            vec![1.into()],
                            vec![
                                "indexed".to_string().into(),
                                2.into(),
                                "updated".to_string().into(),
                            ]
                        )
                        .await?
                );
                assert_eq!(INDEXED_CLONES.with(std::cell::Cell::get), 3);
                Ok::<_, io::Error>(())
            })
            .await?;

        table.validate().await?;

        drop(table);
    }

    fixture.close()
}

#[tokio::test]
async fn first_emptiness_and_deletion_share_the_row_predicate() -> io::Result<()> {
    let fixture = TestDir::new("table_plan_predicate")?;
    let schema = TableSchema::new(
        vec!["up", "up_name", "down", "down_name"],
        [("by_name".into(), vec!["up_name", "up", "down", "down_name"])],
    );
    let dir = Cache::<File>::new(
        1024 * BLOCK_SIZE,
        None,
        0,
        std::time::Duration::from_secs(3),
    )
    .load(fixture.path().to_owned())?;
    let table = TableLock::create(schema, Collator::new(), dir.clone()).await?;
    {
        let mut writer = table.write().await;
        writer
            .upsert(
                vec![1.into()],
                vec!["a".to_string().into(), 1.into(), "x".to_string().into()],
            )
            .await?;
        writer
            .upsert(
                vec![2.into()],
                vec!["b".to_string().into(), 9.into(), "y".to_string().into()],
            )
            .await?;
    }
    let range = Range::from_iter([
        ("up".to_string(), (Value::from(1)..Value::from(3)).into()),
        (
            "up_name".to_string(),
            b_table::ColumnRange::Eq("absent".to_string().into()),
        ),
    ]);
    {
        let read = table.read().await;
        assert!(
            read.rows(range.clone(), &[], false, None)
                .await?
                .try_next()
                .await?
                .is_none()
        );
        assert!(read.first(range.clone(), &[], None).await?.is_none());
        assert!(read.is_empty(range.clone()).await?);
        let multi = Range::from_iter([
            (
                "up_name".to_string(),
                b_table::ColumnRange::from(
                    Value::from("a".to_string())..Value::from("z".to_string()),
                ),
            ),
            (
                "down".to_string(),
                b_table::ColumnRange::from(Value::from(0)..Value::from(10)),
            ),
        ]);
        let supported = Range::from_iter([(
            "up_name".to_string(),
            b_table::ColumnRange::from(Value::from("a".to_string())..Value::from("z".to_string())),
        )]);
        let order: Vec<_> = ["up_name", "up"].into_iter().map(String::from).collect();
        let expected_row = |key| {
            if key == 1 {
                vec![
                    1.into(),
                    "a".to_string().into(),
                    1.into(),
                    "x".to_string().into(),
                ]
            } else {
                vec![
                    2.into(),
                    "b".to_string().into(),
                    9.into(),
                    "y".to_string().into(),
                ]
            }
        };
        for reverse in [false, true] {
            let mut rows = read.rows(supported.clone(), &order, reverse, None).await?;
            let expected = if reverse { [2, 1] } else { [1, 2] };
            for key in expected {
                assert_eq!(
                    rows.try_next().await?.unwrap().as_slice(),
                    expected_row(key).as_slice()
                );
            }
            assert!(rows.try_next().await?.is_none());
        }
        assert_eq!(
            read.first(supported.clone(), &order, None)
                .await?
                .unwrap()
                .as_slice(),
            &[1.into()]
        );
        assert_eq!(read.count(supported.clone()).await?, 2);
        assert!(!read.is_empty(supported.clone()).await?);
        let mut counted = multi.clone().into_inner();
        counted.insert(
            "up".to_string(),
            b_table::ColumnRange::from(Value::from(0)..Value::from(3)),
        );
        let counted = Range::from_iter(counted);
        for rejected in [multi, counted] {
            for reverse in [false, true] {
                assert_eq!(
                    read.rows(rejected.clone(), &[], reverse, None)
                        .await
                        .err()
                        .unwrap()
                        .kind(),
                    io::ErrorKind::Unsupported
                );
            }
            assert_eq!(
                read.first(rejected.clone(), &[], None)
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::Unsupported
            );
            assert_eq!(
                read.count(rejected.clone()).await.unwrap_err().kind(),
                io::ErrorKind::Unsupported
            );
            assert_eq!(
                read.is_empty(rejected).await.unwrap_err().kind(),
                io::ErrorKind::Unsupported
            );
        }
        for select in [vec![], vec!["missing".to_string()]] {
            assert_eq!(
                read.rows(supported.clone(), &["up".into()], false, Some(&select))
                    .await
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::Unsupported
            );
            assert_eq!(
                read.first(supported.clone(), &["up".into()], Some(&select))
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::Unsupported
            );
        }
        assert_eq!(
            read.rows(supported, &["up".into()], false, None)
                .await
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::Unsupported
        );
    }
    let rejected = Range::from_iter([
        (
            "up_name".into(),
            b_table::ColumnRange::from(Value::from("a".to_string())..Value::from("z".to_string())),
        ),
        (
            "down".into(),
            b_table::ColumnRange::from(Value::from(0)..Value::from(10)),
        ),
    ]);
    assert_eq!(
        table
            .write()
            .await
            .delete_range(rejected)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(table.write().await.delete_range(range).await?, 0);
    assert_eq!(table.read().await.count(Range::default()).await?, 2);
    {
        let read = table.read().await;
        let mut rows = read.rows(Range::default(), &[], false, None).await?;
        drop(read);
        assert!(rows.try_next().await?.is_some());
        drop(rows);
        drop(table.try_write()?);
    }
    {
        let primary = dir.read().await.get_dir("primary").unwrap().clone();
        let file = primary.read().await.files().next().unwrap().clone();
        let mut node = file.write::<Node<Value>>().await?;
        let Node::Leaf(rows) = &mut *node else {
            panic!("expected a root leaf")
        };
        rows[0].pop();
    }
    let read = table.read().await;
    for error in [
        read.count(Range::default()).await.unwrap_err(),
        read.is_empty(Range::default()).await.unwrap_err(),
    ] {
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(freqfs::IntegrityError::is_marked(&error));
    }
    drop(read);
    drop(table);
    drop(dir);
    fixture.close()
}

#[tokio::test]
async fn upsert_validates_before_mutation_and_reopens() -> io::Result<()> {
    let fixture = TestDir::new("test_table")?;
    let path = fixture.path().to_owned();

    {
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
            let error = table.upsert(vec![], values()).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(!table.delete_row(&[1.into()]).await?);
            assert!(table.get_row(&[1.into()]).await?.is_none());
            assert!(table.upsert(vec![1.into()], values()).await?);
            let original = table.get_row(&[1.into()]).await?.unwrap();
            let error = table.upsert(vec![1.into()], vec![]).await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(table.get_row(&[1.into()]).await?.unwrap(), original);
            assert!(!table.upsert(vec![1.into()], values()).await?);
            assert_eq!(table.get_row(&[1.into()]).await?.unwrap(), original);
            assert!(table.delete_row(&[1.into()]).await?);
            assert!(table.get_row(&[1.into()]).await?.is_none());
            assert!(table.upsert(vec![2.into()], values()).await?);
        }
        drop(table.try_write()?);
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
    }

    fixture.close()
}

#[tokio::test]
async fn range_deletion_uses_reordered_covering_rows() -> io::Result<()> {
    let schema = || {
        TableSchema::new(
            vec!["up", "up_name", "down", "down_name"],
            [("cover".into(), vec!["down", "down_name", "up_name", "up"])],
        )
    };
    let fixture = TestDir::new("test_table")?;
    let path = fixture.path().to_owned();

    {
        let cache = Cache::<File>::new(
            1024 * BLOCK_SIZE,
            None,
            0,
            std::time::Duration::from_secs(3),
        );
        let table = TableLock::create(schema(), Collator::new(), cache.load(path.clone())?).await?;
        {
            let mut writer = table.write().await;
            for key in 0..40 {
                assert!(
                    writer
                        .upsert(
                            vec![key.into()],
                            vec![
                                "name".to_string().into(),
                                (key + 100).into(),
                                "note".to_string().into(),
                            ]
                        )
                        .await?
                );
            }
        }

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
    }

    fixture.close()
}

#[tokio::test]
async fn equivalent_primary_keys_remain_distinct_and_preserve_partial_failure() -> io::Result<()> {
    use number_general::NumberInstance;

    #[derive(Clone, Eq, PartialEq)]
    struct Magnitude;

    impl Collate for Magnitude {
        type Value = Value;

        fn cmp(&self, a: &Value, b: &Value) -> Ordering {
            match (a, b) {
                (Value::Number(a), Value::Number(b)) => {
                    NumberCollator::default().cmp(&a.abs(), &b.abs())
                }
                (a, b) => Collator::new().cmp(a, b),
            }
        }
    }

    let fixture = TestDir::new("test_table")?;
    let path = fixture.path().to_owned();

    {
        let dir = Cache::<File>::new(
            1024 * BLOCK_SIZE,
            None,
            0,
            std::time::Duration::from_secs(3),
        )
        .load(path.clone())?;
        let schema = TableSchema::new(
            vec!["up", "up_name", "down", "down_name"],
            [("name".into(), vec!["up_name", "up"])],
        );
        let table = TableLock::create(schema, Magnitude, dir.clone()).await?;
        let values =
            |name: &str| vec![name.to_string().into(), 2.into(), "note".to_string().into()];

        for (key, name) in [(1, "a"), (-1, "c")] {
            assert!(
                table
                    .write()
                    .await
                    .upsert(vec![key.into()], values(name))
                    .await?
            );
        }

        for (key, name) in [(1, "a"), (-1, "c")] {
            assert!(table.read().await.contains(&[key.into()]).await?);
            assert_eq!(
                table
                    .read()
                    .await
                    .get_value(&[key.into()])
                    .await?
                    .unwrap()
                    .as_slice(),
                values(name)
            );
        }
        table.validate().await?;

        let range = Range::from_iter([(
            "up_name".to_string(),
            b_table::ColumnRange::Eq("c".to_string().into()),
        )]);
        let columns = ["up", "up_name", "down", "down_name"].map(str::to_string);
        let mut expected: Vec<Value> = vec![(-1).into()];
        expected.extend(values("c"));
        {
            let read = table.read().await;
            for reverse in [false, true] {
                let rows = read
                    .rows(range.clone(), &[], reverse, None)
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?;
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].as_slice(), expected.as_slice());
            }
            assert_eq!(
                read.first(range.clone(), &[], Some(&columns))
                    .await?
                    .unwrap()
                    .as_slice(),
                expected.as_slice()
            );
        }
        assert_eq!(table.write().await.delete_range(range).await?, 1);
        assert!(
            table
                .read()
                .await
                .get_value(&[(-1).into()])
                .await?
                .is_none()
        );
        assert_eq!(
            table
                .read()
                .await
                .get_value(&[1.into()])
                .await?
                .unwrap()
                .as_slice(),
            values("a")
        );
        table.validate().await?;
        assert!(
            table
                .write()
                .await
                .upsert(vec![(-1).into()], values("c"))
                .await?
        );

        let error = table
            .write()
            .await
            .upsert(vec![1.into()], values("z"))
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
            values("a")
        );

        // Auxiliary publication precedes the rejected relocation of the primary row.
        let auxiliary = dir.read().await.get_dir("name").unwrap().clone();
        let root = auxiliary
            .read()
            .await
            .read_file::<Node<Value>>("00000000-0000-0000-0000-000000000000")
            .await?
            .clone();
        let Node::Leaf(rows) = root else {
            panic!("small auxiliary leaf")
        };
        assert_eq!(
            rows,
            [
                vec!["c".to_string().into(), (-1).into()],
                vec!["z".to_string().into(), 1.into()]
            ]
        );
        assert!(table.validate().await.is_err());

        drop(table);
        drop(auxiliary);
        drop(dir);
    }

    fixture.close()
}

#[tokio::test]
async fn ambiguous_primary_keys_fail_before_auxiliary_changes() -> io::Result<()> {
    let fixture = TestDir::new("test_table")?;
    let path = fixture.path().to_owned();

    {
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
        let values =
            |name: &str| vec![name.to_string().into(), 2.into(), "note".to_string().into()];
        // Corrupt the native fixture directly: ordinary Table upsert preserves
        // primary-key uniqueness and cannot construct this ambiguous storage.
        const ROOT: &str = "00000000-0000-0000-0000-000000000000";
        let primary = dir.read().await.get_dir("primary").unwrap().clone();
        let primary_root = primary.read().await.get_file(ROOT).unwrap().clone();
        *primary_root.write::<Node<Value>>().await? = Node::Leaf(
            ["a", "c"]
                .map(|name| {
                    let mut row = vec![1.into()];
                    row.extend(values(name));
                    row
                })
                .into(),
        );
        let auxiliary = dir.read().await.get_dir("name").unwrap().clone();
        let auxiliary_root = auxiliary.read().await.get_file(ROOT).unwrap().clone();
        *auxiliary_root.write::<Node<Value>>().await? = Node::Leaf(
            ["a", "c"]
                .map(|name| vec![name.to_string().into(), 1.into()])
                .into(),
        );
        let error = table
            .write()
            .await
            .upsert(vec![1.into()], values("b"))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            table
                .read()
                .await
                .get_row(&[1.into()])
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert!(freqfs::IntegrityError::is_marked(
            &table.read().await.get_row(&[1.into()]).await.unwrap_err()
        ));
        let range = Range::from_iter([(
            "up_name".to_string(),
            b_table::ColumnRange::Eq("a".to_string().into()),
        )]);
        for reverse in [false, true] {
            let reader = table.read().await;
            let mut rows = reader.rows(range.clone(), &[], reverse, None).await?;
            let error = rows.try_next().await.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(freqfs::IntegrityError::is_marked(&error));
            drop(rows);
            drop(reader);
            drop(table.try_write()?);
        }
        assert_eq!(table.read().await.count(Range::default()).await?, 2);

        // Ambiguous primary resolution fails before any auxiliary update.
        let auxiliary = dir.read().await.get_dir("name").unwrap().clone();
        let root = auxiliary
            .read()
            .await
            .read_file::<Node<Value>>("00000000-0000-0000-0000-000000000000")
            .await?
            .clone();
        let Node::Leaf(rows) = root else {
            panic!("small auxiliary leaf")
        };
        assert_eq!(
            rows,
            [
                vec!["a".to_string().into(), 1.into()],
                vec!["c".to_string().into(), 1.into()],
            ]
        );
        let error = table.validate().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(freqfs::IntegrityError::is_marked(&error));

        drop(table);
        drop(auxiliary);
        drop(dir);
    }

    fixture.close()
}

#[tokio::test]
async fn selected_columns_avoid_unselected_payload_clones() -> io::Result<()> {
    let fixture = TestDir::new("test_table_projection")?;
    {
        let schema = TableSchema::new(
            vec!["up", "up_name", "down", "down_name"],
            [
                ("down".into(), vec!["down", "up", "up_name", "down_name"]),
                ("name".into(), vec!["up_name", "up"]),
            ],
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

        super::observations::VALUE_CLONES
            .scope(std::cell::Cell::new(0), async {
                let reader = table.read().await;
                assert_eq!(
                    reader
                        .count(Range::default())
                        .await
                        .expect("zero-field table count"),
                    20
                );
                assert!(
                    !reader
                        .is_empty(Range::default())
                        .await
                        .expect("zero-field table emptiness")
                );
                assert_eq!(
                    super::observations::VALUE_CLONES.with(std::cell::Cell::get),
                    0
                );
                assert!(
                    reader
                        .rows(Range::default(), &[], false, Some(&[]))
                        .await
                        .is_err()
                );
                Ok::<_, io::Error>(())
            })
            .await?;

        // Exercise the native index with the same clone-observed values.
        let (name, schema) = &table.schema().auxiliary()[1];
        let native = dir.read().await.get_dir(name).unwrap().clone();
        let index = b_tree::BTreeLock::load(schema.clone(), Collator::new(), native)?;
        super::observations::VALUE_CLONES
            .scope(std::cell::Cell::new(0), async {
                let read = index.read().await;
                assert!(
                    !read
                        .is_empty(b_tree::Range::default())
                        .await
                        .expect("native index emptiness")
                );
                assert_eq!(
                    super::observations::VALUE_CLONES.with(std::cell::Cell::get),
                    0
                );
                assert!(
                    read.first(b_tree::Range::<Value>::default())
                        .await?
                        .is_some()
                );
                assert_eq!(
                    super::observations::VALUE_CLONES.with(std::cell::Cell::get),
                    2
                );
                assert!(
                    read.last(b_tree::Range::<Value>::default())
                        .await?
                        .is_some()
                );
                assert_eq!(
                    super::observations::VALUE_CLONES.with(std::cell::Cell::get),
                    4
                );
                Ok::<_, io::Error>(())
            })
            .await?;
        drop(index);

        INDEXED_CLONES
            .scope(std::cell::Cell::new(0), async {
                for order in [vec![], vec!["down".to_string()]] {
                    for reverse in [false, true] {
                        let selection = ["down".to_string(), "up".to_string(), "up".to_string()];
                        let reader = table.read().await;
                        let mut rows = reader
                            .rows(Range::default(), &order, reverse, Some(&selection))
                            .await?;
                        let mut expected: Vec<_> = (0..20).collect();
                        if reverse {
                            expected.reverse();
                        }
                        for key in expected {
                            assert_eq!(
                                rows.try_next().await?.unwrap().as_slice(),
                                &[(key + 100).into(), key.into(), key.into()]
                            );
                        }
                        assert!(rows.try_next().await?.is_none());
                    }
                }
                assert_eq!(INDEXED_CLONES.with(std::cell::Cell::get), 0);

                let reader = table.read().await;
                let selection = ["up".to_string()];
                let mut rows = reader
                    .rows(Range::default(), &[], false, Some(&selection))
                    .await?;
                assert_eq!(rows.try_next().await?.unwrap().as_slice(), &[0.into()]);
                drop(reader);
                assert!(table.try_write().is_err());
                drop(rows);
                drop(table.try_write()?);
                assert_eq!(INDEXED_CLONES.with(std::cell::Cell::get), 0);
                let reader = table.read().await;
                for order in [
                    vec![],
                    vec!["down".to_string()],
                    vec!["up_name".to_string()],
                ] {
                    for reverse in [false, true] {
                        let selection = [
                            "up_name".to_string(),
                            "down".to_string(),
                            "up_name".to_string(),
                            "up".to_string(),
                        ];
                        INDEXED_CLONES.with(|count| count.set(0));
                        let mut rows = reader
                            .rows(Range::default(), &order, reverse, Some(&selection))
                            .await?;
                        let mut expected: Vec<_> = (0..20).collect();
                        if reverse {
                            expected.reverse();
                        }
                        for key in expected {
                            assert_eq!(
                                rows.try_next().await?.unwrap().as_slice(),
                                &[
                                    "indexed".to_string().into(),
                                    (key + 100).into(),
                                    "indexed".to_string().into(),
                                    key.into(),
                                ]
                            );
                        }
                        assert!(rows.try_next().await?.is_none());
                        // One page-to-owned copy and one duplicate-output copy per row.
                        assert_eq!(INDEXED_CLONES.with(std::cell::Cell::get), 40);
                    }
                }

                for selection in [vec![], vec!["missing".to_string()]] {
                    let error = reader
                        .rows(Range::default(), &[], false, Some(&selection))
                        .await
                        .err()
                        .expect("invalid column selection");
                    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
                }

                // Filtering and ordering must be supported by the same literal index prefix.
                let range = Range::from_iter([(
                    "up_name".to_string(),
                    b_table::ColumnRange::Eq("indexed".to_string().into()),
                )]);
                assert_eq!(
                    reader
                        .rows(range.clone(), &["up".into()], false, None)
                        .await
                        .err()
                        .unwrap()
                        .kind(),
                    io::ErrorKind::Unsupported
                );
                let order = ["up_name".into(), "up".into()];
                let mut owned = reader.rows(range, &order, false, None).await?;
                let mut expected = Vec::new();
                while let Some(row) = owned.try_next().await? {
                    expected.push(row[0].clone());
                }
                assert_eq!(expected, (0..20).map(Value::from).collect::<Vec<_>>());
                drop(owned);
                drop(reader);

                let primary = dir.read().await.get_dir("primary").unwrap().clone();
                let root = primary
                    .read()
                    .await
                    .get_file("00000000-0000-0000-0000-000000000000")
                    .unwrap()
                    .clone();
                let blocked = root.write::<Node<Value>>().await?;
                let reader = table.read().await;
                let mut visiting = Box::pin(async {
                    let mut rows = reader.rows(Range::default(), &[], false, None).await?;
                    rows.try_next().await
                });
                assert!(futures::poll!(&mut visiting).is_pending());
                drop(visiting);
                drop(reader);
                drop(blocked);
                drop(root.try_write::<Node<Value>>()?);
                drop(table.try_write()?);

                let reader = table.read().await;
                drop(reader.rows(Range::default(), &[], false, None));
                drop(reader);
                drop(table.try_write()?);

                // A valid auxiliary row must not turn a missing primary row into a panic.
                *root.write::<Node<Value>>().await? = Node::Leaf(vec![]);
                let range = Range::from_iter([(
                    "up_name".to_string(),
                    b_table::ColumnRange::Eq("indexed".to_string().into()),
                )]);
                let order = ["up_name".to_string()];
                let columns = ["up", "up_name", "down", "down_name"].map(str::to_string);
                let reader = table.read().await;
                for reverse in [false, true] {
                    let mut rows = reader.rows(range.clone(), &order, reverse, None).await?;
                    assert_eq!(
                        rows.try_next().await.unwrap_err().kind(),
                        io::ErrorKind::InvalidData
                    );
                }
                assert_eq!(
                    reader
                        .first(range, &order, Some(&columns))
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::InvalidData
                );
                for select in [vec![], vec!["absent".to_string()]] {
                    assert_eq!(
                        reader
                            .rows(Range::default(), &[], false, Some(&select))
                            .await
                            .err()
                            .unwrap()
                            .kind(),
                        io::ErrorKind::InvalidInput
                    );
                    assert_eq!(
                        reader
                            .first(Range::default(), &[], Some(&select))
                            .await
                            .unwrap_err()
                            .kind(),
                        io::ErrorKind::InvalidInput
                    );
                }
                drop(reader);

                *root.write::<Node<Value>>().await? = Node::Leaf(vec![vec![0.into()]]);
                drop(root.try_write::<Node<Value>>()?);

                let reader = table.read().await;
                assert_eq!(
                    reader
                        .first(Range::default(), &[], None)
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::InvalidData
                );
                assert_eq!(
                    reader.get_value(&[0.into()]).await.unwrap_err().kind(),
                    io::ErrorKind::InvalidData
                );
                drop(reader);

                // Unsupported queries reject before touching even malformed storage.
                // A supported scan still reports the native integrity error.
                *root.write::<Node<Value>>().await? = Node::Leaf(vec![vec![]]);
                let range = Range::from_iter([(
                    "up_name".to_string(),
                    b_table::ColumnRange::Eq("indexed".to_string().into()),
                )]);
                let order = ["up".to_string()];
                for reverse in [false, true] {
                    let reader = table.read().await;
                    assert_eq!(
                        reader
                            .rows(range.clone(), &order, reverse, None)
                            .await
                            .err()
                            .unwrap()
                            .kind(),
                        io::ErrorKind::Unsupported
                    );
                    let error = match reader.rows(Range::default(), &order, reverse, None).await {
                        Ok(mut rows) => rows.try_next().await.unwrap_err(),
                        Err(error) => error,
                    };
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                    assert_eq!(
                        error.to_string(),
                        "stored row does not match the BTree schema"
                    );
                }
                drop(root.try_write::<Node<Value>>()?);
                drop(table.try_write()?);
                Ok::<_, io::Error>(())
            })
            .await?;
    }

    fixture.close()
}

#[tokio::test]
async fn queries_require_one_supporting_index() -> io::Result<()> {
    for compatible in [false, true] {
        let fixture = TestDir::new("table_complete_prefix")?;
        let table = TableLock::create(
            TableSchema::new(
                vec!["a", "d", "b", "c"],
                [
                    ("by_b".to_string(), vec!["b", "a"]),
                    (
                        "by_c".to_string(),
                        if compatible {
                            vec!["b", "a", "c"]
                        } else {
                            vec!["a", "c"]
                        },
                    ),
                ],
            ),
            Collator::new(),
            Cache::<File>::new(
                1024 * BLOCK_SIZE,
                Some(4),
                0,
                std::time::Duration::from_secs(3),
            )
            .load(fixture.path().to_owned())?,
        )
        .await?;
        for (key, b, c) in [(1, 1, 2), (2, 2, 3), (3, 20, 4), (4, 3, 20)] {
            table
                .write()
                .await
                .upsert(vec![key.into()], vec![0.into(), b.into(), c.into()])
                .await?;
        }
        let range = Range::from_iter([
            (
                "b".to_string(),
                b_table::ColumnRange::from(Value::from(0)..Value::from(10)),
            ),
            (
                "c".to_string(),
                b_table::ColumnRange::from(Value::from(0)..Value::from(10)),
            ),
        ]);
        let supported = Range::from_iter([(
            "b".to_string(),
            b_table::ColumnRange::from(Value::from(0)..Value::from(10)),
        )]);
        let read = table.read().await;
        for reverse in [false, true] {
            assert_eq!(
                read.rows(range.clone(), &[], reverse, None)
                    .await
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::Unsupported
            );
            let rows = read
                .rows(supported.clone(), &["b".into(), "a".into()], reverse, None)
                .await?;
            let owned: Vec<_> = rows.map_ok(|row| row.into_vec()).try_collect().await?;
            let mut expected: Vec<Vec<Value>> = vec![
                vec![1.into(), 0.into(), 1.into(), 2.into()],
                vec![2.into(), 0.into(), 2.into(), 3.into()],
                vec![4.into(), 0.into(), 3.into(), 20.into()],
            ];
            if reverse {
                expected.reverse();
            }
            assert_eq!(owned, expected);
        }
        assert_eq!(
            read.first(range.clone(), &[], None)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            read.count(range.clone()).await.unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            read.is_empty(range.clone()).await.unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            read.first(supported.clone(), &[], None)
                .await?
                .unwrap()
                .as_slice(),
            &[Value::from(1)]
        );
        assert_eq!(read.count(supported.clone()).await?, 3);
        assert!(!read.is_empty(supported).await?);
        drop(read);
        assert_eq!(
            table
                .write()
                .await
                .delete_range(range)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
        let actual: Vec<_> = table
            .read()
            .await
            .rows(Range::default(), &[], false, None)
            .await?
            .map_ok(|row| row.into_vec())
            .try_collect()
            .await?;
        assert_eq!(
            actual,
            vec![
                vec![1.into(), 0.into(), 1.into(), 2.into()],
                vec![2.into(), 0.into(), 2.into(), 3.into()],
                vec![3.into(), 0.into(), 20.into(), 4.into()],
                vec![4.into(), 0.into(), 3.into(), 20.into()]
            ]
        );
        drop(table);
        fixture.close()?;
    }
    Ok(())
}
