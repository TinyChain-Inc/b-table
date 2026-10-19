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
async fn callback_update_validates_before_mutation_and_reopens() -> io::Result<()> {
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
            .read_file::<_, Node<Value>>("00000000-0000-0000-0000-000000000000")
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
        assert_eq!(table.read().await.count(Range::default()).await?, 2);

        // Ambiguous primary resolution fails before any auxiliary update.
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
                vec!["a".to_string().into(), 1.into()],
                vec!["c".to_string().into(), 1.into()],
            ]
        );
        table.validate().await?;

        drop(table);
        drop(auxiliary);
        drop(dir);
    }

    fixture.close()
}
