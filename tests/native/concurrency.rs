use super::*;

#[tokio::test]
async fn auxiliary_mutations_overlap_and_release_guards() -> io::Result<()> {
    const ROOT: &str = "00000000-0000-0000-0000-000000000000";
    for operation in ["insert", "update", "delete", "ordered"] {
        for cancel in [false, true] {
            let fixture = TestDir::new("test_table")?;
            let path = fixture.path().to_owned();

            {
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
            }

            fixture.close()?;
        }
    }

    Ok(())
}

#[tokio::test]
async fn auxiliary_error_releases_pending_sibling_before_primary() -> io::Result<()> {
    const ROOT: &str = "00000000-0000-0000-0000-000000000000";
    let fixture = TestDir::new("test_table")?;
    let path = fixture.path().to_owned();

    {
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
        let mut broken = right.write::<Node<Value>>().await?;
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
    }

    fixture.close()
}
