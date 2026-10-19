use super::*;

// Timing entrypoint; use the shared fensor comparison runner, separately from correctness tests.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual multi-index performance measurement"]
async fn benchmark_index_mutations() -> io::Result<()> {
    if let Ok(workers) = std::env::var("TOKIO_WORKER_THREADS") {
        assert_eq!(
            tokio::runtime::Handle::current().metrics().num_workers(),
            workers.parse::<usize>().expect("Tokio worker count"),
        );
    }

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
        let fixture = TestDir::new("test_table")?;
        let path = fixture.path().to_owned();

        {
            let cache =
                || Cache::<File>::new(64 * 1024 * 1024, None, 0, std::time::Duration::from_secs(3));
            let table =
                TableLock::create(schema(), Collator::new(), cache().load(path.clone())?).await?;
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
                let table = table.read().await;
                for key in 0..128 {
                    assert!(table.get_row(&[key.into()]).await?.is_some());
                }
            }
            emit("lookup", "resident", start.elapsed());

            let start = std::time::Instant::now();
            {
                let mut rows = table.read().await.into_rows().await?;
                let mut count = 0;
                while let Some(row) = rows.try_next().await? {
                    std::hint::black_box(row);
                    count += 1;
                }
                assert_eq!(count, 128);
            }
            emit("scan", "resident", start.elapsed());

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

            let table = TableLock::load(schema(), Collator::new(), cache().load(path.clone())?)?;
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

            table
                .write()
                .await
                .upsert_sorted(futures::stream::iter(
                    (0..128).map(|key| Ok((vec![key.into()], values("range")))),
                ))
                .await?;
            let start = std::time::Instant::now();
            assert_eq!(
                table.write().await.delete_range(Range::default()).await?,
                128
            );
            emit("delete_range", "resident", start.elapsed());
            table.validate().await?;

            drop(table);
        }

        fixture.close()?;
    }

    Ok(())
}
