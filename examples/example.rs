use std::io;
use std::ops::Bound;

use freqfs::Cache;
use futures::TryStreamExt;

use self::adapters::{BLOCK_SIZE, Collator, File, TableSchema, Value};
use self::directory::TestDir;
use b_table::{Range, TableLock};

#[path = "support/adapters.rs"]
mod adapters;
#[path = "../tests/common/directory.rs"]
mod directory;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    // set up the test directory
    let fixture = TestDir::new("test_table")?;
    let path = fixture.path().to_owned();

    {
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
    }

    fixture.close()
}

#[cfg(test)]
#[path = "../tests/common/observations.rs"]
mod observations;
