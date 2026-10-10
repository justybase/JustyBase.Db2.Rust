use justybase_db2::{Db2ImportColumn, Db2ImportPolicy, Db2ImportTarget, Db2Session};

fn identifier(value: &str) -> bool {
    let parts: Vec<_> = value.split('_').collect();
    (parts.len() == 3 || (parts.len() == 4 && parts[3] == "D"))
        && parts[0] == "TW"
        && parts[1].len() == 12
        && parts[1].bytes().all(|b| b.is_ascii_hexdigit())
        && ["N", "T", "X", "G"].contains(&parts[2])
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if ![3, 6].contains(&args.len())
        || !identifier(&args[0])
        || !identifier(&args[1])
        || !["direct", "prepared", "batch", "atomic", "checkpoint"].contains(&args[2].as_str())
    {
        return Err("invalid controlled fixture".into());
    }
    let count: usize = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(10);
    let batch_rows: usize = args.get(4).map(|s| s.parse()).transpose()?.unwrap_or(3);
    let batch_bytes: usize = args.get(5).map(|s| s.parse()).transpose()?.unwrap_or(4096);
    if count == 0 || batch_rows == 0 || batch_bytes == 0 {
        return Err("invalid fixture limits".into());
    }
    let dsn = std::env::var("DB2_TEST_DSN")?;
    let mut source = Db2Session::connect_dsn(&dsn).await?;
    let destination_dsn = std::env::var("DB2_DEST_TEST_DSN").unwrap_or_else(|_| dsn.clone());
    let mut destination = Db2Session::connect_dsn(&destination_dsn).await?;
    let sql = format!("SELECT * FROM {} ORDER BY ID", args[0]);
    let result = source.query(&sql).await?;
    assert_eq!(result.rows.len(), count);
    let insert = format!(
        "INSERT INTO {} VALUES ({})",
        args[1],
        vec!["?"; result.columns.len()].join(",")
    );
    match args[2].as_str() {
        "atomic" | "checkpoint" => {
            let mut reader = source.query_stream(&sql).await?;
            let target = Db2ImportTarget::new(
                None,
                &args[1],
                result
                    .columns
                    .iter()
                    .map(|c| Db2ImportColumn::new(&c.name))
                    .collect(),
            )?;
            let policy = if args[2] == "atomic" {
                Db2ImportPolicy::Atomic {
                    max_batch_rows: batch_rows,
                    max_batch_bytes: batch_bytes,
                }
            } else {
                Db2ImportPolicy::CommitEveryRows {
                    rows: batch_rows,
                    max_batch_bytes: batch_bytes,
                }
            };
            let outcome = destination
                .import_rows_from(&mut reader, &target, policy, |_| Ok(()))
                .await;
            if let Ok(expected) = std::env::var("DB2_FAULT_EXPECTED_PROGRESS") {
                let error = outcome.expect_err("expected destination reset to fail import");
                assert_eq!(error.progress.committed_rows, expected.parse::<u64>()?);
                assert!(destination.is_closed());
                drop(reader);
                source.close().await?;
                destination.close().await?;
                println!("PASS Rust destination reset: confirmed progress and discarded session");
                return Ok(());
            }
            let progress = outcome?;
            assert_eq!(progress.source_rows_read, count as u64);
            assert_eq!(progress.committed_rows, count as u64);
        }
        "batch" => {
            assert_eq!(
                destination
                    .execute_batch_with_params(&insert, &result.rows)
                    .await?,
                count as i64
            );
        }
        "prepared" => {
            let stmt = destination.prepare(&insert).await?;
            for row in &result.rows {
                assert_eq!(destination.execute_prepared(&stmt, row).await?, 1);
            }
        }
        _ => {
            for row in &result.rows {
                assert_eq!(destination.execute_with_params(&insert, row).await?, 1);
            }
        }
    }
    source.close().await?;
    destination.close().await?;
    Ok(())
}
