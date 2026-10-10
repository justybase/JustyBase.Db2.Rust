use justybase_db2::{
    Db2ImportColumn, Db2ImportPolicy, Db2ImportProgress, Db2ImportTarget, Db2Session, Db2Value,
};

const ROWS_PER_CHECKPOINT: usize = 64;
const MAX_BATCH_BYTES: usize = 4096;
const SIMPLE_COLUMNS: [&str; 4] = ["ID", "A", "B", "C"];
const BLOB_COLUMNS: [&str; 2] = ["ID", "B"];
const TEXT_LOB_COLUMNS: [&str; 4] = ["ID", "C", "D", "X"];
const MIXED_COLUMNS: [&str; 14] = [
    "ID",
    "SMALL_VALUE",
    "INTEGER_VALUE",
    "BIGINT_VALUE",
    "DECIMAL_VALUE",
    "DECFLOAT_VALUE",
    "REAL_VALUE",
    "DOUBLE_VALUE",
    "BOOLEAN_VALUE",
    "DATE_VALUE",
    "TIME_VALUE",
    "TIMESTAMP_VALUE",
    "TEXT_VALUE",
    "BINARY_VALUE",
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 4 {
        return Err("expected source table, destination table, scenario, row count".into());
    }
    let (source_table, destination_table, scenario) = (&args[0], &args[1], args[2].as_str());
    let row_count: usize = args[3].parse()?;
    let mixed_types = scenario.starts_with("mixed-");
    let blob_import = matches!(scenario, "blob-atomic" | "blob-checkpoint");
    let text_lob_import = matches!(scenario, "text-lob-atomic" | "text-lob-checkpoint");
    let valid_row_count = if blob_import {
        row_count == 1
    } else if text_lob_import {
        row_count == 3
    } else if mixed_types {
        row_count == 4
    } else {
        (130..=1000).contains(&row_count)
    };
    if !is_fixture_identifier(source_table)
        || !is_fixture_identifier(destination_table)
        || !valid_row_count
        || ![
            "atomic",
            "checkpoint",
            "blob-atomic",
            "blob-checkpoint",
            "text-lob-atomic",
            "text-lob-checkpoint",
            "atomic-failure",
            "checkpoint-failure",
            "mixed-atomic",
            "mixed-checkpoint",
        ]
        .contains(&scenario)
    {
        return Err("invalid bounded-import oracle arguments".into());
    }

    let dsn = std::env::var("DB2_TEST_DSN")?;
    let mut source_session = Db2Session::connect_dsn(&dsn).await?;
    let mut destination = Db2Session::connect_dsn(&dsn).await?;
    let selected_columns = if mixed_types {
        &MIXED_COLUMNS[..]
    } else if text_lob_import {
        &TEXT_LOB_COLUMNS[..]
    } else if blob_import {
        &BLOB_COLUMNS[..]
    } else {
        &SIMPLE_COLUMNS[..]
    };
    let select_list = selected_columns.join(", ");
    let source_sql = format!("SELECT {select_list} FROM \"{source_table}\" ORDER BY ID");
    if blob_import {
        let mut probe = source_session
            .query_stream(&format!("SELECT B FROM \"{source_table}\" ORDER BY ID"))
            .await?;
        let row = probe
            .next()
            .await?
            .ok_or("progressive BLOB import probe returned no row")?;
        let Db2Value::Lob(locator) = &row[0] else {
            return Err("live Db2 did not return a progressive BLOB locator".into());
        };
        let mut lob = probe.open_lob(locator).await?;
        while lob.next_chunk().await?.is_some() {}
        let _ = lob;
        if probe.next().await?.is_some() {
            return Err("progressive BLOB import probe returned extra rows".into());
        }
        drop(probe);
    }
    if text_lob_import {
        let mut probe = source_session
            .query_stream(&format!(
                "SELECT C, D, X FROM \"{source_table}\" WHERE ID = 1"
            ))
            .await?;
        let row = probe
            .next()
            .await?
            .ok_or("CLOB/DBCLOB/XML import probe returned no row")?;
        let mut value_kinds = Vec::with_capacity(row.len());
        for value in &row {
            match value {
                Db2Value::Lob(locator) => {
                    value_kinds.push("progressive locator");
                    let mut lob = probe.open_lob(locator).await?;
                    while lob.next_chunk().await?.is_some() {}
                }
                Db2Value::Text(_) => value_kinds.push("materialized text"),
                Db2Value::Null => value_kinds.push("NULL"),
                _ => {
                    return Err(
                        "CLOB/DBCLOB/XML import probe returned an unsupported value kind".into(),
                    );
                }
            }
        }
        if row.len() != 3 || probe.next().await?.is_some() {
            return Err("CLOB/DBCLOB/XML import probe returned an unexpected row shape".into());
        }
        println!("CLOB/DBCLOB/XML source representations: {value_kinds:?}");
        drop(probe);
    }
    let mut source = source_session.query_stream(&source_sql).await?;
    let target = Db2ImportTarget::new(
        None,
        destination_table,
        selected_columns
            .iter()
            .map(|name| {
                let column = Db2ImportColumn::new(*name);
                if mixed_types || blob_import || text_lob_import {
                    column
                } else {
                    column.with_sql_type(497)
                }
            })
            .collect(),
    )?;

    let rows_per_checkpoint = if blob_import || text_lob_import {
        1
    } else if mixed_types {
        2
    } else {
        ROWS_PER_CHECKPOINT
    };
    let policy = match scenario {
        "atomic" | "blob-atomic" | "text-lob-atomic" | "atomic-failure" | "mixed-atomic" => {
            Db2ImportPolicy::Atomic {
                max_batch_rows: rows_per_checkpoint,
                max_batch_bytes: if text_lob_import {
                    2_000_000
                } else {
                    MAX_BATCH_BYTES
                },
            }
        }
        "checkpoint"
        | "blob-checkpoint"
        | "text-lob-checkpoint"
        | "checkpoint-failure"
        | "mixed-checkpoint" => Db2ImportPolicy::CommitEveryRows {
            rows: rows_per_checkpoint,
            max_batch_bytes: if text_lob_import {
                2_000_000
            } else {
                MAX_BATCH_BYTES
            },
        },
        _ => unreachable!(),
    };
    let mut observed = Vec::<Db2ImportProgress>::new();
    let result = destination
        .import_rows_from(&mut source, &target, policy, |snapshot| {
            observed.push(snapshot);
            Ok(())
        })
        .await;

    match scenario {
        "atomic" | "mixed-atomic" | "blob-atomic" | "text-lob-atomic" => {
            let progress = result?;
            if progress.source_rows_read != row_count as u64
                || progress.committed_rows != row_count as u64
                || progress.committed_batches != 1
                || observed.len() != 1
                || observed[0].committed_rows != 0
            {
                return Err(format!("unexpected atomic import progress: {progress:?}").into());
            }
            println!(
                "PASS Rust {}atomic import: {row_count} rows, one confirmed commit",
                if text_lob_import {
                    "progressive CLOB/DBCLOB/XML "
                } else if mixed_types {
                    "mixed-type "
                } else if blob_import {
                    "progressive BLOB "
                } else {
                    ""
                }
            );
        }
        "checkpoint" | "mixed-checkpoint" | "blob-checkpoint" | "text-lob-checkpoint" => {
            let progress = result?;
            let expected_batches = row_count.div_ceil(rows_per_checkpoint);
            if progress.source_rows_read != row_count as u64
                || progress.committed_rows != row_count as u64
                || progress.committed_batches != expected_batches as u64
                || observed.len() != expected_batches
            {
                return Err(format!("unexpected checkpoint import progress: {progress:?}").into());
            }
            for (index, snapshot) in observed.iter().enumerate() {
                let expected_read = ((index + 1) * rows_per_checkpoint).min(row_count) as u64;
                let expected_committed = (index * rows_per_checkpoint).min(row_count) as u64;
                if snapshot.source_rows_read != expected_read
                    || snapshot.committed_rows != expected_committed
                {
                    return Err(format!("unexpected checkpoint snapshot: {snapshot:?}").into());
                }
            }
            println!(
                "PASS Rust {}checkpoint import: {row_count} rows across {expected_batches} confirmed commits",
                if text_lob_import {
                    "CLOB/DBCLOB/XML "
                } else if mixed_types {
                    "mixed-type "
                } else if blob_import {
                    "progressive BLOB "
                } else {
                    ""
                }
            );
        }
        "atomic-failure" => {
            let error = result.expect_err("the pre-seeded duplicate key must fail");
            if error.failed_source_row != Some(4)
                || error.progress.committed_rows != 0
                || error.progress.committed_batches != 0
                || error.cause.sql_code != Some(-803)
                || error.cause.sql_state.as_deref() != Some("23505")
            {
                return Err(format!("unexpected atomic row error: {error:?}").into());
            }
            println!("PASS Rust atomic failure: duplicate source row 4 rolled back");
        }
        "checkpoint-failure" => {
            let error = result.expect_err("the pre-seeded duplicate key must fail");
            if error.failed_source_row != Some(130)
                || error.progress.committed_rows != 128
                || error.progress.committed_batches != 2
                || error.cause.sql_code != Some(-803)
                || error.cause.sql_state.as_deref() != Some("23505")
                || observed.len() != 2
            {
                return Err(format!("unexpected checkpoint row error: {error:?}").into());
            }
            println!(
                "PASS Rust checkpoint failure: row 130 rolled back; 128 earlier rows remain committed"
            );
        }
        _ => unreachable!(),
    }

    let reusable = destination.query("VALUES 29").await?;
    if reusable.rows != vec![vec![Db2Value::Integer(29)]] {
        return Err("destination session was not reusable after the import".into());
    }
    while source.next().await?.is_some() {}
    drop(source);
    source_session.close().await?;
    destination.close().await?;
    Ok(())
}

fn is_fixture_identifier(value: &str) -> bool {
    value.len() <= 64
        && value.starts_with("BP_")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}
