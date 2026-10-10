use justybase_db2::{Db2BatchOptions, Db2Session, Db2Value};
use std::{env, time::Instant};

#[derive(Clone, Copy)]
enum Scenario {
    Integer,
    IntegerVarchar,
    TenColumns,
    TwentyColumns,
    Decimal,
    Mixed,
}

impl Scenario {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "integer" => Ok(Self::Integer),
            "integer-varchar" => Ok(Self::IntegerVarchar),
            "ten-columns" => Ok(Self::TenColumns),
            "twenty-columns" => Ok(Self::TwentyColumns),
            "decimal" => Ok(Self::Decimal),
            "mixed" => Ok(Self::Mixed),
            _ => Err(format!(
                "unknown scenario {value:?}; expected integer, integer-varchar, ten-columns, twenty-columns, decimal, or mixed"
            )),
        }
    }

    fn table_columns(self) -> String {
        match self {
            Self::Integer => "C0 INTEGER NOT NULL".into(),
            Self::IntegerVarchar => "C0 INTEGER NOT NULL, C1 VARCHAR(20) NOT NULL".into(),
            Self::TenColumns => numbered_columns(10),
            Self::TwentyColumns => numbered_columns(20),
            Self::Decimal => "C0 DECIMAL(20,5) NOT NULL".into(),
            Self::Mixed => "C0 INTEGER NOT NULL, C1 VARCHAR(20) NOT NULL, C2 VARBINARY(16) NOT NULL, C3 DECIMAL(18,4) NOT NULL, C4 DECFLOAT(34) NOT NULL".into(),
        }
    }

    fn parameter_count(self) -> usize {
        match self {
            Self::Integer | Self::Decimal => 1,
            Self::IntegerVarchar => 2,
            Self::TenColumns => 10,
            Self::TwentyColumns => 20,
            Self::Mixed => 5,
        }
    }

    fn estimated_row_bytes(self) -> usize {
        match self {
            Self::Integer => 4,
            Self::IntegerVarchar => 24,
            Self::TenColumns => 40,
            Self::TwentyColumns => 80,
            Self::Decimal => 16,
            Self::Mixed => 4 + 20 + 16 + 18 + 34,
        }
    }

    fn row(self, index: usize) -> Vec<Db2Value> {
        match self {
            Self::Integer => vec![Db2Value::Integer(index as i64)],
            Self::IntegerVarchar => vec![
                Db2Value::Integer(index as i64),
                Db2Value::Text(format!("row-{index:08}")),
            ],
            Self::TenColumns | Self::TwentyColumns => (0..self.parameter_count())
                .map(|column| Db2Value::Integer(index as i64 + column as i64))
                .collect(),
            Self::Decimal => vec![Db2Value::Decimal("1234567890.12345".into())],
            Self::Mixed => vec![
                Db2Value::Integer(index as i64),
                Db2Value::Text(format!("row-{index:08}")),
                Db2Value::Bytes(vec![0x5a; 16]),
                Db2Value::Decimal("1234567890.1234".into()),
                Db2Value::Decimal("12345678901234567890.12345678901234".into()),
            ],
        }
    }
}

fn numbered_columns(count: usize) -> String {
    (0..count)
        .map(|index| format!("C{index} INTEGER NOT NULL"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_row_counts(value: Option<String>) -> Result<Vec<usize>, String> {
    value
        .unwrap_or_else(|| "1000,10000,100000,1000000".into())
        .split(',')
        .map(|part| {
            let rows = part
                .parse::<usize>()
                .map_err(|_| format!("invalid row count {part:?}"))?;
            if rows == 0 {
                return Err("row counts must be positive".into());
            }
            Ok(rows)
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let scenario = Scenario::parse(&args.next().unwrap_or_else(|| "integer".into()))?;
    let row_counts = parse_row_counts(args.next())?;
    let batch_rows = args
        .next()
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| format!("invalid batch row limit {value:?}"))
        })
        .transpose()?
        .unwrap_or_else(|| Db2BatchOptions::default().max_rows);
    if batch_rows == 0 {
        return Err("batch row limit must be positive".into());
    }
    if args.next().is_some() {
        return Err(
            "usage: batch_benchmark [scenario] [comma-separated-row-counts] [batch-rows]".into(),
        );
    }
    let dsn = env::var("DB2_TEST_DSN")?;
    let mut db = Db2Session::connect_dsn(&dsn).await?;
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos()
        % 1_000_000_000_000;
    let table = format!("RDB2B{suffix:012}");
    let create = format!("CREATE TABLE {table} ({})", scenario.table_columns());
    let insert = format!(
        "INSERT INTO {table} VALUES ({})",
        placeholders(scenario.parameter_count())
    );

    db.execute(&create).await?;
    let result = async {
        for rows in row_counts {
            let parameters = (0..rows)
                .map(|index| scenario.row(index))
                .collect::<Vec<_>>();
            db.set_auto_commit(false);
            db.benchmark_reset_request_batches();
            let started = Instant::now();
            let outcome = db
                .execute_batch_with_params_and_batch_options(
                    &insert,
                    &parameters,
                    Db2BatchOptions::new(batch_rows, Db2BatchOptions::default().max_encoded_bytes),
                )
                .await;
            let batch_elapsed = started.elapsed();
            match outcome {
                Ok(affected) => {
                    db.commit().await?;
                    let request_batches = db.benchmark_request_batches_sent();
                    db.set_auto_commit(true);
                    let elapsed = started.elapsed();
                    let rows_per_second = rows as f64 / elapsed.as_secs_f64();
                    let megabytes_per_second =
                        (rows * scenario.estimated_row_bytes()) as f64 / elapsed.as_secs_f64()
                            / 1_000_000.0;
                    let count = db
                        .query(&format!("SELECT COUNT(*) FROM {table}"))
                        .await?;
                    if count.rows.first().and_then(|row| row.first())
                        != Some(&Db2Value::Integer(rows as i64))
                    {
                        return Err(format!("DB2 row count did not match {rows}").into());
                    }
                    println!(
                        "scenario={} rows={} batch_rows={} rows_affected={} elapsed_ms={:.3} batch_ms={:.3} rows_per_sec={:.0} estimated_mb_per_sec={:.2} request_batches={}",
                        args_scenario_name(scenario),
                        rows,
                        batch_rows,
                        affected,
                        elapsed.as_secs_f64() * 1000.0,
                        batch_elapsed.as_secs_f64() * 1000.0,
                        rows_per_second,
                        megabytes_per_second,
                        request_batches,
                    );
                }
                Err(error) => {
                    let request_batches = db.benchmark_request_batches_sent();
                    let rollback = db.rollback().await;
                    db.set_auto_commit(true);
                    return Err(format!(
                        "batch failed at parameter set {:?} after {} completed rows and {request_batches} request batches (rollback={rollback:?}): {error}",
                        error.failed_parameter_set_index,
                        error.completed_parameter_sets
                    )
                    .into());
                }
            }
            drop(parameters);
            db.execute(&format!("DELETE FROM {table}")).await?;
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    let cleanup = db.execute(&format!("DROP TABLE {table}")).await;
    result?;
    cleanup?;
    Ok(())
}

fn args_scenario_name(scenario: Scenario) -> &'static str {
    match scenario {
        Scenario::Integer => "integer",
        Scenario::IntegerVarchar => "integer-varchar",
        Scenario::TenColumns => "ten-columns",
        Scenario::TwentyColumns => "twenty-columns",
        Scenario::Decimal => "decimal",
        Scenario::Mixed => "mixed",
    }
}
