use justybase_db2::{Db2Error, Db2Session, Db2Value};
use std::time::{SystemTime, UNIX_EPOCH};

/// Boundary probe results: `(sums, boundary_rows, descriptor_rows, all_rows)`.
type BoundaryRun = (
    Vec<i64>,
    Vec<Vec<Db2Value>>,
    Vec<Vec<Db2Value>>,
    Vec<Vec<Db2Value>>,
);

async fn current_schema(db: &mut Db2Session) -> String {
    match &db.query("VALUES CURRENT SCHEMA").await.unwrap().rows[0][0] {
        Db2Value::Text(value) => format!("\"{}\"", value.trim_end().replace('"', "\"\"")),
        other => panic!("unexpected CURRENT SCHEMA type: {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_parameter_descriptor_continuations_preserve_all_values() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("WB_R_{:X}", stamp & 0x0000_ffff_ffff_ffff);
    let schema = current_schema(&mut db).await;
    let mut created = false;

    let run: Result<BoundaryRun, Db2Error> = async {
        let mut boundary_sums = Vec::new();
        let mut boundary_rows = Vec::new();
        for count in [83usize, 84, 85] {
            let sql = format!(
                "VALUES ({})",
                std::iter::repeat_n("COALESCE(CAST(? AS INTEGER), 0)", count)
                    .collect::<Vec<_>>().join(" + ")
            );
            let parameters = (0..count)
                .map(|index| {
                    if index % 11 == 0 {
                        Db2Value::Null
                    } else {
                        Db2Value::Integer(index as i64)
                    }
                })
                .collect::<Vec<_>>();
            let expected = parameters.iter().filter_map(|value| match value {
                Db2Value::Integer(value) => Some(value),
                _ => None,
            }).sum();
            let result = db.query_with_params(&sql, &parameters).await?;
            boundary_sums.push(expected);
            boundary_rows.extend(result.rows);
        }

        db.execute(&format!(
            "CREATE TABLE {schema}.{table} (ID INTEGER NOT NULL PRIMARY KEY, V1 INTEGER, V2 VARCHAR(32), V3 SMALLINT)"
        ))
        .await?;
        created = true;

        let expected_rows = (0..37)
            .map(|index| {
                vec![
                    Db2Value::Integer(90_000 + index),
                    if index % 8 == 0 {
                        Db2Value::Null
                    } else {
                        Db2Value::Integer(index * 13)
                    },
                    if index % 5 == 0 {
                        Db2Value::Null
                    } else {
                        Db2Value::Text(format!("row-{index}"))
                    },
                    if index % 7 == 0 {
                        Db2Value::Null
                    } else {
                        Db2Value::Integer(index - 18)
                    },
                ]
            })
            .collect::<Vec<_>>();
        let groups = std::iter::repeat_n("(?, ?, ?, ?)", expected_rows.len())
            .collect::<Vec<_>>().join(", ");
        let sql = format!("INSERT INTO {table} VALUES {groups}");
        let parameters = expected_rows.iter().flatten().cloned().collect::<Vec<_>>();
        assert_eq!(parameters.len(), 148);
        db.execute_with_params(&sql, &parameters).await?;

        let actual_rows = db
            .query(&format!("SELECT ID, V1, V2, V3 FROM {table} ORDER BY ID"))
            .await?
            .rows;
        Ok((boundary_sums, boundary_rows, expected_rows, actual_rows))
    }
    .await;

    if created {
        let cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
        if run.is_ok() {
            cleanup.unwrap();
        }
    }

    let (boundary_sums, boundary_rows, expected_rows, actual_rows) = run.unwrap();
    assert_eq!(boundary_sums.len(), 3);
    assert_eq!(
        boundary_rows,
        boundary_sums
            .into_iter()
            .map(|sum| vec![Db2Value::Integer(sum)])
            .collect::<Vec<_>>()
    );
    assert_eq!(actual_rows, expected_rows);
}
