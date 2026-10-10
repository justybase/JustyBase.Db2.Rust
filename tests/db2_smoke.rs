//! Minimal live smoke tests for `scripts/test-db2-integration.sh`.
//!
//! The script starts a throwaway Db2 container and exports `DB2_TEST_DSN` (and
//! `DB2_IT_SCHEMA`, a schema created for that run). The tests are `#[ignore]`d,
//! so a plain `cargo test` never needs Db2; run them with
//! `cargo test --test db2_smoke -- --ignored --test-threads=1`.

use justybase_db2::{Db2Error, Db2Session, Db2Value};
use std::time::{SystemTime, UNIX_EPOCH};

async fn connect() -> Db2Session {
    let dsn = std::env::var("DB2_TEST_DSN")
        .expect("DB2_TEST_DSN must be set; run scripts/test-db2-integration.sh");
    Db2Session::connect_dsn(&dsn).await.expect("connect to Db2")
}

/// The schema created for this run, or the session's current schema when the
/// tests are pointed at a database by hand.
async fn schema(db: &mut Db2Session) -> String {
    if let Ok(name) = std::env::var("DB2_IT_SCHEMA") {
        return format!("\"{}\"", name.replace('"', "\"\""));
    }
    match &db.query("VALUES CURRENT SCHEMA").await.unwrap().rows[0][0] {
        Db2Value::Text(value) => format!("\"{}\"", value.trim_end().replace('"', "\"\"")),
        other => panic!("unexpected CURRENT SCHEMA value: {other:?}"),
    }
}

fn unique_table(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos % 1_000_000_000)
}

fn text(value: &str) -> Db2Value {
    Db2Value::Text(value.to_string())
}

async fn count(db: &mut Db2Session, table: &str) -> i64 {
    match &db
        .query(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .unwrap()
        .rows[0][0]
    {
        Db2Value::Integer(n) => *n,
        other => panic!("unexpected COUNT(*) value: {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database (scripts/test-db2-integration.sh)"]
async fn smoke_connect_query_close() {
    let mut db = connect().await;
    let result = db.query("VALUES 1").await.unwrap();
    assert_eq!(result.rows, vec![vec![Db2Value::Integer(1)]]);
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database (scripts/test-db2-integration.sh)"]
async fn smoke_create_insert_select_update_delete_with_types_and_nulls() {
    let mut db = connect().await;
    let table = format!("{}.{}", schema(&mut db).await, unique_table("SMOKE_DML"));
    db.execute(&format!(
        "CREATE TABLE {table} (ID INTEGER NOT NULL PRIMARY KEY, BIG BIGINT, NAME VARCHAR(50), \
         CODE CHAR(4), AMOUNT DECIMAL(12,3), BORN DATE, STAMP TIMESTAMP, FLAG BOOLEAN)"
    ))
    .await
    .unwrap();

    let inserted = db
        .execute(&format!(
            "INSERT INTO {table} VALUES \
             (1, 9223372036854775807, 'alpha', 'AB', 123.456, '2024-02-29', '2024-02-29 23:59:58.123456', TRUE), \
             (2, -5, 'Zażółć 東京', 'CD', -0.001, '1999-12-31', '2000-01-01 00:00:00', FALSE), \
             (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        ))
        .await
        .unwrap();
    assert_eq!(inserted, 3);

    let rows = db
        .query(&format!(
            "SELECT ID, BIG, NAME, CODE, AMOUNT, BORN, STAMP, FLAG FROM {table} ORDER BY ID"
        ))
        .await
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Db2Value::Integer(1));
    assert_eq!(rows[0][1], Db2Value::Integer(i64::MAX));
    assert_eq!(rows[0][2], text("alpha"));
    assert_eq!(rows[0][3], text("AB  "), "CHAR(4) keeps its blank padding");
    assert_eq!(rows[0][4], Db2Value::Decimal("123.456".to_string()));
    assert_eq!(rows[0][5], text("2024-02-29"));
    assert_eq!(rows[0][6], text("2024-02-29-23.59.58.123456"));
    assert_eq!(rows[0][7], Db2Value::Boolean(true));
    assert_eq!(rows[1][1], Db2Value::Integer(-5));
    assert_eq!(rows[1][2], text("Zażółć 東京"));
    assert_eq!(rows[1][4], Db2Value::Decimal("-0.001".to_string()));
    assert_eq!(rows[1][7], Db2Value::Boolean(false));
    assert!(
        rows[2][1..].iter().all(|value| *value == Db2Value::Null),
        "row 3 is all NULL: {:?}",
        rows[2]
    );

    let updated = db
        .execute(&format!(
            "UPDATE {table} SET NAME = 'changed' WHERE ID IN (1, 2)"
        ))
        .await
        .unwrap();
    assert_eq!(updated, 2);
    let names = db
        .query(&format!(
            "SELECT NAME FROM {table} WHERE NAME IS NOT NULL ORDER BY ID"
        ))
        .await
        .unwrap()
        .rows;
    assert_eq!(names, vec![vec![text("changed")], vec![text("changed")]]);

    assert_eq!(
        db.execute(&format!("DELETE FROM {table} WHERE ID = 3"))
            .await
            .unwrap(),
        1
    );
    assert_eq!(count(&mut db, &table).await, 2);

    db.execute(&format!("DROP TABLE {table}")).await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database (scripts/test-db2-integration.sh)"]
async fn smoke_parameterized_and_prepared_statements() {
    let mut db = connect().await;
    let table = format!("{}.{}", schema(&mut db).await, unique_table("SMOKE_PARAM"));
    db.execute(&format!(
        "CREATE TABLE {table} (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(30))"
    ))
    .await
    .unwrap();

    let insert = format!("INSERT INTO {table} VALUES (?, ?)");
    assert_eq!(
        db.execute_with_params(&insert, &[Db2Value::Integer(1), text("one")])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.execute_with_params(&insert, &[Db2Value::Integer(2), Db2Value::Null])
            .await
            .unwrap(),
        1
    );

    let select = format!("SELECT NAME FROM {table} WHERE ID = ?");
    let one = db
        .query_with_params(&select, &[Db2Value::Integer(1)])
        .await
        .unwrap();
    assert_eq!(one.rows, vec![vec![text("one")]]);
    let two = db
        .query_with_params(&select, &[Db2Value::Integer(2)])
        .await
        .unwrap();
    assert_eq!(two.rows, vec![vec![Db2Value::Null]]);
    let none = db
        .query_with_params(&select, &[Db2Value::Integer(99)])
        .await
        .unwrap();
    assert!(none.rows.is_empty());

    let statement = db.prepare(&select).await.unwrap();
    for _ in 0..3 {
        let result = db
            .query_prepared(&statement, &[Db2Value::Integer(1)])
            .await
            .unwrap();
        assert_eq!(result.rows, vec![vec![text("one")]]);
    }

    db.execute(&format!("DROP TABLE {table}")).await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database (scripts/test-db2-integration.sh)"]
async fn smoke_transaction_rollback_then_commit() {
    let mut db = connect().await;
    let table = format!("{}.{}", schema(&mut db).await, unique_table("SMOKE_TX"));
    db.execute(&format!(
        "CREATE TABLE {table} (ID INTEGER NOT NULL PRIMARY KEY)"
    ))
    .await
    .unwrap();

    let mut tx = db.begin_transaction().unwrap();
    tx.execute(&format!("INSERT INTO {table} VALUES (1)"))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        count(&mut db, &table).await,
        0,
        "rolled-back row must not exist"
    );

    let mut tx = db.begin_transaction().unwrap();
    tx.execute(&format!("INSERT INTO {table} VALUES (2)"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(count(&mut db, &table).await, 1, "committed row must exist");

    // A second session sees the committed row.
    let mut other = connect().await;
    assert_eq!(count(&mut other, &table).await, 1);
    other.close().await.unwrap();

    db.execute(&format!("DROP TABLE {table}")).await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database (scripts/test-db2-integration.sh)"]
async fn smoke_result_shape_column_names_and_count() {
    let mut db = connect().await;
    let result = db
        .query("SELECT 7 AS SEVEN, CAST('x' AS VARCHAR(5)) AS LETTER, CAST(NULL AS INTEGER) AS NOTHING FROM SYSIBM.SYSDUMMY1")
        .await
        .unwrap();
    let names: Vec<&str> = result
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(names, ["SEVEN", "LETTER", "NOTHING"]);
    assert_eq!(
        result.rows,
        vec![vec![Db2Value::Integer(7), text("x"), Db2Value::Null]]
    );
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database (scripts/test-db2-integration.sh)"]
async fn smoke_invalid_sql_returns_error_and_session_stays_usable() {
    let mut db = connect().await;
    let error: Db2Error = db.query("SELEC 1 FROM SYSIBM.SYSDUMMY1").await.unwrap_err();
    assert_eq!(error.sql_code, Some(-104), "unexpected error: {error}");

    let missing = db
        .query("SELECT * FROM IT_SMOKE_NO_SUCH_TABLE")
        .await
        .unwrap_err();
    assert_eq!(missing.sql_code, Some(-204), "unexpected error: {missing}");

    // The same session keeps working after both failures.
    assert_eq!(
        db.query("VALUES 2").await.unwrap().rows,
        vec![vec![Db2Value::Integer(2)]]
    );
    db.close().await.unwrap();
}
