use justybase_db2::{Db2Session, Db2Value};
use serde_json::{Value, json};
use std::{fs, path::PathBuf};

fn cases() -> Vec<(String, String, String)> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/parity/cases.tsv");
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|line| {
            let mut parts = line.splitn(3, '|');
            (
                parts.next().unwrap().into(),
                parts.next().unwrap().into(),
                parts.next().unwrap().into(),
            )
        })
        .collect()
}

fn value(v: &Db2Value) -> Value {
    match v {
        Db2Value::Null => Value::Null,
        Db2Value::Boolean(b) => json!(b),
        Db2Value::Integer(n) => json!(n.to_string()),
        Db2Value::Float(n) => json!(n.to_string()),
        Db2Value::Decimal(s) | Db2Value::Text(s) => json!(s),
        Db2Value::Bytes(bytes) => {
            json!(bytes.iter().map(|b| format!("{b:02x}")).collect::<String>())
        }
        Db2Value::Lob(lob) => json!({"length":lob.byte_length,"type":lob.sql_type}),
        Db2Value::ProgressiveLobReference { length, .. } => json!({"length":length}),
    }
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN; run scripts/test-db2-parity.sh"]
async fn rust_parity_cases_emit_canonical_jsonl() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let tier = std::env::var("DB2_PARITY_TIER").unwrap_or_else(|_| "smoke".into());
    let mut lines = Vec::new();
    for (id, _case_tier, sql) in cases()
        .into_iter()
        .filter(|(_, t, _)| tier == "full" || t == "smoke")
    {
        let r = match db.query(&sql).await {
            Ok(r) => r,
            Err(e) if id == "invalid_sql" || id == "missing_table" => {
                lines.push(json!({"case":id,"rows":[],"metadata":[],"sqlcode":e.sql_code,"sqlstate":e.sql_state}));
                continue;
            }
            Err(e) => panic!("case {id}: {e}"),
        };
        let rows: Vec<Vec<Value>> = r
            .rows
            .iter()
            .map(|row| row.iter().map(value).collect())
            .collect();
        let metadata: Vec<Value> = r.columns.iter().map(|c| {
            let base=c.sql_type & 0xfffe;
            let kind=justybase_db2::types::sqltype::SqlType(base).name();
            let kind=kind.split(" (").next().unwrap_or("UNKNOWN");
            let numeric=matches!(kind,"DECIMAL"|"NUMERIC"|"DECFLOAT");
            json!({"type":kind,"precision":if numeric {json!(c.precision)} else {Value::Null},"scale":if numeric {json!(c.scale)} else {Value::Null},"nullable":c.nullable})
        }).collect();
        lines.push(
            json!({"case":id,"rows":rows,"metadata":metadata,"sqlcode":0,"sqlstate":"00000"}),
        );
    }
    let schema = std::env::var("DB2_IT_SCHEMA").unwrap();
    let table = format!("\"{schema}\".PARITY_DML");
    let _ = db.execute(&format!("DROP TABLE {table}")).await;
    db.execute(&format!("CREATE TABLE {table} (ID INTEGER NOT NULL PRIMARY KEY, AMOUNT DECIMAL(12,3), TXT VARCHAR(40))")).await.unwrap();
    let insert = format!("INSERT INTO {table} VALUES (?, ?, ?)");
    let first = db
        .execute_with_params(
            &insert,
            &[
                Db2Value::Integer(1),
                Db2Value::Decimal("123.456".into()),
                Db2Value::Text("Zażółć 東京".into()),
            ],
        )
        .await
        .unwrap();
    let many = db
        .execute(&format!(
            "INSERT INTO {table} VALUES (2, -0.125, 'two'), (3, 0.000, '')"
        ))
        .await
        .unwrap();
    let update0 = db
        .execute(&format!("UPDATE {table} SET TXT='x' WHERE ID=99"))
        .await
        .unwrap();
    let update1 = db
        .execute(&format!("UPDATE {table} SET TXT='one' WHERE ID=1"))
        .await
        .unwrap();
    let updatemany = db
        .execute(&format!("UPDATE {table} SET AMOUNT=AMOUNT WHERE ID<=2"))
        .await
        .unwrap();
    let delete0 = db
        .execute(&format!("DELETE FROM {table} WHERE ID=99"))
        .await
        .unwrap();
    let deletemany = db
        .execute(&format!("DELETE FROM {table} WHERE ID<=2"))
        .await
        .unwrap();
    let final_rows = db
        .query(&format!("SELECT ID, AMOUNT, TXT FROM {table} ORDER BY ID"))
        .await
        .unwrap();
    lines.push(json!({"case":"dml_prepared_counts","rows":[[first,many,update0,update1,updatemany,delete0,deletemany]],"metadata":[],"sqlcode":0,"sqlstate":"00000"}));
    lines.push(json!({"case":"dml_final_state","rows":final_rows.rows.iter().map(|r|r.iter().map(value).collect::<Vec<_>>()).collect::<Vec<_>>(),"metadata":[],"sqlcode":0,"sqlstate":"00000"}));
    let tx_sql = format!("INSERT INTO {table} (ID) VALUES (4)");
    let mut tx = db.begin_transaction().unwrap();
    tx.execute(&tx_sql).await.unwrap();
    let mut observer = Db2Session::connect_dsn(&dsn).await.unwrap();
    let before = observer
        .query(&format!("SELECT COUNT(*) FROM {table} WHERE ID=4"))
        .await
        .unwrap();
    observer.close().await.unwrap();
    tx.rollback().await.unwrap();
    let rolled_back = db
        .query(&format!("SELECT COUNT(*) FROM {table} WHERE ID=4"))
        .await
        .unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.execute(&tx_sql).await.unwrap();
    tx.commit().await.unwrap();
    let mut observer = Db2Session::connect_dsn(&dsn).await.unwrap();
    let committed = observer
        .query(&format!("SELECT COUNT(*) FROM {table} WHERE ID=4"))
        .await
        .unwrap();
    observer.close().await.unwrap();
    let number = |v: &Db2Value| match v {
        Db2Value::Integer(n) => json!(n),
        other => value(other),
    };
    lines.push(json!({"case":"transaction_visibility","rows":[[number(&before.rows[0][0]),number(&rolled_back.rows[0][0]),number(&committed.rows[0][0])]],"metadata":[],"sqlcode":0,"sqlstate":"00000"}));
    db.execute(&format!("DROP TABLE {table}")).await.unwrap();
    let output = lines
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let path = std::env::var("DB2_PARITY_OUTPUT").unwrap();
    fs::write(path, output).unwrap();
    db.close().await.unwrap();
}
