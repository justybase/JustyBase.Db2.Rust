use justybase_db2::{Db2Session, Db2Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 2
        || args[0].len() != 35
        || !args[0].starts_with("DM_")
        || !args[0][3..].bytes().all(|b| b.is_ascii_hexdigit())
        || !["direct", "prepared"].contains(&args[1].as_str())
    {
        return Err("invalid controlled fixture".into());
    }
    let table = &args[0];
    let mut db = Db2Session::connect_dsn(&std::env::var("DB2_TEST_DSN")?).await?;
    let cases = [
        (format!("INSERT INTO {table} VALUES (1,10)"), 1),
        (
            format!("INSERT INTO {table} VALUES (2,20),(3,30),(4,40)"),
            3,
        ),
        (format!("UPDATE {table} SET V=V+1 WHERE ID=0"), 0),
        (format!("UPDATE {table} SET V=V+1 WHERE ID=1"), 1),
        (format!("UPDATE {table} SET V=V+1"), 4),
        (format!("DELETE FROM {table} WHERE ID=0"), 0),
        (format!("DELETE FROM {table} WHERE ID=1"), 1),
        (format!("DELETE FROM {table}"), 3),
        (format!("DELETE FROM {table}"), 0),
    ];
    for (sql, expected) in cases {
        let count = if args[1] == "direct" {
            db.execute(&sql).await?
        } else {
            let stmt = db.prepare(&sql).await?;
            db.execute_prepared(&stmt, &[]).await?
        };
        assert_eq!(
            count,
            expected,
            "{}",
            sql.split_whitespace().next().unwrap()
        );
    }
    let sql = format!("INSERT INTO {table} VALUES (?,?)");
    let rows: Vec<_> = (5..=7)
        .map(|id| vec![Db2Value::Integer(id), Db2Value::Integer(id * 10)])
        .collect();
    assert_eq!(db.execute_batch_with_params(&sql, &rows).await?, 3);
    assert_eq!(db.execute_batch_with_params(&sql, &[]).await?, 0);
    assert_eq!(
        db.query(&format!("SELECT * FROM {table} ORDER BY ID"))
            .await?
            .rows,
        rows
    );
    db.close().await?;
    Ok(())
}
