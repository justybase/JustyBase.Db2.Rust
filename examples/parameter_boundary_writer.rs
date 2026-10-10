use justybase_db2::{Db2Session, Db2Value};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 3 {
        return Err("expected fixture table, mode, rows".into());
    }
    let table = &args[0];
    let rows: usize = args[2].parse()?;
    if table.len() != 35
        || !table.starts_with("BP_")
        || !table[3..].bytes().all(|b| b.is_ascii_hexdigit())
        || ![21, 22, 37, 85, 275, 1000].contains(&rows)
        || !["direct", "prepared"].contains(&args[1].as_str())
    {
        return Err("invalid controlled fixture arguments".into());
    }
    let mut db = Db2Session::connect_dsn(&std::env::var("DB2_TEST_DSN")?).await?;
    for width in [85, 169] {
        let sql = format!(
            "SELECT {} FROM SYSIBM.SYSDUMMY1",
            (1..=width)
                .map(|i| format!("{i} AS C{i}"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let result = db.query(&sql).await?;
        let expected: Vec<_> = (1..=width).map(Db2Value::Integer).collect();
        if result.columns.len() != width as usize || result.rows != vec![expected] {
            return Err("wide result descriptor/value mismatch".into());
        }
    }
    let sql = format!(
        "INSERT INTO {table} VALUES {}",
        vec!["(?,?,?,?)"; rows].join(",")
    );
    let values: Vec<_> = (1..=rows as i64)
        .flat_map(|i| {
            [
                Db2Value::Integer(i),
                Db2Value::Integer(i * 101),
                if i % 2 == 0 {
                    Db2Value::Null
                } else {
                    Db2Value::Integer(-i)
                },
                Db2Value::Integer(i * 10007),
            ]
        })
        .collect();
    if args[1] == "prepared" {
        let statement = db.prepare(&sql).await?;
        db.execute_prepared(&statement, &values).await?;
    } else {
        db.execute_with_params(&sql, &values).await?;
    }
    db.close().await?;
    Ok(())
}
