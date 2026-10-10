use justybase_db2::{Db2Session, Db2Value};
use std::time::Instant;

const SQL: &str = "SELECT a.COLNO, a.COLNAME, b.COLNO FROM SYSCAT.COLUMNS a CROSS JOIN SYSCAT.COLUMNS b FETCH FIRST 10000 ROWS ONLY";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dsn = std::env::var("DB2_TEST_DSN")?;
    let mut db = Db2Session::connect_dsn(&dsn).await?;
    db.query(SQL).await?;
    let mut times = Vec::new();
    for iteration in 0..7 {
        let started = Instant::now();
        let result = db.query(SQL).await?;
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        if result.rows.len() != 10_000 {
            return Err(format!("expected 10000 rows; got {}", result.rows.len()).into());
        }
        let mut checksum = 0i64;
        for row in &result.rows {
            if let [
                Db2Value::Integer(first),
                Db2Value::Text(name),
                Db2Value::Integer(last),
            ] = row.as_slice()
            {
                checksum += first + name.chars().count() as i64 + last;
            } else {
                return Err("unexpected result type".into());
            }
        }
        times.push(elapsed);
        println!(
            "rust iteration={} rows={} checksum={} ms={:.2}",
            iteration + 1,
            result.rows.len(),
            checksum,
            elapsed
        );
    }
    times.sort_by(f64::total_cmp);
    println!("median rust={:.2} ms", times[3]);
    Ok(())
}
