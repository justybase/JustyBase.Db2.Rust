//! Usage example for `justybase-db2`: connect, query, a prepared statement, a
//! transaction, and error handling that stays valid as the public enums grow.
//!
//! Run against a Db2 LUW server:
//!
//! ```text
//! DB2_DSN=db2://user:password@localhost:50000/TESTDB cargo run --example usage
//! ```
//!
//! Without `DB2_DSN` the example prints a hint and exits successfully.

use justybase_db2::{Db2OperationOutcome, Db2Session, Db2Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(dsn) = std::env::var("DB2_DSN") else {
        println!("set DB2_DSN=db2://user:password@host:port/database to run this example");
        return Ok(());
    };
    let mut db = Db2Session::connect_dsn(&dsn).await?;

    let scalar = db.query("VALUES 1").await?;
    println!("VALUES 1 -> {:?}", scalar.rows);

    let statement = db.prepare("VALUES CAST(? AS INTEGER)").await?;
    let bound = db
        .query_prepared(&statement, &[Db2Value::Integer(42)])
        .await?;
    println!("prepared -> {:?}", bound.rows);

    // A transaction that is not committed is rolled back.
    let mut transaction = db.begin_transaction()?;
    let inside = transaction.query("VALUES 2").await?;
    println!("inside transaction -> {:?}", inside.rows);
    transaction.rollback().await?;

    // Match the operation outcome with a wildcard arm: the enum is non_exhaustive,
    // so new outcome variants are not a breaking change for this code.
    match db.query("VALUES 1 +").await {
        Ok(_) => println!("unexpectedly succeeded"),
        Err(error) => {
            let guidance = match error.outcome {
                Db2OperationOutcome::NotStarted => "not sent; safe to retry",
                Db2OperationOutcome::Confirmed => "server confirmed; inspect before retrying",
                _ => "outcome unknown; do not retry blindly",
            };
            println!("SQLSTATE {:?}: {guidance}", error.sql_state);
        }
    }

    db.close().await?;
    Ok(())
}
