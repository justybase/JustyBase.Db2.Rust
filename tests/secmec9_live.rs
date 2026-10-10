//! Opt-in live test against a SERVER_ENCRYPT profile, e.g.
//! `DB2_SECMEC9_TEST_DSN=db2://DB2INST1:<password>@localhost:50002/TESTDB?security_mechanism=9`.
//! A plain early return counts as passed, not ignored, so the serial gate
//! stays green when the profile is absent.
use justybase_db2::{Db2Session, Db2Value};

#[tokio::test]
async fn secmec9_live_authentication_against_server_encrypt_profile() {
    let Ok(dsn) = std::env::var("DB2_SECMEC9_TEST_DSN") else {
        return;
    };
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let scalar = db.query("VALUES 42").await.unwrap();
    assert_eq!(scalar.rows, vec![vec![Db2Value::Integer(42)]]);
    let bound = db
        .query_with_params("VALUES CAST(? AS INTEGER)", &[Db2Value::Integer(43)])
        .await
        .unwrap();
    assert_eq!(bound.rows, vec![vec![Db2Value::Integer(43)]]);
}
