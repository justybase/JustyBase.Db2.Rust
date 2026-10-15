//! Opt-in live TLS trust-store checks. They run against a TLS endpoint in front of
//! a Db2 LUW server, for example:
//!
//! `DB2_TLS_TEST_DSN=db2s://DB2INST1:<password>@127.0.0.1:50001/TESTDB`
//! `DB2_TLS_TEST_CA=/path/to/ca.pem` (CA that issued the endpoint certificate)
//! `DB2_TLS_TEST_UNTRUSTED_CA=/path/to/other-ca.pem` (any CA that did not)
//!
//! Without these variables the tests return early and count as passed, so the
//! serial live gate stays green when no TLS endpoint is configured.
use justybase_db2::{Db2Session, Db2Value, parse_dsn};

fn tls_config(ca_path: &str) -> Option<justybase_db2::Config> {
    let Ok(dsn) = std::env::var("DB2_TLS_TEST_DSN") else {
        return None;
    };
    let mut config = parse_dsn(&dsn).expect("DB2_TLS_TEST_DSN must be a valid db2s:// DSN");
    assert!(
        config.use_ssl,
        "DB2_TLS_TEST_DSN must use the db2s:// scheme"
    );
    config.ssl_root_ca_path = ca_path.to_string();
    Some(config)
}

#[tokio::test]
async fn tls_live_connects_when_configured_ca_issued_the_server_certificate() {
    let Ok(ca) = std::env::var("DB2_TLS_TEST_CA") else {
        return;
    };
    let Some(config) = tls_config(&ca) else {
        return;
    };
    let mut db = Db2Session::connect(config).await.unwrap();
    let result = db.query("VALUES 42").await.unwrap();
    assert_eq!(result.rows, vec![vec![Db2Value::Integer(42)]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn tls_live_rejects_server_certificate_from_an_untrusted_ca() {
    let Ok(untrusted) = std::env::var("DB2_TLS_TEST_UNTRUSTED_CA") else {
        return;
    };
    let Some(config) = tls_config(&untrusted) else {
        return;
    };
    let Err(error) = Db2Session::connect(config).await else {
        panic!("connection succeeded with a CA that did not issue the server certificate");
    };
    assert!(error.to_string().contains("TLS"), "{error}");
}
