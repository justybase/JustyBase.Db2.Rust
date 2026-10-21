//! Opt-in live tests against an AUTHENTICATION=SERVER_ENCRYPT profile, e.g.
//! `DB2_SECMEC7_TEST_DSN=db2://DB2INST1:<password>@localhost:50000/TESTDB?security_mechanism=7`.
//! A plain early return counts as passed, not ignored, so the serial gate
//! stays green when the profile is absent.
use justybase_db2::{Db2ErrorKind, Db2Session, Db2Value, parse_dsn};

fn dsn_with_mechanism(dsn: &str, mechanism: Option<&str>) -> String {
    let (base, query) = dsn.split_once('?').unwrap_or((dsn, ""));
    let mut pairs: Vec<String> = query
        .split('&')
        .filter(|pair| {
            !pair.is_empty()
                && !pair.to_ascii_lowercase().starts_with("security_mechanism=")
                && !pair.to_ascii_lowercase().starts_with("secmec=")
        })
        .map(str::to_string)
        .collect();
    if let Some(mechanism) = mechanism {
        pairs.push(format!("security_mechanism={mechanism}"));
    }
    if pairs.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", pairs.join("&"))
    }
}

#[tokio::test]
async fn secmec7_live_authentication_against_server_encrypt_profile() {
    let Ok(dsn) = std::env::var("DB2_SECMEC7_TEST_DSN") else {
        return;
    };
    let mut db = Db2Session::connect_dsn(&dsn_with_mechanism(&dsn, Some("7")))
        .await
        .unwrap();
    let scalar = db.query("VALUES 42").await.unwrap();
    assert_eq!(scalar.rows, vec![vec![Db2Value::Integer(42)]]);
    let bound = db
        .query_with_params("VALUES CAST(? AS INTEGER)", &[Db2Value::Integer(43)])
        .await
        .unwrap();
    assert_eq!(bound.rows, vec![vec![Db2Value::Integer(43)]]);
}

#[tokio::test]
async fn secmec7_live_profile_rejects_explicit_mechanism_3_without_switching() {
    let Ok(dsn) = std::env::var("DB2_SECMEC7_TEST_DSN") else {
        return;
    };
    let error = match Db2Session::connect_dsn(&dsn_with_mechanism(&dsn, Some("3"))).await {
        Ok(_) => panic!("SERVER_ENCRYPT must reject an explicit SECMEC 3"),
        Err(error) => error,
    };
    assert_eq!(error.kind, Db2ErrorKind::Unsupported);
    let message = error.to_string();
    assert!(
        message.contains("rejected security mechanism 3") && message.contains("7"),
        "{message}"
    );
}

#[tokio::test]
async fn secmec7_live_profile_unset_mechanism_connects_through_an_encrypting_mechanism() {
    let Ok(dsn) = std::env::var("DB2_SECMEC7_TEST_DSN") else {
        return;
    };
    let mut db = Db2Session::connect_dsn(&dsn_with_mechanism(&dsn, None))
        .await
        .unwrap();
    let scalar = db.query("VALUES 44").await.unwrap();
    assert_eq!(scalar.rows, vec![vec![Db2Value::Integer(44)]]);
}

#[tokio::test]
async fn secmec7_live_profile_reports_a_wrong_password_as_an_authentication_failure() {
    let Ok(dsn) = std::env::var("DB2_SECMEC7_TEST_DSN") else {
        return;
    };
    let mut config = parse_dsn(&dsn_with_mechanism(&dsn, Some("7"))).unwrap();
    config.password.push_str("-wrong");
    let error = match Db2Session::connect(config).await {
        Ok(_) => panic!("a wrong password must fail"),
        Err(error) => error,
    };
    assert_eq!(error.kind, Db2ErrorKind::AuthenticationFailed);
    assert!(error.to_string().contains("SECCHKCD=15"), "{error}");
}
