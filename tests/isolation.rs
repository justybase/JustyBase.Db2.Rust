use justybase_db2::{Db2IsolationLevel, Db2Pool, Db2Session, Db2Value};

async fn current_schema(db: &mut Db2Session) -> String {
    match &db.query("VALUES CURRENT SCHEMA").await.unwrap().rows[0][0] {
        Db2Value::Text(value) => format!("\"{}\"", value.trim_end().replace('"', "\"\"")),
        other => panic!("unexpected CURRENT SCHEMA type: {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and live DB2"]
async fn live_ur_reads_dirty_data_and_rs_rr_wait_for_rollback() {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut writer = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut reader = Db2Session::connect_dsn(&dsn).await.unwrap();
    let table = format!(
        "ISO_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut writer).await;
    writer
        .execute(&format!(
            "CREATE TABLE {schema}.{table} (ID INT NOT NULL PRIMARY KEY, V INT)"
        ))
        .await
        .unwrap();
    writer
        .execute(&format!("INSERT INTO {table} VALUES (1,0)"))
        .await
        .unwrap();
    for level in [
        Db2IsolationLevel::UncommittedRead,
        Db2IsolationLevel::ReadStability,
        Db2IsolationLevel::RepeatableRead,
    ] {
        let mut write = writer.begin_transaction().unwrap();
        write
            .execute(&format!("UPDATE {table} SET V=99 WHERE ID=1"))
            .await
            .unwrap();
        let mut read = reader
            .begin_transaction_with_isolation(level)
            .await
            .unwrap();
        let sql = format!("SELECT V FROM {table} WHERE ID=1");
        {
            // Keep the future alive: abandoning a partially completed exchange poisons the session.
            let pending = read.query(&sql);
            tokio::pin!(pending);
            if level == Db2IsolationLevel::UncommittedRead {
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(15), &mut pending)
                        .await
                        .unwrap()
                        .unwrap()
                        .rows[0][0],
                    Db2Value::Integer(99)
                );
                write.rollback().await.unwrap();
            } else {
                assert!(
                    tokio::time::timeout(Duration::from_millis(200), &mut pending)
                        .await
                        .is_err()
                );
                write.rollback().await.unwrap();
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(15), &mut pending)
                        .await
                        .unwrap()
                        .unwrap()
                        .rows[0][0],
                    Db2Value::Integer(0)
                );
            }
        }
        read.rollback().await.unwrap();
        assert_eq!(
            reader.query(&sql).await.unwrap().rows[0][0],
            Db2Value::Integer(0)
        );
    }
    writer
        .execute(&format!("DROP TABLE {schema}.{table}"))
        .await
        .unwrap();
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}
async fn register(db: &mut Db2Session) -> String {
    match &db.query("VALUES CURRENT ISOLATION").await.unwrap().rows[0][0] {
        Db2Value::Text(value) => value.trim().to_owned(),
        _ => panic!("unexpected isolation type"),
    }
}
#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and live DB2"]
async fn live_scoped_isolation_restores_register_after_commit_and_rollback() {
    let mut db = Db2Session::connect_dsn(&std::env::var("DB2_TEST_DSN").unwrap())
        .await
        .unwrap();
    let baseline = register(&mut db).await;
    for (level, code) in [
        (Db2IsolationLevel::UncommittedRead, "UR"),
        (Db2IsolationLevel::CursorStability, "CS"),
        (Db2IsolationLevel::ReadStability, "RS"),
        (Db2IsolationLevel::RepeatableRead, "RR"),
    ] {
        for commit in [false, true] {
            let mut tx = db.begin_transaction_with_isolation(level).await.unwrap();
            assert_eq!(
                tx.query("VALUES CURRENT ISOLATION").await.unwrap().rows,
                vec![vec![Db2Value::Text(code.into())]]
            );
            if commit {
                tx.commit().await.unwrap();
            } else {
                tx.rollback().await.unwrap();
            }
            assert_eq!(register(&mut db).await, baseline);
        }
    }
    db.execute("SET CURRENT ISOLATION UR").await.unwrap();
    db.begin_transaction_with_isolation(Db2IsolationLevel::CursorStability)
        .await
        .unwrap()
        .rollback()
        .await
        .unwrap();
    assert_eq!(register(&mut db).await, "UR");
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and live DB2"]
async fn live_pool_resets_raw_schema_and_isolation_and_reuses_the_same_session() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let table = format!(
        "C2POOL_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut setup = Db2Session::connect_dsn(&dsn).await.unwrap();
    let initial_schema =
        match setup.query("VALUES CURRENT SCHEMA").await.unwrap().rows[0][0].clone() {
            Db2Value::Text(value) => value.trim().to_owned(),
            other => panic!("unexpected CURRENT SCHEMA type: {other:?}"),
        };
    let qualified_table = format!("\"{}\".{table}", initial_schema.replace('"', "\"\""));
    setup
        .execute(&format!("CREATE TABLE {qualified_table} (V INT)"))
        .await
        .unwrap();
    setup
        .execute(&format!("INSERT INTO {qualified_table} VALUES (73)"))
        .await
        .unwrap();
    setup.close().await.unwrap();

    let pool = Db2Pool::connect_dsn(&dsn, 1).unwrap();
    let (base_schema, base_isolation, base_client_registers) = {
        let mut lease = pool.acquire().await.unwrap();
        let schema = lease.query("VALUES CURRENT SCHEMA").await.unwrap();
        let isolation = lease.query("VALUES CURRENT ISOLATION").await.unwrap();
        let client_registers = lease.query("SELECT COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_ACCTNG, '') FROM SYSIBM.SYSDUMMY1").await.unwrap();
        let base_schema = match &schema.rows[0][0] {
            Db2Value::Text(value) => value.clone(),
            other => panic!("unexpected CURRENT SCHEMA type: {other:?}"),
        };
        let base_isolation = match &isolation.rows[0][0] {
            Db2Value::Text(value) => value.clone(),
            other => panic!("unexpected CURRENT ISOLATION type: {other:?}"),
        };

        lease
            .execute("SET CURRENT SCHEMA \"C2POOLALT\"")
            .await
            .unwrap();
        lease.execute("SET CURRENT ISOLATION UR").await.unwrap();
        lease
            .execute("CALL SYSPROC.WLM_SET_CLIENT_INFO('C2_WLM_USER', 'C2_WLM_HOST', 'C2_WLM_APPL', 'C2_WLM_ACCT', NULL)")
            .await
            .unwrap();
        (
            base_schema,
            base_isolation,
            client_registers.rows[0].clone(),
        )
    };

    assert_eq!(pool.session_count(), 1);
    {
        let mut lease = pool.acquire().await.unwrap();
        assert_eq!(pool.session_count(), 1);
        let schema = lease.query("VALUES CURRENT SCHEMA").await.unwrap();
        let isolation = lease.query("VALUES CURRENT ISOLATION").await.unwrap();
        assert_eq!(schema.rows[0][0], Db2Value::Text(base_schema));
        assert_eq!(isolation.rows[0][0], Db2Value::Text(base_isolation));
        let client_info = lease.query("SELECT COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_ACCTNG, '') FROM SYSIBM.SYSDUMMY1").await.unwrap();
        assert_eq!(client_info.rows, vec![base_client_registers]);
        assert_eq!(
            lease
                .query(&format!("SELECT V FROM {qualified_table}"))
                .await
                .unwrap()
                .rows,
            vec![vec![Db2Value::Integer(73)]],
            "the session scoped table proves this is the same physical DB2 session"
        );
    }
    pool.close().await.unwrap();
    assert_eq!(pool.session_count(), 0);
    let mut cleanup = Db2Session::connect_dsn(&dsn).await.unwrap();
    cleanup
        .execute(&format!("DROP TABLE {qualified_table}"))
        .await
        .unwrap();
    cleanup.close().await.unwrap();
}
