use justybase_db2::{
    Db2ClientInfo, Db2CommandOptions, Db2Error, Db2ErrorKind, Db2ImportColumn, Db2ImportPolicy,
    Db2ImportTarget, Db2InputParameter, Db2Parameter, Db2ParameterDirection, Db2Pool, Db2Session,
    Db2Value, Db2ValueRef, Db2Warning,
};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::sync::mpsc;

async fn current_schema(db: &mut Db2Session) -> String {
    match &db.query("VALUES CURRENT SCHEMA").await.unwrap().rows[0][0] {
        Db2Value::Text(value) => format!("\"{}\"", value.trim_end().replace('"', "\"\"")),
        other => panic!("unexpected CURRENT SCHEMA type: {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_sqlca_warnings_are_reported_for_materialized_prepared_and_streamed_queries() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let (sender, mut warnings) = mpsc::unbounded_channel::<Db2Warning>();
    db.set_warning_handler(move |warning| {
        let _ = sender.send(warning);
    });
    let sql = "SELECT COUNT(*), CAST(AVG(X) AS DOUBLE) FROM (VALUES (1), (CAST(NULL AS INTEGER)), (3)) AS T(X)";

    let result = db.query(sql).await.unwrap();
    assert_eq!(
        result.rows,
        vec![vec![Db2Value::Integer(3), Db2Value::Float(2.0)]]
    );
    let warning = warnings.try_recv().unwrap();
    assert_eq!(warning.sql_state, "01003");
    assert!(warning.warning_flags.starts_with('W'));
    assert!(
        warnings.try_recv().is_err(),
        "materialized query should emit exactly one warning"
    );

    let statement = db.prepare(sql).await.unwrap();
    let result = db.query_prepared(&statement, &[]).await.unwrap();
    assert_eq!(
        result.rows,
        vec![vec![Db2Value::Integer(3), Db2Value::Float(2.0)]]
    );
    assert_eq!(warnings.try_recv().unwrap().sql_state, "01003");
    assert!(
        warnings.try_recv().is_err(),
        "prepared query should emit exactly one warning"
    );

    let mut rows = Vec::new();
    {
        let mut reader = db.query_stream(sql).await.unwrap();
        while let Some(row) = reader.next().await.unwrap() {
            rows.push(row);
        }
    }
    assert_eq!(rows, vec![vec![Db2Value::Integer(3), Db2Value::Float(2.0)]]);
    assert_eq!(warnings.try_recv().unwrap().sql_state, "01003");
    assert!(
        warnings.try_recv().is_err(),
        "streamed query should emit exactly one warning"
    );

    db.clear_warning_handler();
    db.query(sql).await.unwrap();
    assert!(
        warnings.try_recv().is_err(),
        "clearing the handler should stop notifications"
    );

    let pool = Db2Pool::connect_dsn(&dsn, 1).unwrap();
    let (pool_sender, mut pooled_warnings) = mpsc::unbounded_channel::<Db2Warning>();
    {
        let mut lease = pool.acquire().await.unwrap();
        lease.set_warning_handler(move |warning| {
            let _ = pool_sender.send(warning);
        });
        lease.query(sql).await.unwrap();
        assert_eq!(pooled_warnings.try_recv().unwrap().sql_state, "01003");
        assert!(pooled_warnings.try_recv().is_err());
    }
    {
        let mut lease = pool.acquire().await.unwrap();
        lease.query(sql).await.unwrap();
        assert!(
            pooled_warnings.try_recv().is_err(),
            "the prior lease's handler must be cleared"
        );
    }
    pool.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_typed_nulls_integer_boundaries_and_floats() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    assert_eq!(db.query("VALUES (CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(30)), CAST(NULL AS DATE), CAST(NULL AS BLOB(100)))").await.unwrap().rows,
        vec![vec![Db2Value::Null, Db2Value::Null, Db2Value::Null, Db2Value::Null]]);
    assert_eq!(db.query("VALUES (CAST(-2147483648 AS INTEGER), CAST(9223372036854775807 AS BIGINT), CAST(-32768 AS SMALLINT))").await.unwrap().rows,
        vec![vec![Db2Value::Integer(i32::MIN as i64), Db2Value::Integer(i64::MAX), Db2Value::Integer(i16::MIN as i64)]]);
    assert_eq!(db.query("VALUES (CAST(1.25 AS REAL), CAST(-2.5 AS DOUBLE), CAST(TRUE AS BOOLEAN), CAST(FALSE AS BOOLEAN))").await.unwrap().rows,
        vec![vec![Db2Value::Float(1.25), Db2Value::Float(-2.5), Db2Value::Boolean(true), Db2Value::Boolean(false)]]);
    let fractional_sql = "VALUES (CAST(? AS SMALLINT), CAST(? AS SMALLINT), CAST(? AS SMALLINT), CAST(? AS SMALLINT), CAST(? AS SMALLINT), CAST(? AS SMALLINT), CAST(? AS SMALLINT), CAST(? AS SMALLINT))";
    let fractional_parameters = [
        Db2Value::Float(-3.75),
        Db2Value::Float(-2.5),
        Db2Value::Float(-1.5),
        Db2Value::Float(-0.5),
        Db2Value::Float(0.5),
        Db2Value::Float(1.5),
        Db2Value::Float(2.5),
        Db2Value::Float(3.75),
    ];
    let fractional_rows = db
        .query_with_params(fractional_sql, &fractional_parameters)
        .await
        .unwrap()
        .rows;
    assert_eq!(
        fractional_rows,
        vec![vec![
            Db2Value::Integer(-3),
            Db2Value::Integer(-2),
            Db2Value::Integer(-1),
            Db2Value::Integer(0),
            Db2Value::Integer(0),
            Db2Value::Integer(1),
            Db2Value::Integer(2),
            Db2Value::Integer(3),
        ]]
    );
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_empty_and_null_lob_values_remain_distinct() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let expressions = [
        "CAST(NULL AS BLOB(1M))",
        "CAST(X'' AS BLOB(1M))",
        "CAST(NULL AS CLOB(1M))",
        "CAST('' AS CLOB(1M))",
        "CAST(NULL AS DBCLOB(1M))",
        "CAST('' AS DBCLOB(1M))",
        "CAST('DBCLOB 東京' AS DBCLOB(1M))",
        "CAST(NULL AS XML)",
    ];
    let expected = vec![
        Db2Value::Null,
        Db2Value::Bytes(Vec::new()),
        Db2Value::Null,
        Db2Value::Text(String::new()),
        Db2Value::Null,
        Db2Value::Text(String::new()),
        Db2Value::Text("DBCLOB 東京".into()),
        Db2Value::Null,
    ];

    for (expression, expected_value) in expressions.iter().zip(&expected) {
        let query = format!("VALUES {expression}");
        assert_eq!(
            db.query(&query).await.unwrap().rows,
            vec![vec![expected_value.clone()]],
            "{expression}"
        );
    }
    let query = format!("VALUES ({})", expressions.join(", "));
    assert_eq!(db.query(&query).await.unwrap().rows, vec![expected.clone()]);

    let mut reader = db.query_stream(&query).await.unwrap();
    assert_eq!(reader.next().await.unwrap(), Some(expected));
    assert_eq!(reader.next().await.unwrap(), None);
    drop(reader);

    assert_eq!(
        db.query("VALUES 29").await.unwrap().rows,
        vec![vec![Db2Value::Integer(29)]]
    );
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_zero_length_typed_lob_streams_insert_empty_values() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut empty_blob = tokio::io::BufReader::new(std::io::Cursor::new(Vec::<u8>::new()));
    let mut blob_parameters = [Db2InputParameter::BlobStream {
        source: &mut empty_blob,
        length: 0,
    }];
    let blob_length = db
        .query_with_blob_streams("VALUES LENGTH(CAST(? AS BLOB(1M)))", &mut blob_parameters)
        .await
        .unwrap();
    assert_eq!(blob_length.rows, vec![vec![Db2Value::Integer(0)]]);
    let table = format!(
        "RS_ZERO_LOB_STREAM_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (ID INTEGER NOT NULL PRIMARY KEY, C CLOB(1M), D DBCLOB(1M), X XML)"
    ))
    .await
    .unwrap();

    let result = async {
        let null = Db2Value::Null;
        let first_id = Db2Value::Integer(1);
        let mut empty_clob = tokio::io::BufReader::new(std::io::Cursor::new(Vec::<u8>::new()));
        let mut parameters = [
            Db2InputParameter::Value(&first_id),
            Db2InputParameter::ClobStream {
                source: &mut empty_clob,
                length: 0,
            },
            Db2InputParameter::Value(&null),
            Db2InputParameter::Value(&null),
        ];
        let inserted = db
            .execute_with_blob_streams(
                &format!("INSERT INTO {table} VALUES (?, ?, ?, ?)"),
                &mut parameters,
            )
            .await?;
        if inserted != 1 {
            return Err(Db2Error::other(format!(
                "zero-length CLOB stream reported SQLCARD row count {inserted}"
            )));
        }

        let second_id = Db2Value::Integer(2);
        let mut empty_dbclob = tokio::io::BufReader::new(std::io::Cursor::new(Vec::<u8>::new()));
        let mut parameters = [
            Db2InputParameter::Value(&second_id),
            Db2InputParameter::Value(&null),
            Db2InputParameter::DbClobStream {
                source: &mut empty_dbclob,
                length: 0,
            },
            Db2InputParameter::Value(&null),
        ];
        let inserted = db
            .execute_with_blob_streams(
                &format!("INSERT INTO {table} VALUES (?, ?, ?, ?)"),
                &mut parameters,
            )
            .await?;
        if inserted != 1 {
            return Err(Db2Error::other(format!(
                "zero-length DBCLOB stream reported SQLCARD row count {inserted}"
            )));
        }

        let third_id = Db2Value::Integer(3);
        let xml = b"<empty/>";
        let mut xml_source = tokio::io::BufReader::new(std::io::Cursor::new(xml));
        let mut parameters = [
            Db2InputParameter::Value(&third_id),
            Db2InputParameter::Value(&null),
            Db2InputParameter::Value(&null),
            Db2InputParameter::XmlStream {
                source: &mut xml_source,
                length: xml.len() as u64,
            },
        ];
        let inserted = db
            .execute_with_blob_streams(
                &format!("INSERT INTO {table} VALUES (?, ?, ?, ?)"),
                &mut parameters,
            )
            .await?;
        if inserted != 1 {
            return Err(Db2Error::other(format!(
                "XML stream reported SQLCARD row count {inserted}"
            )));
        }

        let fourth_id = Db2Value::Integer(4);
        let mut empty_clob = tokio::io::BufReader::new(std::io::Cursor::new(Vec::<u8>::new()));
        let mut empty_dbclob =
            tokio::io::BufReader::new(std::io::Cursor::new(Vec::<u8>::new()));
        let xml = b"<multiple-empty-streams/>";
        let mut xml_source = tokio::io::BufReader::new(std::io::Cursor::new(xml));
        let mut parameters = [
            Db2InputParameter::Value(&fourth_id),
            Db2InputParameter::ClobStream {
                source: &mut empty_clob,
                length: 0,
            },
            Db2InputParameter::DbClobStream {
                source: &mut empty_dbclob,
                length: 0,
            },
            Db2InputParameter::XmlStream {
                source: &mut xml_source,
                length: xml.len() as u64,
            },
        ];
        let combined_affected = db
            .execute_with_blob_streams(&format!("INSERT INTO {table} VALUES (?, ?, ?, ?)"), &mut parameters)
            .await?;

        let rows = db
            .query(&format!(
                "SELECT ID, C, D, XMLSERIALIZE(X AS CLOB(1M)) FROM {table} ORDER BY ID"
            ))
            .await?;
        if combined_affected != 1
            || rows.rows.len() != 4
            || rows.rows[0]
                != vec![
                    Db2Value::Integer(1),
                    Db2Value::Text(String::new()),
                    Db2Value::Null,
                    Db2Value::Null,
                ]
            || rows.rows[1]
                != vec![
                    Db2Value::Integer(2),
                    Db2Value::Null,
                    Db2Value::Text(String::new()),
                    Db2Value::Null,
                ]
            || rows.rows[2][0] != Db2Value::Integer(3)
            || rows.rows[2][1] != Db2Value::Null
            || rows.rows[2][2] != Db2Value::Null
            || !matches!(&rows.rows[2][3], Db2Value::Text(value) if value.contains("<empty"))
            || rows.rows[3][0] != Db2Value::Integer(4)
            || rows.rows[3][1] != Db2Value::Text(String::new())
            || rows.rows[3][2] != Db2Value::Text(String::new())
            || !matches!(&rows.rows[3][3], Db2Value::Text(value) if value.contains("multiple-empty-streams"))
        {
            return Err(Db2Error::other(format!(
                "zero-length typed LOB streams changed row, empty, or NULL semantics (SQLCARD reported {combined_affected} affected rows; query returned {})",
                rows.rows.len()
            )));
        }
        Ok::<(), Db2Error>(())
    }
    .await;
    let cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
    result.unwrap();
    cleanup.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_date_time_unicode_and_binary_values() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    assert_eq!(db.query("VALUES (CAST('2024-02-29' AS DATE), CAST('23:59:58' AS TIME), CAST('2024-02-29 23:59:58.123456' AS TIMESTAMP))").await.unwrap().rows,
        vec![vec![Db2Value::Text("2024-02-29".into()), Db2Value::Text("23.59.58".into()), Db2Value::Text("2024-02-29-23.59.58.123456".into())]]);
    assert_eq!(
        db.query("VALUES (CAST('Zażółć 東京' AS VARCHAR(50)), CAST(X'00FF7F' AS VARBINARY(10)))")
            .await
            .unwrap()
            .rows,
        vec![vec![
            Db2Value::Text("Zażółć 東京".into()),
            Db2Value::Bytes(vec![0, 255, 127])
        ]]
    );
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_temporal_text_parameters_match_ibm_provider() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let sql = "VALUES (CAST(? AS DATE), CAST(? AS DATE), CAST(? AS TIME), CAST(? AS TIME), CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))";
    let values = vec![
        Db2Value::Text("2024-2-9".into()),
        Db2Value::Text("02/29/2024".into()),
        Db2Value::Text("12:34".into()),
        Db2Value::Text("23.59.58".into()),
        Db2Value::Text("2024-02-29T23:59:58.123456".into()),
        Db2Value::Text("2024-02-29".into()),
    ];
    let expected = vec![vec![
        Db2Value::Text("2024-02-09".into()),
        Db2Value::Text("2024-02-29".into()),
        Db2Value::Text("12.34.00".into()),
        Db2Value::Text("23.59.58".into()),
        Db2Value::Text("2024-02-29-23.59.58.123456".into()),
        Db2Value::Text("2024-02-29-00.00.00.000000".into()),
    ]];
    assert_eq!(
        db.query_with_params(sql, &values).await.unwrap().rows,
        expected
    );
    let statement = db.prepare(sql).await.unwrap();
    assert_eq!(
        db.query_prepared(&statement, &values).await.unwrap().rows,
        expected
    );
    let mut stream = db.query_stream_with_params(sql, &values).await.unwrap();
    assert_eq!(stream.next().await.unwrap(), expected.first().cloned());
    assert_eq!(stream.next().await.unwrap(), None);
    stream.close().await.unwrap();

    let edge_sql = "VALUES (CAST(? AS TIME), CAST(? AS DATE), CAST(? AS TIME))";
    let edge_values = vec![
        Db2Value::Text("24:00:00".into()),
        Db2Value::Text("2024-02-29T23:59:58.123456".into()),
        Db2Value::Text("2024-02-29 23:59:58.123456".into()),
    ];
    assert_eq!(
        db.query_with_params(edge_sql, &edge_values)
            .await
            .unwrap()
            .rows,
        vec![vec![
            Db2Value::Text("24.00.00".into()),
            Db2Value::Text("2024-02-29".into()),
            Db2Value::Text("23.59.58".into()),
        ]]
    );

    let timestamp_precision12_sql = "VALUES CAST(CAST(? AS TIMESTAMP(12)) AS VARCHAR(32))";
    let timestamp_precision12_values =
        vec![Db2Value::Text("2024-02-29T23:59:58.123456789012".into())];
    let timestamp_precision12_expected = vec![vec![Db2Value::Text(
        "2024-02-29-23.59.58.123456789012".into(),
    )]];
    assert_eq!(
        db.query_with_params(timestamp_precision12_sql, &timestamp_precision12_values)
            .await
            .unwrap()
            .rows,
        timestamp_precision12_expected
    );
    let precision12_statement = db.prepare(timestamp_precision12_sql).await.unwrap();
    assert_eq!(
        db.query_prepared(&precision12_statement, &timestamp_precision12_values)
            .await
            .unwrap()
            .rows,
        timestamp_precision12_expected
    );
    let mut precision12_stream = db
        .query_stream_with_params(timestamp_precision12_sql, &timestamp_precision12_values)
        .await
        .unwrap();
    assert_eq!(
        precision12_stream.next().await.unwrap(),
        timestamp_precision12_expected.first().cloned()
    );
    assert_eq!(precision12_stream.next().await.unwrap(), None);
    precision12_stream.close().await.unwrap();

    for (kind, value) in [
        ("DATE", "2024-02-30"),
        ("TIME", "23:59:58.123456"),
        ("TIME", "24:00:01"),
        ("TIMESTAMP", "2024-02-30 23:59:58"),
        ("TIMESTAMP", "2024-02-29 23:59:58+02:00"),
    ] {
        let sql = format!("VALUES CAST(? AS {kind})");
        assert!(
            db.query_with_params(&sql, &[Db2Value::Text(value.into())])
                .await
                .is_err()
        );
    }
    assert_eq!(
        db.query("VALUES 29").await.unwrap().rows,
        vec![vec![Db2Value::Integer(29)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_ordered_multirow_result_set() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    assert_eq!(
        db.query("SELECT N FROM (VALUES 1, 2, 3, 4, 5) AS T(N) ORDER BY N DESC")
            .await
            .unwrap()
            .rows,
        vec![
            vec![Db2Value::Integer(5)],
            vec![Db2Value::Integer(4)],
            vec![Db2Value::Integer(3)],
            vec![Db2Value::Integer(2)],
            vec![Db2Value::Integer(1)]
        ]
    );
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_query_stream_crosses_reply_blocks_and_drains_after_early_stop() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let query = "SELECT ROW_NUMBER() OVER() AS N FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) A(X), (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) B(X), (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) C(X), (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) D(X), (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) E(X) FETCH FIRST 30000 ROWS ONLY";

    let mut reader = db.query_stream(query).await.unwrap();
    assert_eq!(reader.columns().len(), 1);
    let (mut count, mut checksum) = (0i64, 0i64);
    while let Some(row) = reader.next().await.unwrap() {
        let Db2Value::Integer(value) = &row[0] else {
            panic!("expected BIGINT row number")
        };
        count += 1;
        checksum += *value;
    }
    assert_eq!(count, 30_000);
    assert_eq!(checksum, 450_015_000);
    drop(reader);

    let mut bound_reader = db
        .query_stream_with_params("VALUES CAST(? AS INTEGER)", &[Db2Value::Integer(42)])
        .await
        .unwrap();
    assert_eq!(bound_reader.columns().len(), 1);
    assert_eq!(
        bound_reader.next().await.unwrap(),
        Some(vec![Db2Value::Integer(42)])
    );
    assert_eq!(bound_reader.next().await.unwrap(), None);
    drop(bound_reader);

    let blob: Vec<u8> = (0..70_003).map(|index| (index * 31) as u8).collect();
    let mut lob_reader = db
        .query_stream_with_params(
            "VALUES CAST(? AS BLOB(1M))",
            &[Db2Value::Bytes(blob.clone())],
        )
        .await
        .unwrap();
    assert_eq!(
        lob_reader.next().await.unwrap(),
        Some(vec![Db2Value::Bytes(blob)])
    );
    assert_eq!(lob_reader.next().await.unwrap(), None);
    drop(lob_reader);

    let clob_text = "CLOB café 東京 🚀 ".repeat(5_000);
    let clob_bytes = clob_text.as_bytes().to_vec();
    let mut clob_source = tokio::io::BufReader::new(std::io::Cursor::new(clob_bytes.clone()));
    let mut clob_parameters = [Db2InputParameter::ClobStream {
        source: &mut clob_source,
        length: clob_bytes.len() as u64,
    }];
    let mut clob_reader = db
        .query_stream_with_blob_streams("VALUES CAST(? AS CLOB(2M))", &mut clob_parameters)
        .await
        .unwrap();
    assert_eq!(
        clob_reader.next().await.unwrap(),
        Some(vec![Db2Value::Text(clob_text)])
    );
    assert_eq!(clob_reader.next().await.unwrap(), None);
    drop(clob_reader);

    let dbclob_text = "DBCLOB 東京 🚀 ".repeat(5_000);
    let dbclob_bytes = dbclob_text.as_bytes().to_vec();
    let mut dbclob_source = tokio::io::BufReader::new(std::io::Cursor::new(dbclob_bytes.clone()));
    let mut dbclob_parameters = [Db2InputParameter::DbClobStream {
        source: &mut dbclob_source,
        length: dbclob_bytes.len() as u64,
    }];
    let mut dbclob_reader = db
        .query_stream_with_blob_streams("VALUES CAST(? AS DBCLOB(2M))", &mut dbclob_parameters)
        .await
        .unwrap();
    assert_eq!(
        dbclob_reader.next().await.unwrap(),
        Some(vec![Db2Value::Text(dbclob_text)])
    );
    assert_eq!(dbclob_reader.next().await.unwrap(), None);
    drop(dbclob_reader);

    let xml_text = format!(
        "<root><message>XML café 東京 🚀</message><body>{}</body></root>",
        "x".repeat(100_000)
    );
    let xml_bytes = xml_text.as_bytes().to_vec();
    let mut xml_source = tokio::io::BufReader::new(std::io::Cursor::new(xml_bytes.clone()));
    let mut xml_parameters = [Db2InputParameter::XmlStream {
        source: &mut xml_source,
        length: xml_bytes.len() as u64,
    }];
    let mut xml_reader = db
        .query_stream_with_blob_streams(
            "VALUES XMLSERIALIZE(CAST(? AS XML) AS CLOB(2M))",
            &mut xml_parameters,
        )
        .await
        .unwrap();
    assert_eq!(
        xml_reader.next().await.unwrap(),
        Some(vec![Db2Value::Text(xml_text)])
    );
    assert_eq!(xml_reader.next().await.unwrap(), None);
    drop(xml_reader);

    let mut reader = db.query_stream(query).await.unwrap();
    assert!(reader.next().await.unwrap().is_some());
    reader.close().await.unwrap();
    assert_eq!(
        db.query("VALUES 17").await.unwrap().rows,
        vec![vec![Db2Value::Integer(17)]]
    );

    let pool = Db2Pool::connect_dsn(&dsn, 1).unwrap();
    let mut lease = pool.acquire().await.unwrap();
    let mut pooled_reader = lease.query_stream(query).await.unwrap();
    assert!(pooled_reader.next().await.unwrap().is_some());
    pooled_reader.close().await.unwrap();
    drop(lease);
    let mut lease = pool.acquire().await.unwrap();
    assert_eq!(
        lease.query("VALUES 18").await.unwrap().rows,
        vec![vec![Db2Value::Integer(18)]]
    );
    drop(lease);
    pool.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_query_stream_next_ref_borrows_decoded_text_from_a_flat_row_batch() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut reader = db
        .query_stream("VALUES (CAST(7 AS INTEGER), CAST('borrowed text' AS VARCHAR(20)))")
        .await
        .unwrap();
    {
        let row = reader.next_ref().await.unwrap().unwrap();
        assert_eq!(row.get(0), Some(Db2ValueRef::Integer(7)));
        assert_eq!(row.get(1), Some(Db2ValueRef::Text("borrowed text")));
    }
    assert!(reader.next_ref().await.unwrap().is_none());
    drop(reader);
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_streamed_blob_inputs_round_trip_across_query_dml_batch_and_transaction_paths() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();

    let (mut first, first_stats) = PatternReadStream::new(80_003);
    let (mut second, second_stats) = PatternReadStream::new(16_003);
    let middle = Db2Value::Integer(73);
    let mut mixed = [
        Db2InputParameter::BlobStream {
            source: &mut first,
            length: 80_003,
        },
        Db2InputParameter::Value(&middle),
        Db2InputParameter::BlobStream {
            source: &mut second,
            length: 16_003,
        },
    ];
    let round_trip = db
        .query_with_blob_streams(
            "VALUES (CAST(? AS BLOB(1M)), CAST(? AS INTEGER), LENGTH(CAST(? AS BLOB(1M))))",
            &mut mixed,
        )
        .await
        .unwrap();
    let expected = pattern_bytes(80_003);
    assert_eq!(
        round_trip.rows,
        vec![vec![
            Db2Value::Bytes(expected),
            Db2Value::Integer(73),
            Db2Value::Integer(16_003),
        ]]
    );
    assert_eq!(first_stats.0.load(Ordering::Acquire), 80_003);
    assert_eq!(second_stats.0.load(Ordering::Acquire), 16_003);
    assert!(first_stats.1.load(Ordering::Acquire) <= 32_765);

    let (mut query_stream_source, _) = PatternReadStream::new(2_000_123);
    let mut stream_parameters = [Db2InputParameter::BlobStream {
        source: &mut query_stream_source,
        length: 2_000_123,
    }];
    let mut rows = db
        .query_stream_with_blob_streams(
            "VALUES LENGTH(CAST(? AS BLOB(10M)))",
            &mut stream_parameters,
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap(),
        Some(vec![Db2Value::Integer(2_000_123)])
    );
    assert_eq!(rows.next().await.unwrap(), None);
    drop(rows);

    let table = format!(
        "RS_DB2_STREAM_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (ID INT NOT NULL PRIMARY KEY, B BLOB(10M))"
    ))
    .await
    .unwrap();
    let write_result = async {
        let first_id = Db2Value::Integer(1);
        let (mut insert_stream, insert_stats) = PatternReadStream::new(80_003);
        let mut insert = [
            Db2InputParameter::Value(&first_id),
            Db2InputParameter::BlobStream {
                source: &mut insert_stream,
                length: 80_003,
            },
        ];
        assert_eq!(
            db.execute_with_blob_streams(
                &format!("INSERT INTO {table} VALUES (?, ?)"),
                &mut insert
            )
            .await?,
            1
        );
        assert_eq!(insert_stats.0.load(Ordering::Acquire), 80_003);

        let second_id = Db2Value::Integer(2);
        let third_id = Db2Value::Integer(3);
        let (mut second_stream, _) = PatternReadStream::new(90_007);
        let (mut third_stream, _) = PatternReadStream::new(100_009);
        let mut batch = vec![
            vec![
                Db2InputParameter::Value(&second_id),
                Db2InputParameter::BlobStream {
                    source: &mut second_stream,
                    length: 90_007,
                },
            ],
            vec![
                Db2InputParameter::Value(&third_id),
                Db2InputParameter::BlobStream {
                    source: &mut third_stream,
                    length: 100_009,
                },
            ],
        ];
        assert_eq!(
            db.execute_batch_with_blob_streams(
                &format!("INSERT INTO {table} VALUES (?, ?)"),
                &mut batch
            )
            .await?,
            2
        );

        let before_rollback = db
            .query(&format!("SELECT LENGTH(B) FROM {table} WHERE ID = 1"))
            .await?;
        assert_eq!(before_rollback.rows, vec![vec![Db2Value::Integer(80_003)]]);
        let mut transaction = db.begin_transaction().unwrap();
        let (mut rollback_stream, _) = PatternReadStream::new(200_011);
        let update_id = Db2Value::Integer(1);
        let mut update = [
            Db2InputParameter::BlobStream {
                source: &mut rollback_stream,
                length: 200_011,
            },
            Db2InputParameter::Value(&update_id),
        ];
        transaction
            .execute_with_blob_streams(
                &format!("UPDATE {table} SET B = ? WHERE ID = ?"),
                &mut update,
            )
            .await?;
        transaction.rollback().await?;
        let after_rollback = db
            .query(&format!("SELECT LENGTH(B) FROM {table} WHERE ID = 1"))
            .await?;
        assert_eq!(after_rollback.rows, vec![vec![Db2Value::Integer(80_003)]]);
        Ok::<(), justybase_db2::Db2Error>(())
    }
    .await;
    let cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
    write_result.unwrap();
    cleanup.unwrap();

    let (mut short, _) = PatternReadStream::new(40_000);
    let mut short_parameters = [Db2InputParameter::BlobStream {
        source: &mut short,
        length: 100_000,
    }];
    let error = db
        .query_with_blob_streams("VALUES LENGTH(CAST(? AS BLOB(1M)))", &mut short_parameters)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("early eof"), "{error}");
    assert!(db.is_closed());
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_streamed_clob_dbclob_and_xml_inputs_preserve_utf8_content() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();

    let clob_text = "CLOB café 東京 🚀 ".repeat(5_000);
    let clob_bytes = clob_text.as_bytes().to_vec();
    let mut clob_source = tokio::io::BufReader::new(std::io::Cursor::new(clob_bytes.clone()));
    let mut clob_parameters = [Db2InputParameter::ClobStream {
        source: &mut clob_source,
        length: clob_bytes.len() as u64,
    }];
    let clob = db
        .query_with_blob_streams("VALUES CAST(? AS CLOB(2M))", &mut clob_parameters)
        .await
        .unwrap();
    assert_eq!(clob.rows, vec![vec![Db2Value::Text(clob_text)]]);
    assert_eq!(clob_source.get_ref().position(), clob_bytes.len() as u64);

    let dbclob_text = "DBCLOB café 東京 🚀 ".repeat(5_000);
    let dbclob_bytes = dbclob_text.as_bytes().to_vec();
    let mut dbclob_source = tokio::io::BufReader::new(std::io::Cursor::new(dbclob_bytes.clone()));
    let mut dbclob_parameters = [Db2InputParameter::DbClobStream {
        source: &mut dbclob_source,
        length: dbclob_bytes.len() as u64,
    }];
    let dbclob = db
        .query_with_blob_streams("VALUES CAST(? AS DBCLOB(2M))", &mut dbclob_parameters)
        .await
        .unwrap();
    assert_eq!(dbclob.rows, vec![vec![Db2Value::Text(dbclob_text.clone())]]);
    assert_eq!(
        dbclob_source.get_ref().position(),
        dbclob_bytes.len() as u64
    );

    let xml_text = format!(
        "<root><message>XML café 東京 🚀</message><body>{}</body></root>",
        "x".repeat(100_000)
    );
    let xml_bytes = xml_text.as_bytes().to_vec();
    let table = format!(
        "RS_DB2_XML_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (D DBCLOB(2M), X XML)"
    ))
    .await
    .unwrap();
    let xml_result = async {
        let mut dbclob_table_source =
            tokio::io::BufReader::new(std::io::Cursor::new(dbclob_bytes.clone()));
        let mut xml_source = tokio::io::BufReader::new(std::io::Cursor::new(xml_bytes.clone()));
        let mut insert_parameters = [
            Db2InputParameter::DbClobStream {
                source: &mut dbclob_table_source,
                length: dbclob_bytes.len() as u64,
            },
            Db2InputParameter::XmlStream {
                source: &mut xml_source,
                length: xml_bytes.len() as u64,
            },
        ];
        assert_eq!(
            db.execute_with_blob_streams(
                &format!("INSERT INTO {table} VALUES (?, ?)"),
                &mut insert_parameters
            )
            .await?,
            1
        );
        assert_eq!(
            dbclob_table_source.get_ref().position(),
            dbclob_bytes.len() as u64
        );
        assert_eq!(xml_source.get_ref().position(), xml_bytes.len() as u64);
        let stored_dbclob = db.query(&format!("SELECT D FROM {table}")).await?;
        assert_eq!(
            stored_dbclob.rows,
            vec![vec![Db2Value::Text(dbclob_text.clone())]]
        );
        let stored = db
            .query(&format!(
                "SELECT XMLSERIALIZE(X AS CLOB(2M)) FROM {table} WHERE X IS NOT NULL"
            ))
            .await?;
        assert_eq!(stored.rows, vec![vec![Db2Value::Text(xml_text)]]);
        Ok::<(), justybase_db2::Db2Error>(())
    }
    .await;
    let cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
    xml_result.unwrap();
    cleanup.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_short_text_lob_inputs_invalidate_their_sessions() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");

    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let (mut clob_source, _) = PatternReadStream::new(40_000);
    let mut clob_parameters = [Db2InputParameter::ClobStream {
        source: &mut clob_source,
        length: 100_000,
    }];
    let error = db
        .query_with_blob_streams("VALUES LENGTH(CAST(? AS CLOB(1M)))", &mut clob_parameters)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("early eof"), "{error}");
    assert!(db.is_closed());

    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let (mut dbclob_source, _) = PatternReadStream::new(40_000);
    let mut dbclob_parameters = [Db2InputParameter::DbClobStream {
        source: &mut dbclob_source,
        length: 100_000,
    }];
    let error = db
        .query_with_blob_streams(
            "VALUES LENGTH(CAST(? AS DBCLOB(1M)))",
            &mut dbclob_parameters,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("early eof"), "{error}");
    assert!(db.is_closed());

    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let (mut xml_source, _) = PatternReadStream::new(40_000);
    let mut xml_parameters = [Db2InputParameter::XmlStream {
        source: &mut xml_source,
        length: 100_000,
    }];
    let error = db
        .query_with_blob_streams(
            "VALUES XMLSERIALIZE(CAST(? AS XML) AS CLOB(1M))",
            &mut xml_parameters,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("early eof"), "{error}");
    assert!(db.is_closed());

    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let first_text = "first text LOB 🚀".to_string() + &"a".repeat(40_000);
    let first_bytes = first_text.as_bytes().to_vec();
    let mut first_source = tokio::io::BufReader::new(std::io::Cursor::new(first_bytes.clone()));
    let (mut second_source, _) = PatternReadStream::new(40_000);
    let mut parameters = [
        Db2InputParameter::ClobStream {
            source: &mut first_source,
            length: first_bytes.len() as u64,
        },
        Db2InputParameter::DbClobStream {
            source: &mut second_source,
            length: 100_000,
        },
    ];
    let error = db
        .query_with_blob_streams(
            "VALUES (LENGTH(CAST(? AS CLOB(1M))), LENGTH(CAST(? AS DBCLOB(1M))))",
            &mut parameters,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("early eof"), "{error}");
    assert_eq!(first_source.get_ref().position(), first_bytes.len() as u64);
    assert!(db.is_closed());
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_query_stream_interrupt_ends_the_open_cursor_and_preserves_the_session() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let handle = db.interrupt_handle().unwrap();
    let query = db
        .query_stream("SELECT COUNT(*) FROM SYSCAT.COLUMNS A, SYSCAT.COLUMNS B, SYSCAT.COLUMNS C");
    let interrupt = async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        handle.interrupt().await.unwrap();
    };
    let error = {
        let (stream, ()) = tokio::join!(query, interrupt);
        match stream {
            Ok(reader) => {
                drop(reader);
                panic!("Db2 should cancel the active row stream")
            }
            Err(error) => error,
        }
    };
    assert!(
        error.to_string().contains("SQLCODE=-952 SQLSTATE=57014"),
        "{error}"
    );
    assert_eq!(
        db.query("VALUES 19").await.unwrap().rows,
        vec![vec![Db2Value::Integer(19)]]
    );
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_graphic_and_timestamp_parameters_preserve_padding_and_microseconds() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    assert_eq!(
        db.query_with_params("VALUES CAST(? AS CHAR(5))", &[Db2Value::Text("abc".into())])
            .await
            .unwrap()
            .rows,
        vec![vec![Db2Value::Text("abc  ".into())]]
    );
    assert_eq!(
        db.query_with_params(
            "VALUES CAST(? AS VARCHAR(5))",
            &[Db2Value::Text("abc".into())]
        )
        .await
        .unwrap()
        .rows,
        vec![vec![Db2Value::Text("abc".into())]]
    );
    assert_eq!(
        db.query_with_params(
            "VALUES CAST(? AS GRAPHIC(5))",
            &[Db2Value::Text("abc".into())]
        )
        .await
        .unwrap()
        .rows,
        vec![vec![Db2Value::Text("abc  ".into())]]
    );
    assert_eq!(
        db.query_with_params(
            "VALUES CAST(? AS VARGRAPHIC(10))",
            &[Db2Value::Text("東京".into())]
        )
        .await
        .unwrap()
        .rows,
        vec![vec![Db2Value::Text("東京".into())]]
    );
    assert_eq!(
        db.query_with_params(
            "VALUES CAST(? AS TIMESTAMP)",
            &[Db2Value::Text("2026-08-26 12:34:56.123456".into())]
        )
        .await
        .unwrap()
        .rows,
        vec![vec![Db2Value::Text("2026-08-26-12.34.56.123456".into())]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_sql_statements_and_parameter_blocks_span_multiple_dss_segments() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    for size in [16, 32, 64, 128, 1024].map(|kib| kib * 1024) {
        let mut query = String::from("SELECT COUNT(*) FROM SYSIBM.SYSDUMMY1 WHERE 1 IN (1");
        let mut value = 2;
        while query.len() < size {
            query.push_str(&format!(", {value}"));
            value += 1;
        }
        query.push(')');
        assert_eq!(
            db.query(&query).await.unwrap().rows,
            vec![vec![Db2Value::Integer(1)]]
        );
        assert_eq!(
            db.query_with_params(&(query + " AND 1 = ?"), &[Db2Value::Integer(1)])
                .await
                .unwrap()
                .rows,
            vec![vec![Db2Value::Integer(1)]]
        );
        db.execute(&format!(
            "SET CURRENT QUERY OPTIMIZATION = 5{}",
            " ".repeat(size)
        ))
        .await
        .unwrap();
    }
    for count in [1, 2, 4] {
        let query = format!(
            "SELECT {} FROM SYSIBM.SYSDUMMY1",
            vec!["LENGTH(CAST(? AS VARCHAR(16000)))"; count].join(" + ")
        );
        let parameters = (0..count)
            .map(|index| Db2Value::Text(((b'a' + index as u8) as char).to_string().repeat(16_000)))
            .collect::<Vec<_>>();
        assert_eq!(
            db.query_with_params(&query, &parameters)
                .await
                .unwrap()
                .rows,
            vec![vec![Db2Value::Integer(16_000 * count as i64)]]
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_queries_decode_scalars_report_sql_errors_and_ping() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();

    let values = db
        .query("VALUES (CAST(42 AS INTEGER), CAST(7 AS SMALLINT), CAST(9 AS BIGINT))")
        .await
        .unwrap();
    assert_eq!(
        values.rows,
        vec![vec![
            Db2Value::Integer(42),
            Db2Value::Integer(7),
            Db2Value::Integer(9)
        ]]
    );

    let decimal = db.query("VALUES CAST(125 AS DECIMAL(5,2))").await.unwrap();
    assert_eq!(decimal.rows, vec![vec![Db2Value::Decimal("125.00".into())]]);

    let decfloat = db.query("VALUES (CAST('123.456' AS DECFLOAT(16)), CAST('-987.654' AS DECFLOAT(16)), CAST('Infinity' AS DECFLOAT(16)), CAST('NaN' AS DECFLOAT(16)))").await.unwrap();
    assert_eq!(
        decfloat.rows,
        vec![vec![
            Db2Value::Decimal("123.456".into()),
            Db2Value::Decimal("-987.654".into()),
            Db2Value::Decimal("Infinity".into()),
            Db2Value::Decimal("NaN".into())
        ]]
    );
    let wide_decfloat = db
        .query("VALUES CAST('12345678901234567890.12345678901234' AS DECFLOAT(34))")
        .await
        .unwrap();
    assert_eq!(
        wide_decfloat.rows,
        vec![vec![Db2Value::Decimal(
            "12345678901234567890.12345678901234".into()
        )]]
    );
    let null_decfloat = db.query("VALUES CAST(NULL AS DECFLOAT(16))").await.unwrap();
    assert_eq!(null_decfloat.rows, vec![vec![Db2Value::Null]]);

    let text = db
        .query("VALUES CAST(CURRENT SERVER AS VARCHAR(128))")
        .await
        .unwrap();
    assert!(
        matches!(text.rows.first().and_then(|row| row.first()), Some(Db2Value::Text(value)) if !value.is_empty())
    );

    let error = db.query("SELECT FROM SYSIBM.SYSDUMMY1").await.unwrap_err();
    assert!(error.to_string().contains("SQLCODE="));
    db.ping().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_prepared_statements_bind_queries_and_dml_parameters() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();

    let statement = db.prepare("VALUES CAST(? AS INTEGER)").await.unwrap();
    assert_eq!(statement.parameter_columns.len(), 1);
    assert_eq!(
        db.query_prepared(&statement, &[Db2Value::Integer(42)])
            .await
            .unwrap()
            .rows,
        vec![vec![Db2Value::Integer(42)]]
    );
    let other_statement = db.prepare("VALUES CAST(? AS BIGINT)").await.unwrap();
    assert_eq!(
        db.query_prepared(&other_statement, &[Db2Value::Integer(73)])
            .await
            .unwrap()
            .rows,
        vec![vec![Db2Value::Integer(73)]]
    );
    assert_eq!(
        db.query_prepared(&statement, &[Db2Value::Integer(73)])
            .await
            .unwrap()
            .rows,
        vec![vec![Db2Value::Integer(73)]]
    );
    let typed = db.query_with_params(
        "VALUES (CAST(? AS DECIMAL(7,2)), CAST(? AS VARCHAR(32)), CAST(? AS DATE), CAST(? AS BOOLEAN))",
        &[Db2Value::Decimal("125.50".into()), Db2Value::Text("Teclado Mecânico".into()), Db2Value::Text("2026-08-26".into()), Db2Value::Boolean(true)],
    ).await.unwrap();
    assert_eq!(
        typed.rows,
        vec![vec![
            Db2Value::Decimal("125.50".into()),
            Db2Value::Text("Teclado Mecânico".into()),
            Db2Value::Text("2026-08-26".into()),
            Db2Value::Boolean(true)
        ]]
    );
    let typed_nulls = db.query_with_params(
        "VALUES (CAST(? AS INTEGER), CAST(? AS VARCHAR(30)), CAST(? AS DECIMAL(9,3)), CAST(? AS DATE), CAST(? AS BOOLEAN), CAST(? AS XML))",
        &[Db2Value::Null, Db2Value::Null, Db2Value::Null, Db2Value::Null, Db2Value::Null, Db2Value::Null],
    ).await.unwrap();
    assert_eq!(typed_nulls.rows, vec![vec![Db2Value::Null; 6]]);
    let typed_null_types = [
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "REAL",
        "DOUBLE",
        "DECIMAL(9,3)",
        "DECFLOAT(16)",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "CHAR(5)",
        "VARCHAR(12)",
        "GRAPHIC(5)",
        "VARGRAPHIC(10)",
        "BINARY(4)",
        "VARBINARY(10)",
        "BLOB(100)",
        "CLOB(100)",
        "DBCLOB(100)",
        "XML",
        "BOOLEAN",
    ];
    let typed_null_sql = format!(
        "VALUES ({})",
        typed_null_types
            .iter()
            .map(|sql_type| format!("CAST(? AS {sql_type})"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let typed_null_parameters = vec![Db2Value::Null; typed_null_types.len()];
    let typed_null_matrix = db
        .query_with_params(&typed_null_sql, &typed_null_parameters)
        .await
        .unwrap();
    assert_eq!(
        typed_null_matrix.rows,
        vec![vec![Db2Value::Null; typed_null_types.len()]]
    );
    let varbinary = vec![0, 0x7f, 0x80, 0xff];
    assert_eq!(
        db.query_with_params(
            "VALUES CAST(? AS VARBINARY(10))",
            &[Db2Value::Bytes(varbinary.clone())],
        )
        .await
        .unwrap()
        .rows,
        vec![vec![Db2Value::Bytes(varbinary)]]
    );
    let decimal_precisions = db.query_with_params(
        "VALUES (CAST(? AS DECIMAL(4,2)), CAST(? AS DECIMAL(5,2)), CAST(? AS DECIMAL(8,3)), CAST(? AS DECIMAL(9,3)))",
        &[Db2Value::Decimal("1.25".into()), Db2Value::Decimal("1.25".into()), Db2Value::Decimal("12345.678".into()), Db2Value::Decimal("123456.789".into())],
    ).await.unwrap();
    assert_eq!(
        decimal_precisions.rows,
        vec![vec![
            Db2Value::Decimal("1.25".into()),
            Db2Value::Decimal("1.25".into()),
            Db2Value::Decimal("12345.678".into()),
            Db2Value::Decimal("123456.789".into()),
        ]]
    );
    assert_eq!(
        db.query_with_params(
            "VALUES CAST(? AS DECIMAL(5,2))",
            &[Db2Value::Decimal("1.236".into())],
        )
        .await
        .unwrap()
        .rows,
        vec![vec![Db2Value::Decimal("1.23".into())]]
    );
    assert!(
        db.query_with_params(
            "VALUES CAST(? AS DECIMAL(2,2))",
            &[Db2Value::Decimal("1.25".into())],
        )
        .await
        .is_err()
    );
    assert!(
        db.query_with_params("VALUES CAST(? AS SMALLINT)", &[Db2Value::Integer(32768)],)
            .await
            .is_err()
    );
    assert_eq!(
        db.query("VALUES 29").await.unwrap().rows,
        vec![vec![Db2Value::Integer(29)]]
    );
    let parameter_boundaries = db.query_with_params(
        "VALUES (CAST(? AS SMALLINT), CAST(? AS INTEGER), CAST(? AS BIGINT), CAST(? AS DECIMAL(9,3)), CAST(? AS REAL), CAST(? AS DOUBLE))",
        &[Db2Value::Integer(i16::MIN as i64), Db2Value::Integer(i32::MIN as i64), Db2Value::Integer(i64::MAX), Db2Value::Decimal("-123456.789".into()), Db2Value::Float(1.25), Db2Value::Float(-2.5)],
    ).await.unwrap();
    assert_eq!(
        parameter_boundaries.rows,
        vec![vec![
            Db2Value::Integer(i16::MIN as i64),
            Db2Value::Integer(i32::MIN as i64),
            Db2Value::Integer(i64::MAX),
            Db2Value::Decimal("-123456.789".into()),
            Db2Value::Float(1.25),
            Db2Value::Float(-2.5),
        ]]
    );
    let typed_decfloat = db.query_with_params(
        "VALUES (CAST(? AS DECFLOAT(16)), CAST(? AS DECFLOAT(16)), CAST(? AS DECFLOAT(16)), CAST(? AS DECFLOAT(34)), CAST(? AS DECFLOAT(16)))",
        &[Db2Value::Decimal("123.456".into()), Db2Value::Decimal("-987.654".into()), Db2Value::Decimal("Infinity".into()), Db2Value::Decimal("12345678901234567890.12345678901234".into()), Db2Value::Decimal("NaN".into())],
    ).await.unwrap();
    assert_eq!(
        typed_decfloat.rows,
        vec![vec![
            Db2Value::Decimal("123.456".into()),
            Db2Value::Decimal("-987.654".into()),
            Db2Value::Decimal("Infinity".into()),
            Db2Value::Decimal("12345678901234567890.12345678901234".into()),
            Db2Value::Decimal("NaN".into())
        ]]
    );
    let lob_bytes = (0..70_003)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    for size in [32_766, 32_767, 32_768, lob_bytes.len()] {
        let length = db
            .query_with_params(
                "VALUES LENGTH(CAST(? AS BLOB(1M)))",
                &[Db2Value::Bytes(lob_bytes[..size].to_vec())],
            )
            .await
            .unwrap();
        assert_eq!(length.rows, vec![vec![Db2Value::Integer(size as i64)]]);
    }
    let clob_text = "CLOB café ".repeat(80);
    let dbclob_text = "DBCLOB 東京 ".repeat(80);
    let xml_text = "<root><message>XML café</message></root>";
    let lob_table = format!(
        "RS_DB2_LOB_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{lob_table} (B BLOB(1M), C CLOB(1M), D DBCLOB(1M), X XML)"
    ))
    .await
    .unwrap();
    let lob_round_trip = async {
        db.execute_with_params(&format!("INSERT INTO {lob_table} (B) VALUES (?)"), &[Db2Value::Bytes(lob_bytes.clone())]).await?;
        let blob_count = db.query(&format!("SELECT 1 FROM {lob_table} WHERE B IS NOT NULL")).await?;
        assert_eq!(blob_count.rows.len(), 1, "EXTDTA BLOB parameter inserted no row");
        db.execute(&format!("DELETE FROM {lob_table}")).await?;
        assert_eq!(db.execute_with_params(&format!("INSERT INTO {lob_table} VALUES (?, ?, ?, ?)"), &[Db2Value::Bytes(lob_bytes.clone()), Db2Value::Text(clob_text.clone()), Db2Value::Text(dbclob_text.clone()), Db2Value::Text(xml_text.into())]).await?, 1);
        let result = db.query(&format!("SELECT B, C, D, XMLSERIALIZE(X AS VARCHAR(1024)) FROM {lob_table}")).await?;
        let row = &result.rows[0];
        assert!(matches!(&row[0], Db2Value::Bytes(bytes) if bytes.len() == lob_bytes.len() && bytes == &lob_bytes), "BLOB round-trip length/content mismatch");
        assert_eq!(row[1], Db2Value::Text(clob_text.clone()));
        assert_eq!(row[2], Db2Value::Text(dbclob_text.clone()));
        assert_eq!(row[3], Db2Value::Text(xml_text.into()));
        Ok::<(), justybase_db2::Db2Error>(())
    }.await;
    let lob_cleanup = db
        .execute(&format!("DROP TABLE {schema}.{lob_table}"))
        .await;
    match lob_round_trip {
        Ok(()) => match lob_cleanup {
            Ok(_) => {}
            Err(error)
                if error.sql_code == Some(-204) && error.sql_state.as_deref() == Some("42704") =>
            {
                // Treat only exact object-not-found as idempotent fixture cleanup.
            }
            Err(error) => panic!("prepared-statement LOB fixture cleanup failed: {error}"),
        },
        Err(work_error) => {
            if let Err(cleanup_error) = lob_cleanup {
                eprintln!("prepared-statement LOB cleanup also failed: {cleanup_error}");
            }
            panic!("prepared-statement LOB round trip failed: {work_error}");
        }
    }

    let table = format!(
        "RS_DB2_PARAMS_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!("CREATE TABLE {schema}.{table} (V INTEGER)"))
        .await
        .unwrap();
    let dml = async {
        let insert = db
            .prepare(&format!("INSERT INTO {table} (V) VALUES (?)"))
            .await?;
        assert_eq!(
            db.execute_prepared(&insert, &[Db2Value::Integer(91)])
                .await?,
            1
        );
        assert_eq!(
            db.execute_with_params(
                &format!("INSERT INTO {table} (V) VALUES (?)"),
                &[Db2Value::Integer(92)]
            )
            .await?,
            1
        );
        let batch = vec![vec![Db2Value::Integer(93)], vec![Db2Value::Integer(94)]];
        assert_eq!(
            db.execute_batch_with_params(&format!("INSERT INTO {table} (V) VALUES (?)"), &batch)
                .await?,
            2
        );
        assert_eq!(
            db.execute_batch_with_params(&format!("INSERT INTO {table} (V) VALUES (?)"), &[])
                .await?,
            0
        );
        assert_eq!(
            db.query_with_params(
                &format!("SELECT V FROM {table} WHERE V = ?"),
                &[Db2Value::Integer(91)]
            )
            .await?
            .rows,
            vec![vec![Db2Value::Integer(91)]]
        );
        assert_eq!(
            db.query_with_params(
                &format!("SELECT V FROM {table} WHERE V = ?"),
                &[Db2Value::Integer(92)]
            )
            .await?
            .rows,
            vec![vec![Db2Value::Integer(92)]]
        );
        assert_eq!(
            db.query_with_params(
                &format!("SELECT V FROM {table} WHERE V > ? ORDER BY V"),
                &[Db2Value::Integer(92)]
            )
            .await?
            .rows,
            vec![vec![Db2Value::Integer(93)], vec![Db2Value::Integer(94)]]
        );
        Ok::<(), justybase_db2::Db2Error>(())
    }
    .await;
    let cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
    dml.unwrap();
    cleanup.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_prepared_parameter_query_materializes_large_blob_with_exact_checksum() {
    const BLOB_LENGTH: usize = 2_500_123;

    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let table = format!(
        "RS_PARAM_BLOB_FETCH_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (ID INT NOT NULL PRIMARY KEY, B BLOB(10M))"
    ))
    .await
    .unwrap();

    let expected = pattern_bytes(BLOB_LENGTH);
    let expected_checksum = ring::digest::digest(&ring::digest::SHA256, &expected)
        .as_ref()
        .to_vec();
    let round_trip = async {
        db.execute_with_params(
            &format!("INSERT INTO {table} (ID, B) VALUES (?, ?)"),
            &[Db2Value::Integer(1), Db2Value::Bytes(expected.clone())],
        )
        .await?;
        let statement = db
            .prepare(&format!("SELECT B FROM {table} WHERE ID = ?"))
            .await?;
        let result = db
            .query_prepared(&statement, &[Db2Value::Integer(1)])
            .await?;
        if result.rows.len() != 1 || result.rows[0].len() != 1 {
            return Err(Db2Error::other(format!(
                "expected one row with one BLOB cell, got {} rows",
                result.rows.len()
            )));
        }
        let Db2Value::Bytes(bytes) = result.rows.into_iter().next().unwrap().remove(0) else {
            return Err(Db2Error::other(
                "prepared BLOB query returned a non-binary value".into(),
            ));
        };
        let actual_length = bytes.len();
        let actual_checksum = ring::digest::digest(&ring::digest::SHA256, &bytes)
            .as_ref()
            .to_vec();
        let mut reader = db
            .query_stream_with_params(
                &format!("SELECT B FROM {table} WHERE ID = ?"),
                &[Db2Value::Integer(1)],
            )
            .await?;
        let row = reader
            .next()
            .await?
            .ok_or_else(|| Db2Error::other("large BLOB row stream returned no row".into()))?;
        let Db2Value::Lob(streamed_blob) = &row[0] else {
            return Err(Db2Error::other(
                "large BLOB row stream returned a materialized value instead of a locator".into(),
            ));
        };
        if streamed_blob.byte_length != BLOB_LENGTH as u64 {
            return Err(Db2Error::other(
                "large BLOB locator length did not match the source".into(),
            ));
        }
        let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
        let mut streamed_length = 0usize;
        let mut lob_reader = reader.open_lob(streamed_blob).await?;
        while let Some(chunk) = lob_reader.next_chunk().await? {
            streamed_length += chunk.len();
            digest.update(&chunk);
        }
        let streamed_checksum = digest.finish().as_ref().to_vec();
        let _ = lob_reader;
        if streamed_length != BLOB_LENGTH || streamed_checksum != expected_checksum {
            return Err(Db2Error::other(
                "streamed BLOB byte count or SHA-256 did not match the source".into(),
            ));
        }
        if reader.next().await?.is_some() {
            return Err(Db2Error::other(
                "large BLOB row stream returned an extra row".into(),
            ));
        }
        reader.close().await?;
        Ok::<_, Db2Error>((actual_length, actual_checksum))
    }
    .await;
    let cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;

    let (actual_length, actual_checksum) = round_trip.unwrap();
    cleanup.unwrap();
    assert_eq!(actual_length, BLOB_LENGTH);
    assert_eq!(actual_checksum, expected_checksum);
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_row_import_pipes_progressive_blob_with_bounded_memory() {
    const BLOB_LENGTH: usize = 2_500_123;

    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let source_table = format!("RS_IMPORT_BLOB_SOURCE_{suffix}");
    let destination_table = format!("RS_IMPORT_BLOB_DEST_{suffix}");
    let checkpoint_table = format!("RS_IMPORT_BLOB_CHECKPOINT_{suffix}");
    let mut source_session = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut destination = Db2Session::connect_dsn(&dsn).await.unwrap();
    let source_schema = match &source_session
        .query("VALUES CURRENT SCHEMA")
        .await
        .unwrap()
        .rows[0][0]
    {
        Db2Value::Text(schema) => schema.trim_end().replace('"', "\"\""),
        other => panic!("expected source CURRENT SCHEMA text, got {other:?}"),
    };
    let destination_schema = match &destination
        .query("VALUES CURRENT SCHEMA")
        .await
        .unwrap()
        .rows[0][0]
    {
        Db2Value::Text(schema) => schema.trim_end().replace('"', "\"\""),
        other => panic!("expected destination CURRENT SCHEMA text, got {other:?}"),
    };
    let qualified_source_table = format!("\"{source_schema}\".\"{source_table}\"");
    let qualified_destination_table = format!("\"{destination_schema}\".\"{destination_table}\"");
    let qualified_checkpoint_table = format!("\"{destination_schema}\".\"{checkpoint_table}\"");
    let mut source_created = false;
    let mut destination_created = false;
    let mut checkpoint_created = false;

    let result = async {
        source_session
            .execute(&format!(
                "CREATE TABLE {qualified_source_table} (ID INTEGER NOT NULL PRIMARY KEY, B BLOB(10M))"
            ))
            .await?;
        source_created = true;
        destination
            .execute(&format!(
                "CREATE TABLE {qualified_destination_table} (ID INTEGER NOT NULL PRIMARY KEY, B BLOB(10M))"
            ))
            .await?;
        destination_created = true;
        destination
            .execute(&format!(
                "CREATE TABLE {qualified_checkpoint_table} (ID INTEGER NOT NULL PRIMARY KEY, B BLOB(10M))"
            ))
            .await?;
        checkpoint_created = true;

        let expected = pattern_bytes(BLOB_LENGTH);
        let expected_digest = ring::digest::digest(&ring::digest::SHA256, &expected);
        let id = Db2Value::Integer(1);
        let mut blob_source = tokio::io::BufReader::new(std::io::Cursor::new(expected));
        let mut insert = [
            Db2InputParameter::Value(&id),
            Db2InputParameter::BlobStream {
                source: &mut blob_source,
                length: BLOB_LENGTH as u64,
            },
        ];
        source_session
            .execute_with_blob_streams(
                &format!("INSERT INTO {qualified_source_table} (ID, B) VALUES (?, ?)"),
                &mut insert,
            )
            .await?;

        // Probe the live server policy first so this test cannot silently pass by taking
        // the already-materialized-value import path.
        let mut probe = source_session
            .query_stream(&format!("SELECT B FROM {qualified_source_table} WHERE ID = 1"))
            .await?;
        let probe_row = probe
            .next()
            .await?
            .ok_or_else(|| Db2Error::other("source BLOB probe returned no row".into()))?;
        let Db2Value::Lob(locator) = &probe_row[0] else {
            return Err(Db2Error::other(
                "live DB2 did not return a progressive BLOB locator for the import fixture".into(),
            ));
        };
        if locator.byte_length != BLOB_LENGTH as u64 {
            return Err(Db2Error::other(
                "source progressive BLOB locator reported the wrong byte length".into(),
            ));
        }
        let mut probe_lob = probe.open_lob(locator).await?;
        while probe_lob.next_chunk().await?.is_some() {}
        let _ = probe_lob;
        if probe.next().await?.is_some() {
            return Err(Db2Error::other(
                "source BLOB probe returned extra rows".into(),
            ));
        }
        drop(probe);

        let mut source_rows = source_session
            .query_stream(&format!("SELECT ID, B FROM {qualified_source_table} ORDER BY ID"))
            .await?;
        let target = Db2ImportTarget::new(
            Some(&destination_schema),
            &destination_table,
            vec![Db2ImportColumn::new("ID"), Db2ImportColumn::new("B")],
        )?;
        let progress = destination
            .import_rows_from(
                &mut source_rows,
                &target,
                Db2ImportPolicy::Atomic {
                    max_batch_rows: 4,
                    max_batch_bytes: 64,
                },
                |_| Ok(()),
            )
            .await
            .map_err(|error| error.cause)?;
        if progress.source_rows_read != 1
            || progress.committed_rows != 1
            || progress.committed_batches != 1
        {
            return Err(Db2Error::other(format!(
                "unexpected progressive BLOB import progress: {progress:?}"
            )));
        }
        drop(source_rows);

        let mut source_rows = source_session
            .query_stream(&format!("SELECT ID, B FROM {qualified_source_table} ORDER BY ID"))
            .await?;
        let checkpoint_target = Db2ImportTarget::new(
            Some(&destination_schema),
            &checkpoint_table,
            vec![Db2ImportColumn::new("ID"), Db2ImportColumn::new("B")],
        )?;
        let mut snapshots = Vec::new();
        let checkpoint_progress = destination
            .import_rows_from(
                &mut source_rows,
                &checkpoint_target,
                Db2ImportPolicy::CommitEveryRows {
                    rows: 1,
                    max_batch_bytes: 64,
                },
                |snapshot| {
                    snapshots.push(snapshot);
                    Ok(())
                },
            )
            .await
            .map_err(|error| error.cause)?;
        if checkpoint_progress.source_rows_read != 1
            || checkpoint_progress.committed_rows != 1
            || checkpoint_progress.committed_batches != 1
            || snapshots.len() != 1
            || snapshots[0].committed_rows != 0
        {
            return Err(Db2Error::other(format!(
                "unexpected progressive BLOB checkpoint import progress: {checkpoint_progress:?}"
            )));
        }

        for table in [&qualified_destination_table, &qualified_checkpoint_table] {
            let mut imported = destination
                .query_stream(&format!("SELECT ID, B FROM {table} ORDER BY ID"))
                .await?;
            let row = imported
                .next()
                .await?
                .ok_or_else(|| Db2Error::other("imported BLOB query returned no row".into()))?;
            if row[0] != Db2Value::Integer(1) {
                return Err(Db2Error::other(
                    "imported BLOB row has an incorrect ID".into(),
                ));
            }
            let Db2Value::Lob(locator) = &row[1] else {
                return Err(Db2Error::other(
                    "imported BLOB did not remain streamable from its destination".into(),
                ));
            };
            let mut imported_lob = imported.open_lob(locator).await?;
            let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
            let mut length = 0usize;
            while let Some(chunk) = imported_lob.next_chunk().await? {
                length += chunk.len();
                digest.update(&chunk);
            }
            let _ = imported_lob;
            if length != BLOB_LENGTH || digest.finish().as_ref() != expected_digest.as_ref() {
                return Err(Db2Error::other(
                    "progressively imported BLOB did not match its source bytes".into(),
                ));
            }
            if imported.next().await?.is_some() {
                return Err(Db2Error::other(
                    "destination BLOB query returned extra rows".into(),
                ));
            }
        }
        Ok::<(), Db2Error>(())
    }
    .await;

    let source_cleanup = if source_created {
        source_session
            .execute(&format!("DROP TABLE {qualified_source_table}"))
            .await
    } else {
        Ok(0)
    };
    let destination_cleanup = if destination_created {
        destination
            .execute(&format!("DROP TABLE {qualified_destination_table}"))
            .await
    } else {
        Ok(0)
    };
    let checkpoint_cleanup = if checkpoint_created {
        destination
            .execute(&format!("DROP TABLE {qualified_checkpoint_table}"))
            .await
    } else {
        Ok(0)
    };
    result.unwrap();
    source_cleanup.unwrap();
    destination_cleanup.unwrap();
    checkpoint_cleanup.unwrap();
    source_session.close().await.unwrap();
    destination.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_import_cancellation_during_progressive_source_lob_preserves_checkpoint() {
    const SECOND_BLOB_LENGTH: usize = 3_000_000;
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut source_config = justybase_db2::parse_dsn(&dsn).unwrap();
    if source_config.use_ssl {
        panic!("the GETNXTCHK observer for this live test requires the plaintext TCP test DSN");
    }
    let remote_host = source_config.host.clone();
    let remote_port = source_config.port;
    let request_armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (proxy_port, mut observed_getnext, proxy_task) =
        spawn_getnxtchk_observer(remote_host, remote_port, request_armed.clone()).await;
    source_config.host = "127.0.0.1".into();
    source_config.port = proxy_port;
    let mut source_session = Db2Session::connect(source_config).await.unwrap();
    let interrupt_handle = source_session.interrupt_handle().unwrap();
    let mut destination = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let source_table = format!("RS_IMPORT_CKPT_SOURCE_{suffix}");
    let target_table = format!("RS_IMPORT_CKPT_TARGET_{suffix}");
    let mut source_created = false;
    let mut target_created = false;
    let schema = current_schema(&mut destination).await;

    let result = async {
        let ddl = |table: &str| {
            format!(
                "CREATE TABLE {schema}.{table} (ID INTEGER NOT NULL PRIMARY KEY, B BLOB(4M), C CLOB(4M), D DBCLOB(4M), X XML)"
            )
        };
        source_session.execute(&ddl(&source_table)).await?;
        source_created = true;
        destination.execute(&ddl(&target_table)).await?;
        target_created = true;

        source_session
            .execute(&format!(
                "INSERT INTO {source_table} VALUES (1, BLOB(X'010203FF'), CAST('checkpoint clob' AS CLOB(32)), CAST('checkpoint dbclob' AS DBCLOB(32)), XMLPARSE(DOCUMENT '<root>checkpoint</root>'))"
            ))
            .await?;
        {
            let id = Db2Value::Integer(2);
            let mut blob = std::io::Cursor::new(pattern_bytes(SECOND_BLOB_LENGTH));
            let mut parameters = [
                Db2InputParameter::Value(&id),
                Db2InputParameter::BlobStream {
                    source: &mut blob,
                    length: SECOND_BLOB_LENGTH as u64,
                },
            ];
            source_session
                .execute_with_blob_streams(
                    &format!(
                        "INSERT INTO {source_table} (ID, B, C, D, X) VALUES (?, ?, CAST('later clob' AS CLOB(32)), CAST('later dbclob' AS DBCLOB(32)), XMLPARSE(DOCUMENT '<root>later</root>'))"
                    ),
                    &mut parameters,
                )
                .await?;
        }

        let mut source_rows = source_session
            .query_stream(&format!(
                "SELECT ID, B, C, D, X FROM {source_table} ORDER BY ID"
            ))
            .await?;
        let target = Db2ImportTarget::new(
            None,
            &target_table,
            vec![
                Db2ImportColumn::new("ID"),
                Db2ImportColumn::new("B"),
                Db2ImportColumn::new("C"),
                Db2ImportColumn::new("D"),
                Db2ImportColumn::new("X"),
            ],
        )?;

        let mut progress_snapshots = Vec::new();
        let armed_for_second_row = request_armed.clone();
        let mut interrupt_acknowledged = false;
        let outcome = {
            let import = destination.import_rows_from(
                &mut source_rows,
                &target,
                Db2ImportPolicy::CommitEveryRows {
                    rows: 1,
                    max_batch_bytes: 64 * 1024,
                },
                |snapshot| {
                    progress_snapshots.push(snapshot);
                    if snapshot.source_rows_read == 1 {
                        // Arm the observer after the first checkpoint so the next GETNXTCHK
                        // it sees belongs to the large second-row BLOB.
                        armed_for_second_row.store(true, Ordering::Release);
                    }
                    Ok(())
                },
            );
            tokio::pin!(import);
            tokio::select! {
                outcome = &mut import => Some(outcome),
                request = observed_getnext.recv() => {
                    if request.is_none() {
                        return Err(Db2Error::other(
                            "GETNXTCHK observer closed before the second source locator".into(),
                        ));
                    }
                    // Db2 acknowledges SQLINTR here, but on Db2 11.5.8 an in-flight
                    // GETNXTCHK is not guaranteed to end with -952. Abandoning the import
                    // future is Rust's documented cancellation contract: the source
                    // locator becomes uncertain and must be discarded, while the
                    // destination keeps its earlier confirmed checkpoint and stays
                    // reusable. The deterministic -952 branch is covered offline.
                    interrupt_acknowledged = interrupt_handle.interrupt().await.is_ok();
                    None
                }
            }
        };
        if let Some(outcome) = outcome {
            return Err(Db2Error::other(format!(
                "progressive import completed before the second-row GETNXTCHK could be cancelled: {outcome:?}"
            )));
        }
        eprintln!(
            "Live Rust progressive source-locator cancellation: SQLINTR acknowledged={interrupt_acknowledged}"
        );
        if progress_snapshots.len() != 1
            || progress_snapshots[0].source_rows_read != 1
            || progress_snapshots[0].committed_rows != 0
            || progress_snapshots[0].committed_batches != 0
        {
            return Err(Db2Error::other(format!(
                "checkpoint callback fired unexpectedly before cancellation: {progress_snapshots:?}"
            )));
        }

        drop(source_rows);
        if !source_session.is_closed() {
            return Err(Db2Error::other(
                "an abandoned progressive source locator must discard its session".into(),
            ));
        }
        if destination.is_closed() {
            return Err(Db2Error::other(
                "destination session was discarded even though Rust spools the source LOB before writing the row".into(),
            ));
        }
        if destination.query("VALUES 23").await?.rows != vec![vec![Db2Value::Integer(23)]] {
            return Err(Db2Error::other(
                "destination session was not reusable after cancelling the source locator".into(),
            ));
        }
        let count = destination
            .query(&format!("SELECT COUNT(*) FROM {target_table}"))
            .await?
            .rows;
        if count != vec![vec![Db2Value::Integer(1)]] {
            return Err(Db2Error::other(format!(
                "cancelled import did not leave exactly the first checkpoint: {count:?}"
            )));
        }
        let rows = destination
            .query(&format!(
                "SELECT ID, B, C, D, X FROM {target_table} ORDER BY ID"
            ))
            .await?
            .rows;
        if rows.len() != 1
            || rows[0][0] != Db2Value::Integer(1)
            || rows[0][1] != Db2Value::Bytes(vec![1, 2, 3, 0xff])
            || rows[0][2] != Db2Value::Text("checkpoint clob".into())
            || rows[0][3] != Db2Value::Text("checkpoint dbclob".into())
            || rows[0][4] != Db2Value::Text("<root>checkpoint</root>".into())
        {
            return Err(Db2Error::other(format!(
                "canceled progressive import did not preserve exactly the first committed row: {rows:?}"
            )));
        }
        if let Some(ibm_line) = ibm_verify_imported_table(&target_table) {
            // System.Text.Json escapes `<` and `>` as \u003C / \u003E by default.
            let expected = format!(
                "{{\"{target_table}\":[[1,\"010203ff\",\"checkpoint clob\",\"checkpoint dbclob\",\"\\u003Croot\\u003Echeckpoint\\u003C/root\\u003E\"]]}}"
            );
            if ibm_line.trim() != expected {
                return Err(Db2Error::other(format!(
                    "official IBM .NET provider read back unexpected checkpoint values ({ibm_line})"
                )));
            }
        }
        Ok::<_, Db2Error>(())
    }
    .await;

    let _ = source_session.close().await;
    proxy_task.abort();
    let target_cleanup = if target_created {
        destination
            .execute(&format!("DROP TABLE {schema}.{target_table}"))
            .await
    } else {
        Ok(0)
    };
    let source_cleanup = if source_created {
        destination
            .execute(&format!("DROP TABLE {schema}.{source_table}"))
            .await
    } else {
        Ok(0)
    };
    let destination_close = destination.close().await;
    result.unwrap();
    target_cleanup.unwrap();
    source_cleanup.unwrap();
    destination_close.unwrap();
}

async fn spawn_getnxtchk_observer(
    remote_host: String,
    remote_port: u16,
    armed: Arc<std::sync::atomic::AtomicBool>,
) -> (
    u16,
    mpsc::UnboundedReceiver<tokio::time::Instant>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let (request_tx, request_rx) = mpsc::unbounded_channel();
    let request_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let server = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((client, _)) = accepted else { break };
                    let remote_host = remote_host.clone();
                    let armed = armed.clone();
                    let request_seen = request_seen.clone();
                    let request_tx = request_tx.clone();
                    connections.spawn(async move {
                        let Ok(upstream) = tokio::net::TcpStream::connect((remote_host.as_str(), remote_port)).await else {
                            return;
                        };
                        let (mut client_read, mut client_write) = client.into_split();
                        let (mut upstream_read, mut upstream_write) = upstream.into_split();
                        let to_upstream = async {
                            let mut buffer = [0u8; 8192];
                            let mut inspection = Vec::new();
                            loop {
                                let count = client_read.read(&mut buffer).await?;
                                if count == 0 {
                                    let _ = upstream_write.shutdown().await;
                                    break;
                                }
                                if armed.load(Ordering::Acquire) {
                                    inspection.extend_from_slice(&buffer[..count]);
                                }
                                upstream_write.write_all(&buffer[..count]).await?;
                                if armed.load(Ordering::Acquire)
                                    && consume_getnxtchk_request(&mut inspection)
                                    && !request_seen.swap(true, Ordering::AcqRel)
                                {
                                    let _ = request_tx.send(tokio::time::Instant::now());
                                }
                            }
                            Ok::<(), io::Error>(())
                        };
                        let to_client = async {
                            tokio::io::copy(&mut upstream_read, &mut client_write)
                                .await
                                .map(|_| ())
                        };
                        let _ = tokio::join!(to_upstream, to_client);
                    });
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    (port, request_rx, server)
}

fn consume_getnxtchk_request(buffer: &mut Vec<u8>) -> bool {
    let mut observed = false;
    loop {
        if buffer.len() < 10 {
            break;
        }
        if buffer[2] != 0xd0 {
            buffer.remove(0);
            continue;
        }
        let encoded_length = u16::from_be_bytes([buffer[0], buffer[1]]);
        let segment_length = usize::from(encoded_length & 0x7fff);
        if segment_length < 10 {
            buffer.remove(0);
            continue;
        }
        if buffer.len() < segment_length {
            break;
        }
        let first_codepoint = u16::from_be_bytes([buffer[8], buffer[9]]);
        observed |= first_codepoint == 0x2015;
        buffer.drain(..segment_length);
    }
    observed
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_row_import_preserves_clob_dbclob_xml_empty_and_null_values() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let source_table = format!("RS_ITS_{suffix}");
    let destination_table = format!("RS_IMPORT_TEXT_DEST_{suffix}");
    let checkpoint_table = format!("RS_IMPORT_TEXT_CHECKPOINT_{suffix}");
    let mut source_session = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut destination = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut source_created = false;
    let mut destination_created = false;
    let mut checkpoint_created = false;
    let schema = current_schema(&mut destination).await;

    let result = async {
        let ddl = |table: &str| {
            format!(
                "CREATE TABLE {schema}.{table} (ID INTEGER NOT NULL PRIMARY KEY, C CLOB(2M), D DBCLOB(2M), X XML)"
            )
        };
        source_session.execute(&ddl(&source_table)).await?;
        source_created = true;
        destination.execute(&ddl(&destination_table)).await?;
        destination_created = true;
        destination.execute(&ddl(&checkpoint_table)).await?;
        checkpoint_created = true;

        let clob = "CLOB café Zażółć 東京 🚀 ".repeat(8_000);
        let dbclob = "DBCLOB 漢字 Zażółć 🚀 ".repeat(8_000);
        let xml = format!(
            "<root><message>XML café 東京 🚀</message><body>{}</body></root>",
            "x".repeat(180_000)
        );
        let insert_sql = format!(
            "INSERT INTO {source_table} (ID, C, D, X) VALUES (?, ?, ?, ?)"
        );
        let id = Db2Value::Integer(1);
        let clob_bytes = clob.as_bytes();
        let dbclob_bytes = dbclob.as_bytes();
        let xml_bytes = xml.as_bytes();
        let mut clob_source = tokio::io::BufReader::new(std::io::Cursor::new(clob_bytes));
        let mut dbclob_source = tokio::io::BufReader::new(std::io::Cursor::new(dbclob_bytes));
        let mut xml_source = tokio::io::BufReader::new(std::io::Cursor::new(xml_bytes));
        let mut values = [
            Db2InputParameter::Value(&id),
            Db2InputParameter::ClobStream {
                source: &mut clob_source,
                length: clob_bytes.len() as u64,
            },
            Db2InputParameter::DbClobStream {
                source: &mut dbclob_source,
                length: dbclob_bytes.len() as u64,
            },
            Db2InputParameter::XmlStream {
                source: &mut xml_source,
                length: xml_bytes.len() as u64,
            },
        ];
        source_session
            .execute_with_blob_streams(&insert_sql, &mut values)
            .await?;

        source_session
            .execute(&format!(
                "INSERT INTO {source_table} (ID, C, D, X) VALUES (2, '', '', XMLPARSE(DOCUMENT '<empty/>'))"
            ))
            .await?;

        let id = Db2Value::Integer(3);
        let null = Db2Value::Null;
        let mut values = [
            Db2InputParameter::Value(&id),
            Db2InputParameter::Value(&null),
            Db2InputParameter::Value(&null),
            Db2InputParameter::Value(&null),
        ];
        source_session
            .execute_with_blob_streams(&insert_sql, &mut values)
            .await?;

        let source_count = source_session
            .query(&format!("SELECT COUNT(*) FROM {source_table}"))
            .await?;
        if source_count.rows != vec![vec![Db2Value::Integer(3)]] {
            return Err(Db2Error::other(
                "CLOB/DBCLOB/XML fixture did not contain all three source rows".into(),
            ));
        }

        // Record only public value kinds. Db2 decides whether each large text/XML value uses
        // progressive EXTDTA or a materialized EXTDTA representation on this server profile.
        let mut probe = source_session
            .query_stream(&format!(
                "SELECT C, D, X FROM {source_table} WHERE ID = 1"
            ))
            .await?;
        let row = probe
            .next()
            .await?
            .ok_or_else(|| Db2Error::other("text LOB import probe returned no row".into()))?;
        if row.len() != 3 {
            return Err(Db2Error::other(
                "text LOB import probe returned an unexpected field count".into(),
            ));
        }
        let mut value_kinds = Vec::with_capacity(row.len());
        for value in &row {
            match value {
                Db2Value::Lob(locator) => {
                    value_kinds.push("progressive locator");
                    let mut lob = probe.open_lob(locator).await?;
                    while lob.next_chunk().await?.is_some() {}
                }
                Db2Value::Text(_) => value_kinds.push("materialized text"),
                Db2Value::Null => value_kinds.push("NULL"),
                _other => {
                    return Err(Db2Error::other(format!(
                        "text LOB import probe returned an unsupported value type for SQL type {}",
                        probe.columns()[value_kinds.len()].sql_type
                    )));
                }
            }
        }
        if probe.next().await?.is_some() {
            return Err(Db2Error::other(
                "text LOB import probe returned extra rows".into(),
            ));
        }
        eprintln!("text/XML import source value kinds: {value_kinds:?}");
        drop(probe);

        for (table, policy) in [
            (
                destination_table.as_str(),
                Db2ImportPolicy::Atomic {
                    max_batch_rows: 4,
                    max_batch_bytes: 2_000_000,
                },
            ),
            (
                checkpoint_table.as_str(),
                Db2ImportPolicy::CommitEveryRows {
                    rows: 1,
                    max_batch_bytes: 2_000_000,
                },
            ),
        ] {
            let mut source_rows = source_session
                .query_stream(&format!(
                    "SELECT ID, C, D, X FROM {source_table} ORDER BY ID"
                ))
                .await?;
            let target = Db2ImportTarget::new(
                None,
                table,
                vec![
                    Db2ImportColumn::new("ID"),
                    Db2ImportColumn::new("C"),
                    Db2ImportColumn::new("D"),
                    Db2ImportColumn::new("X"),
                ],
            )?;
            let mut snapshots = Vec::new();
            let progress = destination
                .import_rows_from(&mut source_rows, &target, policy, |snapshot| {
                    snapshots.push(snapshot);
                    Ok(())
                })
                .await
                .map_err(|error| error.cause)?;
            if progress.source_rows_read != 3 || progress.committed_rows != 3 {
                return Err(Db2Error::other(format!(
                    "unexpected CLOB/DBCLOB/XML import progress: {progress:?}"
                )));
            }
            if table == destination_table && progress.committed_batches != 1 {
                return Err(Db2Error::other(format!(
                    "unexpected atomic CLOB/DBCLOB/XML import progress: {progress:?}"
                )));
            }
            if table == checkpoint_table
                && (progress.committed_batches != 3 || snapshots.len() != 3)
            {
                return Err(Db2Error::other(format!(
                    "unexpected checkpoint CLOB/DBCLOB/XML import progress: {progress:?}"
                )));
            }
            drop(source_rows);

            let result = destination
                .query(&format!(
                    "SELECT ID, C, D, XMLSERIALIZE(X AS CLOB(2M)) FROM {table} ORDER BY ID"
                ))
                .await?;
            let text_length = |value: &Db2Value| match value {
                Db2Value::Text(value) => Some(value.len()),
                Db2Value::Null => None,
                _ => Some(usize::MAX),
            };
            let row_one_ok = result.rows.len() == 3
                && result.rows[0][0] == Db2Value::Integer(1)
                && result.rows[0][1] == Db2Value::Text(clob.clone())
                && result.rows[0][2] == Db2Value::Text(dbclob.clone())
                && matches!(&result.rows[0][3], Db2Value::Text(value) if value.contains("XML café 東京 🚀") && value.contains(&"x".repeat(180_000)));
            let row_two_ok = result.rows.len() == 3
                && result.rows[1][1] == Db2Value::Text(String::new())
                && result.rows[1][2] == Db2Value::Text(String::new())
                && matches!(&result.rows[1][3], Db2Value::Text(value) if value.contains("<empty"));
            let row_three_ok = result.rows.len() == 3
                && result.rows[2][1] == Db2Value::Null
                && result.rows[2][2] == Db2Value::Null
                && result.rows[2][3] == Db2Value::Null;
            if !row_one_ok || !row_two_ok || !row_three_ok {
                let actual_lengths = result
                    .rows
                    .iter()
                    .map(|row| row.iter().skip(1).map(text_length).collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                eprintln!(
                    "text/XML import check for {table}: row count {}, row checks [{row_one_ok}, {row_two_ok}, {row_three_ok}], actual character byte lengths {actual_lengths:?}",
                    result.rows.len()
                );
                return Err(Db2Error::other(format!(
                    "CLOB/DBCLOB/XML source values changed during import into {table}"
                )));
            }
        }
        Ok::<(), Db2Error>(())
    }
    .await;

    let source_cleanup = if source_created {
        source_session
            .execute(&format!("DROP TABLE {schema}.{source_table}"))
            .await
    } else {
        Ok(0)
    };
    let destination_cleanup = if destination_created {
        destination
            .execute(&format!("DROP TABLE {schema}.{destination_table}"))
            .await
    } else {
        Ok(0)
    };
    let checkpoint_cleanup = if checkpoint_created {
        destination
            .execute(&format!("DROP TABLE {schema}.{checkpoint_table}"))
            .await
    } else {
        Ok(0)
    };
    result.unwrap();
    source_cleanup.unwrap();
    destination_cleanup.unwrap();
    checkpoint_cleanup.unwrap();
    source_session.close().await.unwrap();
    destination.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_row_import_empty_source_is_a_noop_for_both_policies() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("RS_IMPORT_EMPTY_{suffix}");
    let mut destination = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut source_session = Db2Session::connect_dsn(&dsn).await.unwrap();
    let schema = current_schema(&mut destination).await;
    destination
        .execute(&format!(
            "CREATE TABLE {schema}.{table} (ID INTEGER NOT NULL PRIMARY KEY)"
        ))
        .await
        .unwrap();

    let result = async {
        for policy in [
            Db2ImportPolicy::Atomic {
                max_batch_rows: 8,
                max_batch_bytes: 1024,
            },
            Db2ImportPolicy::CommitEveryRows {
                rows: 8,
                max_batch_bytes: 1024,
            },
        ] {
            let mut source = source_session
                .query_stream(&format!("SELECT ID FROM {table} WHERE 1 = 0"))
                .await?;
            let target = Db2ImportTarget::new(
                None,
                &table,
                vec![Db2ImportColumn::new("ID")],
            )?;
            let mut observer_called = false;
            let progress = destination
                .import_rows_from(&mut source, &target, policy, |_| {
                    observer_called = true;
                    Ok(())
                })
                .await
                .map_err(|error| error.cause)?;
            if progress != Default::default() || observer_called {
                return Err(Db2Error::other(format!(
                    "empty import was not a no-op: progress={progress:?}, observer_called={observer_called}"
                )));
            }
            drop(source);
            let count = destination
                .query(&format!("SELECT COUNT(*) FROM {table}"))
                .await?;
            if count.rows != vec![vec![Db2Value::Integer(0)]] {
                return Err(Db2Error::other(
                    "empty import unexpectedly inserted rows".into(),
                ));
            }
        }
        Ok::<(), Db2Error>(())
    }
    .await;

    let cleanup = destination
        .execute(&format!("DROP TABLE {schema}.{table}"))
        .await;
    result.unwrap();
    cleanup.unwrap();
    source_session.close().await.unwrap();
    destination.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_row_import_allows_single_rows_over_the_soft_byte_limit() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        % 1_000_000_000;
    let atomic_table = format!("RS_OVA_{suffix:09}");
    let checkpoint_table = format!("RS_OVC_{suffix:09}");
    let mut destination = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut source_session = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut created_atomic = false;
    let mut created_checkpoint = false;
    let schema = current_schema(&mut destination).await;

    let result = async {
        for (table, policy) in [
            (
                atomic_table.as_str(),
                Db2ImportPolicy::Atomic {
                    max_batch_rows: 8,
                    max_batch_bytes: 1,
                },
            ),
            (
                checkpoint_table.as_str(),
                Db2ImportPolicy::CommitEveryRows {
                    rows: 8,
                    max_batch_bytes: 1,
                },
            ),
        ] {
            destination
                .execute(&format!(
                    "CREATE TABLE {schema}.{table} (ID INTEGER NOT NULL PRIMARY KEY)"
                ))
                .await?;
            if table == atomic_table {
                created_atomic = true;
            } else {
                created_checkpoint = true;
            }
            let mut source = source_session
                .query_stream("VALUES CAST(73 AS INTEGER)")
                .await?;
            let target = Db2ImportTarget::new(None, table, vec![Db2ImportColumn::new("ID")])?;
            let progress = destination
                .import_rows_from(&mut source, &target, policy, |_| Ok(()))
                .await
                .map_err(|error| error.cause)?;
            if progress.source_rows_read != 1
                || progress.committed_rows != 1
                || progress.committed_batches != 1
                || progress.estimated_committed_bytes <= 1
            {
                return Err(Db2Error::other(format!(
                    "oversized singleton row did not commit as one batch: {progress:?}"
                )));
            }
            drop(source);
            let readback = destination
                .query(&format!("SELECT ID FROM {table}"))
                .await?;
            if readback.rows != vec![vec![Db2Value::Integer(73)]] {
                return Err(Db2Error::other(
                    "oversized singleton import readback did not match".into(),
                ));
            }
        }
        Ok::<(), Db2Error>(())
    }
    .await;

    let cleanup_checkpoint = if created_checkpoint {
        destination
            .execute(&format!("DROP TABLE {schema}.{checkpoint_table}"))
            .await
    } else {
        Ok(0)
    };
    let cleanup_atomic = if created_atomic {
        destination
            .execute(&format!("DROP TABLE {schema}.{atomic_table}"))
            .await
    } else {
        Ok(0)
    };
    result.unwrap();
    cleanup_checkpoint.unwrap();
    cleanup_atomic.unwrap();
    source_session.close().await.unwrap();
    destination.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_row_import_combines_exact_row_and_byte_boundaries() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let atomic_table = format!("RS_IMPORT_BOUNDARY_A_{suffix}");
    let checkpoint_table = format!("RS_IMPORT_BOUNDARY_C_{suffix}");
    let values = [
        "a".repeat(11),
        "b".repeat(12),
        "c".repeat(4),
        "d".repeat(4),
        "e".repeat(4),
        "f".repeat(13),
        "g".repeat(11),
        "h".repeat(4),
        "i".repeat(50),
    ];
    let source_values = values
        .iter()
        .enumerate()
        .map(|(index, value)| format!("({}, CAST('{value}' AS VARCHAR(100)))", index + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let source_sql = format!("SELECT V FROM (VALUES {source_values}) AS T(ID, V) ORDER BY ID");
    let mut expected_values = values.to_vec();
    expected_values.sort_by(|left, right| left.len().cmp(&right.len()).then(left.cmp(right)));
    let expected_rows = expected_values
        .into_iter()
        .map(|value| vec![Db2Value::Text(value)])
        .collect::<Vec<_>>();

    let mut destination = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut source_session = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut created_atomic = false;
    let mut created_checkpoint = false;
    let schema = current_schema(&mut destination).await;

    let result = async {
        for (table, policy) in [
            (
                atomic_table.as_str(),
                Db2ImportPolicy::Atomic {
                    max_batch_rows: 3,
                    max_batch_bytes: 39,
                },
            ),
            (
                checkpoint_table.as_str(),
                Db2ImportPolicy::CommitEveryRows {
                    rows: 3,
                    max_batch_bytes: 39,
                },
            ),
        ] {
            destination
                .execute(&format!(
                    "CREATE TABLE {schema}.{table} (VALUE VARCHAR(100))"
                ))
                .await?;
            if table == atomic_table {
                created_atomic = true;
            } else {
                created_checkpoint = true;
            }

            let target = Db2ImportTarget::new(
                None,
                table,
                vec![Db2ImportColumn::new("VALUE")],
            )?;
            let mut source = source_session.query_stream(&source_sql).await?;
            let mut snapshots = Vec::new();
            let progress = destination
                .import_rows_from(&mut source, &target, policy, |snapshot| {
                    snapshots.push(snapshot);
                    Ok(())
                })
                .await
                .map_err(|error| error.cause)?;
            drop(source);

            let expected_batches = if table == atomic_table { 1 } else { 5 };
            if progress.source_rows_read != 9
                || progress.committed_rows != 9
                || progress.committed_batches != expected_batches
                || progress.estimated_committed_bytes != 185
            {
                return Err(Db2Error::other(format!(
                    "row/byte boundary import progress mismatch for {table}: {progress:?}"
                )));
            }
            let expected_snapshots = if table == atomic_table {
                vec![(9, 0, 0, 0)]
            } else {
                vec![
                    (2, 0, 0, 0),
                    (5, 2, 1, 39),
                    (7, 5, 2, 75),
                    (9, 6, 3, 96),
                    (9, 8, 4, 127),
                ]
            };
            let actual_snapshots = snapshots
                .iter()
                .map(|snapshot| (
                    snapshot.source_rows_read,
                    snapshot.committed_rows,
                    snapshot.committed_batches,
                    snapshot.estimated_committed_bytes,
                ))
                .collect::<Vec<_>>();
            if actual_snapshots != expected_snapshots {
                return Err(Db2Error::other(format!(
                    "row/byte boundary checkpoint snapshots mismatch for {table}: {actual_snapshots:?}"
                )));
            }

            let readback = destination
                .query(&format!(
                    "SELECT VALUE FROM {table} ORDER BY LENGTH(VALUE), VALUE"
                ))
                .await?;
            if readback.rows != expected_rows {
                return Err(Db2Error::other(format!(
                    "row/byte boundary import readback mismatch for {table}"
                )));
            }
        }
        Ok::<(), Db2Error>(())
    }
    .await;

    let cleanup_checkpoint = if created_checkpoint {
        destination
            .execute(&format!("DROP TABLE {schema}.{checkpoint_table}"))
            .await
    } else {
        Ok(0)
    };
    let cleanup_atomic = if created_atomic {
        destination
            .execute(&format!("DROP TABLE {schema}.{atomic_table}"))
            .await
    } else {
        Ok(0)
    };
    result.unwrap();
    cleanup_checkpoint.unwrap();
    cleanup_atomic.unwrap();
    source_session.close().await.unwrap();
    destination.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_progressive_clob_and_dbclob_streams_preserve_unicode_bytes() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let table = format!(
        "RS_PROGRESSIVE_TEXT_LOB_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (C CLOB(2M), D DBCLOB(2M))"
    ))
    .await
    .unwrap();

    let clob = "Zażółć gęślą jaźń – café 東京 🚀 ".repeat(5_000);
    let dbclob = "漢字と日本語 – Zażółć 🚀 ".repeat(5_000);
    let clob_bytes = clob.as_bytes().to_vec();
    let dbclob_source_bytes = dbclob.as_bytes().to_vec();
    let write = async {
        let mut clob_source = tokio::io::BufReader::new(std::io::Cursor::new(clob_bytes.clone()));
        let mut dbclob_source =
            tokio::io::BufReader::new(std::io::Cursor::new(dbclob_source_bytes.clone()));
        let mut parameters = [
            Db2InputParameter::ClobStream {
                source: &mut clob_source,
                length: clob_bytes.len() as u64,
            },
            Db2InputParameter::DbClobStream {
                source: &mut dbclob_source,
                length: dbclob_source_bytes.len() as u64,
            },
        ];
        db.execute_with_blob_streams(
            &format!("INSERT INTO {table} (C, D) VALUES (?, ?)"),
            &mut parameters,
        )
        .await?;

        let mut reader = db
            .query_stream(&format!("SELECT C, D FROM {table}"))
            .await?;
        let row = reader
            .next()
            .await?
            .ok_or_else(|| Db2Error::other("text LOB query returned no row".into()))?;
        let value_kind = |value: &Db2Value| match value {
            Db2Value::Lob(_) => "progressive locator",
            Db2Value::Text(_) => "materialized text",
            Db2Value::Bytes(_) => "materialized bytes",
            Db2Value::Null => "NULL",
            _ => "unexpected scalar",
        };
        let sql_types = reader
            .columns()
            .iter()
            .map(|column| column.sql_type)
            .collect::<Vec<_>>();
        let value_kinds = row.iter().map(value_kind).collect::<Vec<_>>();
        // This diagnostic deliberately reports only protocol metadata and public value kinds,
        // never the test payload. Db2 can choose a progressive locator (marker 0x03) or the
        // ordinary EXTDTA path (marker 0x02) by LOB type/runtime policy.
        eprintln!(
            "text LOB row-stream result: SQL types {sql_types:?}, value kinds {value_kinds:?}"
        );

        let clob_actual = match &row[0] {
            Db2Value::Lob(clob_lob) => {
                if clob_lob.encoding != justybase_db2::Db2LobEncoding::Utf8
                    || clob_lob.byte_length != clob_bytes.len() as u64
                {
                    return Err(Db2Error::other(
                        "CLOB locator metadata did not match UTF-8 source bytes".into(),
                    ));
                }
                let mut bytes = Vec::new();
                let mut lob_reader = reader.open_lob(clob_lob).await?;
                while let Some(chunk) = lob_reader.next_chunk().await? {
                    bytes.extend_from_slice(&chunk);
                }
                let _ = lob_reader;
                bytes
            }
            Db2Value::Text(value) => value.as_bytes().to_vec(),
            other => {
                return Err(Db2Error::other(format!(
                    "CLOB returned unsupported value kind {} for SQL type {}",
                    value_kind(other),
                    sql_types[0]
                )));
            }
        };
        let dbclob_actual = match &row[1] {
            Db2Value::Lob(dbclob_lob) => {
                if dbclob_lob.encoding != justybase_db2::Db2LobEncoding::Utf16Be
                    || dbclob_lob.byte_length != dbclob.encode_utf16().count() as u64 * 2
                {
                    return Err(Db2Error::other(
                        "DBCLOB locator metadata did not match UTF-16BE source bytes".into(),
                    ));
                }
                let mut bytes = Vec::new();
                let mut lob_reader = reader.open_lob(dbclob_lob).await?;
                while let Some(chunk) = lob_reader.next_chunk().await? {
                    bytes.extend_from_slice(&chunk);
                }
                let _ = lob_reader;
                bytes
            }
            Db2Value::Text(value) => value.encode_utf16().flat_map(u16::to_be_bytes).collect(),
            other => {
                return Err(Db2Error::other(format!(
                    "DBCLOB returned unsupported value kind {} for SQL type {}",
                    value_kind(other),
                    sql_types[1]
                )));
            }
        };
        if clob_actual != clob_bytes
            || String::from_utf16(
                &dbclob_actual
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
                    .collect::<Vec<_>>(),
            )
            .ok()
            .as_deref()
                != Some(dbclob.as_str())
        {
            return Err(Db2Error::other(
                "streamed/materialized CLOB or DBCLOB content differed from the source".into(),
            ));
        }
        reader.close().await?;
        Ok::<_, Db2Error>(())
    }
    .await;
    let cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
    write.unwrap();
    cleanup.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_large_clob_and_dbclob_locators_use_byte_budgets() {
    // Db2 returns progressive locators for these large text values on the verified
    // profile. A progressive DBCLOB locator advertises UTF-16 code units while its
    // GETNXTCHK chunks are UTF-16BE bytes, so the byte budget must be doubled.
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let table = format!(
        "RS_BIG_TEXT_LOB_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (C CLOB(32M), D DBCLOB(32M))"
    ))
    .await
    .unwrap();

    let clob = "é".repeat(4_000_000);
    let dbclob = "漢".repeat(4_000_000);
    let clob_bytes = clob.as_bytes().to_vec();
    let dbclob_source_bytes = dbclob.as_bytes().to_vec();

    let work = async {
        let mut clob_source =
            tokio::io::BufReader::new(std::io::Cursor::new(clob_bytes.clone()));
        let mut dbclob_source =
            tokio::io::BufReader::new(std::io::Cursor::new(dbclob_source_bytes.clone()));
        let mut parameters = [
            Db2InputParameter::ClobStream {
                source: &mut clob_source,
                length: clob_bytes.len() as u64,
            },
            Db2InputParameter::DbClobStream {
                source: &mut dbclob_source,
                length: dbclob_source_bytes.len() as u64,
            },
        ];
        db.execute_with_blob_streams(
            &format!("INSERT INTO {table} (C, D) VALUES (?, ?)"),
            &mut parameters,
        )
        .await?;

        let mut reader = db.query_stream(&format!("SELECT C, D FROM {table}")).await?;
        let row = reader
            .next()
            .await?
            .ok_or_else(|| Db2Error::other("large text LOB query returned no row".into()))?;
        let Db2Value::Lob(clob_lob) = &row[0] else {
            return Err(Db2Error::other(
                "the profile did not return a progressive CLOB locator for the large value".into(),
            ));
        };
        let Db2Value::Lob(dbclob_lob) = &row[1] else {
            return Err(Db2Error::other(
                "the profile did not return a progressive DBCLOB locator for the large value".into(),
            ));
        };
        if clob_lob.encoding != justybase_db2::Db2LobEncoding::Utf8
            || clob_lob.byte_length != clob_bytes.len() as u64
        {
            return Err(Db2Error::other(format!(
                "large CLOB locator advertised {} bytes with {:?} encoding, expected {} UTF-8 bytes",
                clob_lob.byte_length,
                clob_lob.encoding,
                clob_bytes.len()
            )));
        }
        let expected_dbclob_bytes = dbclob.encode_utf16().count() as u64 * 2;
        if dbclob_lob.encoding != justybase_db2::Db2LobEncoding::Utf16Be
            || dbclob_lob.byte_length != expected_dbclob_bytes
        {
            return Err(Db2Error::other(format!(
                "large DBCLOB locator advertised {} bytes, expected {} UTF-16BE bytes",
                dbclob_lob.byte_length, expected_dbclob_bytes
            )));
        }
        let mut clob_actual = Vec::new();
        let mut clob_reader = reader.open_lob(clob_lob).await?;
        while let Some(chunk) = clob_reader.next_chunk().await? {
            clob_actual.extend_from_slice(&chunk);
        }
        let _ = clob_reader;
        let mut dbclob_actual = Vec::new();
        let mut dbclob_reader = reader.open_lob(dbclob_lob).await?;
        while let Some(chunk) = dbclob_reader.next_chunk().await? {
            dbclob_actual.extend_from_slice(&chunk);
        }
        let _ = dbclob_reader;
        if clob_actual != clob_bytes {
            return Err(Db2Error::other(
                "large progressive CLOB bytes differed from the source".into(),
            ));
        }
        let decoded = String::from_utf16(
            &dbclob_actual
                .as_chunks::<2>().0.iter()
                .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
                .collect::<Vec<_>>(),
        )
        .map_err(|_| Db2Error::other("large progressive DBCLOB was not valid UTF-16".into()))?;
        if decoded != dbclob {
            return Err(Db2Error::other(
                "large progressive DBCLOB content differed from the source".into(),
            ));
        }
        reader.close().await?;
        Ok::<_, Db2Error>(())
    }
    .await;
    let cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
    work.unwrap();
    cleanup.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_mixed_null_and_value_parameters_keep_their_positions_across_execution_modes() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let table = format!(
        "RS_MIXED_PARAMS_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (ID INTEGER NOT NULL PRIMARY KEY, LABEL VARCHAR(30), AMOUNT DECIMAL(9,3), ENABLED BOOLEAN)"
    ))
    .await
    .unwrap();

    let mixed_sql = "VALUES (CAST(? AS SMALLINT), CAST(? AS VARCHAR(20)), CAST(? AS DECIMAL(9,3)), CAST(? AS BOOLEAN), CAST(? AS VARBINARY(10)), CAST(? AS INTEGER))";
    let mixed_parameters = || {
        vec![
            Db2Value::Integer(12),
            Db2Value::Null,
            Db2Value::Decimal("42.125".into()),
            Db2Value::Boolean(true),
            Db2Value::Bytes(vec![0, 0x7f, 0x80, 0xff]),
            Db2Value::Null,
        ]
    };

    let work = async {
        let direct = db.query_with_params(mixed_sql, &mixed_parameters()).await?;
        let query_statement = db.prepare(mixed_sql).await?;
        let prepared = db
            .query_prepared(&query_statement, &mixed_parameters())
            .await?;

        let insert_sql =
            format!("INSERT INTO {table} (ID, LABEL, AMOUNT, ENABLED) VALUES (?, ?, ?, ?)");
        let prepared_insert = db.prepare(&insert_sql).await?;
        let prepared_count = db
            .execute_prepared(
                &prepared_insert,
                &[
                    Db2Value::Integer(1),
                    Db2Value::Null,
                    Db2Value::Decimal("12.375".into()),
                    Db2Value::Boolean(true),
                ],
            )
            .await?;
        let direct_count = db
            .execute_with_params(
                &insert_sql,
                &[
                    Db2Value::Integer(2),
                    Db2Value::Text("direct".into()),
                    Db2Value::Null,
                    Db2Value::Boolean(false),
                ],
            )
            .await?;
        let batch_count = db
            .execute_batch_with_params(
                &insert_sql,
                &[
                    vec![
                        Db2Value::Integer(3),
                        Db2Value::Null,
                        Db2Value::Null,
                        Db2Value::Null,
                    ],
                    vec![
                        Db2Value::Integer(4),
                        Db2Value::Text("batch".into()),
                        Db2Value::Decimal("4.25".into()),
                        Db2Value::Boolean(true),
                    ],
                ],
            )
            .await?;
        let rows = db
            .query(&format!(
                "SELECT ID, LABEL, AMOUNT, ENABLED FROM {table} ORDER BY ID"
            ))
            .await?;
        Ok::<_, Db2Error>((
            direct.rows,
            prepared.rows,
            prepared_count,
            direct_count,
            batch_count,
            rows.rows,
        ))
    }
    .await;
    let cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
    let (direct, prepared, prepared_count, direct_count, batch_count, rows) = match work {
        Ok(results) => {
            cleanup.unwrap();
            results
        }
        Err(work_error) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("mixed-parameter cleanup also failed: {cleanup_error}");
            }
            panic!("mixed-parameter workload failed: {work_error}");
        }
    };

    let expected_query = vec![vec![
        Db2Value::Integer(12),
        Db2Value::Null,
        Db2Value::Decimal("42.125".into()),
        Db2Value::Boolean(true),
        Db2Value::Bytes(vec![0, 0x7f, 0x80, 0xff]),
        Db2Value::Null,
    ]];
    assert_eq!(direct, expected_query);
    assert_eq!(prepared, expected_query);
    assert_eq!(prepared_count, 1);
    assert_eq!(direct_count, 1);
    assert_eq!(batch_count, 2);
    assert_eq!(
        rows,
        vec![
            vec![
                Db2Value::Integer(1),
                Db2Value::Null,
                Db2Value::Decimal("12.375".into()),
                Db2Value::Boolean(true),
            ],
            vec![
                Db2Value::Integer(2),
                Db2Value::Text("direct".into()),
                Db2Value::Null,
                Db2Value::Boolean(false),
            ],
            vec![
                Db2Value::Integer(3),
                Db2Value::Null,
                Db2Value::Null,
                Db2Value::Null,
            ],
            vec![
                Db2Value::Integer(4),
                Db2Value::Text("batch".into()),
                Db2Value::Decimal("4.250".into()),
                Db2Value::Boolean(true),
            ],
        ]
    );
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_boolean_parameter_conversions_match_db2_and_ibm_provider() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let sql = format!("VALUES ({})", ["CAST(? AS BOOLEAN)"; 12].join(", "));
    let values = [
        "true", " FALSE ", "YES", "no", "y", "N", "On", "OFF", "0", "2", "-0", " +2 ",
    ]
    .map(|value| Db2Value::Text(value.into()));
    let expected = vec![vec![
        Db2Value::Boolean(true),
        Db2Value::Boolean(false),
        Db2Value::Boolean(true),
        Db2Value::Boolean(false),
        Db2Value::Boolean(true),
        Db2Value::Boolean(false),
        Db2Value::Boolean(true),
        Db2Value::Boolean(false),
        Db2Value::Boolean(false),
        Db2Value::Boolean(true),
        Db2Value::Boolean(false),
        Db2Value::Boolean(true),
    ]];

    assert_eq!(
        db.query_with_params(&sql, &values).await.unwrap().rows,
        expected
    );
    let statement = db.prepare(&sql).await.unwrap();
    assert_eq!(
        db.query_prepared(&statement, &values).await.unwrap().rows,
        expected
    );
    {
        let mut stream = db.query_stream_with_params(&sql, &values).await.unwrap();
        assert_eq!(stream.next().await.unwrap(), expected.first().cloned());
        stream.close().await.unwrap();
    }

    let integer_values = [
        Db2Value::Integer(0),
        Db2Value::Integer(2),
        Db2Value::Integer(-1),
    ];
    assert_eq!(
        db.query_with_params(
            "VALUES (CAST(? AS BOOLEAN), CAST(? AS BOOLEAN), CAST(? AS BOOLEAN))",
            &integer_values,
        )
        .await
        .unwrap()
        .rows,
        vec![vec![
            Db2Value::Boolean(false),
            Db2Value::Boolean(true),
            Db2Value::Boolean(true),
        ]]
    );
    for invalid in [
        Db2Value::Text("".into()),
        Db2Value::Text(" ".into()),
        Db2Value::Text("1.0".into()),
        Db2Value::Text("maybe".into()),
        Db2Value::Float(1.5),
        Db2Value::Decimal("1.5".into()),
    ] {
        assert!(
            db.query_with_params("VALUES CAST(? AS BOOLEAN)", &[invalid])
                .await
                .is_err()
        );
    }
    assert_eq!(
        db.query("VALUES 29").await.unwrap().rows,
        vec![vec![Db2Value::Integer(29)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_integer_parameter_numeric_text_and_boolean_conversions_match_ibm_provider() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let sql = format!("VALUES ({})", ["CAST(? AS SMALLINT)"; 9].join(", "));
    let values = vec![
        Db2Value::Float(-3.75),
        Db2Value::Decimal("-1.5".into()),
        Db2Value::Text(" 1.5 ".into()),
        Db2Value::Text("1e0".into()),
        Db2Value::Text(".5".into()),
        Db2Value::Text("1.".into()),
        Db2Value::Text("+0001.50".into()),
        Db2Value::Boolean(true),
        Db2Value::Boolean(false),
    ];
    let expected = vec![vec![
        Db2Value::Integer(-3),
        Db2Value::Integer(-1),
        Db2Value::Integer(1),
        Db2Value::Integer(1),
        Db2Value::Integer(0),
        Db2Value::Integer(1),
        Db2Value::Integer(1),
        Db2Value::Integer(1),
        Db2Value::Integer(0),
    ]];

    assert_eq!(
        db.query_with_params(&sql, &values).await.unwrap().rows,
        expected
    );
    let statement = db.prepare(&sql).await.unwrap();
    assert_eq!(
        db.query_prepared(&statement, &values).await.unwrap().rows,
        expected
    );
    {
        let mut stream = db.query_stream_with_params(&sql, &values).await.unwrap();
        assert_eq!(stream.next().await.unwrap(), expected.first().cloned());
        stream.close().await.unwrap();
    }
    for invalid in ["1,5", "abc", ""] {
        assert!(
            db.query_with_params(
                "VALUES CAST(? AS SMALLINT)",
                &[Db2Value::Text(invalid.into())]
            )
            .await
            .is_err()
        );
    }
    assert!(
        db.query_with_params(
            "VALUES CAST(? AS SMALLINT)",
            &[Db2Value::Text("32768.0".into())],
        )
        .await
        .is_err()
    );
    assert_eq!(
        db.query("VALUES 29").await.unwrap().rows,
        vec![vec![Db2Value::Integer(29)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_invalid_dml_and_batch_parameter_conversion_rejects_before_writing_and_keeps_session()
{
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("RS_DB2_BAT_{suffix}");
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (V INTEGER NOT NULL)"
    ))
    .await
    .unwrap();
    let insert_sql = format!("INSERT INTO {table} VALUES (?)");
    let invalid = [Db2Value::Bytes(b"123".to_vec())];
    let work = async {
        let direct = db.execute_with_params(&insert_sql, &invalid).await;
        let statement = db.prepare(&insert_sql).await?;
        let prepared = db.execute_prepared(&statement, &invalid).await;
        let batch = db
            .execute_batch_with_params(
                &insert_sql,
                &[vec![Db2Value::Integer(10)], invalid.to_vec()],
            )
            .await;
        let rows = db.query(&format!("SELECT V FROM {table}")).await?;
        let reusable = db.query("VALUES 29").await?;
        Ok::<_, Db2Error>((direct, prepared, batch, rows.rows, reusable.rows))
    }
    .await;
    let cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
    let (direct, prepared, batch, rows, reusable) = work.unwrap();

    assert_eq!(direct.unwrap_err().kind, Db2ErrorKind::ParameterConversion);
    assert_eq!(
        prepared.unwrap_err().kind,
        Db2ErrorKind::ParameterConversion
    );
    let batch_error = batch.unwrap_err();
    assert_eq!(batch_error.failed_parameter_set_index, Some(1));
    assert_eq!(batch_error.completed_parameter_sets, 0);
    assert_eq!(batch_error.rows_affected, 0);
    assert_eq!(batch_error.cause.kind, Db2ErrorKind::ParameterConversion);
    assert!(rows.is_empty());
    assert_eq!(reusable, vec![vec![Db2Value::Integer(29)]]);
    assert!(!db.is_closed());

    // B1: every query-path invalid pair must also reject locally in DML.
    let invalid_dml_cases: &[(&str, &str, Db2Value, Db2Value, Db2Value)] = &[
        (
            "SMALLINT",
            "SMALLINT",
            Db2Value::Bytes(b"123".to_vec()),
            Db2Value::Integer(1),
            Db2Value::Integer(1),
        ),
        (
            "BLOB",
            "BLOB(20)",
            Db2Value::Boolean(true),
            Db2Value::Bytes(vec![1, 2]),
            Db2Value::Bytes(vec![1, 2]),
        ),
        (
            "VARBINARY",
            "VARBINARY(20)",
            Db2Value::Integer(123),
            Db2Value::Bytes(vec![1]),
            Db2Value::Bytes(vec![1]),
        ),
        (
            "CLOB_BOOL",
            "CLOB(20)",
            Db2Value::Boolean(true),
            Db2Value::Text("hi".into()),
            Db2Value::Text("hi".into()),
        ),
        (
            "CLOB_DEC",
            "CLOB(20)",
            Db2Value::Decimal("12.50".into()),
            Db2Value::Text("hi".into()),
            Db2Value::Text("hi".into()),
        ),
        (
            "DBCLOB",
            "DBCLOB(20)",
            Db2Value::Boolean(false),
            Db2Value::Text("hi".into()),
            Db2Value::Text("hi".into()),
        ),
        (
            "XML",
            "XML",
            Db2Value::Boolean(true),
            Db2Value::Text("<r/>".into()),
            Db2Value::Text("<r/>".into()),
        ),
    ];
    for (name, sql_type, invalid_value, valid_value, expected_value) in invalid_dml_cases {
        // This binary BLOB fixture is encoded as EXTDTA and flushes the earlier
        // row before the next set is converted. The short CLOB/DBCLOB/XML values
        // in this fixture stay inline, so the driver can reject the whole chunk
        // locally without sending its valid prefix.
        let valid_set_was_sent = *name == "BLOB";
        let dml_table = format!("{table}_{name}");
        db.execute(&format!("CREATE TABLE {schema}.{dml_table} (V {sql_type})"))
            .await
            .unwrap();
        let dml_insert = format!("INSERT INTO {dml_table} VALUES (?)");
        let dml_work = async {
            let direct = db
                .execute_with_params(&dml_insert, std::slice::from_ref(invalid_value))
                .await;
            let statement = db.prepare(&dml_insert).await?;
            let prepared = db
                .execute_prepared(&statement, std::slice::from_ref(invalid_value))
                .await;
            let batch = db
                .execute_batch_with_params(
                    &dml_insert,
                    &[vec![valid_value.clone()], vec![invalid_value.clone()]],
                )
                .await;
            let rows = db.query(&format!("SELECT V FROM {dml_table}")).await?;
            let reusable = db.query("VALUES 29").await?;
            Ok::<_, Db2Error>((direct, prepared, batch, rows.rows, reusable.rows))
        }
        .await;
        let dml_cleanup = db
            .execute(&format!("DROP TABLE {schema}.{dml_table}"))
            .await;
        let (direct, prepared, batch, rows, reusable) = dml_work.unwrap();
        assert_eq!(
            direct.unwrap_err().kind,
            Db2ErrorKind::ParameterConversion,
            "{name}"
        );
        assert_eq!(
            prepared.unwrap_err().kind,
            Db2ErrorKind::ParameterConversion,
            "{name}"
        );
        let batch_error = batch.unwrap_err();
        assert_eq!(batch_error.failed_parameter_set_index, Some(1), "{name}");
        assert_eq!(
            batch_error.completed_parameter_sets,
            usize::from(valid_set_was_sent),
            "{name}"
        );
        assert_eq!(
            batch_error.rows_affected,
            i64::from(valid_set_was_sent),
            "{name}"
        );
        assert_eq!(
            batch_error.cause.kind,
            Db2ErrorKind::ParameterConversion,
            "{name}"
        );
        if valid_set_was_sent {
            assert_eq!(rows, vec![vec![expected_value.clone()]], "{name}");
        } else {
            assert!(rows.is_empty(), "{name}");
        }
        assert_eq!(reusable, vec![vec![Db2Value::Integer(29)]], "{name}");
        assert!(!db.is_closed(), "{name}");
        dml_cleanup.unwrap();
    }
    cleanup.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_parameter_source_type_conversions_match_ibm_cli() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();

    let text_sql = "VALUES (CAST(? AS VARCHAR(4)), CAST(? AS CHAR(4)), CAST(? AS GRAPHIC(4)), CAST(? AS VARGRAPHIC(4)))";
    let text_values = [
        Db2Value::Boolean(true),
        Db2Value::Boolean(false),
        Db2Value::Boolean(true),
        Db2Value::Boolean(false),
    ];
    let text_expected = vec![vec![
        Db2Value::Text("1".into()),
        Db2Value::Text("0   ".into()),
        Db2Value::Text("1   ".into()),
        Db2Value::Text("0".into()),
    ]];
    assert_eq!(
        db.query_with_params(text_sql, &text_values)
            .await
            .unwrap()
            .rows,
        text_expected
    );
    let text_statement = db.prepare(text_sql).await.unwrap();
    assert_eq!(
        db.query_prepared(&text_statement, &text_values)
            .await
            .unwrap()
            .rows,
        text_expected
    );
    {
        let mut stream = db
            .query_stream_with_params(text_sql, &text_values)
            .await
            .unwrap();
        assert_eq!(stream.next().await.unwrap(), text_expected.first().cloned());
        stream.close().await.unwrap();
    }

    let binary_sql = "VALUES (CAST(? AS VARCHAR(4)), CAST(? AS CHAR(4)), CAST(? AS VARBINARY(3)), CAST(? AS BLOB(3)), CAST(? AS CLOB(20)), CAST(? AS DBCLOB(20)), CAST(? AS XML))";
    let binary_values = [
        Db2Value::Bytes(vec![0xe6, 0x9d, 0xb1]),
        Db2Value::Bytes(b"123".to_vec()),
        Db2Value::Bytes(b"123".to_vec()),
        Db2Value::Bytes(b"123".to_vec()),
        Db2Value::Bytes(vec![0xc3, 0xa9]),
        Db2Value::Bytes(vec![0x67, 0x71]),
        Db2Value::Bytes(b"<r/>".to_vec()),
    ];
    let binary_expected = vec![vec![
        Db2Value::Text("東".into()),
        Db2Value::Text("123 ".into()),
        Db2Value::Bytes(b"123".to_vec()),
        Db2Value::Bytes(b"123".to_vec()),
        Db2Value::Text("é".into()),
        Db2Value::Text("東".into()),
        Db2Value::Text("<r/>".into()),
    ]];
    assert_eq!(
        db.query_with_params(binary_sql, &binary_values)
            .await
            .unwrap()
            .rows,
        binary_expected
    );
    let binary_statement = db.prepare(binary_sql).await.unwrap();
    assert_eq!(
        db.query_prepared(&binary_statement, &binary_values)
            .await
            .unwrap()
            .rows,
        binary_expected
    );
    {
        let mut stream = db
            .query_stream_with_params(binary_sql, &binary_values)
            .await
            .unwrap();
        assert_eq!(
            stream.next().await.unwrap(),
            binary_expected.first().cloned()
        );
        stream.close().await.unwrap();
    }

    let numeric_sql = "VALUES (CAST(? AS SMALLINT), CAST(? AS INTEGER), CAST(? AS BIGINT), CAST(? AS REAL), CAST(? AS DOUBLE), CAST(? AS DECIMAL(9,3)), CAST(? AS DECFLOAT(16)))";
    let numeric_values = vec![Db2Value::Boolean(true); 7];
    let numeric_expected = vec![vec![
        Db2Value::Integer(1),
        Db2Value::Integer(1),
        Db2Value::Integer(1),
        Db2Value::Float(1.0),
        Db2Value::Float(1.0),
        Db2Value::Decimal("1.000".into()),
        Db2Value::Decimal("1".into()),
    ]];
    assert_eq!(
        db.query_with_params(numeric_sql, &numeric_values)
            .await
            .unwrap()
            .rows,
        numeric_expected
    );
    let numeric_statement = db.prepare(numeric_sql).await.unwrap();
    assert_eq!(
        db.query_prepared(&numeric_statement, &numeric_values)
            .await
            .unwrap()
            .rows,
        numeric_expected
    );
    {
        let mut stream = db
            .query_stream_with_params(numeric_sql, &numeric_values)
            .await
            .unwrap();
        assert_eq!(
            stream.next().await.unwrap(),
            numeric_expected.first().cloned()
        );
        stream.close().await.unwrap();
    }

    let hex_binary_sql = "VALUES (CAST(? AS BINARY(4)), CAST(? AS VARBINARY(4)), CAST(? AS BLOB(4)), CAST(? AS VARBINARY(4)))";
    let hex_binary_values = [
        Db2Value::Text("313233".into()),
        Db2Value::Text("313233".into()),
        Db2Value::Text("313233".into()),
        Db2Value::Text("aBcD".into()),
    ];
    let hex_binary_expected = vec![vec![
        Db2Value::Bytes(vec![49, 50, 51, 0]),
        Db2Value::Bytes(b"123".to_vec()),
        Db2Value::Bytes(b"123".to_vec()),
        Db2Value::Bytes(vec![0xab, 0xcd]),
    ]];
    assert_eq!(
        db.query_with_params(hex_binary_sql, &hex_binary_values)
            .await
            .unwrap()
            .rows,
        hex_binary_expected
    );
    let hex_statement = db.prepare(hex_binary_sql).await.unwrap();
    assert_eq!(
        db.query_prepared(&hex_statement, &hex_binary_values)
            .await
            .unwrap()
            .rows,
        hex_binary_expected
    );
    {
        let mut stream = db
            .query_stream_with_params(hex_binary_sql, &hex_binary_values)
            .await
            .unwrap();
        assert_eq!(
            stream.next().await.unwrap(),
            hex_binary_expected.first().cloned()
        );
        stream.close().await.unwrap();
    }
    assert_eq!(
        db.query_with_params(
            "VALUES (CAST(? AS VARBINARY(4)), CAST(? AS BLOB(4)))",
            &[Db2Value::Text(String::new()), Db2Value::Text(String::new())],
        )
        .await
        .unwrap()
        .rows,
        vec![vec![
            Db2Value::Bytes(Vec::new()),
            Db2Value::Bytes(Vec::new())
        ]]
    );

    let table = format!(
        "RS_BINSTR_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (ID INT NOT NULL PRIMARY KEY, F BINARY(4), V VARBINARY(4), B BLOB(4))"
    ))
    .await
    .unwrap();
    let insert_sql = format!("INSERT INTO {table} VALUES (?, ?, ?, ?)");
    let insert_row = |id, value: &str| {
        vec![
            Db2Value::Integer(id),
            Db2Value::Text(value.into()),
            Db2Value::Text(value.into()),
            Db2Value::Text(value.into()),
        ]
    };
    let binary_rows = async {
        assert_eq!(
            db.execute_with_params(&insert_sql, &insert_row(1, "313233"))
                .await?,
            1
        );
        let insert_statement = db.prepare(&insert_sql).await?;
        assert_eq!(
            db.execute_prepared(&insert_statement, &insert_row(2, "313233"))
                .await?,
            1
        );
        let batch = vec![insert_row(3, "313233"), insert_row(4, "")];
        assert_eq!(db.execute_batch_with_params(&insert_sql, &batch).await?, 2);
        let expected_value = vec![
            Db2Value::Bytes(vec![49, 50, 51, 0]),
            Db2Value::Bytes(b"123".to_vec()),
            Db2Value::Bytes(b"123".to_vec()),
        ];
        let mut expected = vec![
            vec![Db2Value::Integer(1)],
            vec![Db2Value::Integer(2)],
            vec![Db2Value::Integer(3)],
        ];
        for row in &mut expected {
            row.extend(expected_value.clone());
        }
        expected.push(vec![
            Db2Value::Integer(4),
            Db2Value::Bytes(vec![0; 4]),
            Db2Value::Bytes(Vec::new()),
            Db2Value::Bytes(Vec::new()),
        ]);
        assert_eq!(
            db.query(&format!("SELECT ID, F, V, B FROM {table} ORDER BY ID"))
                .await?
                .rows,
            expected
        );
        Ok::<(), Db2Error>(())
    }
    .await;
    let binary_cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
    binary_rows.unwrap();
    binary_cleanup.unwrap();

    let unsupported = [
        (
            "VALUES CAST(? AS SMALLINT)",
            Db2Value::Bytes(b"123".to_vec()),
        ),
        ("VALUES CAST(? AS BLOB(20))", Db2Value::Boolean(true)),
        ("VALUES CAST(? AS VARBINARY(20))", Db2Value::Integer(123)),
        ("VALUES CAST(? AS CLOB(20))", Db2Value::Boolean(true)),
        (
            "VALUES CAST(? AS CLOB(20))",
            Db2Value::Decimal("12.50".into()),
        ),
        ("VALUES CAST(? AS DBCLOB(20))", Db2Value::Boolean(false)),
        ("VALUES CAST(? AS XML)", Db2Value::Boolean(true)),
    ];
    for (sql, value) in unsupported {
        assert!(db.query_with_params(sql, &[value]).await.is_err(), "{sql}");
    }
    let invalid_binary_text = db
        .query_with_params(
            "VALUES CAST(? AS VARBINARY(10))",
            &[Db2Value::Text("123".into())],
        )
        .await
        .unwrap_err();
    assert_eq!(invalid_binary_text.kind, Db2ErrorKind::ParameterConversion);
    let invalid_hex_statement = db.prepare("VALUES CAST(? AS VARBINARY(10))").await.unwrap();
    let invalid_hex_error = db
        .query_prepared(&invalid_hex_statement, &[Db2Value::Text("12G4".into())])
        .await
        .unwrap_err();
    assert_eq!(invalid_hex_error.kind, Db2ErrorKind::ParameterConversion);
    let invalid_length = match db
        .query_stream_with_params(
            "VALUES CAST(? AS VARBINARY(2))",
            &[Db2Value::Text("313233".into())],
        )
        .await
    {
        Ok(_) => panic!("oversized hexadecimal text must fail before opening a stream"),
        Err(error) => error,
    };
    assert_eq!(invalid_length.kind, Db2ErrorKind::ParameterConversion);
    let rejected_numeric_sql = "VALUES CAST(? AS SMALLINT)";
    let rejected_binary = [Db2Value::Bytes(b"123".to_vec())];
    let direct_error = db
        .query_with_params(rejected_numeric_sql, &rejected_binary)
        .await
        .expect_err("binary-to-smallint parameter conversion should fail locally");
    assert_eq!(direct_error.kind, Db2ErrorKind::ParameterConversion);

    let rejected_numeric_statement = db.prepare(rejected_numeric_sql).await.unwrap();
    let prepared_error = db
        .query_prepared(&rejected_numeric_statement, &rejected_binary)
        .await
        .expect_err("prepared binary-to-smallint conversion should fail locally");
    assert_eq!(prepared_error.kind, Db2ErrorKind::ParameterConversion);

    let stream_error = db
        .query_stream_with_params(rejected_numeric_sql, &rejected_binary)
        .await
        .err()
        .expect("streamed binary-to-smallint conversion should fail locally");
    assert_eq!(stream_error.kind, Db2ErrorKind::ParameterConversion);
    assert!(
        !db.is_closed(),
        "local parameter conversion errors must leave the session reusable"
    );
    assert_eq!(
        db.query("VALUES 29").await.unwrap().rows,
        vec![vec![Db2Value::Integer(29)]]
    );

    let invalid_utf8_sql = "VALUES HEX(CAST(? AS VARCHAR(20)))";
    let invalid_utf8 = [Db2Value::Bytes(vec![0xe9])];
    let invalid_utf8_expected = vec![vec![Db2Value::Text("E9".into())]];
    assert_eq!(
        db.query_with_params(invalid_utf8_sql, &invalid_utf8)
            .await
            .unwrap()
            .rows,
        invalid_utf8_expected
    );
    let invalid_utf8_statement = db.prepare(invalid_utf8_sql).await.unwrap();
    assert_eq!(
        db.query_prepared(&invalid_utf8_statement, &invalid_utf8)
            .await
            .unwrap()
            .rows,
        invalid_utf8_expected
    );
    let mut invalid_utf8_stream = db
        .query_stream_with_params(invalid_utf8_sql, &invalid_utf8)
        .await
        .unwrap();
    assert_eq!(
        invalid_utf8_stream.next().await.unwrap(),
        invalid_utf8_expected.first().cloned()
    );
    invalid_utf8_stream.close().await.unwrap();
    assert_eq!(
        db.query("VALUES 29").await.unwrap().rows,
        vec![vec![Db2Value::Integer(29)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_mixed_scalar_parameter_matrix_matches_expected_in_direct_and_prepared_queries() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();

    let binary = vec![0, 0x7f, 0x80, 0xff];
    let cases: Vec<(&str, &str, Db2Value, Db2Value, Db2Value, Db2Value)> = vec![
        (
            "SMALLINT",
            "CAST(? AS SMALLINT)",
            Db2Value::Integer(i16::MIN as i64),
            Db2Value::Null,
            Db2Value::Integer(i16::MIN as i64),
            Db2Value::Null,
        ),
        (
            "INTEGER",
            "CAST(? AS INTEGER)",
            Db2Value::Null,
            Db2Value::Integer(i32::MAX as i64),
            Db2Value::Null,
            Db2Value::Integer(i32::MAX as i64),
        ),
        (
            "BIGINT",
            "CAST(? AS BIGINT)",
            Db2Value::Integer(i64::MAX),
            Db2Value::Null,
            Db2Value::Integer(i64::MAX),
            Db2Value::Null,
        ),
        (
            "REAL",
            "CAST(? AS REAL)",
            Db2Value::Null,
            Db2Value::Float(1.25),
            Db2Value::Null,
            Db2Value::Float(1.25),
        ),
        (
            "DOUBLE",
            "CAST(? AS DOUBLE)",
            Db2Value::Float(-2.5),
            Db2Value::Null,
            Db2Value::Float(-2.5),
            Db2Value::Null,
        ),
        (
            "DECIMAL(9,3)",
            "CAST(? AS DECIMAL(9,3))",
            Db2Value::Null,
            Db2Value::Decimal("42.125".into()),
            Db2Value::Null,
            Db2Value::Decimal("42.125".into()),
        ),
        (
            "DECFLOAT(16)",
            "CAST(? AS DECFLOAT(16))",
            Db2Value::Decimal("Infinity".into()),
            Db2Value::Null,
            Db2Value::Decimal("Infinity".into()),
            Db2Value::Null,
        ),
        (
            "DECFLOAT(34)",
            "CAST(? AS DECFLOAT(34))",
            Db2Value::Null,
            Db2Value::Decimal("1234567890123456789012345678901234".into()),
            Db2Value::Null,
            Db2Value::Decimal("1234567890123456789012345678901234".into()),
        ),
        (
            "DATE",
            "CAST(? AS DATE)",
            Db2Value::Text("2026-08-26".into()),
            Db2Value::Null,
            Db2Value::Text("2026-08-26".into()),
            Db2Value::Null,
        ),
        (
            "TIME",
            "CAST(? AS TIME)",
            Db2Value::Null,
            Db2Value::Text("12:34:56".into()),
            Db2Value::Null,
            Db2Value::Text("12.34.56".into()),
        ),
        (
            "TIMESTAMP",
            "CAST(? AS TIMESTAMP)",
            Db2Value::Text("2026-08-26 12:34:56.123456".into()),
            Db2Value::Null,
            Db2Value::Text("2026-08-26-12.34.56.123456".into()),
            Db2Value::Null,
        ),
        (
            "CHAR(5)",
            "CAST(? AS CHAR(5))",
            Db2Value::Text("ab".into()),
            Db2Value::Null,
            Db2Value::Text("ab   ".into()),
            Db2Value::Null,
        ),
        (
            "VARCHAR(20)",
            "CAST(? AS VARCHAR(20))",
            Db2Value::Null,
            Db2Value::Text("café".into()),
            Db2Value::Null,
            Db2Value::Text("café".into()),
        ),
        (
            "GRAPHIC(5)",
            "CAST(? AS GRAPHIC(5))",
            Db2Value::Text("東".into()),
            Db2Value::Null,
            Db2Value::Text("東    ".into()),
            Db2Value::Null,
        ),
        (
            "VARGRAPHIC(10)",
            "CAST(? AS VARGRAPHIC(10))",
            Db2Value::Null,
            Db2Value::Text("東京".into()),
            Db2Value::Null,
            Db2Value::Text("東京".into()),
        ),
        (
            "BINARY(4)",
            "CAST(? AS BINARY(4))",
            Db2Value::Bytes(binary.clone()),
            Db2Value::Null,
            Db2Value::Bytes(binary.clone()),
            Db2Value::Null,
        ),
        (
            "VARBINARY(10)",
            "CAST(? AS VARBINARY(10))",
            Db2Value::Null,
            Db2Value::Bytes(binary.clone()),
            Db2Value::Null,
            Db2Value::Bytes(binary.clone()),
        ),
        (
            "BLOB(100)",
            "CAST(? AS BLOB(100))",
            Db2Value::Bytes(binary.clone()),
            Db2Value::Null,
            Db2Value::Bytes(binary.clone()),
            Db2Value::Null,
        ),
        (
            "CLOB(100)",
            "CAST(? AS CLOB(100))",
            Db2Value::Null,
            Db2Value::Text("CLOB café".into()),
            Db2Value::Null,
            Db2Value::Text("CLOB café".into()),
        ),
        (
            "DBCLOB(100)",
            "CAST(? AS DBCLOB(100))",
            Db2Value::Text("DBCLOB 東京".into()),
            Db2Value::Null,
            Db2Value::Text("DBCLOB 東京".into()),
            Db2Value::Null,
        ),
        (
            "XML",
            "CAST(? AS XML)",
            Db2Value::Null,
            Db2Value::Text("<root/>".into()),
            Db2Value::Null,
            Db2Value::Text("<root/>".into()),
        ),
        (
            "BOOLEAN",
            "CAST(? AS BOOLEAN)",
            Db2Value::Boolean(true),
            Db2Value::Null,
            Db2Value::Boolean(true),
            Db2Value::Null,
        ),
    ];

    let mut expressions = Vec::with_capacity(cases.len() * 2);
    let mut parameters = Vec::with_capacity(cases.len() * 2);
    let mut expected = Vec::with_capacity(cases.len() * 2);
    for (_, expression, first_parameter, second_parameter, first_value, second_value) in &cases {
        expressions.push(expression.to_owned());
        expressions.push(expression.to_owned());
        parameters.push(first_parameter.clone());
        parameters.push(second_parameter.clone());
        expected.push(first_value.clone());
        expected.push(second_value.clone());
    }
    let sql = format!("VALUES ({})", expressions.join(", "));

    let direct = db.query_with_params(&sql, &parameters).await.unwrap();
    assert_eq!(direct.rows, vec![expected.clone()]);
    let statement = db.prepare(&sql).await.unwrap();
    let prepared = db.query_prepared(&statement, &parameters).await.unwrap();
    assert_eq!(prepared.rows, vec![expected.clone()]);

    let mut streamed = db
        .query_stream_with_params(&sql, &parameters)
        .await
        .unwrap();
    assert_eq!(streamed.next().await.unwrap(), Some(expected.clone()));
    assert_eq!(streamed.next().await.unwrap(), None);
    drop(streamed);

    let mut prepared_stream = db
        .query_prepared_stream(&statement, &parameters)
        .await
        .unwrap();
    assert_eq!(prepared_stream.next().await.unwrap(), Some(expected));
    assert_eq!(prepared_stream.next().await.unwrap(), None);
    drop(prepared_stream);

    let table = format!(
        "RS_TYPE_MATRIX_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let columns = cases
        .iter()
        .enumerate()
        .map(|(index, (sql_type, ..))| format!("C{index} {sql_type}"))
        .collect::<Vec<_>>()
        .join(", ");
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (ID INTEGER NOT NULL PRIMARY KEY, {columns})"
    ))
    .await
    .unwrap();

    let input_row = |id: i32, first: bool| {
        let mut row = vec![Db2Value::Integer(i64::from(id))];
        for case in &cases {
            row.push(if first {
                case.2.clone()
            } else {
                case.3.clone()
            });
        }
        row
    };
    let expected_row = |id: i32, first: bool| {
        let mut row = vec![Db2Value::Integer(i64::from(id))];
        for case in &cases {
            row.push(if first {
                case.4.clone()
            } else {
                case.5.clone()
            });
        }
        row
    };
    let insert_sql = format!(
        "INSERT INTO {table} VALUES ({})",
        vec!["?"; cases.len() + 1].join(", ")
    );
    let dml_result = async {
        let insert = db.prepare(&insert_sql).await?;
        assert_eq!(db.execute_prepared(&insert, &input_row(1, true)).await?, 1);
        assert_eq!(
            db.execute_with_params(&insert_sql, &input_row(2, false))
                .await?,
            1
        );
        let batch = vec![input_row(3, true), input_row(4, false)];
        assert_eq!(db.execute_batch_with_params(&insert_sql, &batch).await?, 2);
        let persisted = db
            .query(&format!("SELECT * FROM {table} ORDER BY ID"))
            .await?;
        assert_eq!(
            persisted.rows,
            vec![
                expected_row(1, true),
                expected_row(2, false),
                expected_row(3, true),
                expected_row(4, false),
            ]
        );
        Ok::<(), Db2Error>(())
    }
    .await;
    let cleanup_result = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
    dml_result.unwrap();
    cleanup_result.unwrap();

    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_queries_materialize_extdta_xml_and_large_objects() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();

    let xml = db.query(r#"SELECT XMLPARSE(DOCUMENT '<root><item id="1">x</item></root>') FROM SYSIBM.SYSDUMMY1"#).await.unwrap();
    assert_eq!(
        xml.rows,
        vec![vec![Db2Value::Text(
            "<root><item id=\"1\">x</item></root>".into()
        )]]
    );

    let null_xml = db
        .query("SELECT CAST(NULL AS XML) FROM SYSIBM.SYSDUMMY1")
        .await
        .unwrap();
    assert_eq!(null_xml.rows, vec![vec![Db2Value::Null]]);

    let large_xml = db.query("SELECT XMLELEMENT(NAME \"r\", XMLAGG(XMLELEMENT(NAME \"c\", COLNAME))) FROM SYSCAT.COLUMNS").await.unwrap();
    let Db2Value::Text(xml_text) = &large_xml.rows[0][0] else {
        panic!("expected large XML text, got {:?}", large_xml.rows[0][0]);
    };
    assert!(
        xml_text.len() >= 100_000,
        "large XML is {} characters",
        xml_text.len()
    );
    assert!(xml_text.starts_with("<r><c>"));
    assert!(xml_text.ends_with("</c></r>"));

    let blob = db.query("SELECT BLOB(CAST(REPEAT('z', 30000) AS VARCHAR(30000) FOR BIT DATA)) || BLOB(CAST(REPEAT('y', 30000) AS VARCHAR(30000) FOR BIT DATA)) FROM SYSIBM.SYSDUMMY1").await.unwrap();
    let Db2Value::Bytes(bytes) = &blob.rows[0][0] else {
        panic!("expected BLOB bytes, got {:?}", blob.rows[0][0]);
    };
    assert_eq!(bytes.len(), 60_000);
    assert_eq!(bytes, &[vec![b'z'; 30_000], vec![b'y'; 30_000]].concat());

    let xml_rows = db
        .query("SELECT XMLELEMENT(NAME \"t\", TABNAME) FROM SYSCAT.TABLES FETCH FIRST 5 ROWS ONLY")
        .await
        .unwrap();
    assert_eq!(xml_rows.rows.len(), 5);
    assert!(
        xml_rows
            .rows
            .iter()
            .all(|row| matches!(&row[0], Db2Value::Text(value) if value.starts_with("<t>")))
    );
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_transaction_handles_commit_and_rollback_atomically() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let table = format!(
        "RS_DB2_TX_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!("CREATE TABLE {schema}.{table} (V INTEGER)"))
        .await
        .unwrap();

    {
        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .execute(&format!("INSERT INTO {table} VALUES (1)"))
            .await
            .unwrap();
        transaction.rollback().await.unwrap();
    }
    assert!(db.auto_commit());
    assert_eq!(
        db.query(&format!("SELECT COUNT(*) FROM {table}"))
            .await
            .unwrap()
            .rows,
        vec![vec![Db2Value::Integer(0)]]
    );

    {
        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .execute(&format!("INSERT INTO {table} VALUES (2)"))
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    }
    assert!(db.auto_commit());
    assert_eq!(
        db.query(&format!("SELECT V FROM {table}"))
            .await
            .unwrap()
            .rows,
        vec![vec![Db2Value::Integer(2)]]
    );

    db.execute(&format!("DROP TABLE {schema}.{table}"))
        .await
        .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_batch_dml_reports_counts_partial_failure_and_keeps_session_usable() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let table = format!(
        "RS_DB2_BATCH_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (V INTEGER NOT NULL UNIQUE)"
    ))
    .await
    .unwrap();
    async {
        assert_eq!(
            db.execute_batch_with_params(
                &format!("INSERT INTO {table} (V) VALUES (?)"),
                &[
                    vec![Db2Value::Integer(1)],
                    vec![Db2Value::Integer(2)],
                    vec![Db2Value::Integer(3)],
                ],
            )
            .await
            .unwrap(),
            3 // Three confirmed successful INSERT parameter sets.
        );
        assert_eq!(
            db.execute_batch_with_params(&format!("INSERT INTO {table} (V) VALUES (?)"), &[])
                .await
                .unwrap(),
            0
        );
        let batch_error = db
            .execute_batch_with_params(
                &format!("INSERT INTO {table} (V) VALUES (?)"),
                &[vec![Db2Value::Integer(4)], vec![Db2Value::Integer(4)]],
            )
            .await
            .unwrap_err();
        assert_eq!(batch_error.failed_parameter_set_index, Some(1));
        assert_eq!(batch_error.completed_parameter_sets, 1);
        assert_eq!(batch_error.rows_affected, 1);
        assert_eq!(batch_error.cause.kind, Db2ErrorKind::SqlServer);
        assert_eq!(batch_error.cause.sql_code, Some(-803));
        assert_eq!(batch_error.cause.sql_state.as_deref(), Some("23505"));
        assert_eq!(
            db.query(&format!("SELECT COUNT(*) FROM {table}"))
                .await
                .unwrap()
                .rows,
            vec![vec![Db2Value::Integer(4)]]
        );
        db.ping().await.unwrap();
    }
    .await;
    let cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
    db.close().await.unwrap();
    cleanup.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_batch_pipeline_restarts_correlation_ids_and_attributes_chunk_errors() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let table = format!(
        "RS_DB2_PIPE_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (V INTEGER NOT NULL UNIQUE)"
    ))
    .await
    .unwrap();
    let result = async {
        db.set_auto_commit(false);
        let long_batch = (0..33_000)
            .map(|value| vec![Db2Value::Integer(value)])
            .collect::<Vec<_>>();
        assert_eq!(
            db.execute_batch_with_params(
                &format!("INSERT INTO {table} (V) VALUES (?)"),
                &long_batch
            )
            .await?,
            33_000
        );
        db.commit().await?;
        db.set_auto_commit(true);
        let mut boundary_batch = (40_000..40_256)
            .map(|value| vec![Db2Value::Integer(value)])
            .collect::<Vec<_>>();
        boundary_batch.push(vec![Db2Value::Integer(40_000)]);
        let error = db
            .execute_batch_with_params(
                &format!("INSERT INTO {table} (V) VALUES (?)"),
                &boundary_batch,
            )
            .await
            .unwrap_err();
        assert_eq!(error.failed_parameter_set_index, Some(256));
        assert_eq!(error.completed_parameter_sets, 256);
        assert_eq!(error.cause.sql_code, Some(-803));
        assert_eq!(
            db.query(&format!("SELECT COUNT(*) FROM {table}"))
                .await?
                .rows,
            vec![vec![Db2Value::Integer(33_256)]]
        );
        db.ping().await?;
        Ok::<(), Db2Error>(())
    }
    .await;
    let cleanup = db.execute(&format!("DROP TABLE {schema}.{table}")).await;
    db.close().await.unwrap();
    result.unwrap();
    cleanup.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_insert_results_return_generated_identity_values() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let table = format!(
        "RS_DB2_ID_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = current_schema(&mut db).await;
    db.execute(&format!(
        "CREATE TABLE {schema}.{table} (ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY, V VARCHAR(32))"
    ))
    .await
    .unwrap();

    let direct = db
        .execute_insert_with_params(
            &format!("INSERT INTO {table} (V) VALUES (?)"),
            &[Db2Value::Text("direct".into())],
        )
        .await
        .unwrap();
    assert_eq!(
        direct,
        justybase_db2::Db2InsertResult {
            rows_affected: 1,
            last_insert_id: 1
        }
    );
    let statement = db
        .prepare(&format!("INSERT INTO {table} (V) VALUES (?)"))
        .await
        .unwrap();
    let prepared = db
        .execute_insert_prepared(&statement, &[Db2Value::Text("prepared".into())])
        .await
        .unwrap();
    assert_eq!(
        prepared,
        justybase_db2::Db2InsertResult {
            rows_affected: 1,
            last_insert_id: 2
        }
    );
    assert_eq!(
        db.query(&format!("SELECT ID, V FROM {table} ORDER BY ID"))
            .await
            .unwrap()
            .rows,
        vec![
            vec![Db2Value::Integer(1), Db2Value::Text("direct".into())],
            vec![Db2Value::Integer(2), Db2Value::Text("prepared".into())]
        ]
    );

    db.execute(&format!("DROP TABLE {schema}.{table}"))
        .await
        .unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_client_info_updates_and_restores_accounting_registers() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let baseline = db.client_info();
    db.set_client_info(Db2ClientInfo {
        application_name: "audit'service".into(),
        workstation_name: "justybase-db2-worker".into(),
        user_id: "audit_user".into(),
        accounting: "cost_center_7".into(),
        ..Db2ClientInfo::default()
    })
    .await
    .unwrap();
    assert_eq!(db.client_info().application_name, "audit'service");
    assert_eq!(db.client_info().accounting, "cost_center_7");
    let current = db.query("SELECT COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_ACCTNG, '') FROM SYSIBM.SYSDUMMY1").await.unwrap();
    assert_eq!(
        current.rows,
        vec![vec![
            Db2Value::Text("audit'service".into()),
            Db2Value::Text("justybase-db2-worker".into()),
            Db2Value::Text("audit_user".into()),
            Db2Value::Text("cost_center_7".into()),
        ]]
    );

    db.reset_client_info(baseline.clone()).await.unwrap();
    assert_eq!(db.client_info(), baseline);
    assert!(db.client_info_matches(&baseline));
    let restored = db.query("SELECT COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_ACCTNG, '') FROM SYSIBM.SYSDUMMY1").await.unwrap();
    assert_eq!(
        restored.rows,
        vec![vec![
            Db2Value::Text(baseline.application_name),
            Db2Value::Text(baseline.workstation_name),
            Db2Value::Text(baseline.user_id),
            Db2Value::Text(baseline.accounting),
        ]]
    );
    db.close().await.unwrap();

    let mut config = justybase_db2::parse_dsn(&dsn).unwrap();
    config.client_appl_name = "configured_app".into();
    config.client_wrkstn_name = "configured_worker".into();
    config.client_userid = "configured_user".into();
    config.client_acctng = "configured_acct".into();
    let mut configured = Db2Session::connect(config).await.unwrap();
    let initial = configured.query("SELECT COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_ACCTNG, '') FROM SYSIBM.SYSDUMMY1").await.unwrap();
    assert_eq!(
        initial.rows,
        vec![vec![
            Db2Value::Text("configured_app".into()),
            Db2Value::Text("configured_worker".into()),
            Db2Value::Text("configured_user".into()),
            Db2Value::Text("configured_acct".into()),
        ]]
    );
    configured.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_pool_reuses_one_session_and_closes_it() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let pool = Db2Pool::connect_dsn(&dsn, 1).unwrap();
    let first = pool.query("VALUES CAST(42 AS INTEGER)").await.unwrap();
    assert_eq!(first.rows, vec![vec![Db2Value::Integer(42)]]);
    assert_eq!(pool.session_count(), 1);
    assert_eq!(pool.idle_session_count(), 1);

    let second = pool
        .query_with_params("VALUES CAST(? AS INTEGER)", &[Db2Value::Integer(73)])
        .await
        .unwrap();
    assert_eq!(second.rows, vec![vec![Db2Value::Integer(73)]]);
    assert_eq!(pool.session_count(), 1);
    assert_eq!(pool.idle_session_count(), 1);

    let baseline = {
        let mut lease = pool.acquire().await.unwrap();
        let baseline = lease.client_info();
        lease
            .set_client_info(Db2ClientInfo {
                application_name: "pooled_tenant".into(),
                user_id: "tenant_user".into(),
                accounting: "tenant_batch".into(),
                ..Db2ClientInfo::default()
            })
            .await
            .unwrap();
        baseline
    };
    {
        let mut lease = pool.acquire().await.unwrap();
        assert_eq!(lease.client_info(), baseline);
        let registers = lease.query("SELECT COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_ACCTNG, '') FROM SYSIBM.SYSDUMMY1").await.unwrap();
        assert_eq!(
            registers.rows,
            vec![vec![
                Db2Value::Text(baseline.application_name),
                Db2Value::Text(baseline.workstation_name),
                Db2Value::Text(baseline.user_id),
                Db2Value::Text(baseline.accounting),
            ]]
        );
    }

    pool.close().await.unwrap();
    assert_eq!(pool.session_count(), 0);
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_pool_returns_a_session_after_server_sql_error() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let pool = Db2Pool::connect_dsn(&dsn, 1).unwrap();
    let error = pool
        .query("SELECT FROM SYSIBM.SYSDUMMY1")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("SQLCODE="));
    assert_eq!(pool.session_count(), 1);
    assert_eq!(pool.idle_session_count(), 1);

    let result = pool.query("VALUES 91").await.unwrap();
    assert_eq!(result.rows, vec![vec![Db2Value::Integer(91)]]);
    assert_eq!(pool.session_count(), 1);
    assert_eq!(pool.idle_session_count(), 1);
    pool.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_pool_discards_an_abandoned_transaction_before_reuse() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let pool = Db2Pool::connect_dsn(&dsn, 1).unwrap();
    let table = format!(
        "RS_POOL_ABANDON_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = {
        let mut lease = pool.acquire().await.unwrap();
        current_schema(&mut lease).await
    };
    pool.execute(&format!("CREATE TABLE {schema}.{table} (V INTEGER)"))
        .await
        .unwrap();

    let work = async {
        let mut lease = pool.acquire().await?;
        let mut transaction = lease.begin_transaction()?;
        transaction
            .execute(&format!("INSERT INTO {table} VALUES (1)"))
            .await?;
        drop(transaction);
        assert!(lease.is_closed());
        drop(lease);
        assert_eq!(
            pool.session_count(),
            0,
            "the uncertain session must be discarded"
        );

        let rows = pool.query(&format!("SELECT COUNT(*) FROM {table}")).await?;
        assert_eq!(rows.rows, vec![vec![Db2Value::Integer(0)]]);
        assert_eq!(
            pool.session_count(),
            1,
            "pool should create a fresh session"
        );
        Ok::<(), Db2Error>(())
    }
    .await;
    let cleanup = pool.execute(&format!("DROP TABLE {schema}.{table}")).await;
    match work {
        Ok(()) => {
            match cleanup {
                Ok(_) => {}
                Err(error)
                    if error.sql_code == Some(-204)
                        && error.sql_state.as_deref() == Some("42704") =>
                {
                    // Treat only exact object-not-found as idempotent fixture cleanup.
                }
                Err(error) => panic!("abandoned-transaction fixture cleanup failed: {error}"),
            }
        }
        Err(work_error) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("abandoned-transaction cleanup also failed: {cleanup_error}");
            }
            panic!("abandoned-transaction workload failed: {work_error}");
        }
    }
    pool.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_admin_command_runs_parameterized_runstats_through_pool() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let pool = Db2Pool::connect_dsn(&dsn, 1).unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("RS_ADMIN_{suffix:X}");
    let mut created = false;
    let schema = current_schema(&mut db).await;

    let operation = async {
        if !db
            .execute_admin_command(" \t\r\n")
            .await
            .unwrap_err()
            .to_string()
            .contains("cannot be empty")
        {
            return Err(Db2Error::other(
                "blank admin command was not rejected".into(),
            ));
        }
        if !pool
            .execute_admin_command("RUNSTATS\0 ON TABLE X")
            .await
            .unwrap_err()
            .to_string()
            .contains("NUL")
        {
            return Err(Db2Error::other(
                "NUL-containing admin command was not rejected".into(),
            ));
        }
        let schema_result = db.query("VALUES CURRENT SCHEMA").await?;
        let Some(Db2Value::Text(schema)) = schema_result.rows.first().and_then(|row| row.first())
        else {
            return Err(Db2Error::other(
                "DB2 did not return the current schema".into(),
            ));
        };
        if !schema.chars().all(|character| {
            character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
        }) {
            return Err(Db2Error::other(format!(
                "unexpected current schema {schema:?}"
            )));
        }
        db.execute(&format!(
            "CREATE TABLE {schema}.{table} (ID INTEGER NOT NULL)"
        ))
        .await?;
        created = true;
        pool.execute_admin_command(&format!("RUNSTATS ON TABLE {schema}.{table}"))
            .await?;

        let stats = db
            .query_with_params(
                "SELECT STATS_TIME FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?",
                &[
                    Db2Value::Text(schema.to_owned()),
                    Db2Value::Text(table.clone()),
                ],
            )
            .await?;
        let Some(stats_time) = stats.rows.first().and_then(|row| row.first()) else {
            return Err(Db2Error::other(
                "ADMIN_CMD did not leave table statistics metadata".into(),
            ));
        };
        if *stats_time == Db2Value::Null {
            return Err(Db2Error::other(
                "ADMIN_CMD did not update STATS_TIME".into(),
            ));
        }
        Ok::<(), Db2Error>(())
    }
    .await;

    let cleanup = if created {
        db.execute(&format!("DROP TABLE {schema}.{table}")).await
    } else {
        Ok(0)
    };
    let db_close = db.close().await;
    let pool_close = pool.close().await;
    operation.unwrap();
    cleanup.unwrap();
    db_close.unwrap();
    pool_close.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_interrupt_returns_minus_952_and_preserves_session() {
    use std::time::Instant;

    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let handle = db.interrupt_handle().unwrap();
    let query = async {
        let outcome = db
            .query("SELECT COUNT(*) FROM SYSCAT.COLUMNS a, SYSCAT.COLUMNS b, SYSCAT.COLUMNS c")
            .await;
        let error = outcome.unwrap_err();
        assert_eq!(error.sql_code, Some(-952), "{error}");
        assert_eq!(error.sql_state.as_deref(), Some("57014"), "{error}");
        Instant::now()
    };
    let interrupt = async {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let started = Instant::now();
        handle.interrupt().await.unwrap();
        (started, Instant::now())
    };
    let (operation_completed_at, (interrupt_started_at, interrupt_acknowledged_at)) =
        tokio::join!(query, interrupt);
    let interrupt_acknowledgment_ms = interrupt_acknowledged_at
        .duration_since(interrupt_started_at)
        .as_secs_f64()
        * 1000.0;
    let operation_completion_ms = operation_completed_at
        .duration_since(interrupt_started_at)
        .as_secs_f64()
        * 1000.0;
    eprintln!(
        "Live SQLINTR: interrupt acknowledgment {interrupt_acknowledgment_ms:.1} ms; operation completion {operation_completion_ms:.1} ms"
    );
    assert_eq!(
        db.query("VALUES 17").await.unwrap().rows,
        vec![vec![Db2Value::Integer(17)]]
    );

    let constant = db.prepare("VALUES 17").await.unwrap();
    assert_eq!(
        db.query_prepared(&constant, &[]).await.unwrap().rows,
        vec![vec![Db2Value::Integer(17)]]
    );

    let statement = db
        .prepare("SELECT COUNT(*) FROM SYSCAT.COLUMNS a, SYSCAT.COLUMNS b, SYSCAT.COLUMNS c")
        .await
        .unwrap();
    let query_handle = db.interrupt_handle().unwrap();
    let query = async {
        let error = db.query_prepared(&statement, &[]).await.unwrap_err();
        assert_eq!(error.sql_code, Some(-952), "{error}");
        assert_eq!(error.sql_state.as_deref(), Some("57014"), "{error}");
        Instant::now()
    };
    let interrupt = async {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let started = Instant::now();
        query_handle.interrupt().await.unwrap();
        (started, Instant::now())
    };
    let (operation_completed_at, (interrupt_started_at, interrupt_acknowledged_at)) =
        tokio::join!(query, interrupt);
    let interrupt_acknowledgment_ms = interrupt_acknowledged_at
        .duration_since(interrupt_started_at)
        .as_secs_f64()
        * 1000.0;
    let operation_completion_ms = operation_completed_at
        .duration_since(interrupt_started_at)
        .as_secs_f64()
        * 1000.0;
    eprintln!(
        "Live prepared SQLINTR: interrupt acknowledgment {interrupt_acknowledgment_ms:.1} ms; operation completion {operation_completion_ms:.1} ms"
    );
    assert_eq!(
        db.query("VALUES 19").await.unwrap().rows,
        vec![vec![Db2Value::Integer(19)]]
    );
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_command_deadline_uses_sqlintr_and_reuses_the_session() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set by the live runner");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let error = db
        .query_with_options(
            "SELECT COUNT(*) FROM SYSCAT.COLUMNS a, SYSCAT.COLUMNS b, SYSCAT.COLUMNS c",
            Db2CommandOptions::new(250, 10_000).with_interrupt_ack_timeout_ms(5_000),
        )
        .await
        .expect_err("the deliberately large catalog scan should hit its command deadline");
    assert_eq!(error.sql_code, Some(-952), "{error}");
    assert_eq!(error.sql_state.as_deref(), Some("57014"), "{error}");
    assert_eq!(
        db.query("VALUES 23").await.unwrap().rows,
        vec![vec![Db2Value::Integer(23)]],
        "the confirmed server cancellation must leave the same physical session usable"
    );
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_interrupt_cancels_a_pool_lease_and_reuses_it() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let pool = Db2Pool::connect_dsn(&dsn, 1).unwrap();
    let lease = pool.acquire().await.unwrap();
    let handle = lease.interrupt_handle().unwrap();
    let digits = (0..10)
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(", ");
    let dimensions = (0..6)
        .map(|index| format!("(VALUES {digits}) AS D{index}(N)"))
        .collect::<Vec<_>>()
        .join(", ");
    let projection = (0..6)
        .map(|index| format!("D{index}.N"))
        .collect::<Vec<_>>()
        .join(" + ");
    let sql = format!(
        "SELECT {projection} AS N FROM {dimensions} ORDER BY RAND() FETCH FIRST 1 ROW ONLY"
    );

    // Keep the lease in the task that owns the query. The separate interrupt handle
    // exercises the same cancellation path exposed to callers holding a pool lease.
    let query = tokio::spawn(async move {
        let mut lease = lease;
        let result = lease.query(&sql).await;
        (lease, result)
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let active_before_interrupt = !query.is_finished();
    let interrupt_started = std::time::Instant::now();
    let interrupt_result = handle.interrupt().await.map_err(|error| error.to_string());
    let interrupt_ack_latency = interrupt_started.elapsed();
    let completion_started = std::time::Instant::now();
    let (lease, query_result) = query.await.unwrap();
    let completion_after_ack = completion_started.elapsed();
    let query_error = match query_result {
        Err(error) => error.to_string(),
        Ok(result) => format!(
            "query finished without SQLINTR after returning {} rows",
            result.rows.len()
        ),
    };
    let lease_open_after_interrupt = !lease.is_closed();
    drop(lease);
    let idle_sessions_after_release = pool.idle_session_count();

    let (reuse_rows, reuse_error) = match pool.acquire().await {
        Ok(mut reused) => match reused.query("VALUES 31").await {
            Ok(result) => (Some(result.rows), None),
            Err(error) => (None, Some(error.to_string())),
        },
        Err(error) => (None, Some(error.to_string())),
    };
    pool.close().await.unwrap();

    assert!(
        active_before_interrupt,
        "the pool-lease query already completed before cancellation"
    );
    assert!(interrupt_result.is_ok(), "{interrupt_result:?}");
    assert!(
        query_error.contains("SQLCODE=-952 SQLSTATE=57014"),
        "{query_error}"
    );
    assert!(
        lease_open_after_interrupt,
        "a complete SQLCODE -952 reply should leave the leased session reusable"
    );
    assert_eq!(idle_sessions_after_release, 1);
    assert!(reuse_error.is_none(), "{reuse_error:?}");
    assert_eq!(reuse_rows, Some(vec![vec![Db2Value::Integer(31)]]));
    println!(
        "pool-lease SQLINTR: ack {:.1} ms; query completion after ack {:.1} ms; released and reacquired session reused",
        interrupt_ack_latency.as_secs_f64() * 1_000.0,
        completion_after_ack.as_secs_f64() * 1_000.0
    );
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_interrupt_cancels_lock_wait_and_decodes_server_diagnostic() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let table = format!(
        "RS_DB2_CANCEL_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut locker = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut waiter = Db2Session::connect_dsn(&dsn).await.unwrap();
    let schema = match &locker.query("VALUES CURRENT SCHEMA").await.unwrap().rows[0][0] {
        Db2Value::Text(schema) => schema.trim_end().replace('"', "\"\""),
        _ => panic!("CURRENT SCHEMA must be text"),
    };
    let qualified_table = format!("\"{schema}\".\"{table}\"");
    locker
        .execute(&format!(
            "CREATE TABLE {qualified_table} (ID INT NOT NULL, V INT NOT NULL, PRIMARY KEY (ID))"
        ))
        .await
        .unwrap();
    locker
        .execute(&format!("INSERT INTO {table} VALUES (1, 0)"))
        .await
        .unwrap();
    let mut transaction = locker.begin_transaction().unwrap();
    transaction
        .execute(&format!("UPDATE {table} SET V = V + 1 WHERE ID = 1"))
        .await
        .unwrap();

    let interrupt_handle = waiter.interrupt_handle().unwrap();
    let update_sql = format!("UPDATE {table} SET V = V + 1 WHERE ID = 1");
    let update = async { waiter.execute(&update_sql).await };
    let interrupt = async {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        interrupt_handle.interrupt().await
    };
    let (update_result, interrupt_result) = tokio::join!(update, interrupt);
    let error = match update_result {
        Err(error) => error.to_string(),
        Ok(affected) => format!("blocked update unexpectedly completed ({affected} rows affected)"),
    };

    transaction.rollback().await.unwrap();
    let reused = waiter.query("VALUES 21").await.unwrap().rows;
    locker
        .execute(&format!("DROP TABLE {qualified_table}"))
        .await
        .unwrap();

    interrupt_result.unwrap();
    assert!(error.contains("SQLCODE:-952"), "{error}");
    assert_eq!(reused, vec![vec![Db2Value::Integer(21)]]);
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_interrupt_cancels_second_batch_set_and_reports_confirmed_progress() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let table = format!(
        "RS_DB2_BATCH_CANCEL_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut locker = Db2Session::connect_dsn(&dsn).await.unwrap();
    let mut waiter = Db2Session::connect_dsn(&dsn).await.unwrap();
    let schema = current_schema(&mut locker).await;
    locker
        .execute(&format!(
            "CREATE TABLE {schema}.{table} (ID INT NOT NULL, V INT NOT NULL, PRIMARY KEY (ID))"
        ))
        .await
        .unwrap();
    locker
        .execute(&format!("INSERT INTO {table} VALUES (1, 0), (2, 0)"))
        .await
        .unwrap();
    let mut transaction = locker.begin_transaction().unwrap();
    transaction
        .execute(&format!("UPDATE {table} SET V = V + 1 WHERE ID = 2"))
        .await
        .unwrap();

    let interrupt_handle = waiter.interrupt_handle().unwrap();
    let update_sql = format!("UPDATE {table} SET V = V + 1 WHERE ID = ?");
    let parameter_sets = vec![vec![Db2Value::Integer(1)], vec![Db2Value::Integer(2)]];
    let batch = waiter.execute_batch_with_params(&update_sql, &parameter_sets);
    let interrupt = async {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        interrupt_handle.interrupt().await
    };
    let (batch_result, interrupt_result) = tokio::join!(batch, interrupt);
    let error = batch_result.expect_err("SQLINTR should interrupt the locked second set");
    interrupt_result.unwrap();

    transaction.rollback().await.unwrap();
    let rows = waiter
        .query(&format!("SELECT ID, V FROM {table} ORDER BY ID"))
        .await
        .unwrap()
        .rows;
    let reused = waiter.query("VALUES 29").await.unwrap().rows;
    let cleanup = locker
        .execute(&format!("DROP TABLE {schema}.{table}"))
        .await;
    cleanup.unwrap();
    assert_eq!(error.cause.sql_code, Some(-952));
    assert_eq!(error.cause.sql_state, None);
    assert_eq!(error.failed_parameter_set_index, Some(1));
    assert_eq!(error.completed_parameter_sets, 1);
    assert_eq!(error.rows_affected, 1);
    assert_eq!(
        rows,
        vec![
            vec![Db2Value::Integer(1), Db2Value::Integer(1)],
            vec![Db2Value::Integer(2), Db2Value::Integer(0)],
        ]
    );
    assert_eq!(reused, vec![vec![Db2Value::Integer(29)]]);
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_call_drains_ordered_procedure_result_sets_and_reuses_the_session() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let procedure = format!("RS_MULTI_{suffix:X}");
    let schema = current_schema(&mut db).await;
    let values = (1..=130)
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(", ");
    let digits = (0..10)
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(", ");
    let dimensions = (0..5)
        .map(|index| format!("(VALUES {digits}) AS D{index}(N)"))
        .collect::<Vec<_>>()
        .join(", ");
    let order = (0..5)
        .map(|index| format!("D{index}.N"))
        .collect::<Vec<_>>()
        .join(", ");
    let large_query = format!(
        "SELECT ROW_NUMBER() OVER (ORDER BY {order}) AS N FROM {dimensions} FETCH FIRST 12000 ROWS ONLY"
    );
    let create_sql = format!(
        "CREATE PROCEDURE {schema}.{procedure} (IN P INTEGER) LANGUAGE SQL DYNAMIC RESULT SETS 4 BEGIN DECLARE C1 CURSOR WITH RETURN TO CALLER FOR SELECT P AS VALUE FROM SYSIBM.SYSDUMMY1; DECLARE C2 CURSOR WITH RETURN TO CALLER FOR SELECT N FROM (VALUES {values}) AS T(N); DECLARE C3 CURSOR WITH RETURN TO CALLER FOR {large_query}; DECLARE C4 CURSOR WITH RETURN TO CALLER FOR SELECT P AS VALUE FROM SYSIBM.SYSDUMMY1 WHERE 1 = 0; OPEN C1; OPEN C2; OPEN C3; OPEN C4; END"
    );
    db.execute(&create_sql).await.unwrap();
    let operation = async {
        let result = db
            .query_with_params(
                &format!("/* procedure results */ -- leading SQL comments\nCALL {procedure}(?)"),
                &[Db2Value::Integer(42)],
            )
            .await?;
        assert_eq!(result.result_sets.len(), 4);
        assert_eq!(
            result.result_sets[0].rows,
            vec![vec![Db2Value::Integer(42)]]
        );
        assert_eq!(result.result_sets[1].rows.len(), 130);
        assert_eq!(
            result.result_sets[1].rows.first(),
            Some(&vec![Db2Value::Integer(1)])
        );
        assert_eq!(
            result.result_sets[1].rows.last(),
            Some(&vec![Db2Value::Integer(130)])
        );
        assert_eq!(result.result_sets[2].rows.len(), 12_000);
        assert_eq!(
            result.result_sets[2].rows.first(),
            Some(&vec![Db2Value::Integer(1)])
        );
        assert_eq!(
            result.result_sets[2].rows.last(),
            Some(&vec![Db2Value::Integer(12_000)])
        );
        assert!(result.result_sets[3].rows.is_empty());
        assert_eq!(result.rows, result.result_sets[0].rows);
        assert_eq!(
            db.execute_with_params(&format!("CALL {procedure}(?)"), &[Db2Value::Integer(42)])
                .await?,
            0
        );
        assert_eq!(
            db.query("VALUES 23").await?.rows,
            vec![vec![Db2Value::Integer(23)]]
        );
        Ok::<(), justybase_db2::Db2Error>(())
    }
    .await;
    let cleanup = db
        .execute(&format!("DROP PROCEDURE {schema}.{procedure}"))
        .await;
    operation.unwrap();
    cleanup.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_procedure_returns_out_parameters_and_multiple_result_sets() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let procedure = format!("RS_OUT_MULTI_{suffix:X}");
    let schema = current_schema(&mut db).await;
    db.execute(&format!("CREATE PROCEDURE {schema}.{procedure} (IN P INTEGER, OUT DOUBLED INTEGER) LANGUAGE SQL DYNAMIC RESULT SETS 2 BEGIN DECLARE C1 CURSOR WITH RETURN TO CALLER FOR SELECT P AS VALUE FROM SYSIBM.SYSDUMMY1; DECLARE C2 CURSOR WITH RETURN TO CALLER FOR SELECT N FROM (VALUES (P), (P + 1)) AS T(N); SET DOUBLED = P * 2; OPEN C1; OPEN C2; END")).await.unwrap();
    let statement = db
        .prepare(&format!("CALL {procedure}(?, ?)"))
        .await
        .unwrap();
    let execution = db
        .execute_prepared_with_outputs(
            &statement,
            &[
                Db2Parameter {
                    value: Db2Value::Integer(21),
                    direction: Db2ParameterDirection::Input,
                },
                Db2Parameter {
                    value: Db2Value::Integer(0),
                    direction: Db2ParameterDirection::Output,
                },
            ],
        )
        .await;
    let cleanup = db
        .execute(&format!("DROP PROCEDURE {schema}.{procedure}"))
        .await;
    let result = execution.unwrap();
    cleanup.unwrap();
    assert_eq!(result.rows_affected, 0);
    assert_eq!(
        result.output_parameters,
        vec![Db2Value::Null, Db2Value::Integer(42)]
    );
    assert_eq!(result.result_sets.len(), 2);
    assert_eq!(
        result.result_sets[0].rows,
        vec![vec![Db2Value::Integer(21)]]
    );
    assert_eq!(
        result.result_sets[1].rows,
        vec![vec![Db2Value::Integer(21)], vec![Db2Value::Integer(22)]]
    );
    assert_eq!(
        db.query("VALUES 5").await.unwrap().rows,
        vec![vec![Db2Value::Integer(5)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_procedure_result_cursors_return_large_blob_clob_and_xml_values() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let procedure = format!("RS_LOB_{suffix:X}");
    let schema = current_schema(&mut db).await;
    let call_options =
        Db2CommandOptions::new(180_000, 60_000).with_interrupt_ack_timeout_ms(30_000);
    let create_sql = format!(
        "CREATE PROCEDURE {schema}.{procedure} () LANGUAGE SQL DYNAMIC RESULT SETS 3 BEGIN DECLARE C1 CURSOR WITH RETURN TO CALLER FOR SELECT CAST(REPEAT('c', 30000) AS CLOB(100K)) || CAST(REPEAT('d', 30000) AS CLOB(100K)) || CAST(REPEAT('e', 10000) AS CLOB(100K)) AS VALUE FROM SYSIBM.SYSDUMMY1; DECLARE C2 CURSOR WITH RETURN TO CALLER FOR SELECT BLOB(CAST(REPEAT('x', 30000) AS VARCHAR(30000) FOR BIT DATA)) || BLOB(CAST(REPEAT('y', 30000) AS VARCHAR(30000) FOR BIT DATA)) || BLOB(CAST(REPEAT('z', 12000) AS VARCHAR(12000) FOR BIT DATA)) AS VALUE FROM SYSIBM.SYSDUMMY1; DECLARE C3 CURSOR WITH RETURN TO CALLER FOR SELECT XMLELEMENT(NAME \"r\", XMLAGG(XMLELEMENT(NAME \"c\", COLNAME))) AS VALUE FROM SYSCAT.COLUMNS; OPEN C1; OPEN C2; OPEN C3; END"
    );
    eprintln!("live procedure LOB fixture phase=create procedure={procedure}");
    db.execute(&create_sql).await.unwrap();
    let execution = async {
        eprintln!("live procedure LOB fixture phase=call procedure={procedure}");
        let result = db
            .query_with_options(&format!("CALL {procedure}()"), call_options)
            .await?;
        let reuse = db.query_with_options("VALUES 31", call_options).await?;
        Ok::<_, justybase_db2::Db2Error>((result, reuse))
    }
    .await;
    eprintln!("live procedure LOB fixture phase=cleanup procedure={procedure}");
    let drop_sql = format!("DROP PROCEDURE {schema}.{procedure}");
    let cleanup = if db.is_closed() {
        // SQLINTR may leave the original session unusable if acknowledgment or
        // command settlement was uncertain. Use a fresh connection for fixture cleanup.
        let mut cleanup_db = Db2Session::connect_dsn(&dsn).await.unwrap();
        let drop_result = cleanup_db
            .execute_with_options(&drop_sql, call_options)
            .await;
        let close_result = cleanup_db.close().await;
        drop_result.and(close_result)
    } else {
        db.execute_with_options(&drop_sql, call_options)
            .await
            .map(|_| ())
    };
    let (result, reuse) = execution.unwrap();
    cleanup.unwrap();
    assert_eq!(result.result_sets.len(), 3);
    assert_eq!(
        result.result_sets[0].rows,
        vec![vec![Db2Value::Text(format!(
            "{}{}{}",
            "c".repeat(30_000),
            "d".repeat(30_000),
            "e".repeat(10_000)
        ))]]
    );
    let expected_blob = [vec![b'x'; 30_000], vec![b'y'; 30_000], vec![b'z'; 12_000]].concat();
    assert_eq!(
        result.result_sets[1].rows,
        vec![vec![Db2Value::Bytes(expected_blob)]]
    );
    let Db2Value::Text(xml) = &result.result_sets[2].rows[0][0] else {
        panic!(
            "expected XML text, got {:?}",
            result.result_sets[2].rows[0][0]
        );
    };
    assert!(
        xml.len() >= 100_000,
        "procedure XML result is {} characters",
        xml.len()
    );
    assert!(xml.starts_with("<r><c>"));
    assert!(xml.ends_with("</c></r>"));
    assert_eq!(reuse.rows, vec![vec![Db2Value::Integer(31)]]);
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_nullable_and_not_null_lob_descriptors_preserve_leading_zero_bytes() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("RS_LOBNULL_{suffix:X}");
    let procedure = format!("RS_LOBNULL_{suffix:X}_P");
    let schema = current_schema(&mut db).await;
    let options = Db2CommandOptions::new(180_000, 60_000);
    let size = 70_000usize;
    let mut bn = vec![0u8; size];
    for (index, slot) in bn.iter_mut().enumerate().skip(1) {
        *slot = index as u8;
    }
    let mut b = vec![0u8; size];
    for (index, slot) in b.iter_mut().enumerate().skip(1) {
        *slot = index.wrapping_mul(3) as u8;
    }
    let cn = format!("\u{0}{}", "a".repeat(size - 1));
    let c = format!("\u{0}{}", "b".repeat(size - 1));
    let dn = format!("\u{0}{}", "漢".repeat(size - 1));
    let d = format!("\u{0}{}", "字".repeat(size - 1));

    let operation = async {
        db.execute_with_options(
            &format!("CREATE TABLE {schema}.{table} (ID INTEGER, BN BLOB(2M) NOT NULL, B BLOB(2M), CN CLOB(2M) NOT NULL, C CLOB(2M), DN DBCLOB(2M) NOT NULL, D DBCLOB(2M))"),
            options,
        )
        .await?;
        db.execute_with_options(
            &format!("INSERT INTO {table} VALUES (1, CAST('' AS BLOB(2M)), NULL, CAST('' AS CLOB(2M)), NULL, CAST('' AS DBCLOB(2M)), NULL)"),
            options,
        )
        .await?;
        for (column, value) in [
            ("BN", Db2Value::Bytes(bn.clone())),
            ("B", Db2Value::Bytes(b.clone())),
            ("CN", Db2Value::Text(cn.clone())),
            ("C", Db2Value::Text(c.clone())),
            ("DN", Db2Value::Text(dn.clone())),
            ("D", Db2Value::Text(d.clone())),
        ] {
            db.execute_with_params(
                &format!("UPDATE {table} SET {column} = ? WHERE ID = 1"),
                &[value],
            )
            .await?;
        }
        db.execute_with_options(
            &format!("INSERT INTO {table} VALUES (2, CAST('' AS BLOB(2M)), NULL, CAST('' AS CLOB(2M)), NULL, CAST('' AS DBCLOB(2M)), NULL)"),
            options,
        )
        .await?;
        db.execute_with_options(
            &format!("CREATE PROCEDURE {schema}.{procedure} () LANGUAGE SQL DYNAMIC RESULT SETS 1 BEGIN DECLARE CUR CURSOR WITH RETURN TO CALLER FOR SELECT ID, BN, B, CN, C, DN, D FROM {table} ORDER BY ID; OPEN CUR; END"),
            options,
        )
        .await?;

        let query = db
            .query_with_options(
                &format!("SELECT ID, BN, B, CN, C, DN, D FROM {table} ORDER BY ID"),
                options,
            )
            .await?;
        let call = db
            .query_with_options(&format!("CALL {procedure}()"), options)
            .await?;
        let reuse = db.query_with_options("VALUES 29", options).await?;
        Ok::<_, justybase_db2::Db2Error>((query, call, reuse))
    }
    .await;

    let _ = db
        .execute_with_options(&format!("DROP PROCEDURE {schema}.{procedure}"), options)
        .await;
    let _ = db
        .execute_with_options(&format!("DROP TABLE {schema}.{table}"), options)
        .await;

    let (query, call, reuse) = operation.unwrap_or_else(|error| panic!("LOB descriptor fixture operation failed: {error}"));
    assert_eq!(call.result_sets.len(), 1);
    for rows in [&query.rows, &call.result_sets[0].rows] {
        assert_eq!(rows[0][0], Db2Value::Integer(1));
        assert_eq!(rows[0][1], Db2Value::Bytes(bn.clone()));
        assert_eq!(rows[0][2], Db2Value::Bytes(b.clone()));
        assert_eq!(rows[0][3], Db2Value::Text(cn.clone()));
        assert_eq!(rows[0][4], Db2Value::Text(c.clone()));
        assert_eq!(rows[0][5], Db2Value::Text(dn.clone()));
        assert_eq!(rows[0][6], Db2Value::Text(d.clone()));
        assert_eq!(rows[1][0], Db2Value::Integer(2));
        assert_eq!(rows[1][1], Db2Value::Bytes(Vec::new()));
        assert_eq!(rows[1][2], Db2Value::Null);
        assert_eq!(rows[1][3], Db2Value::Text(String::new()));
        assert_eq!(rows[1][4], Db2Value::Null);
        assert_eq!(rows[1][5], Db2Value::Text(String::new()));
        assert_eq!(rows[1][6], Db2Value::Null);
    }
    assert_eq!(reuse.rows, vec![vec![Db2Value::Integer(29)]]);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_interrupt_cancels_a_procedure_cursor_statement_and_preserves_the_session() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let procedure = format!("RS_CANCEL_{suffix:X}");
    let schema = current_schema(&mut db).await;
    let digits = (0..10)
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(", ");
    let dimensions = (0..6)
        .map(|index| format!("(VALUES {digits}) AS D{index}(N)"))
        .collect::<Vec<_>>()
        .join(", ");
    let projection = (0..6)
        .map(|index| format!("D{index}.N"))
        .collect::<Vec<_>>()
        .join(" + ");
    db.execute(&format!(
        "CREATE PROCEDURE {schema}.{procedure} () LANGUAGE SQL DYNAMIC RESULT SETS 1 BEGIN DECLARE C CURSOR WITH RETURN TO CALLER FOR SELECT {projection} AS N FROM {dimensions} ORDER BY RAND() FETCH FIRST 1 ROW ONLY; OPEN C; END"
    ))
    .await
    .unwrap();

    // Run the query on a separate worker: a fast series of ready socket reads must not
    // starve the cancellation timer on a current-thread executor.
    let db = Arc::new(tokio::sync::Mutex::new(db));
    let query_db = Arc::clone(&db);
    let call_sql = format!("CALL {procedure}()");
    let query = tokio::spawn(async move {
        let mut session = query_db.lock().await;
        session.query(&call_sql).await
    });
    let handle = db.lock().await.interrupt_handle().unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let active_before_interrupt = !query.is_finished();
    let interrupt_started = std::time::Instant::now();
    let interrupt_result = handle.interrupt().await.map_err(|error| error.to_string());
    let interrupt_ack_latency = interrupt_started.elapsed();
    let completion_started = std::time::Instant::now();
    let query_result = match query.await {
        Ok(result) => result,
        Err(error) => Err(Db2Error::other(format!(
            "procedure query task failed: {error}"
        ))),
    };
    let completion_after_ack = completion_started.elapsed();
    let (query_error, rows_returned) = match query_result {
        Err(error) => (error.to_string(), 0),
        Ok(result) => (
            format!(
                "CALL finished without SQLINTR after returning {} rows",
                result.rows.len()
            ),
            result.rows.len(),
        ),
    };

    let mut session = db.lock().await;
    let session_open_after_interrupt = !session.is_closed();
    let reuse_error = if session_open_after_interrupt {
        match session.query("VALUES 29").await {
            Ok(result) if result.rows == vec![vec![Db2Value::Integer(29)]] => None,
            Ok(result) => Some(format!("unexpected post-interrupt rows: {:?}", result.rows)),
            Err(error) => Some(error.to_string()),
        }
    } else {
        Some("session was closed after a complete SQLCODE -952 reply".into())
    };
    let session_close = session.close().await;
    drop(session);
    drop(db);

    let mut cleanup = Db2Session::connect_dsn(&dsn).await.unwrap();
    let existing = cleanup
        .query_with_params(
            "SELECT COUNT(*) FROM SYSCAT.ROUTINES WHERE ROUTINENAME = ?",
            &[Db2Value::Text(procedure.clone())],
        )
        .await
        .unwrap();
    let Db2Value::Integer(routine_count) = &existing.rows[0][0] else {
        panic!(
            "expected an integer routine count, got {:?}",
            existing.rows[0][0]
        );
    };
    if *routine_count > 0 {
        cleanup
            .execute(&format!("DROP PROCEDURE {schema}.{procedure}"))
            .await
            .unwrap();
    }
    cleanup.close().await.unwrap();

    assert!(
        active_before_interrupt,
        "the procedure call already completed before cancellation"
    );
    assert!(interrupt_result.is_ok(), "{interrupt_result:?}");
    assert!(
        query_error.contains("SQLCODE=-952 SQLSTATE=57014"),
        "{query_error}"
    );
    assert!(
        session_open_after_interrupt,
        "a complete SQLCODE -952 reply should leave the session reusable"
    );
    assert!(reuse_error.is_none(), "{reuse_error:?}");
    session_close.unwrap();
    println!(
        "procedure cursor SQLINTR: ack {:.1} ms; query completion after ack {:.1} ms; session reused; rows returned before cancel: {rows_returned}",
        interrupt_ack_latency.as_secs_f64() * 1_000.0,
        completion_after_ack.as_secs_f64() * 1_000.0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_interrupt_cancels_the_second_procedure_cursor_and_cleans_every_cursor() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let procedure = format!("RS_CANCEL2_{suffix:X}");
    let table = format!("RS_CANCEL2_T_{suffix:X}");
    let schema = match &db.query("VALUES CURRENT SCHEMA").await.unwrap().rows[0][0] {
        Db2Value::Text(value) => value.trim().to_owned(),
        other => panic!("expected current schema text, got {other:?}"),
    };
    let qualified_table = format!("{schema}.{table}");
    let mut locker = Db2Session::connect_dsn(&dsn).await.unwrap();
    db.execute(&format!(
        "CREATE TABLE {qualified_table} (ID INT NOT NULL PRIMARY KEY, V INT NOT NULL)"
    ))
    .await
    .unwrap();
    db.execute(&format!("INSERT INTO {qualified_table} VALUES (1, 0)"))
        .await
        .unwrap();
    db.execute(&format!(
        "CREATE PROCEDURE {schema}.{procedure} () LANGUAGE SQL DYNAMIC RESULT SETS 2 BEGIN DECLARE C1 CURSOR WITH RETURN TO CALLER FOR SELECT 41 AS N FROM SYSIBM.SYSDUMMY1; DECLARE C2 CURSOR WITH RETURN TO CALLER FOR SELECT V AS N FROM {qualified_table} WHERE ID = 1; OPEN C1; OPEN C2; END"
    ))
    .await
    .unwrap();

    // Hold a row lock so processing the second cursor reaches a confirmed server-side wait.
    // Db2 may wait at cursor OPEN or its first fetch; this avoids timing against fast CNTQRY round trips.
    let mut transaction = locker.begin_transaction().unwrap();
    transaction
        .execute(&format!("UPDATE {qualified_table} SET V = 1 WHERE ID = 1"))
        .await
        .unwrap();

    let db = Arc::new(tokio::sync::Mutex::new(db));
    // Capture the cloneable handle before the query task acquires the session mutex.
    // Looking it up afterward races with the long-running query and can wait until CALL
    // has already completed, turning the interrupt into an idle-session no-op.
    let handle = db.lock().await.interrupt_handle().unwrap();
    let query_db = Arc::clone(&db);
    let call_sql = format!("CALL {procedure}()");
    let query = tokio::spawn(async move {
        let mut session = query_db.lock().await;
        session.query(&call_sql).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let active_before_interrupt = !query.is_finished();
    let interrupt_started = std::time::Instant::now();
    let interrupt_result = handle.interrupt().await.map_err(|error| error.to_string());
    let interrupt_ack_latency = interrupt_started.elapsed();
    let completion_started = std::time::Instant::now();
    let query_result = match query.await {
        Ok(result) => result,
        Err(error) => Err(Db2Error::other(format!(
            "procedure query task failed: {error}"
        ))),
    };
    let completion_after_ack = completion_started.elapsed();
    let query_error = match query_result {
        Err(error) => error.to_string(),
        Ok(result) => format!(
            "CALL finished without SQLINTR after result sets with row counts {:?}; active before interrupt={active_before_interrupt}, ack={:.1}ms, completion after ack={:.1}ms",
            result
                .result_sets
                .iter()
                .map(|set| set.rows.len())
                .collect::<Vec<_>>(),
            interrupt_ack_latency.as_secs_f64() * 1_000.0,
            completion_after_ack.as_secs_f64() * 1_000.0,
        ),
    };

    transaction.rollback().await.unwrap();
    locker.close().await.unwrap();
    let mut session = db.lock().await;
    let session_open_after_interrupt = !session.is_closed();
    let reuse_error = if session_open_after_interrupt {
        match session.query("VALUES 29").await {
            Ok(result) if result.rows == vec![vec![Db2Value::Integer(29)]] => None,
            Ok(result) => Some(format!("unexpected post-interrupt rows: {:?}", result.rows)),
            Err(error) => Some(error.to_string()),
        }
    } else {
        Some("session was closed after a complete SQLCODE -952 reply".into())
    };
    let session_close = session.close().await;
    drop(session);
    drop(db);

    let mut cleanup = Db2Session::connect_dsn(&dsn).await.unwrap();
    let existing = cleanup
        .query_with_params(
            "SELECT COUNT(*) FROM SYSCAT.ROUTINES WHERE ROUTINENAME = ?",
            &[Db2Value::Text(procedure.clone())],
        )
        .await
        .unwrap();
    let Db2Value::Integer(routine_count) = &existing.rows[0][0] else {
        panic!(
            "expected an integer routine count, got {:?}",
            existing.rows[0][0]
        );
    };
    if *routine_count > 0 {
        cleanup
            .execute(&format!("DROP PROCEDURE {schema}.{procedure}"))
            .await
            .unwrap();
    }
    cleanup
        .execute(&format!("DROP TABLE {qualified_table}"))
        .await
        .unwrap();
    cleanup.close().await.unwrap();

    assert!(interrupt_result.is_ok(), "{interrupt_result:?}");
    assert!(
        query_error.contains("SQLCODE=-952 SQLSTATE=57014"),
        "{query_error}"
    );
    assert!(
        session_open_after_interrupt,
        "a complete SQLCODE -952 reply should leave the session reusable"
    );
    assert!(active_before_interrupt, "{query_error}");
    assert!(reuse_error.is_none(), "{reuse_error:?}");
    session_close.unwrap();
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_rejects_out_of_range_inout_value_before_sending_and_keeps_the_session() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let procedure = format!("RS_INOUT_RANGE_{suffix:X}");
    let schema = current_schema(&mut db).await;
    let create_sql = format!(
        "CREATE PROCEDURE {schema}.{procedure} (IN a INT, INOUT b SMALLINT, OUT c INT) LANGUAGE SQL BEGIN SET c = a + b; SET b = b + 1; END"
    );
    db.execute(&create_sql).await.unwrap();
    let execution = async {
        let statement = db.prepare(&format!("CALL {procedure}(?, ?, ?)")).await?;
        db.execute_prepared_with_outputs(
            &statement,
            &[
                Db2Parameter {
                    value: Db2Value::Integer(1),
                    direction: Db2ParameterDirection::Input,
                },
                Db2Parameter {
                    value: Db2Value::Integer(32768),
                    direction: Db2ParameterDirection::InputOutput,
                },
                Db2Parameter {
                    value: Db2Value::Integer(0),
                    direction: Db2ParameterDirection::Output,
                },
            ],
        )
        .await
    }
    .await;
    let cleanup = db
        .execute(&format!("DROP PROCEDURE {schema}.{procedure}"))
        .await;
    let error = execution.unwrap_err();
    cleanup.unwrap();
    // The encoder rejects the value before any SQLDTA is written, so the session
    // must remain usable rather than desynchronized.
    assert!(error.to_string().contains("signed 16-bit range"), "{error}");
    assert_eq!(
        db.query("VALUES 7").await.unwrap().rows,
        vec![vec![Db2Value::Integer(7)]]
    );
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_procedure_error_after_output_assignment_keeps_the_session_usable() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let procedure = format!("RS_OUT_FAIL_{suffix:X}");
    let schema = current_schema(&mut db).await;
    let create_sql = format!(
        "CREATE PROCEDURE {schema}.{procedure} (OUT a INT, OUT b VARCHAR(20)) LANGUAGE SQL BEGIN SET a = 11; SET b = 'set-before-failure'; SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT = 'invalid output value'; END"
    );
    db.execute(&create_sql).await.unwrap();
    let execution = async {
        let statement = db.prepare(&format!("CALL {procedure}(?, ?)")).await?;
        db.execute_prepared_with_outputs(
            &statement,
            &[
                Db2Parameter {
                    value: Db2Value::Integer(0),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text(String::new()),
                    direction: Db2ParameterDirection::Output,
                },
            ],
        )
        .await
    }
    .await;
    let cleanup = db
        .execute(&format!("DROP PROCEDURE {schema}.{procedure}"))
        .await;
    let error = execution.unwrap_err();
    cleanup.unwrap();
    let message = error.to_string();
    assert!(message.contains("SQLCODE"), "{message}");
    assert!(message.contains("75001"), "{message}");
    assert_eq!(
        db.query("VALUES 7").await.unwrap().rows,
        vec![vec![Db2Value::Integer(7)]]
    );
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_procedure_returns_out_and_inout_parameters() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let procedure = format!("RS_OUT_{suffix:X}");
    let schema = current_schema(&mut db).await;
    let create_sql = format!(
        "CREATE OR REPLACE PROCEDURE {schema}.{procedure} (IN a INT, IN b INT, OUT sum_val INT, OUT prod_val INT, INOUT msg VARCHAR(100)) LANGUAGE SQL BEGIN SET sum_val = a + b; SET prod_val = a * b; SET msg = 'OK: ' || msg; END"
    );
    db.execute(&create_sql).await.unwrap();
    let execution = async {
        let statement = db
            .prepare(&format!("CALL {procedure}(?, ?, ?, ?, ?)"))
            .await?;
        db.execute_prepared_with_outputs(
            &statement,
            &[
                Db2Parameter {
                    value: Db2Value::Integer(15),
                    direction: Db2ParameterDirection::Input,
                },
                Db2Parameter {
                    value: Db2Value::Integer(4),
                    direction: Db2ParameterDirection::Input,
                },
                Db2Parameter {
                    value: Db2Value::Integer(0),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Integer(0),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text("Alpha".into()),
                    direction: Db2ParameterDirection::InputOutput,
                },
            ],
        )
        .await
    }
    .await;
    let cleanup = db
        .execute(&format!("DROP PROCEDURE {schema}.{procedure}"))
        .await;
    let result = execution.unwrap();
    cleanup.unwrap();
    assert_eq!(result.rows_affected, 0);
    assert_eq!(
        result.output_parameters,
        vec![
            Db2Value::Null,
            Db2Value::Null,
            Db2Value::Integer(19),
            Db2Value::Integer(60),
            Db2Value::Text("OK: Alpha".into()),
        ]
    );
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_typed_procedure_out_parameters_preserve_db2_values() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let procedure = format!("RS_TYPED_OUT_{suffix:X}");
    let schema = current_schema(&mut db).await;
    let create_sql = format!(
        "CREATE PROCEDURE {schema}.{procedure} (OUT small_value SMALLINT, OUT decimal_value DECIMAL(9,3), OUT decfloat_value DECFLOAT(16), OUT date_value DATE, OUT timestamp_value TIMESTAMP, OUT text_value VARCHAR(30), OUT varbinary_value VARBINARY(10), OUT blob_value BLOB(100), OUT clob_value CLOB(100), OUT dbclob_value DBCLOB(100), OUT xml_value XML, OUT boolean_value BOOLEAN) LANGUAGE SQL BEGIN SET small_value = -32768; SET decimal_value = 42.125; SET decfloat_value = DECFLOAT('Infinity'); SET date_value = DATE('2026-08-26'); SET timestamp_value = TIMESTAMP('2026-08-26 12:34:56.123456'); SET text_value = 'Zażółć 東京'; SET varbinary_value = CAST(X'007F80FF' AS VARBINARY(10)); SET blob_value = BLOB(X'007F80FF'); SET clob_value = 'CLOB café'; SET dbclob_value = CAST('DBCLOB 東京' AS DBCLOB(100)); SET xml_value = XMLPARSE(DOCUMENT '<root/>'); SET boolean_value = TRUE; END"
    );
    db.execute(&create_sql).await.unwrap();
    let execution = async {
        let statement = db
            .prepare(&format!("CALL {procedure}({})", ["?"; 12].join(", ")))
            .await?;
        db.execute_prepared_with_outputs(
            &statement,
            &[
                Db2Parameter {
                    value: Db2Value::Integer(0),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Decimal("0".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Decimal("0".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text("2000-01-01".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text("2000-01-01 00:00:00".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text(String::new()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Bytes(Vec::new()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Bytes(Vec::new()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text(String::new()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text(String::new()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text("<empty/>".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Boolean(false),
                    direction: Db2ParameterDirection::Output,
                },
            ],
        )
        .await
    }
    .await;
    let cleanup = db
        .execute(&format!("DROP PROCEDURE {schema}.{procedure}"))
        .await;
    let result = execution.unwrap();
    cleanup.unwrap();
    assert_eq!(result.rows_affected, 0);
    assert_eq!(
        result.output_parameters,
        vec![
            Db2Value::Integer(-32768),
            Db2Value::Decimal("42.125".into()),
            Db2Value::Decimal("Infinity".into()),
            Db2Value::Text("2026-08-26".into()),
            Db2Value::Text("2026-08-26-12.34.56.123456".into()),
            Db2Value::Text("Zażółć 東京".into()),
            Db2Value::Bytes(vec![0, 0x7f, 0x80, 0xff]),
            Db2Value::Bytes(vec![0, 0x7f, 0x80, 0xff]),
            Db2Value::Text("CLOB café".into()),
            Db2Value::Text("DBCLOB 東京".into()),
            Db2Value::Text("<root/>".into()),
            Db2Value::Boolean(true),
        ]
    );
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_typed_procedure_out_nulls_stay_null() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let procedure = format!("RS_NULLOUT_{suffix:X}");
    let schema = current_schema(&mut db).await;
    let create_sql = format!(
        "CREATE PROCEDURE {schema}.{procedure} (OUT small_value SMALLINT, OUT decimal_value DECIMAL(9,3), OUT decfloat_value DECFLOAT(16), OUT date_value DATE, OUT timestamp_value TIMESTAMP, OUT text_value VARCHAR(30), OUT varbinary_value VARBINARY(10), OUT blob_value BLOB(100), OUT clob_value CLOB(100), OUT dbclob_value DBCLOB(100), OUT xml_value XML, OUT boolean_value BOOLEAN) LANGUAGE SQL BEGIN SET small_value = NULL; SET decimal_value = NULL; SET decfloat_value = NULL; SET date_value = NULL; SET timestamp_value = NULL; SET text_value = NULL; SET varbinary_value = NULL; SET blob_value = NULL; SET clob_value = NULL; SET dbclob_value = NULL; SET xml_value = NULL; SET boolean_value = NULL; END"
    );
    db.execute(&create_sql).await.unwrap();
    let execution = async {
        let statement = db
            .prepare(&format!("CALL {procedure}({})", ["?"; 12].join(", ")))
            .await?;
        db.execute_prepared_with_outputs(
            &statement,
            &[
                Db2Parameter {
                    value: Db2Value::Integer(1),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Decimal("1".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Decimal("1".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text("2026-01-02".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text("2026-01-02 03:04:05.000001".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text("seed".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Bytes(vec![1]),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Bytes(vec![2]),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text("seed".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text("seed".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Text("<seed/>".into()),
                    direction: Db2ParameterDirection::Output,
                },
                Db2Parameter {
                    value: Db2Value::Boolean(true),
                    direction: Db2ParameterDirection::Output,
                },
            ],
        )
        .await
    }
    .await;
    let cleanup = db
        .execute(&format!("DROP PROCEDURE {schema}.{procedure}"))
        .await;
    let result = execution.unwrap();
    cleanup.unwrap();
    assert_eq!(result.output_parameters.len(), 12);
    assert_eq!(result.output_parameters, vec![Db2Value::Null; 12]);
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_procedure_out_lob_with_result_cursor_routes_extdta_to_owner() {
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let procedure = format!("RS_OUTLOB_{suffix:X}");
    let schema = current_schema(&mut db).await;
    let call_options = Db2CommandOptions::new(60_000, 15_000).with_interrupt_ack_timeout_ms(10_000);
    let create_sql = format!(
        "CREATE PROCEDURE {schema}.{procedure} (OUT out_blob BLOB(100), OUT out_text VARCHAR(20), INOUT inout_value INTEGER) LANGUAGE SQL DYNAMIC RESULT SETS 1 BEGIN DECLARE C CURSOR WITH RETURN TO CALLER FOR SELECT 42 AS V, BLOB(X'007F80FF') AS PAYLOAD FROM SYSIBM.SYSDUMMY1; SET out_blob = BLOB(X'007F80FF'); SET out_text = 'hello-世界'; SET inout_value = inout_value + 1; OPEN C; END"
    );
    db.execute(&create_sql).await.unwrap();
    let execution = async {
        let statement = db.prepare(&format!("CALL {procedure}(?, ?, ?)")).await?;
        let mut reader = db
            .call_procedure_stream_with_options(
                &statement,
                &[
                    Db2Parameter {
                        value: Db2Value::Bytes(Vec::new()),
                        direction: Db2ParameterDirection::Output,
                    },
                    Db2Parameter {
                        value: Db2Value::Text(String::new()),
                        direction: Db2ParameterDirection::Output,
                    },
                    Db2Parameter {
                        value: Db2Value::Integer(7),
                        direction: Db2ParameterDirection::InputOutput,
                    },
                ],
                call_options,
            )
            .await?;
        assert_eq!(reader.columns()[1].name, "PAYLOAD");
        assert_eq!(
            reader.next().await?,
            Some(vec![
                Db2Value::Integer(42),
                Db2Value::Bytes(vec![0, 0x7f, 0x80, 0xff]),
            ])
        );
        assert!(reader.output_parameters().is_none());
        assert!(!reader.next_result_set().await?);
        assert_eq!(
            reader.output_parameters(),
            Some(
                &[
                    Db2Value::Bytes(vec![0, 0x7f, 0x80, 0xff]),
                    Db2Value::Text("hello-世界".into()),
                    Db2Value::Integer(8),
                ][..]
            )
        );
        let terminal = reader.finish().await?;
        assert_eq!(terminal.rows_affected, 0);
        let reuse = db.query("VALUES 91").await?;
        assert_eq!(reuse.rows, vec![vec![Db2Value::Integer(91)]]);
        Ok::<(), Db2Error>(())
    }
    .await;
    let drop_sql = format!("DROP PROCEDURE {schema}.{procedure}");
    let cleanup = if db.is_closed() {
        let mut cleanup_db = Db2Session::connect_dsn(&dsn).await.unwrap();
        let drop_result = cleanup_db
            .execute_with_options(&drop_sql, call_options)
            .await;
        let close_result = cleanup_db.close().await;
        drop_result.and(close_result)
    } else {
        db.execute_with_options(&drop_sql, call_options)
            .await
            .map(|_| ())
    };
    execution.unwrap();
    cleanup.unwrap();
}

struct PatternReadStream {
    length: usize,
    position: usize,
    bytes_read: Arc<AtomicUsize>,
    max_requested: Arc<AtomicUsize>,
}

impl PatternReadStream {
    fn new(length: usize) -> (Self, (Arc<AtomicUsize>, Arc<AtomicUsize>)) {
        let bytes_read = Arc::new(AtomicUsize::new(0));
        let max_requested = Arc::new(AtomicUsize::new(0));
        (
            Self {
                length,
                position: 0,
                bytes_read: bytes_read.clone(),
                max_requested: max_requested.clone(),
            },
            (bytes_read, max_requested),
        )
    }
}

impl AsyncRead for PatternReadStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let requested = buffer.remaining();
        this.max_requested.fetch_max(requested, Ordering::AcqRel);
        let count = requested.min(this.length.saturating_sub(this.position));
        let start = this.position;
        let output = buffer.initialize_unfilled_to(count);
        for (offset, byte) in output.iter_mut().enumerate() {
            *byte = (((start + offset) * 37 + 11) % 251) as u8;
        }
        buffer.advance(count);
        this.position += count;
        this.bytes_read.store(this.position, Ordering::Release);
        Poll::Ready(Ok(()))
    }
}

fn pattern_bytes(length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| ((index * 37 + 11) % 251) as u8)
        .collect()
}

/// Optional independent oracle supplied by the harness that qualifies several drivers
/// together (db2-conformance): `DB2_IBM_IMPORT_ORACLE` names an executable that accepts
/// `--verify-ts-import <table>` and prints one JSON line with the rows the official IBM
/// provider reads back. Returns `None` (after printing a marker a qualification run can
/// refuse) when it is not set; the driver-side assertions still run. This crate never
/// depends on any other driver repository.
fn ibm_verify_imported_table(table: &str) -> Option<String> {
    let Some(oracle) = std::env::var_os("DB2_IBM_IMPORT_ORACLE") else {
        println!("IBM import oracle verification skipped: DB2_IBM_IMPORT_ORACLE is not set");
        return None;
    };
    let result = std::process::Command::new(oracle)
        .args(["--verify-ts-import", table])
        .output()
        .expect("failed to launch the official IBM import oracle");
    if !result.status.success() {
        let stdout = String::from_utf8_lossy(&result.stdout);
        let stderr = String::from_utf8_lossy(&result.stderr);
        let tail = |text: &str| {
            text.lines()
                .rev()
                .take(20)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n")
        };
        panic!(
            "official IBM import oracle failed (exit {:?}):\n{}\n{}",
            result.status.code(),
            tail(stderr.trim()),
            tail(stdout.trim())
        );
    }
    Some(
        String::from_utf8_lossy(&result.stdout)
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .unwrap_or_default()
            .to_string(),
    )
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_dropped_commands_discard_transport_and_pool_task_abort_releases_slot() {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let dsn = std::env::var("DB2_TEST_DSN").unwrap();
    let table_name = format!(
        "RS_DROP_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut locker = Db2Session::connect_dsn(&dsn).await.unwrap();
    let schema_result = locker.query("VALUES CURRENT SCHEMA").await.unwrap();
    let schema = match schema_result.rows.first().and_then(|row| row.first()) {
        Some(Db2Value::Text(schema)) => schema.trim().to_owned(),
        other => panic!("unexpected CURRENT SCHEMA result: {other:?}"),
    };
    // Keep this cancellation fixture deterministic; IBM CLI/.NET unqualified DDL behavior is
    // exercised independently by the official-provider DDL oracle.
    let quote = |identifier: &str| format!("\"{}\"", identifier.replace('"', "\"\""));
    let table = format!("{}.{}", quote(&schema), quote(&table_name));
    locker
        .execute(&format!(
            "CREATE TABLE {table} (ID INT NOT NULL PRIMARY KEY, V INT)"
        ))
        .await
        .unwrap();
    locker
        .execute(&format!("INSERT INTO {table} VALUES (1, 0)"))
        .await
        .unwrap();
    let mut transaction = locker.begin_transaction().unwrap();
    transaction
        .execute(&format!("UPDATE {table} SET V = 10 WHERE ID = 1"))
        .await
        .unwrap();
    let mut db = Db2Session::connect_dsn(&dsn).await.unwrap();
    db.set_auto_commit(false);
    let sql = format!("UPDATE {table} SET V = 20 WHERE ID = 1");
    let mut command = Box::pin(db.execute(&sql));
    tokio::select! {
        result = &mut command => panic!("locked DML unexpectedly completed: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
    }
    drop(command);
    let closed = db.is_closed();
    let next = db.query("VALUES 29").await;
    drop(db);
    let pool = Db2Pool::connect_dsn(&dsn, 1).unwrap();
    // Hold the physical session open before starting the locked command so the
    // task's delay tests command cancellation rather than connection establishment.
    let lease = pool.acquire().await.unwrap();
    drop(lease);
    let worker_pool = pool.clone();
    let select = format!("SELECT V FROM {table} WHERE ID = 1 WITH RS");
    let worker = tokio::spawn(async move { worker_pool.query(&select).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let still_running = !worker.is_finished();
    worker.abort();
    let aborted = worker.await.unwrap_err().is_cancelled();
    let counts = (pool.session_count(), pool.idle_session_count());
    transaction.rollback().await.unwrap();
    let result = pool
        .query(&format!("SELECT V FROM {table} WHERE ID = 1"))
        .await
        .unwrap();
    let reused = pool.query("VALUES 29").await.unwrap();
    pool.close().await.unwrap();
    locker
        .execute(&format!("DROP TABLE {table}"))
        .await
        .unwrap();
    assert!(closed);
    assert_eq!(next.unwrap_err().kind, Db2ErrorKind::ConnectionClosed);
    assert!(still_running && aborted);
    assert_eq!(counts, (0, 0));
    assert_eq!(result.rows, vec![vec![Db2Value::Integer(0)]]);
    assert_eq!(reused.rows, vec![vec![Db2Value::Integer(29)]]);
}

#[tokio::test]
#[ignore = "requires DB2_TEST_DSN and a live Db2 database"]
async fn live_dropped_pool_wait_future_preserves_active_lease() {
    let dsn = std::env::var("DB2_TEST_DSN").expect("DB2_TEST_DSN must be set");
    let pool = Db2Pool::connect_dsn(&dsn, 1).unwrap();
    let mut lease = pool.acquire().await.unwrap();
    {
        // Poll the waiting future, then drop it while the only slot is leased.
        // This is Rust's cancellation contract; no SQLINTR should be sent.
        let pending = pool.acquire();
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut pending)
                .await
                .is_err()
        );
    }
    assert_eq!(pool.session_count(), 1);
    assert_eq!(pool.idle_session_count(), 0);
    assert!(!lease.is_closed());
    assert_eq!(
        lease.query("VALUES 17").await.unwrap().rows,
        vec![vec![Db2Value::Integer(17)]]
    );
    lease.release().await.unwrap();
    let mut next = tokio::time::timeout(std::time::Duration::from_secs(5), pool.acquire())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        next.query("VALUES 19").await.unwrap().rows,
        vec![vec![Db2Value::Integer(19)]]
    );
    assert_eq!(pool.session_count(), 1);
    next.release().await.unwrap();
    assert_eq!(pool.idle_session_count(), 1);
    pool.close().await.unwrap();
    assert_eq!(pool.session_count(), 0);
}
