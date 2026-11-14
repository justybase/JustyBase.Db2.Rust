#![forbid(unsafe_code)]

mod admin;
pub mod config;
pub mod converters;
pub mod diagnostics;
mod error;
mod import;
pub mod network;
#[doc(hidden)]
pub mod replay;
pub mod resources;
pub mod security;
pub mod types;

pub use config::{Config, ConfigError, ProtocolLimits, TlsOverride, parse_dsn, redact_dsn};
pub use error::{Db2BatchError, Db2Error, Db2ErrorKind, Db2OperationOutcome, Db2Warning};
pub use import::{
    Db2ImportColumn, Db2ImportError, Db2ImportOptions, Db2ImportPolicy, Db2ImportProgress,
    Db2ImportTarget,
};
pub use network::pool::{Db2Pool, Db2PoolOptions, Db2PoolStats, Db2PooledSession, PoolResetPolicy};
pub use network::session::{
    Db2BatchOptions, Db2ClientInfo, Db2CommandContext, Db2CommandOptions, Db2ExecutionResult,
    Db2InputParameter, Db2InsertResult, Db2InterruptHandle, Db2IsolationLevel, Db2LobEncoding,
    Db2LobReader, Db2LobValue, Db2Parameter, Db2ParameterDirection, Db2ProcedureReader,
    Db2ProcedureResult, Db2QueryResult, Db2ResultSet, Db2RowReader, Db2RowRef, Db2Session,
    Db2Statement, Db2Transaction, Db2Value, Db2ValueRef,
};

#[cfg(test)]
mod contract_document_tests {
    use std::fmt::Write as _;

    use ring::digest::{SHA256, digest};

    #[test]
    fn normative_contract_copy_is_self_consistent() {
        let text = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/docs/DRIVER-CONTRACT.md"
        ))
        .expect("read docs/DRIVER-CONTRACT.md")
        .replace("\r\n", "\n");
        let (header, body) = text.split_once('\n').expect("contract header line");
        let expected = header
            .strip_prefix("<!-- normative; v1; sha256:")
            .and_then(|value| {
                value.strip_suffix("; generated from the maintainer contract; do not edit -->")
            })
            .expect("contract header shape");
        assert_eq!(expected.len(), 64);
        assert!(
            expected
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        );
        let mut actual = String::with_capacity(64);
        for byte in digest(&SHA256, body.as_bytes()).as_ref() {
            write!(&mut actual, "{byte:02x}").expect("write SHA-256 hex");
        }
        assert_eq!(actual, expected);
        assert!(!text.contains("db2-conformance"));
    }
}

#[cfg(test)]
mod jdc_vector_tests {
    use std::fmt::Write as _;

    use ring::digest::{SHA256, digest};
    use serde_json::Value;

    use crate::error::Db2Error;
    use crate::{
        Config, Db2BatchOptions, Db2ErrorKind, Db2ImportOptions, Db2ImportPolicy,
        Db2OperationOutcome, Db2PoolOptions, ProtocolLimits, parse_dsn,
    };

    fn vector_text() -> String {
        std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/jdc/JDC-v1.vectors.json"
        ))
        .expect("read JDC vector copy")
        .replace("\r\n", "\n")
    }

    fn hex(bytes: &[u8]) -> String {
        let mut text = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut text, "{byte:02x}").expect("write SHA-256 hex");
        }
        text
    }

    fn canonical(kind: Db2ErrorKind) -> &'static str {
        match kind {
            Db2ErrorKind::InvalidConfiguration => "configuration",
            Db2ErrorKind::ConnectionClosed => "connection-closed",
            Db2ErrorKind::DatabaseNotFound => "database-not-found",
            Db2ErrorKind::AuthenticationFailed => "authentication",
            Db2ErrorKind::AuthorizationFailed => "authorization",
            Db2ErrorKind::ParameterConversion => "parameter-conversion",
            Db2ErrorKind::SqlServer => "sql-server",
            Db2ErrorKind::ServerResponse => "server-response",
            Db2ErrorKind::Transport => "transport",
            Db2ErrorKind::Protocol => "protocol",
            Db2ErrorKind::ResourceLimit => "resource-limit",
            Db2ErrorKind::OperationCancelled => "operation-cancelled",
            Db2ErrorKind::Timeout => "timeout",
            Db2ErrorKind::BatchExecution => "batch-execution",
            Db2ErrorKind::Unsupported => "unsupported",
            Db2ErrorKind::PoolTimeout => "pool-timeout",
            Db2ErrorKind::PoolExhausted => "pool-exhausted",
            Db2ErrorKind::Other => "other",
        }
    }

    fn outcome(value: Db2OperationOutcome) -> &'static str {
        match value {
            Db2OperationOutcome::NotStarted => "not-started",
            Db2OperationOutcome::Confirmed => "confirmed",
            Db2OperationOutcome::Unknown => "unknown",
        }
    }

    fn error_from(input: &Value) -> Db2Error {
        let message = input["message"].as_str().expect("error message");
        match input["kind"].as_str().expect("error kind") {
            "sql" => Db2Error::server_diagnostic(format!(
                "SQLCODE={} SQLSTATE={} {message}",
                input["sql_code"].as_i64().expect("sql_code"),
                input["sql_state"].as_str().expect("sql_state")
            )),
            "configuration" => Db2Error::new(Db2ErrorKind::InvalidConfiguration, message),
            "pool-timeout" => Db2Error::new(Db2ErrorKind::PoolTimeout, message),
            "pool-exhausted" => Db2Error::new(Db2ErrorKind::PoolExhausted, message),
            "timeout" => Db2Error::timeout(message, Db2OperationOutcome::Unknown),
            "server-response" => Db2Error::new(Db2ErrorKind::ServerResponse, message),
            kind => panic!("unmapped error kind {kind}"),
        }
    }

    #[test]
    fn vector_copy_is_self_consistent() {
        let text = vector_text();
        let expected = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/jdc/JDC-v1.vectors.json.sha256"
        ))
        .expect("read JDC vector hash")
        .trim()
        .to_owned();
        assert_eq!(hex(digest(&SHA256, text.as_bytes()).as_ref()), expected);
        assert!(!text.contains("db2-conformance"));

        let vectors: Value = serde_json::from_str(&text).expect("parse JDC vectors");
        assert_eq!(vectors["schema"], "jdc-vectors/v1");
        assert_eq!(vectors["contract"]["version"], "v1");

        let contract = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/docs/DRIVER-CONTRACT.md"
        ))
        .expect("read contract copy")
        .replace("\r\n", "\n");
        let (_, body) = contract.split_once('\n').expect("contract header line");
        assert_eq!(
            vectors["contract"]["sha256"],
            hex(digest(&SHA256, body.as_bytes()).as_ref())
        );
    }

    #[test]
    fn dsn_vectors_match_the_parser() {
        let vectors: Value = serde_json::from_str(&vector_text()).expect("parse JDC vectors");
        for entry in vectors["dsn"]["accept"].as_array().expect("dsn.accept") {
            let id = entry["id"].as_str().expect("dsn id");
            let config =
                parse_dsn(entry["input"].as_str().expect("dsn input")).expect("accepted DSN");
            for (field, value) in entry["expect"].as_object().expect("dsn expect") {
                assert_dsn_value(&config, field, value, id);
            }
        }
        for entry in vectors["dsn"]["reject"].as_array().expect("dsn.reject") {
            let input = entry["input"].as_str().expect("dsn input");
            let error = parse_dsn(input).expect_err("DSN must be rejected");
            assert_eq!(error.kind(), Db2ErrorKind::InvalidConfiguration);
        }
    }

    #[test]
    fn error_vectors_match_classification_and_outcome() {
        let vectors: Value = serde_json::from_str(&vector_text()).expect("parse JDC vectors");
        for entry in vectors["errors"].as_array().expect("errors") {
            let id = entry["id"].as_str().expect("error id");
            let error = error_from(&entry["input"]);
            let expect = &entry["expect"];
            assert_eq!(
                canonical(error.kind),
                expect["category"].as_str().unwrap(),
                "{id}: category"
            );
            assert_eq!(
                outcome(error.outcome),
                expect["outcome"].as_str().unwrap(),
                "{id}: outcome"
            );
            if let Some(code) = expect.get("sql_code") {
                assert_eq!(
                    error.sql_code,
                    Some(code.as_i64().unwrap() as i32),
                    "{id}: sql_code"
                );
            }
            if let Some(state) = expect.get("sql_state") {
                assert_eq!(
                    error.sql_state.as_deref(),
                    state.as_str(),
                    "{id}: sql_state"
                );
            }
            for fragment in expect["message_contains"].as_array().into_iter().flatten() {
                assert!(
                    error.to_string().contains(fragment.as_str().unwrap()),
                    "{id}: message missing {}",
                    fragment.as_str().unwrap()
                );
            }
        }
    }

    #[test]
    fn limit_vectors_match_the_driver_defaults() {
        let vectors: Value = serde_json::from_str(&vector_text()).expect("parse JDC vectors");
        let limits = ProtocolLimits::default();
        for entry in vectors["limits"].as_array().expect("limits") {
            let field = entry["field"].as_str().expect("limit field");
            let actual = match field {
                "max_dss_size" => limits.max_dss_size,
                "max_ddm_object_size" => limits.max_ddm_object_size,
                "max_reply_chain_bytes" => limits.max_reply_chain_bytes,
                "max_reply_chain_packets" => limits.max_reply_chain_packets,
                "max_materialized_lob_size" => limits.max_materialized_lob_size,
                "max_materialized_result_bytes" => crate::resources::MAX_MATERIALIZED_RESULT_BYTES,
                "max_streamed_lob_bytes" => crate::resources::MAX_STREAMED_LOB_BYTES as usize,
                "lob_chunk_bytes" => crate::resources::CHUNK_BYTES,
                other => panic!("unmapped limit field {other}"),
            };
            assert_eq!(
                actual as u64,
                entry["value"].as_u64().expect("limit value"),
                "{}",
                entry["id"].as_str().unwrap()
            );
        }
    }

    #[test]
    fn bound_vectors_match_option_validators() {
        let vectors: Value = serde_json::from_str(&vector_text()).expect("parse JDC vectors");
        for entry in vectors["bounds"].as_array().expect("bounds") {
            let surface = entry["surface"].as_str().expect("bound surface");
            let field = entry["field"].as_str().expect("bound field");
            let validate = |value: i64| {
                let value = value as usize;
                match surface {
                    "batch" if field == "max_rows" => Db2BatchOptions::new(value, 1).validate(),
                    "batch" => Db2BatchOptions::new(1, value).validate(),
                    "pool" => {
                        let options = Db2PoolOptions {
                            max_connections: if field == "max_connections" { value } else { 1 },
                            max_waiters: if field == "max_waiters" { value } else { 1 },
                            ..Db2PoolOptions::default()
                        };
                        options.validate()
                    }
                    "import" => {
                        let options = match field {
                            "max_batch_rows" => Db2ImportOptions {
                                policy: Db2ImportPolicy::Atomic {
                                    max_batch_rows: value,
                                    max_batch_bytes: 8 * 1024 * 1024,
                                },
                                ..Db2ImportOptions::default()
                            },
                            "max_batch_bytes" => Db2ImportOptions {
                                policy: Db2ImportPolicy::Atomic {
                                    max_batch_rows: 256,
                                    max_batch_bytes: value,
                                },
                                ..Db2ImportOptions::default()
                            },
                            "notify_after_rows" => Db2ImportOptions {
                                notify_after_rows: Some(value as u64),
                                ..Db2ImportOptions::default()
                            },
                            other => panic!("unmapped import bound {other}"),
                        };
                        options.validate().map(|_| ())
                    }
                    other => panic!("unmapped bound surface {other}"),
                }
            };
            let min = entry["min"].as_i64().expect("bound min");
            if min > 0 {
                assert!(
                    validate(min - 1).is_err(),
                    "{}: min-1 accepted",
                    entry["id"]
                );
            }
            assert!(validate(min).is_ok(), "{}: min rejected", entry["id"]);
            if let Some(max) = entry.get("max") {
                let max = max.as_i64().expect("bound max");
                assert!(
                    validate(max + 1).is_err(),
                    "{}: max+1 accepted",
                    entry["id"]
                );
                assert!(validate(max).is_ok(), "{}: max rejected", entry["id"]);
            }
        }
    }

    #[test]
    fn default_vectors_match_option_types() {
        let vectors: Value = serde_json::from_str(&vector_text()).expect("parse JDC vectors");
        let pool = Db2PoolOptions::default();
        let batch = Db2BatchOptions::default();
        let import = Db2ImportOptions::default();
        let (import_rows, import_bytes, commit_each) = match import.policy {
            Db2ImportPolicy::Atomic {
                max_batch_rows,
                max_batch_bytes,
            } => (max_batch_rows, max_batch_bytes, false),
            Db2ImportPolicy::CommitEveryRows {
                rows,
                max_batch_bytes,
            } => (rows, max_batch_bytes, true),
        };
        for entry in vectors["defaults"].as_array().expect("defaults") {
            let surface = entry["surface"].as_str().expect("default surface");
            let field = entry["field"].as_str().expect("default field");
            let expected = &entry["value"];
            match (surface, field) {
                ("pool", "max_connections") => {
                    assert_eq!(pool.max_connections as u64, expected.as_u64().unwrap())
                }
                ("pool", "max_waiters") => {
                    assert_eq!(pool.max_waiters as u64, expected.as_u64().unwrap())
                }
                ("pool", "acquire_timeout_ms") => assert_eq!(
                    pool.acquire_timeout.as_millis() as u64,
                    expected.as_u64().unwrap()
                ),
                ("pool", "connect_timeout_ms") => assert_eq!(
                    pool.connect_timeout.as_millis() as u64,
                    expected.as_u64().unwrap()
                ),
                ("pool", "idle_timeout_ms") => assert_eq!(
                    pool.idle_timeout.as_millis() as u64,
                    expected.as_u64().unwrap()
                ),
                ("pool", "max_lifetime_ms") => assert_eq!(
                    pool.max_lifetime.as_millis() as u64,
                    expected.as_u64().unwrap()
                ),
                ("pool", "reap_interval_ms") => assert_eq!(
                    pool.reap_interval.as_millis() as u64,
                    expected.as_u64().unwrap()
                ),
                ("batch", "max_rows") => {
                    assert_eq!(batch.max_rows as u64, expected.as_u64().unwrap())
                }
                ("batch", "max_encoded_bytes") => {
                    assert_eq!(batch.max_encoded_bytes as u64, expected.as_u64().unwrap())
                }
                ("import", "max_batch_rows") => {
                    assert_eq!(import_rows as u64, expected.as_u64().unwrap())
                }
                ("import", "max_batch_bytes") => {
                    assert_eq!(import_bytes as u64, expected.as_u64().unwrap())
                }
                ("import", "notify_after_rows") => {
                    assert_eq!(import.notify_after_rows, Some(expected.as_u64().unwrap()))
                }
                ("import", "table_lock") => {
                    assert_eq!(import.table_lock, expected.as_bool().unwrap())
                }
                ("import", "policy") => assert_eq!(
                    expected.as_str().unwrap(),
                    if commit_each {
                        "commit-every-rows"
                    } else {
                        "atomic"
                    }
                ),
                other => panic!("unmapped default {other:?}"),
            }
        }
    }

    fn assert_dsn_value(config: &Config, field: &str, value: &Value, id: &str) {
        match field {
            "host" => assert_eq!(&config.host, value.as_str().unwrap(), "{id}: host"),
            "port" => assert_eq!(config.port as u64, value.as_u64().unwrap(), "{id}: port"),
            "database" => assert_eq!(&config.database, value.as_str().unwrap(), "{id}: database"),
            "user" => assert_eq!(&config.user, value.as_str().unwrap(), "{id}: user"),
            "password" => assert_eq!(&config.password, value.as_str().unwrap(), "{id}: password"),
            "use_ssl" => assert_eq!(config.use_ssl, value.as_bool().unwrap(), "{id}: use_ssl"),
            "timeout_ms" => assert_eq!(
                config.timeout_ms,
                value.as_i64().unwrap(),
                "{id}: timeout_ms"
            ),
            "block_size" => assert_eq!(
                config.block_size,
                value.as_u64().unwrap() as usize,
                "{id}: block_size"
            ),
            "security_mechanism" => assert_eq!(
                config.security_mechanism,
                value.as_u64().unwrap() as u16,
                "{id}: security_mechanism"
            ),
            "current_schema" => assert_eq!(
                &config.current_schema,
                value.as_str().unwrap(),
                "{id}: current_schema"
            ),
            "ssl_root_ca_path" => assert_eq!(
                &config.ssl_root_ca_path,
                value.as_str().unwrap(),
                "{id}: ssl_root_ca_path"
            ),
            "kerberos_spn" => assert_eq!(
                &config.kerberos_spn,
                value.as_str().unwrap(),
                "{id}: kerberos_spn"
            ),
            "kerberos_realm" => assert_eq!(
                &config.kerberos_realm,
                value.as_str().unwrap(),
                "{id}: kerberos_realm"
            ),
            "krb5_principal" => assert_eq!(
                &config.krb5_principal,
                value.as_str().unwrap(),
                "{id}: krb5_principal"
            ),
            "krb5_config_file" => assert_eq!(
                &config.krb5_config_file,
                value.as_str().unwrap(),
                "{id}: krb5_config_file"
            ),
            "krb5_keytab_file" => assert_eq!(
                &config.krb5_keytab_file,
                value.as_str().unwrap(),
                "{id}: krb5_keytab_file"
            ),
            "krb5_ccache_file" => assert_eq!(
                &config.krb5_ccache_file,
                value.as_str().unwrap(),
                "{id}: krb5_ccache_file"
            ),
            "gssapi_token_file" => assert_eq!(
                &config.gssapi_token_file,
                value.as_str().unwrap(),
                "{id}: gssapi_token_file"
            ),
            "client_appl_name" => assert_eq!(
                &config.client_appl_name,
                value.as_str().unwrap(),
                "{id}: client_appl_name"
            ),
            "client_corr_token" => assert_eq!(
                &config.client_corr_token,
                value.as_str().unwrap(),
                "{id}: client_corr_token"
            ),
            "params" => {
                for (key, expected) in value.as_object().unwrap() {
                    assert_eq!(
                        config.params.get(key).map(String::as_str),
                        expected.as_str(),
                        "{id}: params.{key}"
                    );
                }
            }
            other => panic!("{id}: unmapped DSN field {other}"),
        }
    }
}
