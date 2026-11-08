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

pub use config::{Config, ConfigError, ProtocolLimits, parse_dsn, redact_dsn};
pub use error::{Db2BatchError, Db2Error, Db2ErrorKind, Db2OperationOutcome, Db2Warning};
pub use import::{
    Db2ImportColumn, Db2ImportError, Db2ImportOptions, Db2ImportPolicy, Db2ImportProgress,
    Db2ImportTarget,
};
pub use network::pool::{Db2Pool, Db2PoolOptions, Db2PooledSession, PoolResetPolicy};
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
