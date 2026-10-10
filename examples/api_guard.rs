//! Compile-time guard for the public root re-exports of `justybase-db2`.
//!
//! This example exists so removals or renames in `src/lib.rs` break the build
//! in CI (`cargo build --examples`) even when the item still exists at its
//! canonical module path. It performs no I/O.
#![allow(unused_imports)]

use justybase_db2::{
    Config, ConfigError, Db2BatchError, Db2BatchOptions, Db2ClientInfo, Db2CommandContext,
    Db2CommandOptions, Db2Error, Db2ErrorKind, Db2ExecutionResult, Db2ImportColumn, Db2ImportError,
    Db2ImportOptions, Db2ImportPolicy, Db2ImportProgress, Db2ImportTarget, Db2InputParameter,
    Db2InsertResult, Db2InterruptHandle, Db2IsolationLevel, Db2LobEncoding, Db2LobReader,
    Db2LobValue, Db2Parameter, Db2ParameterDirection, Db2Pool, Db2PooledSession,
    Db2ProcedureReader, Db2ProcedureResult, Db2QueryResult, Db2ResultSet, Db2RowReader, Db2RowRef,
    Db2Session, Db2Statement, Db2Transaction, Db2Value, Db2ValueRef, Db2Warning, PoolResetPolicy,
    ProtocolLimits, parse_dsn, redact_dsn,
};

fn main() {}
