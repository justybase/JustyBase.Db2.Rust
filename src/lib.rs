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
