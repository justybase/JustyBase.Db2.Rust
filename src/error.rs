use std::fmt;

/// A non-error diagnostic returned by Db2 with a successful command or result row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Db2Warning {
    pub sql_code: i32,
    pub sql_state: String,
    pub message: String,
    pub warning_flags: String,
}

/// Stable, machine-readable category for a Db2 driver error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Db2ErrorKind {
    /// A DSN or connection/pool configuration value is invalid.
    InvalidConfiguration,
    /// The session or its owning pool has been closed or discarded.
    ConnectionClosed,
    /// Db2 rejected the requested relational database during connection setup.
    DatabaseNotFound,
    /// Db2 rejected the supplied authentication material.
    AuthenticationFailed,
    /// Db2 denied authorization during connection setup or an operation.
    AuthorizationFailed,
    /// A client-side parameter value cannot be encoded for the described Db2 type.
    ParameterConversion,
    /// Db2 returned a SQL error with a parsed SQLCODE and SQLSTATE.
    SqlServer,
    /// A Db2 server error reply did not contain a complete SQLCODE/SQLSTATE pair.
    ServerResponse,
    /// I/O with the Db2 endpoint failed.
    Transport,
    /// A DRDA/protocol payload could not be parsed or was internally inconsistent.
    Protocol,
    /// A configured bound prevented the driver from materializing server data.
    ResourceLimit,
    /// The requested feature or operation is not implemented for the active profile.
    Unsupported,
    /// A driver error that does not fit another stable category.
    PoolTimeout,
    PoolExhausted,
    Other,
}

/// Whether the failing operation reached a confirmed server result. This is not a retry policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Db2OperationOutcome {
    NotStarted,
    Confirmed,
    Unknown,
}

/// Structured error returned by the Db2 driver.
///
/// `Display` retains a human-readable message. SQL errors expose their numeric code and
/// five-character state separately so callers do not need to parse that message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Db2Error {
    pub kind: Db2ErrorKind,
    pub outcome: Db2OperationOutcome,
    pub message: String,
    pub sql_code: Option<i32>,
    pub sql_state: Option<String>,
    /// Cleanup context attached when session reset fails after the operation error.
    /// The operation error is always preserved; uncertain DML is never retried.
    pub cleanup: Option<Box<Db2Error>>,
}

/// A batch-level error retaining how much work completed before the failing set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Db2BatchError {
    /// Original protocol, conversion, or server failure.
    pub cause: Db2Error,
    /// Zero-based failed parameter set, or `None` when setup failed before execution.
    pub failed_parameter_set_index: Option<usize>,
    /// Number of parameter sets acknowledged as successfully executed.
    pub completed_parameter_sets: usize,
    /// Rows affected by acknowledged successful sets only.
    pub rows_affected: i64,
}

impl Db2BatchError {
    pub(crate) fn new(
        cause: Db2Error,
        failed_parameter_set_index: Option<usize>,
        completed_parameter_sets: usize,
        rows_affected: i64,
    ) -> Self {
        Self {
            cause,
            failed_parameter_set_index,
            completed_parameter_sets,
            rows_affected,
        }
    }
}

impl fmt::Display for Db2BatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(index) = self.failed_parameter_set_index {
            write!(
                f,
                "DB2 batch parameter set {index} failed after {} completed sets and {} affected rows: {}",
                self.completed_parameter_sets, self.rows_affected, self.cause
            )
        } else {
            write!(
                f,
                "DB2 batch setup failed before a parameter set completed: {}",
                self.cause
            )
        }
    }
}

impl std::error::Error for Db2BatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

impl From<Db2Error> for Db2BatchError {
    fn from(cause: Db2Error) -> Self {
        Self::new(cause, None, 0, 0)
    }
}

impl From<Db2BatchError> for Db2Error {
    fn from(error: Db2BatchError) -> Self {
        error.cause
    }
}

impl Db2Error {
    /// Creates an error with an explicit stable category and message.
    pub fn new(kind: Db2ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            outcome: match kind {
                Db2ErrorKind::InvalidConfiguration
                | Db2ErrorKind::PoolTimeout
                | Db2ErrorKind::PoolExhausted => Db2OperationOutcome::NotStarted,
                Db2ErrorKind::SqlServer => Db2OperationOutcome::Confirmed,
                _ => Db2OperationOutcome::Unknown,
            },
            message: message.into(),
            sql_code: None,
            sql_state: None,
            cleanup: None,
        }
    }

    /// Attaches session-cleanup context without replacing the operation error.
    pub fn with_cleanup(mut self, cleanup: Db2Error) -> Self {
        if self.cleanup.is_none() {
            self.cleanup = Some(Box::new(cleanup));
        }
        self
    }

    /// Creates an unclassified application/driver error.
    pub fn other(message: String) -> Self {
        Self::new(Db2ErrorKind::Other, message)
    }

    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::new(Db2ErrorKind::Protocol, message)
    }

    pub(crate) fn resource_limit(message: impl Into<String>) -> Self {
        Self::new(Db2ErrorKind::ResourceLimit, message)
    }

    pub(crate) fn connection_closed(message: impl Into<String>) -> Self {
        Self::new(Db2ErrorKind::ConnectionClosed, message)
    }

    pub(crate) fn sql(code: i32, state: &str, message: &str) -> Self {
        let mut error = Self::new(
            Db2ErrorKind::SqlServer,
            format!(
                "DB2 SQLCODE={code} SQLSTATE={state}{}",
                if message.is_empty() {
                    String::new()
                } else {
                    format!(" {message}")
                }
            ),
        );
        error.sql_code = Some(code);
        error.sql_state = Some(state.to_owned());
        error
    }

    pub(crate) fn server_diagnostic(message: String) -> Self {
        let sql_code = diagnostic_integer(&message, &["SQLCODE:", "SQLCODE="]);
        let sql_state = diagnostic_string(&message, &["SQLSTATE:", "SQLSTATE="])
            .filter(|value| value.len() == 5);
        let kind = if sql_code.is_some() && sql_state.is_some() {
            Db2ErrorKind::SqlServer
        } else {
            Db2ErrorKind::ServerResponse
        };
        Self {
            kind,
            outcome: if sql_code.is_some() {
                Db2OperationOutcome::Confirmed
            } else {
                Db2OperationOutcome::Unknown
            },
            message,
            sql_code,
            sql_state,
            cleanup: None,
        }
    }
}

fn diagnostic_value<'a>(message: &'a str, markers: &[&str]) -> Option<&'a str> {
    markers.iter().find_map(|marker| {
        message
            .find(marker)
            .map(|index| &message[index + marker.len()..])
            .map(|value| value.split_whitespace().next().unwrap_or_default())
            .map(|value| value.trim_matches(|character: char| matches!(character, ',' | ';' | ')')))
            .filter(|value| !value.is_empty())
    })
}

fn diagnostic_integer(message: &str, markers: &[&str]) -> Option<i32> {
    diagnostic_value(message, markers)?.parse().ok()
}

fn diagnostic_string(message: &str, markers: &[&str]) -> Option<String> {
    diagnostic_value(message, markers).map(str::to_owned)
}

impl fmt::Display for Db2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)?;
        if let Some(cleanup) = &self.cleanup {
            write!(f, "; cleanup: {cleanup}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Db2Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cleanup
            .as_deref()
            .map(|cleanup| cleanup as &(dyn std::error::Error + 'static))
    }
}

impl From<String> for Db2Error {
    fn from(value: String) -> Self {
        Self::protocol(value)
    }
}

impl From<&str> for Db2Error {
    fn from(value: &str) -> Self {
        Self::protocol(value)
    }
}

impl From<crate::config::ConfigError> for Db2Error {
    fn from(value: crate::config::ConfigError) -> Self {
        Self::new(Db2ErrorKind::InvalidConfiguration, value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_error_has_structured_code_and_state_and_stable_display() {
        let error = Db2Error::sql(-952, "57014", "statement was interrupted");

        assert_eq!(error.kind, Db2ErrorKind::SqlServer);
        assert_eq!(error.sql_code, Some(-952));
        assert_eq!(error.sql_state.as_deref(), Some("57014"));
        assert_eq!(
            error.to_string(),
            "DB2 SQLCODE=-952 SQLSTATE=57014 statement was interrupted"
        );
    }

    #[test]
    fn server_diagnostic_extracts_sql_fields_without_rewriting_display() {
        let text = "SQLERRP:SQLRI2D0 SQLCODE:-204 SQLSTATE:42704 object not found";
        let error = Db2Error::server_diagnostic(text.into());

        assert_eq!(error.kind, Db2ErrorKind::SqlServer);
        assert_eq!(error.sql_code, Some(-204));
        assert_eq!(error.sql_state.as_deref(), Some("42704"));
        assert_eq!(error.to_string(), text);
    }

    #[test]
    fn incomplete_server_diagnostic_remains_classified_as_server_response() {
        let error = Db2Error::server_diagnostic("SQLERRP:SQLRI2D0".into());
        assert_eq!(error.kind, Db2ErrorKind::ServerResponse);
        assert_eq!(error.sql_code, None);
        assert_eq!(error.sql_state, None);
    }

    #[test]
    fn cleanup_error_is_available_without_replacing_operation_error() {
        let operation = Db2Error::sql(-204, "42704", "missing table");
        let cleanup = Db2Error::sql(-911, "40001", "rollback failed");
        let error = operation.with_cleanup(cleanup.clone());

        assert_eq!(error.kind, Db2ErrorKind::SqlServer);
        assert_eq!(error.sql_code, Some(-204));
        assert_eq!(error.sql_state.as_deref(), Some("42704"));
        assert!(error.to_string().contains("missing table"));
        assert!(error.to_string().contains("rollback failed"));
        assert_eq!(
            std::error::Error::source(&error).and_then(|source| source.downcast_ref::<Db2Error>()),
            Some(&cleanup)
        );
    }

    #[test]
    fn config_error_conversion_preserves_display_and_classifies_without_echoing_dsn() {
        let config_error =
            crate::config::parse_dsn("db2://alice:private@host:70000/DB").unwrap_err();
        let config_display = config_error.to_string();
        assert_eq!(config_error.kind(), Db2ErrorKind::InvalidConfiguration);

        let error = Db2Error::from(config_error);
        assert_eq!(error.kind, Db2ErrorKind::InvalidConfiguration);
        assert_eq!(error.to_string(), config_display);
        assert!(!error.to_string().contains("private"));
    }
}
