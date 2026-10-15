use crate::error::Db2Error;
use crate::network::session::Db2Value;
use std::collections::HashSet;
use std::fmt;

/// One explicitly selected destination column. `expected_sql_type`, when supplied, is
/// checked against the destination parameter descriptor returned by DB2 during prepare.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Db2ImportColumn {
    pub name: String,
    pub expected_sql_type: Option<u16>,
}

impl Db2ImportColumn {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            expected_sql_type: None,
        }
    }

    pub fn with_sql_type(mut self, sql_type: u16) -> Self {
        self.expected_sql_type = Some(sql_type);
        self
    }
}

/// Explicit, quoted DB2 destination and column mapping for row imports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Db2ImportTarget {
    pub schema: Option<String>,
    pub table: String,
    pub columns: Vec<Db2ImportColumn>,
    insert_sql: String,
}

impl Db2ImportTarget {
    pub fn new(
        schema: Option<&str>,
        table: &str,
        columns: Vec<Db2ImportColumn>,
    ) -> Result<Self, Db2Error> {
        if columns.is_empty() {
            return Err(Db2Error::other(
                "DB2 import target must name at least one destination column".into(),
            ));
        }
        let mut names = HashSet::with_capacity(columns.len());
        for column in &columns {
            if column.name.is_empty() || column.name.len() > 128 || column.name.contains('\0') {
                return Err(Db2Error::other(format!(
                    "invalid DB2 import column identifier {:?}",
                    column.name
                )));
            }
            if !names.insert(column.name.clone()) {
                return Err(Db2Error::other(format!(
                    "duplicate DB2 import column identifier {:?}",
                    column.name
                )));
            }
        }
        let table = table.to_owned();
        let qualified_table = match schema {
            Some(schema) => format!(
                "{}.{}",
                quote_identifier(schema)?,
                quote_identifier(&table)?
            ),
            None => quote_identifier(&table)?,
        };
        let column_sql = columns
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        let markers = vec!["?"; columns.len()].join(", ");
        let insert_sql = format!("INSERT INTO {qualified_table} ({column_sql}) VALUES ({markers})");
        Ok(Self {
            schema: schema.map(str::to_owned),
            table,
            columns,
            insert_sql,
        })
    }

    pub fn insert_sql(&self) -> &str {
        &self.insert_sql
    }

    pub(crate) fn qualified_table(&self) -> String {
        // Identifiers are validated in `new`, so quoting cannot fail here.
        let table =
            quote_identifier(&self.table).expect("import table is validated at construction");
        match &self.schema {
            Some(schema) => format!(
                "{}.{}",
                quote_identifier(schema).expect("import schema is validated at construction"),
                table
            ),
            None => table,
        }
    }
}

/// Transaction ownership and in-memory batch limits for a row import.
///
/// `max_batch_bytes` is a soft batch boundary: one row may exceed it and is then sent alone,
/// matching the C# importer. It is not a strict cap on an individual row or wire allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Db2ImportPolicy {
    /// All rows are written in one importer-owned transaction and become visible together.
    Atomic {
        max_batch_rows: usize,
        max_batch_bytes: usize,
    },
    /// Each batch is committed by the importer after at most `rows` source rows.
    CommitEveryRows { rows: usize, max_batch_bytes: usize },
}

/// Import controls beyond [`Db2ImportPolicy`]: progress reporting, table locking,
/// and the per-batch write deadline.
///
/// `notify_after_rows`, when `Some(interval)`, makes the importer invoke the progress
/// observer each time the number of source rows read reaches a multiple of `interval`,
/// in addition to the pre-commit gate. An observer error aborts the import without
/// retry: the atomic transaction rolls back, while `CommitEveryRows` keeps the last
/// confirmed checkpoint. `None` disables periodic reports, so the observer runs only
/// immediately before each commit.
///
/// `table_lock` issues `LOCK TABLE <target> IN EXCLUSIVE MODE` inside the
/// importer-owned transaction before the first write. It requires the atomic policy:
/// combined with `CommitEveryRows` it fails during option validation, before any I/O.
///
/// `command_timeout_ms` overrides the session default command deadline for each batch
/// write. Zero keeps the session default; a positive value is forwarded to the SQLINTR
/// deadline path, matching the C# importer's per-INSERT timeout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Db2ImportOptions {
    pub policy: Db2ImportPolicy,
    pub notify_after_rows: Option<u64>,
    pub table_lock: bool,
    pub command_timeout_ms: u64,
}

impl Default for Db2ImportOptions {
    fn default() -> Self {
        Self {
            policy: Db2ImportPolicy::Atomic {
                max_batch_rows: 256,
                max_batch_bytes: 8 * 1024 * 1024,
            },
            notify_after_rows: None,
            table_lock: false,
            command_timeout_ms: 0,
        }
    }
}

impl Db2ImportOptions {
    pub fn validate(self) -> Result<Self, Db2Error> {
        self.policy.limits()?;
        if self.notify_after_rows == Some(0) {
            return Err(Db2Error::other(
                "DB2 import notify row interval must be greater than zero".into(),
            ));
        }
        if self.table_lock && matches!(self.policy, Db2ImportPolicy::CommitEveryRows { .. }) {
            return Err(Db2Error::other(
                "DB2 import table lock requires one transaction for the duration of the import; CommitEveryRows must not be used".into(),
            ));
        }
        Ok(self)
    }
}

impl Db2ImportPolicy {
    pub(crate) fn limits(self) -> Result<(usize, usize, bool), Db2Error> {
        let (rows, bytes, commit_each_batch) = match self {
            Self::Atomic {
                max_batch_rows,
                max_batch_bytes,
            } => (max_batch_rows, max_batch_bytes, false),
            Self::CommitEveryRows {
                rows,
                max_batch_bytes,
            } => (rows, max_batch_bytes, true),
        };
        if rows == 0 || bytes == 0 {
            return Err(Db2Error::other(
                "DB2 import batch row and byte limits must be greater than zero".into(),
            ));
        }
        Ok((rows, bytes, commit_each_batch))
    }
}

/// Import progress. `committed_rows` counts source rows in transactions confirmed committed
/// by DB2; it is not a claim about DB2's affected-row count for triggers or generated work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Db2ImportProgress {
    /// Number of source rows pulled, including rows staged but not committed.
    pub source_rows_read: u64,
    /// Source rows whose importer-owned transactions are confirmed committed.
    pub committed_rows: u64,
    /// Number of importer-owned transactions confirmed committed.
    pub committed_batches: u64,
    /// Estimated encoded bytes in confirmed committed source rows.
    pub estimated_committed_bytes: u64,
}

/// Import failure with the original DB2/source cause and confirmed progress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Db2ImportError {
    pub cause: Db2Error,
    /// One-based absolute source row for a row-specific read, validation, or write error.
    /// `None` means setup or transaction completion failed outside a specific row.
    pub failed_source_row: Option<u64>,
    pub progress: Db2ImportProgress,
}

impl Db2ImportError {
    pub(crate) fn new(
        cause: Db2Error,
        failed_source_row: Option<u64>,
        progress: Db2ImportProgress,
    ) -> Self {
        Self {
            cause,
            failed_source_row,
            progress,
        }
    }
}

impl fmt::Display for Db2ImportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(row) = self.failed_source_row {
            write!(
                formatter,
                "DB2 row import failed at source row {row} after {} rows were confirmed committed: {}",
                self.progress.committed_rows, self.cause
            )
        } else {
            write!(
                formatter,
                "DB2 row import failed after {} rows were confirmed committed: {}",
                self.progress.committed_rows, self.cause
            )
        }
    }
}

impl std::error::Error for Db2ImportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

pub(crate) fn encoded_row_size_estimate(row: &[Db2Value]) -> Result<usize, Db2Error> {
    row.iter().try_fold(0usize, |size, value| {
        let value_size = match value {
            Db2Value::Null => 2,
            Db2Value::Boolean(_) => 3,
            Db2Value::Integer(_) | Db2Value::Float(_) => 10,
            Db2Value::Decimal(value) | Db2Value::Text(value) => value.len().saturating_add(8),
            Db2Value::Bytes(value) => value.len().saturating_add(8),
            Db2Value::Lob(_) => 32,
            Db2Value::ProgressiveLobReference { .. } => {
                return Err(Db2Error::other(
                    "DB2 import received an unresolved progressive LOB reference; read the row through Db2RowReader::next"
                        .into(),
                ));
            }
        };
        Ok(size.saturating_add(value_size))
    })
}

pub(crate) struct ImportBatch {
    max_rows: usize,
    max_bytes: usize,
    rows: Vec<(u64, Vec<Db2Value>)>,
    estimated_bytes: usize,
}

impl ImportBatch {
    pub(crate) fn new(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            max_rows,
            max_bytes,
            rows: Vec::with_capacity(max_rows.min(1024)),
            estimated_bytes: 0,
        }
    }

    pub(crate) fn should_flush_before(&self, row_bytes: usize) -> bool {
        !self.rows.is_empty()
            && (self.rows.len() >= self.max_rows
                || self.estimated_bytes.saturating_add(row_bytes) > self.max_bytes)
    }

    pub(crate) fn push(&mut self, absolute_row: u64, row: Vec<Db2Value>, bytes: usize) {
        self.estimated_bytes = self.estimated_bytes.saturating_add(bytes);
        self.rows.push((absolute_row, row));
    }

    pub(crate) fn is_full(&self) -> bool {
        self.rows.len() >= self.max_rows || self.estimated_bytes >= self.max_bytes
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    pub(crate) fn take(&mut self) -> Vec<(u64, Vec<Db2Value>)> {
        self.estimated_bytes = 0;
        std::mem::take(&mut self.rows)
    }
}

fn quote_identifier(value: &str) -> Result<String, Db2Error> {
    if value.is_empty() || value.len() > 128 || value.contains('\0') {
        return Err(Db2Error::other(format!(
            "invalid DB2 import identifier {value:?}"
        )));
    }
    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_target_quotes_identifiers_and_builds_explicit_insert() {
        let target = Db2ImportTarget::new(
            Some("Mixed Schema"),
            "Table\"One",
            vec![Db2ImportColumn::new("Id"), Db2ImportColumn::new("Value")],
        )
        .unwrap();
        assert_eq!(
            target.insert_sql(),
            "INSERT INTO \"Mixed Schema\".\"Table\"\"One\" (\"Id\", \"Value\") VALUES (?, ?)"
        );
    }

    #[test]
    fn import_target_rejects_empty_or_duplicate_columns_and_nul_identifiers() {
        assert!(Db2ImportTarget::new(None, "T", Vec::new()).is_err());
        assert!(
            Db2ImportTarget::new(
                None,
                "T",
                vec![Db2ImportColumn::new("A"), Db2ImportColumn::new("A")]
            )
            .is_err()
        );
        assert!(Db2ImportTarget::new(Some("S\0"), "T", vec![Db2ImportColumn::new("A")]).is_err());
    }

    #[test]
    fn import_limits_and_encoded_byte_estimates_are_bounded_and_account_for_locators() {
        assert!(
            Db2ImportPolicy::Atomic {
                max_batch_rows: 0,
                max_batch_bytes: 100,
            }
            .limits()
            .is_err()
        );
        assert_eq!(
            encoded_row_size_estimate(&[
                Db2Value::Integer(1),
                Db2Value::Text("hello".into()),
                Db2Value::Bytes(vec![1, 2, 3]),
            ])
            .unwrap(),
            10 + 13 + 11
        );
        assert!(
            encoded_row_size_estimate(&[Db2Value::ProgressiveLobReference {
                length: 100,
                reference: 1,
            }])
            .is_err()
        );
    }

    #[test]
    fn import_options_validate_notify_interval_and_table_lock_policy() {
        assert!(Db2ImportOptions::default().validate().is_ok());
        assert!(
            Db2ImportOptions {
                notify_after_rows: Some(0),
                ..Db2ImportOptions::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            Db2ImportOptions {
                policy: Db2ImportPolicy::CommitEveryRows {
                    rows: 10,
                    max_batch_bytes: 100,
                },
                table_lock: true,
                ..Db2ImportOptions::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            Db2ImportOptions {
                table_lock: true,
                ..Db2ImportOptions::default()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn import_target_reports_qualified_table_name() {
        let target = Db2ImportTarget::new(Some("S"), "T", vec![Db2ImportColumn::new("A")]).unwrap();
        assert_eq!(target.qualified_table(), "\"S\".\"T\"");
        let unqualified = Db2ImportTarget::new(None, "T", vec![Db2ImportColumn::new("A")]).unwrap();
        assert_eq!(unqualified.qualified_table(), "\"T\"");
    }

    #[test]
    fn import_batch_flushes_at_row_or_estimated_byte_cap() {
        let mut batch = ImportBatch::new(2, 32);
        assert!(!batch.should_flush_before(10));
        batch.push(1, vec![Db2Value::Integer(1)], 10);
        assert!(!batch.should_flush_before(20));
        batch.push(2, vec![Db2Value::Integer(2)], 20);
        assert!(batch.is_full());
        assert!(batch.should_flush_before(1));
        assert_eq!(batch.estimated_bytes(), 30);
        assert_eq!(batch.take().len(), 2);
        assert!(batch.is_empty());
        assert_eq!(batch.estimated_bytes(), 0);

        batch.push(3, vec![Db2Value::Integer(3)], 20);
        assert!(batch.should_flush_before(13));
    }
}
