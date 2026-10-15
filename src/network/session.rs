use crate::admin::{ADMIN_COMMAND_CALL_SQL, normalize_admin_command};
use crate::config::{Config, parse_dsn};
use crate::converters::decfloat::decode_dfp;
use crate::converters::ebcdic::{decode_cp500, encode_cp500};
use crate::converters::sql_dta::{EncodedExternalData, build_sql_dta_with_inputs};
pub use crate::error::{Db2BatchError, Db2Error, Db2ErrorKind};
use crate::import::{
    Db2ImportError, Db2ImportOptions, Db2ImportPolicy, Db2ImportProgress, Db2ImportTarget,
    ImportBatch, encoded_row_size_estimate,
};
use crate::network::codepoint::{self as cp, SecurityMechanism};
use crate::network::ddm::{
    self, ColumnDescription, FieldDescriptor, ManagerLevel, ResultSetPackageSection,
};
use crate::network::dss::{self, DssPacket};
use crate::network::stream::Db2DssStream;
use crate::resources::MAX_MATERIALIZED_RESULT_BYTES;
use rustls_native_certs::load_native_certs;
use std::collections::VecDeque;
use std::fs::File;
use std::future::Future;
use std::io::{BufReader, SeekFrom};
use std::sync::Arc;
#[cfg(feature = "bench-internals")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;
use tempfile::{NamedTempFile, TempPath};
use tokio::io::{AsyncRead, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
};

trait AsyncStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> AsyncStream for T {}
type BoxStream = Box<dyn AsyncStream>;

const PACKAGE_ID: &str = "SYSSH200";
const CONSISTENCY_TOKEN: &str = "SYSLVL01";
const SECTION_NUMBER: u16 = 65;
const BATCH_PIPELINE_DEFAULT_MAX_ROWS: usize = 256;
const BATCH_PIPELINE_DEFAULT_MAX_BYTES: usize = 4 * 1024 * 1024;
const INTERRUPT_ACTIVE: u8 = 0b01;
const INTERRUPT_PENDING: u8 = 0b10;

// Shared ownership only around synchronous poll calls lets pool shutdown drop the
// transport even while a lease is retained. No async mutex is held across await.
#[derive(Clone)]
pub(crate) struct PoolAbort(Arc<std::sync::Mutex<PoolTransportState>>);
struct PoolTransportState {
    stream: Option<BoxStream>,
    read_waker: Option<std::task::Waker>,
    write_waker: Option<std::task::Waker>,
}
struct PoolTransport(PoolAbort);
#[cfg(test)]
mod pool_transport_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn forced_shutdown_wakes_a_pending_read_and_closes_transport() {
        let (client, mut peer) = tokio::io::duplex(16);
        let abort = PoolAbort(Arc::new(std::sync::Mutex::new(PoolTransportState {
            stream: Some(Box::new(client)),
            read_waker: None,
            write_waker: None,
        })));
        let mut transport = PoolTransport(abort.clone());
        let waiting = tokio::spawn(async move {
            let mut byte = [0];
            transport.read(&mut byte).await.unwrap_err().kind()
        });
        tokio::task::yield_now().await;
        abort.abort();
        abort.abort();
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(error, std::io::ErrorKind::ConnectionAborted);
        assert!(abort.is_closed());
        let mut byte = [0];
        assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn forced_shutdown_wakes_a_pending_write() {
        let (client, _peer) = tokio::io::duplex(1);
        let abort = PoolAbort(Arc::new(std::sync::Mutex::new(PoolTransportState {
            stream: Some(Box::new(client)),
            read_waker: None,
            write_waker: None,
        })));
        let mut transport = PoolTransport(abort.clone());
        let waiting =
            tokio::spawn(async move { transport.write_all(&[1, 2]).await.unwrap_err().kind() });
        tokio::task::yield_now().await;
        abort.abort();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
                .await
                .unwrap()
                .unwrap(),
            std::io::ErrorKind::ConnectionAborted
        );
    }
}
impl PoolAbort {
    pub(crate) fn id(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }
    pub(crate) fn abort(&self) {
        let (stream, read, write) = {
            let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
            (
                state.stream.take(),
                state.read_waker.take(),
                state.write_waker.take(),
            )
        };
        drop(stream);
        if let Some(waker) = read {
            waker.wake();
        }
        if let Some(waker) = write {
            waker.wake();
        }
    }
    fn is_closed(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .stream
            .is_none()
    }
}
impl AsyncRead for PoolTransport {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut state = self.0.0.lock().unwrap_or_else(|error| error.into_inner());
        state.read_waker = Some(cx.waker().clone());
        match state.stream.as_mut() {
            Some(stream) => std::pin::Pin::new(stream).poll_read(cx, buffer),
            None => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "Db2 pool transport closed",
            ))),
        }
    }
}
impl AsyncWrite for PoolTransport {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let mut state = self.0.0.lock().unwrap_or_else(|error| error.into_inner());
        state.write_waker = Some(cx.waker().clone());
        match state.stream.as_mut() {
            Some(stream) => std::pin::Pin::new(stream).poll_write(cx, buffer),
            None => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "Db2 pool transport closed",
            ))),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut state = self.0.0.lock().unwrap_or_else(|error| error.into_inner());
        state.write_waker = Some(cx.waker().clone());
        match state.stream.as_mut() {
            Some(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            None => std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "Db2 pool transport closed",
            ))),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut state = self.0.0.lock().unwrap_or_else(|error| error.into_inner());
        state.write_waker = Some(cx.waker().clone());
        match state.stream.as_mut() {
            Some(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            None => std::task::Poll::Ready(Ok(())),
        }
    }
}

/// Per-command timeout policy. `timeout_ms = 0` means that the command has no deadline;
/// pool acquisition and connection setup are governed independently by their own settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Db2CommandOptions {
    /// Budget for the active SQL command after a session/lease has been acquired.
    pub timeout_ms: u64,
    /// Maximum time allowed to connect the interrupt socket and receive SQLINTR acknowledgment.
    pub interrupt_ack_timeout_ms: u64,
    /// Maximum time after the SQLINTR acknowledgment to receive the original command reply.
    pub cleanup_timeout_ms: u64,
}

impl Db2CommandOptions {
    pub const fn new(timeout_ms: u64, cleanup_timeout_ms: u64) -> Self {
        Self {
            timeout_ms,
            interrupt_ack_timeout_ms: 30_000,
            cleanup_timeout_ms,
        }
    }

    pub const fn with_interrupt_ack_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.interrupt_ack_timeout_ms = timeout_ms;
        self
    }

    pub const fn unlimited() -> Self {
        Self {
            timeout_ms: 0,
            interrupt_ack_timeout_ms: 30_000,
            cleanup_timeout_ms: 30_000,
        }
    }
}

/// Per-command session context, mirroring go-db2's context `WithUser` and
/// `WithClientInfo` helpers: the target user and client-info registers are
/// applied immediately before the command on the same session.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Db2CommandContext {
    /// Target session user; `None` leaves the current identity unchanged.
    pub user: Option<String>,
    /// Client-info registers; empty fields keep their current value.
    pub client_info: Option<Db2ClientInfo>,
}

impl Db2CommandContext {
    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    pub fn with_client_info(mut self, info: Db2ClientInfo) -> Self {
        self.client_info = Some(info);
        self
    }
}

/// Bounds the number and encoded size of parameter sets sent in one DRDA reply chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Db2BatchOptions {
    pub max_rows: usize,
    pub max_encoded_bytes: usize,
}

impl Db2BatchOptions {
    pub const fn new(max_rows: usize, max_encoded_bytes: usize) -> Self {
        Self {
            max_rows,
            max_encoded_bytes,
        }
    }
}

impl Default for Db2BatchOptions {
    fn default() -> Self {
        Self::new(
            BATCH_PIPELINE_DEFAULT_MAX_ROWS,
            BATCH_PIPELINE_DEFAULT_MAX_BYTES,
        )
    }
}

/// Coordinates commands on the main connection with an interrupt sent over a second connection.
/// The pending bit prevents a late INTRDBRQS from racing with the next command.
#[derive(Default)]
struct InterruptCoordinator {
    state: AtomicU8,
    changed: Notify,
    exchange_pending: AtomicBool,
    interrupt_sent: AtomicBool,
    poisoned: AtomicBool,
    #[cfg(feature = "bench-internals")]
    request_batches_sent: AtomicU64,
}

impl InterruptCoordinator {
    async fn begin_operation(self: &Arc<Self>) -> OperationGuard {
        loop {
            let notified = self.changed.notified();
            if self
                .state
                .compare_exchange(0, INTERRUPT_ACTIVE, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return OperationGuard(self.clone());
            }
            notified.await;
        }
    }

    fn begin_interrupt(self: &Arc<Self>) -> Option<InterruptGuard> {
        loop {
            let state = self.state.load(Ordering::Acquire);
            if self.poisoned.load(Ordering::Acquire)
                || state & INTERRUPT_ACTIVE == 0
                || state & INTERRUPT_PENDING != 0
            {
                return None;
            }
            if self
                .state
                .compare_exchange(
                    state,
                    state | INTERRUPT_PENDING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Some(InterruptGuard(self.clone()));
            }
        }
    }
}

struct OperationGuard(Arc<InterruptCoordinator>);

impl Drop for OperationGuard {
    fn drop(&mut self) {
        // A request can be partially written or its reply only partially read.
        // Clearing ACTIVE alone would allow a later command to consume stale bytes.
        if self.0.exchange_pending.load(Ordering::Acquire) {
            self.0.poisoned.store(true, Ordering::Release);
        }
        self.0.state.fetch_and(!INTERRUPT_ACTIVE, Ordering::AcqRel);
        self.0.changed.notify_one();
    }
}

// Also guards individual IO futures: a row reader can outlive a dropped
// next() future, so its long-lived OperationGuard is not sufficient.
struct ExchangeIoGuard {
    coordinator: Arc<InterruptCoordinator>,
    completed: bool,
}
impl ExchangeIoGuard {
    fn new(coordinator: Arc<InterruptCoordinator>) -> Self {
        Self {
            coordinator,
            completed: false,
        }
    }
}
impl Drop for ExchangeIoGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.coordinator.poisoned.store(true, Ordering::Release);
        }
    }
}

struct InterruptGuard(Arc<InterruptCoordinator>);

impl Drop for InterruptGuard {
    fn drop(&mut self) {
        // A dropped interrupt may reach DB2 late, after the next operation starts.
        if self.0.interrupt_sent.load(Ordering::Acquire) {
            self.0.poisoned.store(true, Ordering::Release);
        }
        self.0.state.fetch_and(!INTERRUPT_PENDING, Ordering::AcqRel);
        self.0.changed.notify_one();
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Db2Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Decimal(String),
    Text(String),
    Bytes(Vec<u8>),
    /// One-shot locator exposed only by the pull-based row reader. Call
    /// `Db2RowReader::open_lob` while this row is current to read its bytes.
    Lob(Db2LobValue),
    #[doc(hidden)]
    ProgressiveLobReference {
        length: u64,
        reference: u64,
    },
}

/// A borrowed view of a decoded DB2 value. Text and binary variants do not clone their data.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Db2ValueRef<'a> {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Decimal(&'a str),
    Text(&'a str),
    Bytes(&'a [u8]),
    Lob(Db2LobValue),
    ProgressiveLobReference { length: u64, reference: u64 },
}

impl<'a> From<&'a Db2Value> for Db2ValueRef<'a> {
    fn from(value: &'a Db2Value) -> Self {
        match value {
            Db2Value::Null => Self::Null,
            Db2Value::Boolean(value) => Self::Boolean(*value),
            Db2Value::Integer(value) => Self::Integer(*value),
            Db2Value::Float(value) => Self::Float(*value),
            Db2Value::Decimal(value) => Self::Decimal(value),
            Db2Value::Text(value) => Self::Text(value),
            Db2Value::Bytes(value) => Self::Bytes(value),
            Db2Value::Lob(value) => Self::Lob(*value),
            Db2Value::ProgressiveLobReference { length, reference } => {
                Self::ProgressiveLobReference {
                    length: *length,
                    reference: *reference,
                }
            }
        }
    }
}

/// A borrowed row view. Its values are valid until the reader is advanced.
#[derive(Clone, Copy, Debug)]
pub struct Db2RowRef<'a> {
    values: &'a [Db2Value],
}

impl<'a> Db2RowRef<'a> {
    pub fn len(self) -> usize {
        self.values.len()
    }

    pub fn is_empty(self) -> bool {
        self.values.is_empty()
    }

    pub fn get(self, index: usize) -> Option<Db2ValueRef<'a>> {
        self.values.get(index).map(Db2ValueRef::from)
    }

    pub fn iter(self) -> impl ExactSizeIterator<Item = Db2ValueRef<'a>> + 'a {
        self.values.iter().map(Db2ValueRef::from)
    }
}

/// Encoding of the bytes returned by a progressive DB2 LOB locator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Db2LobEncoding {
    Binary,
    Utf8,
    Utf16Be,
}

/// Metadata for a row-stream progressive LOB. Its private reference is valid only on the
/// row where it was returned; use `Db2RowReader::open_lob` to obtain a scoped byte reader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Db2LobValue {
    /// Encoded byte length reported by DB2 (UTF-16BE bytes for DBCLOB).
    pub byte_length: u64,
    /// DRDA SQL type code for this LOB column.
    pub sql_type: u8,
    /// Encoding of the bytes returned by the locator reader.
    pub encoding: Db2LobEncoding,
    reference: u64,
    row_generation: u64,
}

#[derive(Clone, Copy, Debug)]
struct ActiveProgressiveLob {
    value: Db2LobValue,
    remaining: u64,
}

/// A positional input parameter for bounded-memory writes. `Value` borrows an ordinary typed
/// value; each stream variant borrows a forward-only reader for exactly `length` wire bytes.
pub enum Db2InputParameter<'a> {
    Value(&'a Db2Value),
    BlobStream {
        source: &'a mut (dyn AsyncRead + Unpin + Send),
        length: u64,
    },
    /// UTF-8 encoded CLOB bytes.
    ClobStream {
        source: &'a mut (dyn AsyncRead + Unpin + Send),
        length: u64,
    },
    /// UTF-8 encoded text supplied for a DBCLOB parameter. Db2 converts it to
    /// its double-byte character representation.
    DbClobStream {
        source: &'a mut (dyn AsyncRead + Unpin + Send),
        length: u64,
    },
    /// Serialized XML encoded as UTF-8 bytes.
    XmlStream {
        source: &'a mut (dyn AsyncRead + Unpin + Send),
        length: u64,
    },
}

fn value_parameter_inputs(values: &[Db2Value]) -> Vec<Db2InputParameter<'_>> {
    values.iter().map(Db2InputParameter::Value).collect()
}

#[derive(Clone, Debug, PartialEq)]
pub struct Db2QueryResult {
    pub columns: Vec<ColumnDescription>,
    pub rows: Vec<Vec<Db2Value>>,
    /// All dynamic cursor results returned by a stored procedure, in open order.
    pub result_sets: Vec<Db2ResultSet>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Db2ResultSet {
    pub columns: Vec<ColumnDescription>,
    pub rows: Vec<Vec<Db2Value>>,
}

struct ProcedureCursorState {
    section: ResultSetPackageSection,
    columns: Vec<ColumnDescription>,
    fields: Vec<FieldDescriptor>,
    row_data: ReadBuffer,
    extdta: VecDeque<Vec<u8>>,
    ended: bool,
    closed: bool,
    query_id: u64,
    query_id_present: bool,
    next_correlation: u16,
    buffered_rows: VecDeque<Vec<Db2Value>>,
    stalled_qrydta: Vec<u8>,
    stalled_qrydta_count: u8,
}

struct ProcedureCallResult {
    query_result: Db2QueryResult,
    output_parameters: Vec<Db2Value>,
    rows_affected: i64,
}

/// Terminal metadata for a streamed stored-procedure call.
#[derive(Clone, Debug, PartialEq)]
pub struct Db2ProcedureResult {
    pub rows_affected: i64,
    /// Output values become available only after every result cursor has been closed.
    pub output_parameters: Vec<Db2Value>,
}

/// Pull-based reader for dynamic result cursors returned from a stored procedure.
/// It exclusively borrows the session until `finish`, end-of-results, or a confirmed
/// server cancellation. Dropping it before terminal completion discards the session.
pub struct Db2ProcedureReader<'session> {
    session: &'session mut Db2Session,
    cursors: Vec<ProcedureCursorState>,
    current_cursor: usize,
    rows_affected: i64,
    output_parameters: Vec<Db2Value>,
    options: Db2CommandOptions,
    interrupt: Option<Db2InterruptHandle>,
    _operation: OperationGuard,
    completed: bool,
    cancelled: bool,
    terminal_result: Option<Db2ProcedureResult>,
}

#[derive(Default)]
struct ReadBuffer {
    data: Vec<u8>,
    start: usize,
}

impl ReadBuffer {
    fn as_slice(&self) -> &[u8] {
        &self.data[self.start..]
    }

    fn len(&self) -> usize {
        self.data.len() - self.start
    }

    fn is_empty(&self) -> bool {
        self.start == self.data.len()
    }

    fn clear(&mut self) {
        self.data.clear();
        self.start = 0;
    }

    fn append(&mut self, bytes: &[u8]) {
        self.compact_if_needed();
        self.data.extend_from_slice(bytes);
    }

    fn consume(&mut self, length: usize) {
        assert!(
            length <= self.len(),
            "read buffer consume exceeds active data"
        );
        self.start += length;
        self.compact_if_needed();
    }

    fn compact_if_needed(&mut self) {
        if self.start == self.data.len() {
            self.data.clear();
            self.start = 0;
        } else if self.start > 64 * 1024 && self.start > self.data.len() / 2 {
            self.data.copy_within(self.start.., 0);
            self.data.truncate(self.data.len() - self.start);
            self.start = 0;
        }
    }
}

struct QueryStreamState {
    columns: Vec<ColumnDescription>,
    fields: Vec<FieldDescriptor>,
    pending_row_data: ReadBuffer,
    extdta: VecDeque<Vec<u8>>,
    open: bool,
    ended: bool,
    failed: bool,
    query_id: u64,
    next_correlation: u16,
    current_row_generation: u64,
    current_row_lobs: VecDeque<Db2LobValue>,
    active_lob: Option<ActiveProgressiveLob>,
    max_materialized_lob_size: usize,
}

fn expose_row_lobs_in_state(state: &mut QueryStreamState, row: &mut [Db2Value]) {
    state.current_row_generation = state.current_row_generation.wrapping_add(1);
    state.current_row_lobs.clear();
    for (column, value) in row.iter_mut().enumerate() {
        let Db2Value::ProgressiveLobReference { length, reference } = value else {
            continue;
        };
        let sql_type = state.fields.get(column).map_or(0, |field| field.field_type);
        let encoding = if is_dbclob_type(sql_type) {
            Db2LobEncoding::Utf16Be
        } else if is_clob_type(sql_type) {
            Db2LobEncoding::Utf8
        } else {
            Db2LobEncoding::Binary
        };
        let lob = Db2LobValue {
            byte_length: *length,
            sql_type,
            encoding,
            reference: *reference,
            row_generation: state.current_row_generation,
        };
        state.current_row_lobs.push_back(lob);
        *value = Db2Value::Lob(lob);
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SessionDirtyState {
    schema: bool,
    isolation: bool,
    auth: bool,
    client_info: bool,
    unknown: bool,
}

impl SessionDirtyState {
    fn is_clean(self) -> bool {
        !self.schema && !self.isolation && !self.auth && !self.client_info && !self.unknown
    }
}

/// Pull-based row reader. It borrows its session until exhausted or closed.
pub struct Db2RowReader<'a> {
    session: &'a mut Db2Session,
    state: QueryStreamState,
    rows: RowBatchQueue,
    completed: bool,
    commit_after_query: bool,
    committed: bool,
    _operation: OperationGuard,
    options: Db2CommandOptions,
    interrupt: Option<Db2InterruptHandle>,
}

impl<'session> Db2RowReader<'session> {
    pub fn columns(&self) -> &[ColumnDescription] {
        &self.state.columns
    }

    pub async fn next(&mut self) -> Result<Option<Vec<Db2Value>>, Db2Error> {
        if !self.ensure_next_row().await? {
            return Ok(None);
        }
        let row = self
            .rows
            .pop_owned_row()
            .ok_or_else(|| Db2Error::protocol("query row batch lost its next decoded row"))?;
        Ok(Some(self.expose_row_lobs(row)))
    }

    async fn ensure_next_row(&mut self) -> Result<bool, Db2Error> {
        self.session.ensure_open()?;
        self.drain_current_lobs(None).await?;
        if self.rows.has_next() {
            return Ok(true);
        }
        while self.state.open && !self.state.ended {
            let prior_pending = self.state.pending_row_data.len();
            let prior_extdta = self.state.extdta.len();
            let query = ddm::pack_continue_query(
                PACKAGE_ID,
                CONSISTENCY_TOKEN,
                SECTION_NUMBER,
                &self.session.database,
                self.session.query_block_size,
                self.state.query_id,
            );
            let exchange = run_with_command_deadline(self.options, self.interrupt.clone(), async {
                self.session
                    .send(&query, false, true, Some(self.state.next_correlation))
                    .await?;
                self.session.read_reply_chain().await
            })
            .await;
            let replies = match exchange {
                Ok(replies) => replies,
                Err(error) => {
                    if is_confirmed_interrupt(&error) {
                        self.state.ended = true;
                        self.state.failed = true;
                    } else if self
                        .session
                        .interrupt_coordinator
                        .poisoned
                        .load(Ordering::Acquire)
                    {
                        self.session.closed = true;
                    }
                    return Err(error);
                }
            };
            let got_data = replies
                .iter()
                .any(|reply| matches!(reply.codepoint, cp::QRYDTA | cp::EXTDTA));
            let (rows, warnings) = match consume_stream_replies_flat(replies, &mut self.state) {
                Ok(rows) => rows,
                Err(error) => {
                    if !self.state.ended {
                        self.session.closed = true;
                    }
                    return Err(error);
                }
            };
            self.session.report_warnings(warnings);
            self.rows.push(rows);
            if !got_data
                && self.state.pending_row_data.len() == prior_pending
                && self.state.extdta.len() == prior_extdta
                && !self.state.ended
            {
                self.session.closed = true;
                return Err(Db2Error::other(
                    "DB2 CNTQRY returned neither query data nor an end-of-query reply".into(),
                ));
            }
            if self.rows.has_next() {
                return Ok(true);
            }
        }
        if self.state.ended {
            if !self.state.pending_row_data.is_empty() {
                self.session.closed = true;
                return Err(Db2Error::other(
                    "DB2 ended a streamed query in the middle of a row".into(),
                ));
            }
            if self.commit_after_query && !self.committed && !self.state.failed {
                let commit = ddm::pack_commit();
                let exchange =
                    run_with_command_deadline(self.options, self.interrupt.clone(), async {
                        self.session.correlation_id = 1;
                        self.session.send(&commit, false, true, None).await?;
                        self.session.read_reply_chain().await
                    })
                    .await;
                match exchange {
                    Ok(_) => self.committed = true,
                    Err(error) => {
                        if self
                            .session
                            .interrupt_coordinator
                            .poisoned
                            .load(Ordering::Acquire)
                        {
                            self.session.closed = true;
                        }
                        return Err(error);
                    }
                }
            }
            self.completed = true;
        }
        Ok(self.rows.has_next())
    }

    /// Returns a borrowed view of the next row. The view is tied to this mutable
    /// borrow and becomes unavailable when the reader advances.
    pub async fn next_ref(&mut self) -> Result<Option<Db2RowRef<'_>>, Db2Error> {
        if !self.ensure_next_row().await? {
            return Ok(None);
        }
        let values = self
            .rows
            .next_ref(&mut self.state)
            .ok_or_else(|| Db2Error::protocol("query row batch lost its next decoded row"))?;
        Ok(Some(Db2RowRef { values }))
    }

    /// Opens the next progressive LOB in this row. If earlier LOB values were skipped,
    /// they are drained first so DB2 locator requests remain in row and column order.
    pub async fn open_lob<'reader>(
        &'reader mut self,
        value: &Db2LobValue,
    ) -> Result<Db2LobReader<'reader, 'session>, Db2Error>
    where
        'session: 'reader,
    {
        if value.row_generation != self.state.current_row_generation {
            return Err(Db2Error::other(
                "DB2 LOB locator is no longer on the current row".into(),
            ));
        }
        self.drain_current_lobs(Some(value)).await?;
        let Some(next) = self.state.current_row_lobs.pop_front() else {
            return Err(Db2Error::other(
                "DB2 LOB locator has already been consumed or is not in the current row".into(),
            ));
        };
        if next != *value {
            return Err(Db2Error::other(
                "DB2 LOB locator is not available in the current row".into(),
            ));
        }
        if value.byte_length > 0 {
            self.state.active_lob = Some(ActiveProgressiveLob {
                value: *value,
                remaining: value.byte_length,
            });
        }
        Ok(Db2LobReader {
            reader: self,
            value: *value,
            completed: value.byte_length == 0,
        })
    }

    fn expose_row_lobs(&mut self, mut row: Vec<Db2Value>) -> Vec<Db2Value> {
        expose_row_lobs_in_state(&mut self.state, &mut row);
        row
    }

    async fn drain_current_lobs(&mut self, until: Option<&Db2LobValue>) -> Result<(), Db2Error> {
        // Fast path for the common row with no pending locator: skip constructing the
        // per-row tokio timeout that would otherwise be registered on every `next()`.
        if until.is_none()
            && self.state.active_lob.is_none()
            && self.state.current_row_lobs.is_empty()
        {
            return Ok(());
        }
        let cleanup = Duration::from_millis(self.options.cleanup_timeout_ms.max(1));
        let drain = async {
            loop {
                if let Some(active) = self.state.active_lob {
                    while self.state.active_lob.is_some() {
                        let _ = self.fetch_lob_chunk(&active.value).await?;
                    }
                    continue;
                }
                if let Some(target) = until
                    && self.state.current_row_lobs.front() == Some(target)
                {
                    return Ok(());
                }
                let Some(next) = self.state.current_row_lobs.pop_front() else {
                    if until.is_some() {
                        return Err(Db2Error::other(
                            "DB2 LOB locator has already been consumed or is not in the current row"
                                .into(),
                        ));
                    }
                    return Ok(());
                };
                if next.byte_length > 0 {
                    self.state.active_lob = Some(ActiveProgressiveLob {
                        remaining: next.byte_length,
                        value: next,
                    });
                }
            }
        };
        match timeout(cleanup, drain).await {
            Ok(result) => result,
            Err(_) => {
                self.session.closed = true;
                Err(Db2Error::other(
                    "DB2 progressive LOB cleanup exceeded its deadline".into(),
                ))
            }
        }
    }

    async fn fetch_lob_chunk(
        &mut self,
        value: &Db2LobValue,
    ) -> Result<Option<(Vec<u8>, bool)>, Db2Error> {
        let Some(active) = self.state.active_lob else {
            return Ok(None);
        };
        if active.value != *value {
            return Err(Db2Error::other(
                "DB2 progressive LOB reader no longer owns the active locator".into(),
            ));
        }
        let query_id = self.state.query_id;
        let correlation_id = self.state.next_correlation;
        let database = self.session.database.clone();
        let exchange = run_with_command_deadline(self.options, self.interrupt.clone(), async {
            let request = ddm::pack_get_next_chunk(
                PACKAGE_ID,
                CONSISTENCY_TOKEN,
                SECTION_NUMBER,
                &database,
                query_id,
                value.reference,
                65_536,
            );
            self.session
                .send(&request, false, true, Some(correlation_id))
                .await?;
            self.session.read_reply_chain().await
        })
        .await;
        let replies = match exchange {
            Ok(replies) => replies,
            Err(error) => {
                if is_confirmed_interrupt(&error) {
                    self.state.active_lob = None;
                    self.state.current_row_lobs.clear();
                    self.state.ended = true;
                    self.state.failed = true;
                } else {
                    // A failed GETNXTCHK leaves locator/cursor position uncertain, including
                    // ordinary peer resets. Only a server-confirmed SQLINTR proves that the
                    // original exchange ended cleanly enough to retain the session.
                    self.session.closed = true;
                    self.state.active_lob = None;
                    self.state.current_row_lobs.clear();
                    self.state.ended = true;
                    self.state.failed = true;
                }
                return Err(error);
            }
        };
        let parsed = (|| {
            for reply in &replies {
                if reply.codepoint == cp::SQLCARD {
                    let card = self
                        .session
                        .parse_sql_card_and_report_warning(&reply.payload)?;
                    if card.sql_code < 0 {
                        return Err(sql_error(card.sql_code, &card.sql_state, &card.message));
                    }
                } else if reply.codepoint == cp::SQLERRRM {
                    return Err(server_error(&reply.payload));
                }
            }
            let chunk = ddm::parse_progressive_chunk_reply(&replies).map_err(Db2Error::from)?;
            if chunk.data.len() as u64 > active.remaining {
                return Err(Db2Error::other(
                    "DB2 GETNXTCHK returned more bytes than the advertised LOB length".into(),
                ));
            }
            let remaining = active.remaining - chunk.data.len() as u64;
            let ended = chunk.end_of_reference || chunk.is_last_chunk || remaining == 0;
            if ended && remaining != 0 {
                return Err(Db2Error::other(
                    "DB2 ended a progressive LOB before its advertised length".into(),
                ));
            }
            if !ended && chunk.data.is_empty() {
                return Err(Db2Error::other(
                    "DB2 GETNXTCHK made no progress before the final chunk".into(),
                ));
            }
            Ok((chunk.data, remaining, ended))
        })();
        match parsed {
            Ok((data, remaining, ended)) => {
                if ended {
                    self.state.active_lob = None;
                } else if let Some(active) = &mut self.state.active_lob {
                    active.remaining = remaining;
                }
                Ok(Some((data, ended)))
            }
            Err(error) => {
                if is_confirmed_interrupt(&error) {
                    self.state.active_lob = None;
                    self.state.current_row_lobs.clear();
                    self.state.ended = true;
                    self.state.failed = true;
                } else {
                    self.session.closed = true;
                }
                Err(error)
            }
        }
    }

    /// Drains an unread query so the borrowed session can be safely reused.
    pub async fn close(mut self) -> Result<(), Db2Error> {
        while self.next().await?.is_some() {}
        self.completed = true;
        Ok(())
    }
}

/// Borrow-scoped reader for a progressive DB2 locator. Dropping it preserves unread
/// locator state; the next row advance or `Db2RowReader::close` drains that state.
pub struct Db2LobReader<'reader, 'session> {
    reader: &'reader mut Db2RowReader<'session>,
    value: Db2LobValue,
    completed: bool,
}

impl Db2LobReader<'_, '_> {
    pub fn byte_length(&self) -> u64 {
        self.value.byte_length
    }

    /// Returns at most 64 KiB of encoded bytes. `None` marks the end of this locator.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, Db2Error> {
        if self.completed {
            return Ok(None);
        }
        match self.reader.fetch_lob_chunk(&self.value).await? {
            Some((data, ended)) => {
                self.completed = ended;
                Ok(Some(data))
            }
            None => {
                self.completed = true;
                Ok(None)
            }
        }
    }

    /// Drains the remaining locator bytes before releasing the row-reader borrow.
    pub async fn close(mut self) -> Result<(), Db2Error> {
        while self.next_chunk().await?.is_some() {}
        Ok(())
    }
}

impl Db2ProcedureReader<'_> {
    /// Metadata for the current dynamic result cursor, or an empty slice after completion.
    pub fn columns(&self) -> &[ColumnDescription] {
        self.cursors
            .get(self.current_cursor)
            .map_or(&[], |cursor| &cursor.columns)
    }

    /// Current zero-based result-set index. It is `None` after terminal completion.
    pub fn result_set_index(&self) -> Option<usize> {
        (self.current_cursor < self.cursors.len()).then_some(self.current_cursor)
    }

    /// Output values are withheld until all returned cursors are closed and the call is terminal.
    pub fn output_parameters(&self) -> Option<&[Db2Value]> {
        self.terminal_result
            .as_ref()
            .map(|result| result.output_parameters.as_slice())
    }

    /// Pulls the next row from the current cursor. `None` means that cursor is closed; call
    /// `next_result_set()` to advance. Rows from later cursors are not fetched in advance.
    pub async fn next(&mut self) -> Result<Option<Vec<Db2Value>>, Db2Error> {
        if self.cancelled || self.current_cursor >= self.cursors.len() {
            self.finish_terminal().await?;
            return Ok(None);
        }
        loop {
            let index = self.current_cursor;
            let (pending, warnings) = {
                let cursor = &mut self.cursors[index];
                let max_lob_size = self
                    .session
                    .config
                    .protocol_limits
                    .max_materialized_lob_size;
                let warnings = decode_procedure_cursor_available(cursor, max_lob_size)?;
                let unresolved = cursor.buffered_rows.iter().any(|row| {
                    has_unresolved_external_locator(&cursor.fields, std::slice::from_ref(row))
                });
                if !cursor.buffered_rows.is_empty() && !unresolved {
                    (
                        Some(
                            std::mem::take(&mut cursor.buffered_rows)
                                .into_iter()
                                .collect::<Vec<_>>(),
                        ),
                        warnings,
                    )
                } else {
                    (None, warnings)
                }
            };
            self.session.report_warnings(warnings);
            if let Some(mut rows) = pending {
                let (query_id, correlation_id, fields) = {
                    let cursor = &self.cursors[index];
                    (
                        cursor.query_id,
                        cursor.next_correlation,
                        cursor.fields.clone(),
                    )
                };
                let resolved = self
                    .session
                    .resolve_progressive_lobs(&mut rows, &fields, query_id, correlation_id)
                    .await;
                if let Err(error) = resolved {
                    if is_confirmed_interrupt(&error) {
                        self.cancelled = true;
                        self.completed = true;
                        for cursor in &mut self.cursors {
                            cursor.ended = true;
                            cursor.closed = true;
                            cursor.row_data.clear();
                            cursor.extdta.clear();
                            cursor.buffered_rows.clear();
                        }
                    }
                    return Err(error);
                }
                self.cursors[index].buffered_rows.extend(rows);
                return Ok(self.cursors[index].buffered_rows.pop_front());
            }

            if self.cursors[index].ended {
                if !self.cursors[index].row_data.is_empty()
                    || !self.cursors[index].extdta.is_empty()
                    || !self.cursors[index].buffered_rows.is_empty()
                {
                    self.session.closed = true;
                    return Err(Db2Error::other(
                        "DB2 ended a procedure cursor with incomplete row or EXTDTA data".into(),
                    ));
                }
                self.close_current_cursor().await?;
                return Ok(None);
            }

            self.fetch_current_cursor_block().await?;
        }
    }

    /// Drains and closes the current cursor, then advances in server open order. Returns
    /// `false` only after all cursors have reached terminal completion and output values settle.
    pub async fn next_result_set(&mut self) -> Result<bool, Db2Error> {
        if self.cancelled || self.current_cursor >= self.cursors.len() {
            self.finish_terminal().await?;
            return Ok(false);
        }
        while self.next().await?.is_some() {}
        self.current_cursor += 1;
        if self.current_cursor >= self.cursors.len() {
            self.finish_terminal().await?;
            return Ok(false);
        }
        Ok(true)
    }

    /// Drains every remaining cursor and returns terminal rows-affected/output metadata.
    pub async fn finish(mut self) -> Result<Db2ProcedureResult, Db2Error> {
        while self.current_cursor < self.cursors.len() {
            while self.next().await?.is_some() {}
            self.current_cursor += 1;
        }
        self.finish_terminal().await?;
        self.completed = true;
        self.terminal_result.clone().ok_or_else(|| {
            Db2Error::other("DB2 procedure result did not reach terminal completion".into())
        })
    }

    /// Closes a partially consumed procedure call by draining every remaining result
    /// cursor, then returns terminal output parameters and affected-row metadata.
    pub async fn close(self) -> Result<Db2ProcedureResult, Db2Error> {
        self.finish().await
    }

    async fn fetch_current_cursor_block(&mut self) -> Result<(), Db2Error> {
        let index = self.current_cursor;
        let fetch = ddm::pack_continue_procedure_result_set(
            &self.cursors[index].section,
            self.session.query_block_size,
            self.cursors[index].query_id,
        )
        .map_err(Db2Error::from)?;
        let correlation = self.cursors[index].next_correlation;
        let result = run_with_command_deadline(self.options, self.interrupt.clone(), async {
            self.session
                .send(&fetch, false, true, Some(correlation))
                .await?;
            self.session.read_reply_chain().await
        })
        .await;
        let replies = match result {
            Ok(replies) => replies,
            Err(error) if is_confirmed_interrupt(&error) => {
                self.cancelled = true;
                self.completed = true;
                for cursor in &mut self.cursors {
                    cursor.ended = true;
                    cursor.closed = true;
                    cursor.row_data.clear();
                    cursor.extdta.clear();
                    cursor.buffered_rows.clear();
                }
                return Err(error);
            }
            Err(error) => {
                self.session.closed = true;
                return Err(error);
            }
        };
        let mut warnings = Vec::new();
        let prior_row_bytes = self.cursors[index].row_data.len();
        let prior_external_values = self.cursors[index].extdta.len();
        let last_qrydta = match consume_procedure_cursor_replies(
            replies,
            &mut self.cursors[index],
            &mut warnings,
        ) {
            Ok(last_qrydta) => last_qrydta,
            Err(error) => {
                if is_confirmed_interrupt(&error) {
                    self.mark_confirmed_cancel();
                } else {
                    self.session.closed = true;
                }
                return Err(error);
            }
        };
        self.session.report_warnings(warnings);
        let cursor = &mut self.cursors[index];
        if !cursor.ended
            && cursor.row_data.len() == prior_row_bytes
            && cursor.extdta.len() == prior_external_values
        {
            self.session.closed = true;
            return Err(Db2Error::other(
                "DB2 CNTQRY returned neither cursor data nor an end-of-query reply".into(),
            ));
        }
        let fixed_scalar = !cursor.fields.is_empty()
            && cursor.fields.iter().all(|field| {
                matches!(
                    field.field_type,
                    0x02 | 0x03
                        | 0x04
                        | 0x05
                        | 0x06
                        | 0x07
                        | 0x0a
                        | 0x0b
                        | 0x0c
                        | 0x0d
                        | 0x16
                        | 0x17
                        | 0xbe
                        | 0xbf
                )
            });
        if !cursor.ended && fixed_scalar && last_qrydta.is_some() {
            let made_progress =
                decode_rows_incremental(&cursor.fields, cursor.row_data.as_slice())?.1 > 0;
            if made_progress {
                cursor.stalled_qrydta.clear();
                cursor.stalled_qrydta_count = 0;
            } else if let Some(frame) = last_qrydta.as_ref() {
                if cursor.stalled_qrydta.as_slice() == frame.as_slice() {
                    cursor.stalled_qrydta_count = cursor.stalled_qrydta_count.saturating_add(1);
                } else {
                    cursor.stalled_qrydta.clear();
                    if cursor.stalled_qrydta.capacity() < frame.len() {
                        cursor
                            .stalled_qrydta
                            .try_reserve_exact(frame.len())
                            .map_err(|error| {
                                Db2Error::protocol(format!(
                                    "unable to retain stalled QRYDTA fingerprint: {error}"
                                ))
                            })?;
                    }
                    cursor.stalled_qrydta.extend_from_slice(frame.as_slice());
                    cursor.stalled_qrydta_count = 1;
                }
                if cursor.stalled_qrydta_count >= 3 {
                    self.session.closed = true;
                    return Err(Db2Error::protocol(format!(
                        "DB2 repeated a non-progressing procedure QRYDTA fragment ({} bytes)",
                        frame.len()
                    )));
                }
            }
        }
        Ok(())
    }

    async fn close_current_cursor(&mut self) -> Result<(), Db2Error> {
        let index = self.current_cursor;
        if self.cursors[index].closed {
            return Ok(());
        }
        let close = ddm::pack_close_procedure_result_set(
            &self.cursors[index].section,
            self.cursors[index].query_id,
        )
        .map_err(Db2Error::from)?;
        let correlation = self.cursors[index].next_correlation;
        let result = run_with_command_deadline(self.options, self.interrupt.clone(), async {
            self.session
                .send(&close, false, true, Some(correlation))
                .await?;
            self.session.read_reply_chain().await
        })
        .await;
        let replies = match result {
            Ok(replies) => replies,
            Err(error) if is_confirmed_interrupt(&error) => {
                self.cancelled = true;
                self.completed = true;
                for cursor in &mut self.cursors {
                    cursor.ended = true;
                    cursor.closed = true;
                }
                return Err(error);
            }
            Err(error) => {
                self.session.closed = true;
                return Err(error);
            }
        };
        let mut warnings = Vec::new();
        if let Err(error) =
            consume_procedure_cursor_replies(replies, &mut self.cursors[index], &mut warnings)
        {
            if is_confirmed_interrupt(&error) {
                self.mark_confirmed_cancel();
            } else {
                self.session.closed = true;
            }
            return Err(error);
        }
        self.session.report_warnings(warnings);
        self.cursors[index].closed = true;
        Ok(())
    }

    fn mark_confirmed_cancel(&mut self) {
        self.cancelled = true;
        self.completed = true;
        for cursor in &mut self.cursors {
            cursor.ended = true;
            cursor.closed = true;
            cursor.row_data.clear();
            cursor.extdta.clear();
            cursor.buffered_rows.clear();
        }
    }

    async fn finish_terminal(&mut self) -> Result<(), Db2Error> {
        if self.terminal_result.is_some() || self.cancelled {
            return Ok(());
        }
        if self.current_cursor < self.cursors.len() {
            return Ok(());
        }
        if self.session.auto_commit {
            let interrupt = self.interrupt.clone();
            let options = self.options;
            if let Err(error) =
                run_with_command_deadline(options, interrupt, self.session.commit_procedure_call())
                    .await
            {
                if is_confirmed_interrupt(&error) {
                    self.cancelled = true;
                    self.completed = true;
                } else {
                    self.session.closed = true;
                }
                return Err(error);
            }
        }
        self.terminal_result = Some(Db2ProcedureResult {
            rows_affected: self.rows_affected,
            output_parameters: self.output_parameters.clone(),
        });
        self.completed = true;
        Ok(())
    }
}

impl Drop for Db2ProcedureReader<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.session.closed = true;
        }
    }
}

impl Drop for Db2RowReader<'_> {
    fn drop(&mut self) {
        if !self.completed
            && (!self.state.ended
                || (self.commit_after_query && !self.committed)
                || self.state.active_lob.is_some()
                || !self.state.current_row_lobs.is_empty())
        {
            // Drop cannot await a DRDA close/drain; make the pool discard this transport.
            self.session.closed = true;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Db2InsertResult {
    pub rows_affected: i64,
    pub last_insert_id: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Db2ClientInfo {
    pub application_name: String,
    pub workstation_name: String,
    pub user_id: String,
    pub accounting: String,
    pub correlation_token: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PoolServerState {
    session_user: String,
    schema: String,
    isolation: String,
    client_info: Db2ClientInfo,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Db2Statement {
    pub sql: String,
    pub output_columns: Vec<ColumnDescription>,
    pub parameter_columns: Vec<ColumnDescription>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Db2ParameterDirection {
    Input,
    Output,
    InputOutput,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Db2Parameter {
    pub value: Db2Value,
    pub direction: Db2ParameterDirection,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Db2ExecutionResult {
    pub rows_affected: i64,
    /// Values align with the input parameter positions; input-only positions contain `Db2Value::Null`.
    pub output_parameters: Vec<Db2Value>,
    /// Dynamic cursor results returned by a stored procedure, in open order.
    pub result_sets: Vec<Db2ResultSet>,
}

/// Db2 LUW isolation applied to subsequently prepared dynamic SQL.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Db2IsolationLevel {
    #[default]
    Unspecified,
    UncommittedRead,
    CursorStability,
    ReadStability,
    RepeatableRead,
}
impl Db2IsolationLevel {
    fn sql_code(self) -> Option<&'static str> {
        match self {
            Self::Unspecified => None,
            Self::UncommittedRead => Some("UR"),
            Self::CursorStability => Some("CS"),
            Self::ReadStability => Some("RS"),
            Self::RepeatableRead => Some("RR"),
        }
    }
}

/// A transaction scope that exclusively borrows its session. Dropping an unfinished transaction
/// invalidates the session so an uncommitted unit of work cannot leak into later pool use.
pub struct Db2Transaction<'a> {
    session: &'a mut Db2Session,
    completed: bool,
    restore_isolation: Option<String>,
}

impl Db2Transaction<'_> {
    pub async fn query(&mut self, sql: &str) -> Result<Db2QueryResult, Db2Error> {
        self.session.query(sql).await
    }
    pub async fn execute(&mut self, sql: &str) -> Result<i64, Db2Error> {
        self.session.execute(sql).await
    }
    pub async fn query_with_params(
        &mut self,
        sql: &str,
        parameters: &[Db2Value],
    ) -> Result<Db2QueryResult, Db2Error> {
        self.session.query_with_params(sql, parameters).await
    }
    pub async fn query_with_blob_streams(
        &mut self,
        sql: &str,
        parameters: &mut [Db2InputParameter<'_>],
    ) -> Result<Db2QueryResult, Db2Error> {
        self.session.query_with_blob_streams(sql, parameters).await
    }
    pub async fn execute_with_params(
        &mut self,
        sql: &str,
        parameters: &[Db2Value],
    ) -> Result<i64, Db2Error> {
        self.session.execute_with_params(sql, parameters).await
    }
    pub async fn execute_with_blob_streams(
        &mut self,
        sql: &str,
        parameters: &mut [Db2InputParameter<'_>],
    ) -> Result<i64, Db2Error> {
        self.session
            .execute_with_blob_streams(sql, parameters)
            .await
    }
    /// Executes a batch of streamed parameter rows within this transaction.
    pub async fn execute_batch_with_blob_streams(
        &mut self,
        sql: &str,
        parameter_sets: &mut [Vec<Db2InputParameter<'_>>],
    ) -> Result<i64, Db2BatchError> {
        self.session
            .execute_batch_with_blob_streams(sql, parameter_sets)
            .await
    }
    pub async fn execute_insert(&mut self, sql: &str) -> Result<Db2InsertResult, Db2Error> {
        self.session.execute_insert(sql).await
    }
    pub async fn execute_insert_with_params(
        &mut self,
        sql: &str,
        parameters: &[Db2Value],
    ) -> Result<Db2InsertResult, Db2Error> {
        self.session
            .execute_insert_with_params(sql, parameters)
            .await
    }

    pub async fn commit(self) -> Result<(), Db2Error> {
        self.finish(true).await
    }
    pub async fn rollback(self) -> Result<(), Db2Error> {
        self.finish(false).await
    }

    async fn finish(mut self, commit: bool) -> Result<(), Db2Error> {
        let mut result = if commit {
            self.session.commit().await
        } else {
            self.session.rollback().await
        };
        if result.is_ok()
            && let Some(baseline) = self.restore_isolation.take()
        {
            let sql = format!(
                "SET CURRENT ISOLATION {}",
                if baseline.is_empty() {
                    "RESET"
                } else {
                    &baseline
                }
            );
            result = self.session.execute(&sql).await.map(|_| ());
            self.session.prepared_sql = None;
        }
        if result.is_err() {
            self.session.closed = true;
        }
        self.session.set_auto_commit(true);
        self.completed = true;
        result
    }
}

impl Drop for Db2Transaction<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.session.closed = true;
        }
    }
}

impl From<std::io::Error> for Db2Error {
    fn from(value: std::io::Error) -> Self {
        Self::new(Db2ErrorKind::Transport, value.to_string())
    }
}

impl From<dss::DssReadError> for Db2Error {
    fn from(value: dss::DssReadError) -> Self {
        match value {
            dss::DssReadError::Transport(error) => Self::from(error),
            dss::DssReadError::Protocol(message) => Self::protocol(message),
        }
    }
}

/// An async Tokio DRDA connection. Calls borrow the session mutably, so only one exchange can use its byte stream at a time.
pub struct Db2Session {
    stream: BoxStream,
    pool_abort: Option<PoolAbort>,
    config: Config,
    database: String,
    security_mechanism: u16,
    query_block_size: u32,
    correlation_id: u16,
    auto_commit: bool,
    closed: bool,
    prepared_sql: Option<String>,
    current_user: String,
    base_user: String,
    client_info: Db2ClientInfo,
    base_client_info: Db2ClientInfo,
    base_schema: Option<String>,
    base_isolation: Option<String>,
    current_schema: Option<String>,
    current_isolation: Option<String>,
    pool_baseline_captured: bool,
    pool_dirty: SessionDirtyState,
    warning_handler: Option<Arc<dyn Fn(crate::Db2Warning) + Send + Sync>>,
    rdb_interrupt_token: Option<Vec<u8>>,
    interrupt_coordinator: Arc<InterruptCoordinator>,
}

/// A separate DRDA connection can interrupt a command while the session is mutably borrowed.
#[derive(Clone)]
pub struct Db2InterruptHandle {
    config: Config,
    token: Vec<u8>,
    coordinator: Arc<InterruptCoordinator>,
}

impl Db2InterruptHandle {
    /// Requests interruption of an active command; an idle session is left untouched.
    /// Completion of this request does not prove that Db2 stopped the command. Await the
    /// original operation: it may return SQLCODE -952 or complete normally if it won the race.
    pub async fn interrupt(&self) -> Result<(), Db2Error> {
        let Some(_interrupt) = self.coordinator.begin_interrupt() else {
            return Ok(());
        };
        let duration = timeout_duration(self.config.timeout_ms);
        timeout(duration, self.send_interrupt())
            .await
            .map_err(|_| Db2Error::other("DB2 interrupt request timed out".into()))?
    }

    async fn send_interrupt(&self) -> Result<(), Db2Error> {
        let socket = TcpStream::connect((self.config.host.as_str(), self.config.port)).await?;
        socket.set_nodelay(true)?;
        let mut stream: BoxStream = if self.config.use_ssl {
            Box::new(connect_tls(socket, &self.config).await?)
        } else {
            Box::new(socket)
        };
        let machine = std::env::var("HOSTNAME").unwrap_or_else(|_| "justybase-db2-client".into());
        let excsat = ddm::pack_excsat(
            "justybase-db2-interrupt",
            &machine,
            "SQL12010",
            "justybase-db2",
            &ddm::default_manager_levels(),
        )?;
        let (request, next_id) = dss::encode_request_dss(&excsat, 1, false, true)?;
        stream.write_all(&request).await?;
        let negotiation = dss::read_dss_typed_with_limits(
            &mut stream,
            self.config.protocol_limits.max_dss_size,
            self.config.protocol_limits.max_ddm_object_size,
        )
        .await
        .map_err(Db2Error::from)?;
        if negotiation.codepoint != cp::EXCSATRD {
            return Err(Db2Error::other(format!(
                "DB2 interrupt negotiation failed: 0x{:04x}",
                negotiation.codepoint.0
            )));
        }
        let request = ddm::pack_interrupt_rdb_request(&self.token);
        let (bytes, _) = dss::encode_request_dss(&request, next_id, false, true)?;
        self.coordinator
            .interrupt_sent
            .store(true, Ordering::Release);
        stream.write_all(&bytes).await?;
        let reply = dss::read_dss_typed_with_limits(
            &mut stream,
            self.config.protocol_limits.max_dss_size,
            self.config.protocol_limits.max_ddm_object_size,
        )
        .await
        .map_err(Db2Error::from)?;
        if reply.codepoint != cp::CMDCMPRM {
            return Err(Db2Error::other(format!(
                "DB2 interrupt rejected: 0x{:04x}",
                reply.codepoint.0
            )));
        }
        if let Some(severity) = ddm::find_ddm_parameter(&reply.payload, cp::SVRCOD)?
            && severity.len() >= 2
            && u16::from_be_bytes([severity[0], severity[1]]) != 0
        {
            return Err(Db2Error::other(
                "DB2 interrupt returned an error severity".into(),
            ));
        }
        self.coordinator
            .interrupt_sent
            .store(false, Ordering::Release);
        Ok(())
    }
}

async fn run_with_command_deadline<T, F>(
    options: Db2CommandOptions,
    interrupt: Option<Db2InterruptHandle>,
    command: F,
) -> Result<T, Db2Error>
where
    F: Future<Output = Result<T, Db2Error>>,
{
    let coordinator = interrupt
        .as_ref()
        .map(|handle| handle.coordinator.clone())
        .unwrap_or_default();
    let fallback_timeout = interrupt
        .as_ref()
        .map(|handle| timeout_duration(handle.config.timeout_ms))
        .unwrap_or(Duration::from_secs(30));
    run_with_deadline_controller(
        options,
        coordinator,
        fallback_timeout,
        command,
        move || async move {
            match interrupt {
                Some(handle) => handle.interrupt().await,
                None => Err(Db2Error::other("DB2 session has no SQLINTR token".into())),
            }
        },
    )
    .await
}

async fn run_with_deadline_controller<T, F, I, IF>(
    options: Db2CommandOptions,
    coordinator: Arc<InterruptCoordinator>,
    fallback_timeout: Duration,
    command: F,
    interrupt: I,
) -> Result<T, Db2Error>
where
    F: Future<Output = Result<T, Db2Error>>,
    I: FnOnce() -> IF,
    IF: Future<Output = Result<(), Db2Error>>,
{
    tokio::pin!(command);
    if options.timeout_ms == 0 {
        return command.await;
    }
    let deadline = tokio::time::sleep(Duration::from_millis(options.timeout_ms));
    tokio::pin!(deadline);
    tokio::select! {
        biased;
        result = &mut command => result,
        _ = &mut deadline => {
            // ACK means that the request was accepted by the server. It is not proof that
            // the command stopped. Keep reading the original connection until it returns
            // (typically SQLCODE -952), or discard the session if its state stays uncertain.
            let ack_timeout = if options.interrupt_ack_timeout_ms == 0 {
                fallback_timeout
            } else {
                Duration::from_millis(options.interrupt_ack_timeout_ms)
            };
            match timeout(ack_timeout, interrupt()).await {
                Ok(Ok(())) => {
                    let cleanup_timeout_ms = if options.cleanup_timeout_ms == 0 {
                        fallback_timeout.as_millis() as u64
                    } else {
                        options.cleanup_timeout_ms
                    };
                    match timeout(Duration::from_millis(cleanup_timeout_ms), &mut command).await {
                        // Preserve the original server outcome: -952 is the authoritative
                        // cancellation result, while a successful reply means the command won
                        // the race with SQLINTR and completed normally.
                        Ok(original_result) => original_result,
                        Err(_) => {
                            coordinator.poisoned.store(true, Ordering::Release);
                            Err(Db2Error::other(
                                "DB2 command deadline expired; SQLINTR was acknowledged but the original command did not complete before cleanup timed out".into()
                            ))
                        }
                    }
                }
                Ok(Err(error)) => {
                    coordinator.poisoned.store(true, Ordering::Release);
                    Err(Db2Error::other(format!(
                        "DB2 command deadline expired and SQLINTR was not confirmed: {error}"
                    )))
                }
                Err(_) => {
                    coordinator.poisoned.store(true, Ordering::Release);
                    Err(Db2Error::other(
                        "DB2 command deadline expired and the SQLINTR acknowledgment timed out".into()
                    ))
                }
            }
        }
    }
}

impl Db2Session {
    /// Resets per-session send-batch instrumentation for live benchmarks.
    #[cfg(feature = "bench-internals")]
    #[doc(hidden)]
    pub fn benchmark_reset_request_batches(&self) {
        self.interrupt_coordinator
            .request_batches_sent
            .store(0, Ordering::Relaxed);
    }

    /// Returns successful semantic DSS request-group writes since the last reset.
    #[cfg(feature = "bench-internals")]
    #[doc(hidden)]
    pub fn benchmark_request_batches_sent(&self) -> u64 {
        self.interrupt_coordinator
            .request_batches_sent
            .load(Ordering::Relaxed)
    }

    pub async fn connect_dsn(dsn: &str) -> Result<Self, Db2Error> {
        let config = parse_dsn(dsn)?;
        Self::connect(config).await
    }

    pub async fn connect(mut config: Config) -> Result<Self, Db2Error> {
        if config.host.is_empty() {
            return Err(Db2Error::new(
                Db2ErrorKind::InvalidConfiguration,
                "DB2 host is required",
            ));
        }
        if config.database.trim().is_empty() {
            return Err(Db2Error::new(
                Db2ErrorKind::InvalidConfiguration,
                "DB2 database name is required",
            ));
        }
        if config.user.is_empty() {
            return Err(Db2Error::new(
                Db2ErrorKind::InvalidConfiguration,
                "DB2 user name is required",
            ));
        }
        if !matches!(config.security_mechanism, 0 | 3 | 9 | 11) {
            return Err(Db2Error::new(
                Db2ErrorKind::InvalidConfiguration,
                format!(
                    "unsupported DB2 authentication mechanism {}; supported mechanisms are 3 (USRIDPWD), 9 (EUSRIDPWD), and 11 (Kerberos/GSSAPI)",
                    config.security_mechanism
                ),
            ));
        }
        if config.security_mechanism == 11 && config.gssapi_token_file.is_empty() {
            return Err(Db2Error::new(
                Db2ErrorKind::InvalidConfiguration,
                "security mechanism 11 (Kerberos/GSSAPI) requires gssapi_token_file with a caller-supplied GSSAPI token; krb5_keytab/krb5_ccache ticket synthesis is not supported",
            ));
        }
        if config.use_ssl && config.port == 50_000 {
            config.port = 50_001;
        }
        let duration = timeout_duration(config.timeout_ms);
        let socket = timeout(
            duration,
            TcpStream::connect((config.host.as_str(), config.port)),
        )
        .await
        .map_err(|_| {
            Db2Error::other(format!(
                "DB2 TCP connection timed out after {} ms",
                duration.as_millis()
            ))
        })??;
        socket.set_nodelay(true)?;
        let stream: BoxStream = if config.use_ssl {
            Box::new(connect_tls(socket, &config).await?)
        } else {
            Box::new(socket)
        };
        let database = format!(
            "{:<18}",
            config.database.chars().take(18).collect::<String>()
        );
        let security_mechanism = if config.security_mechanism == 0 {
            3
        } else {
            config.security_mechanism
        };
        let query_block_size = if (1024..=1_048_576).contains(&config.block_size) {
            config.block_size as u32
        } else {
            65_535
        };
        let base_user = config.user.clone();
        let mut session = Self {
            stream,
            pool_abort: None,
            config,
            database,
            security_mechanism,
            query_block_size,
            correlation_id: 1,
            auto_commit: true,
            closed: false,
            prepared_sql: None,
            current_user: base_user.clone(),
            base_user,
            client_info: Db2ClientInfo::default(),
            base_client_info: Db2ClientInfo::default(),
            base_schema: None,
            base_isolation: None,
            current_schema: None,
            current_isolation: None,
            pool_baseline_captured: false,
            pool_dirty: SessionDirtyState::default(),
            warning_handler: None,
            rdb_interrupt_token: None,
            interrupt_coordinator: Arc::new(InterruptCoordinator::default()),
        };
        let result = timeout(duration, session.handshake()).await.map_err(|_| {
            Db2Error::other(format!(
                "DB2 handshake timed out after {} ms",
                duration.as_millis()
            ))
        })?;
        if let Err(error) = result {
            session.closed = true;
            return Err(error);
        }
        Ok(session)
    }

    pub(crate) fn install_pool_abort(&mut self) -> PoolAbort {
        if let Some(handle) = &self.pool_abort {
            return handle.clone();
        }
        let stream = std::mem::replace(&mut self.stream, Box::new(tokio::io::duplex(1).0));
        let handle = PoolAbort(Arc::new(std::sync::Mutex::new(PoolTransportState {
            stream: Some(stream),
            read_waker: None,
            write_waker: None,
        })));
        self.stream = Box::new(PoolTransport(handle.clone()));
        self.pool_abort = Some(handle.clone());
        handle
    }

    pub fn auto_commit(&self) -> bool {
        self.auto_commit
    }
    pub fn set_auto_commit(&mut self, enabled: bool) {
        self.auto_commit = enabled;
    }
    pub fn is_closed(&self) -> bool {
        self.closed
            || self.pool_abort.as_ref().is_some_and(PoolAbort::is_closed)
            || self.interrupt_coordinator.poisoned.load(Ordering::Acquire)
    }
    pub fn client_info(&self) -> Db2ClientInfo {
        self.client_info.clone()
    }
    pub fn client_info_matches(&self, info: &Db2ClientInfo) -> bool {
        &self.client_info == info
    }
    pub fn current_session_user(&self) -> &str {
        &self.current_user
    }

    pub fn current_schema(&self) -> Option<&str> {
        self.current_schema.as_deref()
    }

    /// Installs a callback for successful SQLCARD and row-level SQLCA warnings.
    /// Pass a channel sender closure to receive warnings while a row stream is active.
    pub fn set_warning_handler<F>(&mut self, handler: F)
    where
        F: Fn(crate::Db2Warning) + Send + Sync + 'static,
    {
        self.warning_handler = Some(Arc::new(handler));
    }

    /// Removes the warning callback. Pool leases clear callbacks automatically when returned.
    pub fn clear_warning_handler(&mut self) {
        self.warning_handler = None;
    }

    fn report_warning(&self, warning: crate::Db2Warning) {
        if let Some(handler) = self.warning_handler.clone() {
            handler(warning);
        }
    }

    fn report_warnings(&self, warnings: Vec<crate::Db2Warning>) {
        for warning in warnings {
            self.report_warning(warning);
        }
    }

    fn parse_sql_card_and_report_warning(&self, payload: &[u8]) -> Result<ddm::SqlCard, Db2Error> {
        let card = ddm::parse_sql_card(payload, true)?;
        if let Some(warning) = card.warning() {
            self.report_warning(warning);
        }
        Ok(card)
    }
    pub fn interrupt_handle(&self) -> Result<Db2InterruptHandle, Db2Error> {
        self.ensure_open()?;
        let token = self
            .rdb_interrupt_token
            .clone()
            .ok_or_else(|| Db2Error::other("DB2 did not provide an interrupt token".into()))?;
        Ok(Db2InterruptHandle {
            config: self.config.clone(),
            token,
            coordinator: self.interrupt_coordinator.clone(),
        })
    }

    fn deadline_interrupt_handle(&self) -> Option<Db2InterruptHandle> {
        Some(Db2InterruptHandle {
            config: self.config.clone(),
            // An absent token must fail closed if a command deadline expires. Keeping an
            // empty token here lets the common runner attempt SQLINTR and poison the session
            // on rejection, while the public manual interrupt API still reports the absence.
            token: self.rdb_interrupt_token.clone().unwrap_or_default(),
            coordinator: self.interrupt_coordinator.clone(),
        })
    }

    fn default_command_options(&self) -> Db2CommandOptions {
        Db2CommandOptions::new(
            timeout_duration(self.config.timeout_ms)
                .as_millis()
                .min(u64::MAX as u128) as u64,
            timeout_duration(self.config.timeout_ms)
                .as_millis()
                .min(u64::MAX as u128) as u64,
        )
        .with_interrupt_ack_timeout_ms(
            timeout_duration(self.config.timeout_ms)
                .as_millis()
                .min(u64::MAX as u128) as u64,
        )
    }

    /// Switches the active SQL authorization identifier within a Db2 trusted context.
    pub async fn switch_user(&mut self, user: &str) -> Result<(), Db2Error> {
        self.switch_user_with_password_inner(user, None).await
    }

    /// Switches the active SQL authorization identifier. The password enables the
    /// DRDA SECCHK re-authentication fallback when the SQL session-authorization
    /// statements are rejected, mirroring go-db2.
    pub async fn switch_user_with_password(
        &mut self,
        user: &str,
        password: &str,
    ) -> Result<(), Db2Error> {
        self.switch_user_with_password_inner(user, Some(password))
            .await
    }

    async fn switch_user_with_password_inner(
        &mut self,
        user: &str,
        password: Option<&str>,
    ) -> Result<(), Db2Error> {
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let target = validate_user_identifier(user)?;
        let result = run_with_command_deadline(
            self.default_command_options(),
            self.deadline_interrupt_handle(),
            self.switch_user_inner(&target, password, true),
        )
        .await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        if result.is_ok() {
            self.pool_dirty.auth = true;
        }
        result
    }

    /// Sets CURRENT SCHEMA while recording the change for safe, targeted pool reset.
    pub async fn set_current_schema(&mut self, schema: &str) -> Result<(), Db2Error> {
        let quoted = quote_identifier(schema)?;
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let result = run_with_command_deadline(
            self.default_command_options(),
            self.deadline_interrupt_handle(),
            self.execute_sql_set_inner(&format!("SET CURRENT SCHEMA {quoted}")),
        )
        .await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        if result.is_ok() {
            self.current_schema = Some(schema.to_owned());
            self.prepared_sql = None;
            self.pool_dirty.schema = true;
        }
        result
    }

    /// Sets CURRENT ISOLATION while recording the change for safe, targeted pool reset.
    pub async fn set_current_isolation(
        &mut self,
        isolation: Db2IsolationLevel,
    ) -> Result<(), Db2Error> {
        let value = isolation.sql_code().unwrap_or("RESET");
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let result = run_with_command_deadline(
            self.default_command_options(),
            self.deadline_interrupt_handle(),
            self.execute_sql_set_inner(&format!("SET CURRENT ISOLATION {value}")),
        )
        .await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        if result.is_ok() {
            self.current_isolation = Some(value.to_owned());
            self.prepared_sql = None;
            self.pool_dirty.isolation = true;
        }
        result
    }

    /// Executes one Db2 administrative command through SYSPROC.ADMIN_CMD.
    pub async fn execute_admin_command(&mut self, command: &str) -> Result<i64, Db2Error> {
        let normalized =
            normalize_admin_command(command).map_err(|error| Db2Error::other(error.into()))?;
        self.execute_with_params(
            ADMIN_COMMAND_CALL_SQL,
            &[Db2Value::Text(normalized.to_owned())],
        )
        .await
    }

    /// Updates non-empty client registers on this physical session.
    pub async fn set_client_info(&mut self, info: Db2ClientInfo) -> Result<(), Db2Error> {
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let mut target = self.client_info.clone();
        if !info.application_name.is_empty() {
            target.application_name = info.application_name;
        }
        if !info.workstation_name.is_empty() {
            target.workstation_name = info.workstation_name;
        }
        if !info.user_id.is_empty() {
            target.user_id = info.user_id;
        }
        if !info.accounting.is_empty() {
            target.accounting = info.accounting;
        }
        if !info.correlation_token.is_empty() {
            target.correlation_token = info.correlation_token;
        }
        let result = self.update_client_info(target).await;
        if result.is_ok() {
            self.pool_dirty.client_info = true;
        }
        result
    }

    /// Replaces all tracked client registers, including resetting values to empty.
    pub async fn reset_client_info(&mut self, info: Db2ClientInfo) -> Result<(), Db2Error> {
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let result = self.update_client_info(info).await;
        if result.is_ok() {
            self.pool_dirty.client_info = true;
        }
        result
    }

    pub(crate) async fn reset_for_pool(&mut self) -> Result<(), Db2Error> {
        if !self.auto_commit {
            // An abandoned unit of work (TS/C# parity): roll back to clean server
            // state, then discard anyway so the pool cannot reuse this transport.
            let _ = self.rollback().await;
            self.auto_commit = true;
            self.closed = true;
            return Err(Db2Error::other(
                "DB2 session returned with an unfinished transaction; discarded".into(),
            ));
        }
        if self.pool_dirty.is_clean() {
            return Ok(());
        }
        if let Err(error) = self.restore_pool_state().await {
            // A failed register restore leaves server-side session state uncertain.
            // The pool discards this transport instead of handing it to the next borrower.
            self.closed = true;
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn clear_unknown_pool_dirty(&mut self) {
        self.pool_dirty.unknown = false;
    }

    async fn restore_pool_state(&mut self) -> Result<(), Db2Error> {
        if self.pool_baseline_captured {
            if self.pool_dirty.unknown {
                self.restore_pool_server_state().await?;
            } else {
                if self.pool_dirty.auth && self.current_user != self.base_user {
                    self.switch_user_inner(&self.base_user.clone(), None, false)
                        .await?;
                }
                if self.pool_dirty.client_info && self.client_info != self.base_client_info {
                    self.update_client_info_inner(self.base_client_info.clone())
                        .await?;
                }
                if self.pool_dirty.schema
                    && self.current_schema.as_deref() != self.base_schema.as_deref()
                {
                    let schema = self.base_schema.clone().ok_or_else(|| {
                        Db2Error::protocol("pooled schema baseline was not captured")
                    })?;
                    self.execute_sql_set_inner(&format!(
                        "SET CURRENT SCHEMA {}",
                        quote_identifier(&schema)?
                    ))
                    .await?;
                    self.current_schema = Some(schema);
                    self.prepared_sql = None;
                }
                if self.pool_dirty.isolation
                    && self.current_isolation.as_deref() != self.base_isolation.as_deref()
                {
                    let isolation = self.base_isolation.clone().ok_or_else(|| {
                        Db2Error::protocol("pooled isolation baseline was not captured")
                    })?;
                    let target = if isolation.is_empty() {
                        "RESET"
                    } else {
                        &isolation
                    };
                    self.execute_sql_set_inner(&format!("SET CURRENT ISOLATION {target}"))
                        .await?;
                    self.current_isolation = Some(isolation);
                    self.prepared_sql = None;
                }
            }
        } else {
            self.switch_user(&self.base_user.clone()).await?;
            self.reset_client_info(self.base_client_info.clone())
                .await?;
        }
        self.pool_dirty = SessionDirtyState::default();
        Ok(())
    }

    /// Captures DB2's actual session identity and resettable registers before first lease.
    pub(crate) async fn capture_pool_baseline(&mut self) -> Result<(), Db2Error> {
        if self.pool_baseline_captured {
            return Ok(());
        }
        let state = self.read_pool_server_state().await?;
        self.base_user = state.session_user.clone();
        self.current_user = state.session_user;
        self.base_schema = Some(state.schema);
        self.base_isolation = Some(state.isolation);
        self.current_schema = self.base_schema.clone();
        self.current_isolation = self.base_isolation.clone();
        self.base_client_info.application_name = state.client_info.application_name.clone();
        self.base_client_info.workstation_name = state.client_info.workstation_name.clone();
        self.base_client_info.user_id = state.client_info.user_id.clone();
        self.base_client_info.accounting = state.client_info.accounting.clone();
        self.client_info.application_name = state.client_info.application_name;
        self.client_info.workstation_name = state.client_info.workstation_name;
        self.client_info.user_id = state.client_info.user_id;
        self.client_info.accounting = state.client_info.accounting;
        self.prepared_sql = None;
        self.pool_baseline_captured = true;
        self.pool_dirty = SessionDirtyState::default();
        Ok(())
    }

    async fn read_pool_server_state(&mut self) -> Result<PoolServerState, Db2Error> {
        let result = self
            .query(
                "SELECT RTRIM(SESSION_USER), CURRENT SCHEMA, CURRENT ISOLATION, \
                 COALESCE(CURRENT CLIENT_APPLNAME, ''), \
                 COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), \
                 COALESCE(CURRENT CLIENT_USERID, ''), \
                 COALESCE(CURRENT CLIENT_ACCTNG, '') \
                 FROM SYSIBM.SYSDUMMY1",
            )
            .await?;
        let [row] = result.rows.as_slice() else {
            return Err(Db2Error::protocol("invalid pooled session state row count"));
        };
        let [
            Db2Value::Text(session_user),
            Db2Value::Text(schema),
            Db2Value::Text(isolation),
            Db2Value::Text(application_name),
            Db2Value::Text(workstation_name),
            Db2Value::Text(user_id),
            Db2Value::Text(accounting),
        ] = row.as_slice()
        else {
            return Err(Db2Error::protocol("invalid pooled session state row"));
        };
        if session_user.is_empty() || session_user.len() > 128 || session_user.contains('\0') {
            return Err(Db2Error::protocol("invalid SESSION_USER value"));
        }
        if schema.is_empty() || schema.len() > 128 || schema.contains('\0') {
            return Err(Db2Error::protocol("invalid CURRENT SCHEMA value"));
        }
        let isolation = isolation.trim().to_owned();
        if !["", "UR", "CS", "RS", "RR"].contains(&isolation.as_str()) {
            return Err(Db2Error::protocol("unknown CURRENT ISOLATION value"));
        }
        Ok(PoolServerState {
            session_user: session_user.clone(),
            schema: schema.clone(),
            isolation,
            client_info: Db2ClientInfo {
                application_name: application_name.clone(),
                workstation_name: workstation_name.clone(),
                user_id: user_id.clone(),
                accounting: accounting.clone(),
                ..Db2ClientInfo::default()
            },
        })
    }

    async fn restore_pool_server_state(&mut self) -> Result<(), Db2Error> {
        let Some(base_schema) = self.base_schema.clone() else {
            return Err(Db2Error::protocol(
                "pooled schema baseline was not captured",
            ));
        };
        let Some(base_isolation) = self.base_isolation.clone() else {
            return Err(Db2Error::protocol(
                "pooled isolation baseline was not captured",
            ));
        };
        let current = self.read_pool_server_state().await?;

        // Db2 uses SESSION_USER, not the client's configured user cache, for the
        // runtime session authorization ID. Restore first so later cleanup statements
        // execute under the original pooled identity. This also catches CALL and
        // dynamic-SQL changes that text scanning cannot observe.
        if current.session_user != self.base_user {
            let base_user = quote_identifier(&self.base_user)?;
            self.execute(&format!("SET SESSION AUTHORIZATION = {base_user}"))
                .await?;
            self.current_user = self.base_user.clone();
            self.prepared_sql = None;
        }

        let target_client_info = self.base_client_info.clone();
        if current.client_info.accounting != target_client_info.accounting {
            let sql = format!(
                "CALL SYSPROC.WLM_SET_CLIENT_INFO('{}', '{}', '{}', '{}', NULL)",
                escape_sql_value(&target_client_info.user_id),
                escape_sql_value(&target_client_info.workstation_name),
                escape_sql_value(&target_client_info.application_name),
                escape_sql_value(&target_client_info.accounting),
            );
            self.execute(&sql).await?;
        } else {
            for (register, current_value, base_value) in [
                (
                    "APPLNAME",
                    &current.client_info.application_name,
                    &target_client_info.application_name,
                ),
                (
                    "WRKSTNNAME",
                    &current.client_info.workstation_name,
                    &target_client_info.workstation_name,
                ),
                (
                    "USERID",
                    &current.client_info.user_id,
                    &target_client_info.user_id,
                ),
            ] {
                if current_value != base_value {
                    self.execute_sql_set_inner(&format!(
                        "SET CLIENT {register} '{}'",
                        escape_sql_value(base_value)
                    ))
                    .await?;
                }
            }
        }

        if current.schema != base_schema {
            self.execute(&format!(
                "SET CURRENT SCHEMA {}",
                quote_identifier(&base_schema)?
            ))
            .await?;
            self.prepared_sql = None;
        }
        if current.isolation != base_isolation {
            let target = if base_isolation.is_empty() {
                "RESET".to_owned()
            } else {
                base_isolation.clone()
            };
            self.execute(&format!("SET CURRENT ISOLATION {target}"))
                .await?;
            self.prepared_sql = None;
        }

        // Keep client-side caches synchronized only after every server restore succeeded.
        self.client_info.application_name = target_client_info.application_name;
        self.client_info.workstation_name = target_client_info.workstation_name;
        self.client_info.user_id = target_client_info.user_id;
        self.client_info.accounting = target_client_info.accounting;
        self.current_schema = Some(base_schema);
        self.current_isolation = Some(base_isolation);
        self.current_user = self.base_user.clone();
        if !self.auto_commit {
            self.auto_commit = true;
        }
        Ok(())
    }

    pub fn begin_transaction(&mut self) -> Result<Db2Transaction<'_>, Db2Error> {
        self.ensure_open()?;
        if !self.auto_commit {
            return Err(Db2Error::other(
                "a DB2 transaction is already active".into(),
            ));
        }
        self.auto_commit = false;
        Ok(Db2Transaction {
            session: self,
            completed: false,
            restore_isolation: None,
        })
    }

    /// Overrides CURRENT ISOLATION for this scope, restoring its previous value on completion.
    pub async fn begin_transaction_with_isolation(
        &mut self,
        isolation: Db2IsolationLevel,
    ) -> Result<Db2Transaction<'_>, Db2Error> {
        self.ensure_open()?;
        if !self.auto_commit {
            return Err(Db2Error::other(
                "a DB2 transaction is already active".into(),
            ));
        }
        let Some(code) = isolation.sql_code() else {
            return self.begin_transaction();
        };
        let result = self.query("VALUES CURRENT ISOLATION").await?;
        let baseline = match result.rows.as_slice() {
            [row] => match row.as_slice() {
                [Db2Value::Text(value)] => value.trim().to_owned(),
                _ => {
                    return Err(Db2Error::new(
                        Db2ErrorKind::Protocol,
                        "invalid CURRENT ISOLATION value",
                    ));
                }
            },
            _ => {
                return Err(Db2Error::new(
                    Db2ErrorKind::Protocol,
                    "invalid CURRENT ISOLATION row count",
                ));
            }
        };
        if !["", "UR", "CS", "RS", "RR"].contains(&baseline.as_str()) {
            return Err(Db2Error::new(
                Db2ErrorKind::Protocol,
                "unknown CURRENT ISOLATION value",
            ));
        }
        // SET ISOLATION is not transaction controlled; preserve the register explicitly.
        // https://www.ibm.com/docs/en/db2/11.5.x?topic=statements-set-current-isolation
        if let Err(error) = self.execute(&format!("SET CURRENT ISOLATION {code}")).await {
            self.closed = true;
            return Err(error);
        }
        self.prepared_sql = None;
        let mut transaction = self.begin_transaction()?;
        transaction.restore_isolation = Some(baseline);
        Ok(transaction)
    }

    pub async fn query(&mut self, sql: &str) -> Result<Db2QueryResult, Db2Error> {
        self.query_with_options(sql, self.default_command_options())
            .await
    }

    /// Executes a query with a command-specific deadline. The deadline starts after this
    /// session is acquired; a zero timeout disables the command deadline.
    pub async fn query_with_options(
        &mut self,
        sql: &str,
        options: Db2CommandOptions,
    ) -> Result<Db2QueryResult, Db2Error> {
        self.pool_dirty.unknown = true;
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let interrupt = self.deadline_interrupt_handle();
        let result = run_with_command_deadline(options, interrupt, self.query_inner(sql)).await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        result
    }

    /// Starts a pull-based query stream. The mutable borrow keeps the session reserved until it is drained or dropped.
    pub async fn query_stream<'a>(&'a mut self, sql: &str) -> Result<Db2RowReader<'a>, Db2Error> {
        self.query_stream_inner(sql, None, self.default_command_options())
            .await
    }

    pub async fn query_stream_with_options<'a>(
        &'a mut self,
        sql: &str,
        options: Db2CommandOptions,
    ) -> Result<Db2RowReader<'a>, Db2Error> {
        self.query_stream_inner(sql, None, options).await
    }

    /// Starts a pull-based stream over a positional parameterized query.
    pub async fn query_stream_with_params<'a>(
        &'a mut self,
        sql: &str,
        parameters: &[Db2Value],
    ) -> Result<Db2RowReader<'a>, Db2Error> {
        let mut inputs = value_parameter_inputs(parameters);
        self.query_stream_inner(sql, Some(&mut inputs), self.default_command_options())
            .await
    }

    pub async fn query_stream_with_params_and_options<'a>(
        &'a mut self,
        sql: &str,
        parameters: &[Db2Value],
        options: Db2CommandOptions,
    ) -> Result<Db2RowReader<'a>, Db2Error> {
        let mut inputs = value_parameter_inputs(parameters);
        self.query_stream_inner(sql, Some(&mut inputs), options)
            .await
    }

    /// Streams a parameterized query whose BLOB inputs may be supplied by forward-only readers.
    pub async fn query_stream_with_blob_streams<'a>(
        &'a mut self,
        sql: &str,
        parameters: &mut [Db2InputParameter<'_>],
    ) -> Result<Db2RowReader<'a>, Db2Error> {
        self.query_stream_inner(sql, Some(parameters), self.default_command_options())
            .await
    }

    pub async fn query_stream_with_blob_streams_and_options<'a>(
        &'a mut self,
        sql: &str,
        parameters: &mut [Db2InputParameter<'_>],
        options: Db2CommandOptions,
    ) -> Result<Db2RowReader<'a>, Db2Error> {
        self.query_stream_inner(sql, Some(parameters), options)
            .await
    }

    pub async fn query_prepared_stream<'a>(
        &'a mut self,
        statement: &Db2Statement,
        parameters: &[Db2Value],
    ) -> Result<Db2RowReader<'a>, Db2Error> {
        self.query_stream_with_params(&statement.sql, parameters)
            .await
    }

    pub async fn query_prepared_stream_with_options<'a>(
        &'a mut self,
        statement: &Db2Statement,
        parameters: &[Db2Value],
        options: Db2CommandOptions,
    ) -> Result<Db2RowReader<'a>, Db2Error> {
        self.query_stream_with_params_and_options(&statement.sql, parameters, options)
            .await
    }

    /// Starts a parameterized stream with forward-only BLOB readers.
    pub async fn query_prepared_stream_with_blob_streams<'a>(
        &'a mut self,
        statement: &Db2Statement,
        parameters: &mut [Db2InputParameter<'_>],
    ) -> Result<Db2RowReader<'a>, Db2Error> {
        self.query_stream_with_blob_streams(&statement.sql, parameters)
            .await
    }

    async fn query_stream_inner<'a>(
        &'a mut self,
        sql: &str,
        mut parameters: Option<&mut [Db2InputParameter<'_>]>,
        options: Db2CommandOptions,
    ) -> Result<Db2RowReader<'a>, Db2Error> {
        self.pool_dirty.unknown = true;
        let operation = self.interrupt_coordinator.begin_operation().await;
        let interrupt = self.deadline_interrupt_handle();
        let start = run_with_command_deadline(options, interrupt.clone(), async {
            self.ensure_open()?;
            let mut described_columns = Vec::new();
            let mut commit_after_query = false;
            let replies = if let Some(parameters) = parameters.as_deref_mut() {
                let statement = self.prepare_inner(sql).await?;
                if parameters.len() != statement.parameter_columns.len() {
                    return Err(Db2Error::other(format!(
                        "DB2 expected {} parameters, got {}",
                        statement.parameter_columns.len(),
                        parameters.len()
                    )));
                }
                if statement.parameter_columns.is_empty() {
                    self.start_direct_query_stream_inner(sql).await?
                } else {
                    let encoded =
                        build_sql_dta_with_inputs(&statement.parameter_columns, parameters)
                            .map_err(|message| {
                                Db2Error::new(Db2ErrorKind::ParameterConversion, message)
                            })?;
                    self.correlation_id = 1;
                    let open = ddm::pack_open_query_with_params(
                        PACKAGE_ID,
                        CONSISTENCY_TOKEN,
                        SECTION_NUMBER,
                        &self.database,
                        self.query_block_size,
                    );
                    self.send(&open, true, false, None).await?;
                    self.send(
                        &encoded.sql_dta,
                        !encoded.external_data.is_empty(),
                        encoded.external_data.is_empty(),
                        None,
                    )
                    .await?;
                    for (index, external) in encoded.external_data.iter().enumerate() {
                        let last = index + 1 == encoded.external_data.len();
                        self.send_external_data(external, parameters, !last, last)
                            .await?;
                    }
                    described_columns = statement.output_columns.clone();
                    commit_after_query = self.auto_commit;
                    self.read_reply_chain().await?
                }
            } else {
                self.start_direct_query_stream_inner(sql).await?
            };
            Ok::<_, Db2Error>((replies, described_columns, commit_after_query))
        })
        .await;
        let (replies, described_columns, commit_after_query) = match start {
            Ok(result) => result,
            Err(error) => {
                if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
                    self.closed = true;
                }
                return Err(error);
            }
        };
        let mut state = QueryStreamState {
            columns: described_columns,
            fields: Vec::new(),
            pending_row_data: ReadBuffer::default(),
            extdta: VecDeque::new(),
            open: false,
            ended: false,
            failed: false,
            query_id: 0,
            next_correlation: 1,
            current_row_generation: 0,
            current_row_lobs: VecDeque::new(),
            active_lob: None,
            max_materialized_lob_size: self.config.protocol_limits.max_materialized_lob_size,
        };
        let (rows, warnings) = match consume_stream_replies_flat(replies, &mut state) {
            Ok(rows) => rows,
            Err(error) => {
                if !state.ended {
                    self.closed = true;
                }
                return Err(error);
            }
        };
        self.report_warnings(warnings);
        Ok(Db2RowReader {
            session: self,
            state,
            rows: RowBatchQueue::from_batch(rows),
            completed: false,
            commit_after_query,
            committed: false,
            _operation: operation,
            options,
            interrupt,
        })
    }

    async fn start_direct_query_stream_inner(
        &mut self,
        sql: &str,
    ) -> Result<Vec<DssPacket>, Db2Error> {
        self.correlation_id = 1;
        self.prepared_sql = None;
        let prepare = ddm::pack_prepare_sql_statement(
            PACKAGE_ID,
            CONSISTENCY_TOKEN,
            SECTION_NUMBER,
            &self.database,
        );
        self.send(&prepare, true, false, None).await?;
        self.send(&ddm::pack_sql_statement(sql), false, false, None)
            .await?;
        self.prepared_sql = Some(sql.to_string());
        let open = ddm::pack_open_query(
            PACKAGE_ID,
            CONSISTENCY_TOKEN,
            SECTION_NUMBER,
            &self.database,
            self.query_block_size,
        );
        self.send(&open, false, true, None).await?;
        self.read_reply_chain().await
    }

    pub async fn execute(&mut self, sql: &str) -> Result<i64, Db2Error> {
        self.execute_with_options(sql, self.default_command_options())
            .await
    }

    /// Executes a statement with a command-specific deadline. Pool wait and connection
    /// setup are outside this deadline. A deadline uses SQLINTR and awaits the original reply.
    pub async fn execute_with_options(
        &mut self,
        sql: &str,
        options: Db2CommandOptions,
    ) -> Result<i64, Db2Error> {
        self.pool_dirty.unknown = true;
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let interrupt = self.deadline_interrupt_handle();
        let result = run_with_command_deadline(options, interrupt, self.execute_inner(sql)).await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        result
    }

    /// Applies a per-command user/client-info context to this session. The
    /// changes are tracked for pool reset so they cannot leak to another lease.
    pub async fn apply_command_context(
        &mut self,
        context: &Db2CommandContext,
    ) -> Result<(), Db2Error> {
        if let Some(user) = &context.user {
            let target = validate_user_identifier(user)?;
            if target != self.current_user {
                self.switch_user(&target).await?;
            }
        }
        if let Some(info) = &context.client_info {
            self.set_client_info(info.clone()).await?;
        }
        Ok(())
    }

    /// Executes a query after applying a per-command context.
    pub async fn query_with_context(
        &mut self,
        sql: &str,
        context: &Db2CommandContext,
    ) -> Result<Db2QueryResult, Db2Error> {
        self.apply_command_context(context).await?;
        self.query(sql).await
    }

    /// Executes DML after applying a per-command context.
    pub async fn execute_with_context(
        &mut self,
        sql: &str,
        context: &Db2CommandContext,
    ) -> Result<i64, Db2Error> {
        self.apply_command_context(context).await?;
        self.execute(sql).await
    }

    /// Executes a positional parameterized query after applying a per-command context.
    pub async fn query_with_params_and_context(
        &mut self,
        sql: &str,
        parameters: &[Db2Value],
        context: &Db2CommandContext,
    ) -> Result<Db2QueryResult, Db2Error> {
        self.apply_command_context(context).await?;
        self.query_with_params(sql, parameters).await
    }

    /// Executes a positional parameterized DML after applying a per-command context.
    pub async fn execute_with_params_and_context(
        &mut self,
        sql: &str,
        parameters: &[Db2Value],
        context: &Db2CommandContext,
    ) -> Result<i64, Db2Error> {
        self.apply_command_context(context).await?;
        self.execute_with_params(sql, parameters).await
    }

    pub async fn prepare(&mut self, sql: &str) -> Result<Db2Statement, Db2Error> {
        self.prepare_with_options(sql, self.default_command_options())
            .await
    }

    pub async fn prepare_with_options(
        &mut self,
        sql: &str,
        options: Db2CommandOptions,
    ) -> Result<Db2Statement, Db2Error> {
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let result = run_with_command_deadline(
            options,
            self.deadline_interrupt_handle(),
            self.prepare_inner(sql),
        )
        .await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        result
    }

    pub async fn query_with_params(
        &mut self,
        sql: &str,
        parameters: &[Db2Value],
    ) -> Result<Db2QueryResult, Db2Error> {
        self.query_with_params_and_options(sql, parameters, self.default_command_options())
            .await
    }

    pub async fn query_with_params_and_options(
        &mut self,
        sql: &str,
        parameters: &[Db2Value],
        options: Db2CommandOptions,
    ) -> Result<Db2QueryResult, Db2Error> {
        let mut inputs = value_parameter_inputs(parameters);
        self.query_with_blob_streams_and_options(sql, &mut inputs, options)
            .await
    }

    /// Runs a positional query with ordinary values and forward-only BLOB readers in one parameter list.
    pub async fn query_with_blob_streams(
        &mut self,
        sql: &str,
        parameters: &mut [Db2InputParameter<'_>],
    ) -> Result<Db2QueryResult, Db2Error> {
        self.query_with_blob_streams_and_options(sql, parameters, self.default_command_options())
            .await
    }

    pub async fn query_with_blob_streams_and_options(
        &mut self,
        sql: &str,
        parameters: &mut [Db2InputParameter<'_>],
        options: Db2CommandOptions,
    ) -> Result<Db2QueryResult, Db2Error> {
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let interrupt = self.deadline_interrupt_handle();
        let result = run_with_command_deadline(options, interrupt, async {
            let statement = self.prepare_inner(sql).await?;
            self.query_prepared_inputs_inner(&statement, parameters, true)
                .await
        })
        .await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        result
    }

    pub async fn query_prepared(
        &mut self,
        statement: &Db2Statement,
        parameters: &[Db2Value],
    ) -> Result<Db2QueryResult, Db2Error> {
        self.query_prepared_with_options(statement, parameters, self.default_command_options())
            .await
    }

    pub async fn query_prepared_with_options(
        &mut self,
        statement: &Db2Statement,
        parameters: &[Db2Value],
        options: Db2CommandOptions,
    ) -> Result<Db2QueryResult, Db2Error> {
        let mut inputs = value_parameter_inputs(parameters);
        self.query_prepared_with_blob_streams_and_options(statement, &mut inputs, options)
            .await
    }

    /// Runs a prepared query with ordinary values and forward-only BLOB readers.
    pub async fn query_prepared_with_blob_streams(
        &mut self,
        statement: &Db2Statement,
        parameters: &mut [Db2InputParameter<'_>],
    ) -> Result<Db2QueryResult, Db2Error> {
        self.query_prepared_with_blob_streams_and_options(
            statement,
            parameters,
            self.default_command_options(),
        )
        .await
    }

    pub async fn query_prepared_with_blob_streams_and_options(
        &mut self,
        statement: &Db2Statement,
        parameters: &mut [Db2InputParameter<'_>],
        options: Db2CommandOptions,
    ) -> Result<Db2QueryResult, Db2Error> {
        self.pool_dirty.unknown = true;
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let interrupt = self.deadline_interrupt_handle();
        let result = run_with_command_deadline(
            options,
            interrupt,
            self.query_prepared_inputs_inner(statement, parameters, false),
        )
        .await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        result
    }

    pub async fn execute_with_params(
        &mut self,
        sql: &str,
        parameters: &[Db2Value],
    ) -> Result<i64, Db2Error> {
        self.execute_with_params_and_options(sql, parameters, self.default_command_options())
            .await
    }

    pub async fn execute_with_params_and_options(
        &mut self,
        sql: &str,
        parameters: &[Db2Value],
        options: Db2CommandOptions,
    ) -> Result<i64, Db2Error> {
        let mut inputs = value_parameter_inputs(parameters);
        self.execute_with_blob_streams_and_options(sql, &mut inputs, options)
            .await
    }

    /// Executes positional parameters containing ordinary values and forward-only BLOB readers.
    pub async fn execute_with_blob_streams(
        &mut self,
        sql: &str,
        parameters: &mut [Db2InputParameter<'_>],
    ) -> Result<i64, Db2Error> {
        self.execute_with_blob_streams_and_options(sql, parameters, self.default_command_options())
            .await
    }

    pub async fn execute_with_blob_streams_and_options(
        &mut self,
        sql: &str,
        parameters: &mut [Db2InputParameter<'_>],
        options: Db2CommandOptions,
    ) -> Result<i64, Db2Error> {
        self.pool_dirty.unknown = true;
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let interrupt = self.deadline_interrupt_handle();
        let result = run_with_command_deadline(options, interrupt, async {
            let statement = self.prepare_inner(sql).await?;
            self.execute_prepared_inputs_inner(&statement, parameters, true, None, None)
                .await
        })
        .await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        result
    }

    /// Executes an INSERT and returns its affected-row count and generated identity value.
    pub async fn execute_insert(&mut self, sql: &str) -> Result<Db2InsertResult, Db2Error> {
        let rows_affected = self.execute(sql).await?;
        let last_insert_id = self.read_identity_value().await?;
        Ok(Db2InsertResult {
            rows_affected,
            last_insert_id,
        })
    }

    pub async fn execute_insert_with_params(
        &mut self,
        sql: &str,
        parameters: &[Db2Value],
    ) -> Result<Db2InsertResult, Db2Error> {
        let rows_affected = self.execute_with_params(sql, parameters).await?;
        let last_insert_id = self.read_identity_value().await?;
        Ok(Db2InsertResult {
            rows_affected,
            last_insert_id,
        })
    }

    pub async fn execute_insert_with_blob_streams(
        &mut self,
        sql: &str,
        parameters: &mut [Db2InputParameter<'_>],
    ) -> Result<Db2InsertResult, Db2Error> {
        let rows_affected = self.execute_with_blob_streams(sql, parameters).await?;
        let last_insert_id = self.read_identity_value().await?;
        Ok(Db2InsertResult {
            rows_affected,
            last_insert_id,
        })
    }

    pub async fn execute_insert_prepared(
        &mut self,
        statement: &Db2Statement,
        parameters: &[Db2Value],
    ) -> Result<Db2InsertResult, Db2Error> {
        let rows_affected = self.execute_prepared(statement, parameters).await?;
        let last_insert_id = self.read_identity_value().await?;
        Ok(Db2InsertResult {
            rows_affected,
            last_insert_id,
        })
    }

    pub async fn execute_prepared(
        &mut self,
        statement: &Db2Statement,
        parameters: &[Db2Value],
    ) -> Result<i64, Db2Error> {
        self.execute_prepared_with_options(statement, parameters, self.default_command_options())
            .await
    }

    pub async fn execute_prepared_with_options(
        &mut self,
        statement: &Db2Statement,
        parameters: &[Db2Value],
        options: Db2CommandOptions,
    ) -> Result<i64, Db2Error> {
        let mut inputs = value_parameter_inputs(parameters);
        self.execute_prepared_with_blob_streams_and_options(statement, &mut inputs, options)
            .await
    }

    /// Executes a prepared statement with forward-only BLOB readers in the positional input list.
    pub async fn execute_prepared_with_blob_streams(
        &mut self,
        statement: &Db2Statement,
        parameters: &mut [Db2InputParameter<'_>],
    ) -> Result<i64, Db2Error> {
        self.execute_prepared_with_blob_streams_and_options(
            statement,
            parameters,
            self.default_command_options(),
        )
        .await
    }

    pub async fn execute_prepared_with_blob_streams_and_options(
        &mut self,
        statement: &Db2Statement,
        parameters: &mut [Db2InputParameter<'_>],
        options: Db2CommandOptions,
    ) -> Result<i64, Db2Error> {
        self.pool_dirty.unknown = true;
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let interrupt = self.deadline_interrupt_handle();
        let result = run_with_command_deadline(
            options,
            interrupt,
            self.execute_prepared_inputs_inner(statement, parameters, false, None, None),
        )
        .await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        result
    }

    pub async fn execute_prepared_with_outputs(
        &mut self,
        statement: &Db2Statement,
        parameters: &[Db2Parameter],
    ) -> Result<Db2ExecutionResult, Db2Error> {
        self.execute_prepared_with_outputs_and_options(
            statement,
            parameters,
            self.default_command_options(),
        )
        .await
    }

    pub async fn execute_prepared_with_outputs_and_options(
        &mut self,
        statement: &Db2Statement,
        parameters: &[Db2Parameter],
        options: Db2CommandOptions,
    ) -> Result<Db2ExecutionResult, Db2Error> {
        self.pool_dirty.unknown = true;
        if parameters.len() != statement.parameter_columns.len() {
            return Err(Db2Error::other(format!(
                "DB2 expected {} parameters, got {}",
                statement.parameter_columns.len(),
                parameters.len()
            )));
        }
        if !parameters
            .iter()
            .any(|parameter| parameter.direction != Db2ParameterDirection::Input)
        {
            return Err(Db2Error::other(
                "At least one parameter must have Output or InputOutput direction".into(),
            ));
        }
        let values: Vec<Db2Value> = parameters
            .iter()
            .map(|parameter| parameter.value.clone())
            .collect();
        let directions: Vec<Db2ParameterDirection> = parameters
            .iter()
            .map(|parameter| parameter.direction)
            .collect();
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let interrupt = self.deadline_interrupt_handle();
        if is_call_statement(&statement.sql) {
            let mut inputs = value_parameter_inputs(&values);
            let result = run_with_command_deadline(
                options,
                interrupt,
                self.execute_procedure_inputs_inner(
                    statement,
                    &mut inputs,
                    false,
                    Some(&directions),
                ),
            )
            .await;
            if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
                self.closed = true;
            }
            let procedure = result?;
            return Ok(Db2ExecutionResult {
                rows_affected: procedure.rows_affected,
                output_parameters: procedure.output_parameters,
                result_sets: procedure.query_result.result_sets,
            });
        }
        let mut outputs = vec![Db2Value::Null; parameters.len()];
        let result = run_with_command_deadline(
            options,
            interrupt,
            self.execute_prepared_inner(
                statement,
                &values,
                false,
                Some(&directions),
                Some(&mut outputs),
            ),
        )
        .await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        let rows_affected = result?;
        Ok(Db2ExecutionResult {
            rows_affected,
            output_parameters: outputs,
            result_sets: Vec::new(),
        })
    }

    /// Starts a streaming dynamic-result-set CALL. Rows are pulled one at a time from each
    /// cursor; output parameters are exposed only after `finish()` or terminal
    /// `next_result_set()` has drained and closed every cursor.
    pub async fn call_procedure_stream<'session>(
        &'session mut self,
        statement: &Db2Statement,
        parameters: &[Db2Parameter],
    ) -> Result<Db2ProcedureReader<'session>, Db2Error> {
        self.call_procedure_stream_with_options(
            statement,
            parameters,
            self.default_command_options(),
        )
        .await
    }

    pub async fn call_procedure_stream_with_options<'session>(
        &'session mut self,
        statement: &Db2Statement,
        parameters: &[Db2Parameter],
        options: Db2CommandOptions,
    ) -> Result<Db2ProcedureReader<'session>, Db2Error> {
        self.ensure_open()?;
        if !is_call_statement(&statement.sql) {
            return Err(Db2Error::other(
                "call_procedure_stream requires a prepared CALL statement".into(),
            ));
        }
        if parameters.len() != statement.parameter_columns.len() {
            return Err(Db2Error::other(format!(
                "DB2 expected {} parameters, got {}",
                statement.parameter_columns.len(),
                parameters.len()
            )));
        }
        let values = parameters
            .iter()
            .map(|parameter| parameter.value.clone())
            .collect::<Vec<_>>();
        let directions = parameters
            .iter()
            .map(|parameter| parameter.direction)
            .collect::<Vec<_>>();
        let has_outputs = directions
            .iter()
            .any(|direction| *direction != Db2ParameterDirection::Input);
        let output_directions = has_outputs.then_some(directions.as_slice());
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let interrupt = self.deadline_interrupt_handle();
        let mut sent_execute = false;
        let result = run_with_command_deadline(options, interrupt.clone(), async {
            let refreshed = if self.prepared_sql.as_deref() != Some(statement.sql.as_str()) {
                Some(self.prepare_inner(&statement.sql).await?)
            } else {
                None
            };
            let statement = refreshed.as_ref().unwrap_or(statement);
            let mut inputs = value_parameter_inputs(&values);
            let encoded = if inputs.is_empty() {
                None
            } else {
                Some(
                    build_sql_dta_with_inputs(&statement.parameter_columns, &inputs)
                        .map_err(|message| {
                            Db2Error::new(Db2ErrorKind::ParameterConversion, message)
                        })?,
                )
            };
            self.correlation_id = 1;
            let execute = ddm::pack_execute_sql_statement_for_result_sets(
                PACKAGE_ID,
                CONSISTENCY_TOKEN,
                SECTION_NUMBER,
                &self.database,
                u16::MAX,
                self.query_block_size,
                None,
                has_outputs,
            );
            self.send(&execute, encoded.is_some(), encoded.is_none(), None)
                .await?;
            sent_execute = true;
            if let Some(encoded) = &encoded {
                self.send(
                    &encoded.sql_dta,
                    !encoded.external_data.is_empty(),
                    encoded.external_data.is_empty(),
                    None,
                )
                .await?;
                for (index, external) in encoded.external_data.iter().enumerate() {
                    let last = index + 1 == encoded.external_data.len();
                    self.send_external_data(external, &mut inputs, !last, last)
                        .await?;
                }
            }

            let mut sections = Vec::new();
            let mut cursors: Vec<ProcedureCursorState> = Vec::new();
            let mut current_cursor: Option<usize> = None;
            let mut declared_count: Option<usize> = None;
            let mut output_parameters = vec![Db2Value::Null; parameters.len()];
            let mut output_sql_dta_reply: Option<Vec<u8>> = None;
            let mut output_external_data = Vec::new();
            let mut rows_affected = 0i64;
            let mut seen_initial_qrydta = false;

            let response = self.read_reply_chain().await?;
            for reply in response {
                match reply.codepoint {
                    value if value == cp::RSLSETRM => {
                        sections = ddm::parse_result_set_sections(&reply.payload)
                            .map_err(Db2Error::from)?;
                    }
                    value if value == cp::SQLRSLRD => {
                        declared_count = Some(
                            ddm::parse_sql_result_set_count(&reply.payload)
                                .map_err(Db2Error::from)?,
                        );
                    }
                    value if value == cp::SQLDTARD => {
                        if has_outputs {
                            output_sql_dta_reply = Some(
                                reply.payload.try_into_vec().map_err(Db2Error::from)?,
                            );
                        }
                    }
                    value if value == cp::OPNQRYRM => {
                        if cursors.len() >= sections.len() {
                            return Err(Db2Error::other(
                                "DB2 opened more procedure cursors than RSLSETRM declared".into(),
                            ));
                        }
                        let mut cursor = ProcedureCursorState {
                            section: sections[cursors.len()].clone(),
                            columns: Vec::new(),
                            fields: Vec::new(),
                            row_data: ReadBuffer::default(),
                            extdta: VecDeque::new(),
                            ended: false,
                            closed: false,
                            query_id: 0,
                            query_id_present: false,
                            next_correlation: reply.header.correlation_id,
                            buffered_rows: VecDeque::new(),
                            stalled_qrydta: Vec::new(),
                            stalled_qrydta_count: 0,
                        };
                        if let Some(id) = ddm::find_ddm_parameter(&reply.payload, cp::QRYINSID)
                            .map_err(Db2Error::from)?
                            && id.len() >= 8
                        {
                            cursor.query_id = u64::from_be_bytes(id[..8].try_into().unwrap());
                            cursor.query_id_present = true;
                        }
                        cursors.push(cursor);
                        current_cursor = Some(cursors.len() - 1);
                    }
                    value if value == cp::SQLCINRD => {
                        let Some(index) = current_cursor else {
                            return Err(Db2Error::other(
                                "DB2 returned SQLCINRD without an open procedure cursor".into(),
                            ));
                        };
                        cursors[index].columns =
                            ddm::parse_sql_cursor_info(&reply.payload, true)
                                .map_err(Db2Error::from)?;
                    }
                    value if value == cp::QRYDSC => {
                        let Some(index) = current_cursor else {
                            return Err(Db2Error::other(
                                "DB2 returned QRYDSC without an open procedure cursor".into(),
                            ));
                        };
                        cursors[index].fields =
                            ddm::parse_query_descriptor(&reply.payload).map_err(Db2Error::from)?;
                    }
                    value if value == cp::QRYDTA => {
                        let Some(index) = current_cursor else {
                            return Err(Db2Error::other(
                                "DB2 returned QRYDTA without an open procedure cursor".into(),
                            ));
                        };
                        cursors[index].row_data.append(&reply.payload);
                        seen_initial_qrydta = true;
                    }
                    value if value == cp::EXTDTA => {
                        if let Some(index) = current_cursor {
                            if has_outputs && !seen_initial_qrydta {
                                output_external_data
                                    .push(reply.payload.try_into_vec().map_err(Db2Error::from)?);
                            } else {
                                cursors[index]
                                    .extdta
                                    .push_back(reply.payload.try_into_vec().map_err(Db2Error::from)?);
                            }
                        } else if has_outputs {
                            output_external_data
                                .push(reply.payload.try_into_vec().map_err(Db2Error::from)?);
                        } else {
                            return Err(Db2Error::other(
                                "DB2 returned EXTDTA without an open procedure cursor".into(),
                            ));
                        }
                    }
                    value if value == cp::ENDQRYRM => {
                        if let Some(index) = current_cursor {
                            cursors[index].ended = true;
                        }
                    }
                    value if value == cp::SQLCARD => {
                        let card = self.parse_sql_card_and_report_warning(&reply.payload)?;
                        if card.sql_code < 0 || card.sql_code == 464 {
                            return Err(sql_error(card.sql_code, &card.sql_state, &card.message));
                        }
                        rows_affected += card.rows_affected;
                        if card.sql_code == 100
                            && let Some(index) = current_cursor {
                                cursors[index].ended = true;
                            }
                    }
                    value if value == cp::SQLERRRM => {
                        return Err(server_error(&reply.payload));
                    }
                    _ => (),
                }
            }

            if let Some(payload) = output_sql_dta_reply.as_deref() {
                if payload.is_empty() {
                    return Err(Db2Error::other(
                        "DB2 returned an empty SQLDTARD reply payload".into(),
                    ));
                }
                let directions = output_directions
                    .expect("SQLDTARD is collected only when outputs were requested");
                let warnings = apply_procedure_output_parameters_limited(
                    payload,
                    directions,
                    &mut output_parameters,
                    &output_external_data,
                    self.config.protocol_limits.max_materialized_lob_size,
                )?;
                self.report_warnings(warnings);
            } else if !output_external_data.is_empty() {
                return Err(Db2Error::other(
                    "DB2 returned output-parameter EXTDTA without SQLDTARD metadata".into(),
                ));
            } else if let Some(directions) = output_directions {
                ensure_output_parameter_data_returned(directions, false).map_err(|_| {
                    Db2Error::other(
                        "DB2 response chain omitted SQLDTARD for requested output parameters"
                            .into(),
                    )
                })?;
            }

            if declared_count.unwrap_or(0) != sections.len() || cursors.len() != sections.len() {
                return Err(Db2Error::other(format!(
                    "DB2 declared {:?} procedure result sets, returned {} package sections and opened {} cursors",
                    declared_count,
                    sections.len(),
                    cursors.len()
                )));
            }
            if cursors.iter().any(|cursor| !cursor.query_id_present) {
                return Err(Db2Error::other(
                    "DB2 left a procedure cursor open without its query instance ID".into(),
                ));
            }
            Ok((cursors, rows_affected, output_parameters))
        })
        .await;
        let (cursors, rows_affected, output_parameters) = match result {
            Ok(result) => result,
            Err(error) => {
                if sent_execute && !is_confirmed_interrupt(&error) {
                    self.closed = true;
                }
                return Err(error);
            }
        };
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        Ok(Db2ProcedureReader {
            session: self,
            cursors,
            current_cursor: 0,
            rows_affected,
            output_parameters,
            options,
            interrupt,
            _operation,
            completed: false,
            cancelled: false,
            terminal_result: None,
        })
    }

    /// Executes parameter sets in order and returns confirmed partial progress on failure.
    pub async fn execute_batch_with_params(
        &mut self,
        sql: &str,
        parameter_sets: &[Vec<Db2Value>],
    ) -> Result<i64, Db2BatchError> {
        self.execute_batch_with_params_and_options(
            sql,
            parameter_sets,
            self.default_command_options(),
        )
        .await
    }

    pub async fn execute_batch_with_params_and_options(
        &mut self,
        sql: &str,
        parameter_sets: &[Vec<Db2Value>],
        options: Db2CommandOptions,
    ) -> Result<i64, Db2BatchError> {
        self.execute_batch_with_params_and_options_and_limits(
            sql,
            parameter_sets,
            options,
            Db2BatchOptions::default(),
        )
        .await
    }

    /// Executes a bounded pipeline using the session's default command deadline.
    pub async fn execute_batch_with_params_and_batch_options(
        &mut self,
        sql: &str,
        parameter_sets: &[Vec<Db2Value>],
        batch_options: Db2BatchOptions,
    ) -> Result<i64, Db2BatchError> {
        self.execute_batch_with_params_and_options_and_limits(
            sql,
            parameter_sets,
            self.default_command_options(),
            batch_options,
        )
        .await
    }

    /// Executes a bounded pipeline of parameter sets. A row larger than the byte
    /// limit is sent alone; zero limits are rejected before any request is sent.
    pub async fn execute_batch_with_params_and_options_and_limits(
        &mut self,
        sql: &str,
        parameter_sets: &[Vec<Db2Value>],
        command_options: Db2CommandOptions,
        batch_options: Db2BatchOptions,
    ) -> Result<i64, Db2BatchError> {
        if parameter_sets.is_empty() {
            return Ok(0);
        }
        self.pool_dirty.unknown = true;
        if batch_options.max_rows == 0
            || batch_options.max_rows > i16::MAX as usize
            || batch_options.max_encoded_bytes == 0
        {
            return Err(Db2BatchError::new(
                Db2Error::new(
                    Db2ErrorKind::InvalidConfiguration,
                    "DB2 batch row limit must be in 1..=32767 and encoded-byte limit must be positive",
                ),
                None,
                0,
                0,
            ));
        }
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let interrupt = self.deadline_interrupt_handle();
        let mut completed = 0;
        let mut rows_affected = 0;
        let mut active_index = None;
        let result = run_with_command_deadline(command_options, interrupt, async {
            let statement = self.prepare_inner(sql).await?;
            if statement.parameter_columns.is_empty() || is_call_statement(&statement.sql) {
                for (index, parameters) in parameter_sets.iter().enumerate() {
                    active_index = Some(index);
                    rows_affected += self
                        .execute_prepared_inner(&statement, parameters, true, None, None)
                        .await?;
                    completed += 1;
                    active_index = None;
                }
            } else {
                let mut batch = Vec::with_capacity(batch_options.max_rows);
                let mut batch_bytes = 0usize;
                let mut batch_start = 0usize;
                for (index, parameters) in parameter_sets.iter().enumerate() {
                    active_index = Some(index);
                    let encoded = build_sql_dta_with_inputs(
                        &statement.parameter_columns,
                        &value_parameter_inputs(parameters),
                    )
                    .map_err(|message| Db2Error::new(Db2ErrorKind::ParameterConversion, message))?;
                    if !encoded.external_data.is_empty() {
                        if !batch.is_empty() {
                            active_index = Some(batch_start);
                            match self.execute_encoded_batch_chunk(&batch, batch_start).await {
                                Ok(outcome) => {
                                    completed += outcome.completed;
                                    rows_affected += outcome.rows_affected;
                                }
                                Err(failure) => {
                                    completed += failure.completed;
                                    rows_affected += failure.rows_affected;
                                    active_index = Some(failure.failed_index);
                                    return Err(failure.cause);
                                }
                            }
                            batch.clear();
                            batch_bytes = 0;
                        }
                        rows_affected += self
                            .execute_prepared_inner(&statement, parameters, true, None, None)
                            .await?;
                        completed += 1;
                        active_index = None;
                        continue;
                    }
                    if !batch.is_empty()
                        && (batch.len() == batch_options.max_rows
                            || batch_bytes.saturating_add(encoded.sql_dta.len())
                                > batch_options.max_encoded_bytes)
                    {
                        active_index = Some(batch_start);
                        match self.execute_encoded_batch_chunk(&batch, batch_start).await {
                            Ok(outcome) => {
                                completed += outcome.completed;
                                rows_affected += outcome.rows_affected;
                            }
                            Err(failure) => {
                                completed += failure.completed;
                                rows_affected += failure.rows_affected;
                                active_index = Some(failure.failed_index);
                                return Err(failure.cause);
                            }
                        }
                        batch.clear();
                        batch_bytes = 0;
                    }
                    if batch.is_empty() {
                        batch_start = index;
                    }
                    batch_bytes = batch_bytes.saturating_add(encoded.sql_dta.len());
                    batch.push(encoded);
                    active_index = None;
                }
                if !batch.is_empty() {
                    active_index = Some(batch_start);
                    match self.execute_encoded_batch_chunk(&batch, batch_start).await {
                        Ok(outcome) => {
                            completed += outcome.completed;
                            rows_affected += outcome.rows_affected;
                            active_index = None;
                        }
                        Err(failure) => {
                            completed += failure.completed;
                            rows_affected += failure.rows_affected;
                            active_index = Some(failure.failed_index);
                            return Err(failure.cause);
                        }
                    }
                }
            }
            Ok::<(), Db2Error>(())
        })
        .await;
        match result {
            Ok(()) => Ok(rows_affected),
            Err(cause) => {
                if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
                    self.closed = true;
                }
                Err(Db2BatchError::new(
                    cause,
                    active_index,
                    completed,
                    rows_affected,
                ))
            }
        }
    }

    async fn execute_encoded_batch_chunk(
        &mut self,
        encoded_rows: &[crate::converters::sql_dta::EncodedSqlDta],
        start_index: usize,
    ) -> Result<BatchChunkOutcome, BatchChunkFailure> {
        let execute = ddm::pack_execute_sql_statement(
            PACKAGE_ID,
            CONSISTENCY_TOKEN,
            SECTION_NUMBER,
            &self.database,
        );
        let commit = self.auto_commit.then(ddm::pack_commit);
        let request_count = encoded_rows.len() * if commit.is_some() { 3 } else { 2 };
        let mut requests = Vec::with_capacity(request_count);
        // A reply chain is fully drained before the next chunk, so correlation IDs can
        // restart at the driver's established initial value. This also avoids DB2 LUW's
        // signed 16-bit statement-correlation boundary during long-running imports.
        self.correlation_id = 1;
        let first_id = self.correlation_id;
        for (offset, encoded) in encoded_rows.iter().enumerate() {
            let id = first_id.wrapping_add(offset as u16);
            requests.push(dss::DssRequest {
                payload: &execute,
                correlation_id: Some(id),
                next_same_id: true,
                last_packet: false,
            });
            requests.push(dss::DssRequest {
                payload: &encoded.sql_dta,
                correlation_id: Some(id),
                next_same_id: false,
                last_packet: false,
            });
            if let Some(commit) = commit.as_deref() {
                requests.push(dss::DssRequest {
                    payload: commit,
                    correlation_id: Some(id),
                    next_same_id: false,
                    last_packet: false,
                });
            }
        }
        if let Some(last) = requests.last_mut() {
            last.last_packet = true;
        }
        self.send_dss_requests(&requests)
            .await
            .map_err(|cause| BatchChunkFailure {
                cause,
                failed_index: start_index,
                completed: 0,
                rows_affected: 0,
            })?;

        let replies = self
            .read_reply_chain()
            .await
            .map_err(|cause| BatchChunkFailure {
                cause,
                failed_index: start_index,
                completed: 0,
                rows_affected: 0,
            })?;
        let mut sql_cards = Vec::with_capacity(encoded_rows.len());
        let mut failure = None;
        for reply in replies {
            match reply.codepoint {
                cp::SQLCARD => {
                    let card = self
                        .parse_sql_card_and_report_warning(&reply.payload)
                        .map_err(|cause| {
                            self.closed = true;
                            BatchChunkFailure {
                                cause,
                                failed_index: start_index + sql_cards.len(),
                                completed: sql_cards.len(),
                                rows_affected: sql_cards
                                    .iter()
                                    .map(|card: &ddm::SqlCard| card.rows_affected)
                                    .sum(),
                            }
                        })?;
                    sql_cards.push(card);
                    if let Some(card) = sql_cards.last()
                        && card.sql_code < 0
                    {
                        failure = Some(sql_error(card.sql_code, &card.sql_state, &card.message));
                    }
                }
                cp::SQLERRRM => {
                    failure = Some(server_error(&reply.payload));
                    break;
                }
                _ => {}
            }
        }

        let cards_per_set = if self.auto_commit { 2 } else { 1 };
        let failed_card_index = sql_cards.iter().position(|card| card.sql_code < 0);
        let completed_sets = failed_card_index
            .map(|index| index / cards_per_set)
            .unwrap_or(sql_cards.len() / cards_per_set);
        let rows_affected = sql_cards
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                index % cards_per_set == 0 && index / cards_per_set < completed_sets
            })
            .map(|(_, card)| card.rows_affected)
            .sum();
        if let Some(cause) = failure {
            if failed_card_index.is_none() && !is_confirmed_interrupt(&cause) {
                // A chained SQLERRRM without an SQLCARD for the failing command has no
                // precise statement attribution. The confirmed prefix remains usable as
                // progress, but the transport is discarded because later commands may
                // have been accepted before the chain stopped.
                self.closed = true;
            }
            return Err(BatchChunkFailure {
                cause,
                failed_index: start_index + completed_sets,
                completed: completed_sets,
                rows_affected,
            });
        }
        let expected_cards = encoded_rows.len() * cards_per_set;
        if sql_cards.len() != expected_cards {
            self.closed = true;
            return Err(BatchChunkFailure {
                cause: Db2Error::protocol(format!(
                    "DB2 batch reply contained {} SQLCARDs for {} parameter sets (expected {}); session discarded because per-set completion is uncertain",
                    sql_cards.len(),
                    encoded_rows.len(),
                    expected_cards
                )),
                failed_index: start_index + completed_sets,
                completed: completed_sets,
                rows_affected,
            });
        }
        Ok(BatchChunkOutcome {
            completed: encoded_rows.len(),
            rows_affected,
        })
    }

    async fn execute_prepared_batch_for_import(
        &mut self,
        statement: &Db2Statement,
        parameter_sets: &[Vec<Db2Value>],
        options: Db2CommandOptions,
    ) -> Result<(), Db2BatchError> {
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let interrupt = self.deadline_interrupt_handle();
        let mut completed = 0;
        let mut rows_affected = 0;
        let mut active_index = None;
        let batch_options = Db2BatchOptions::default();
        let result = run_with_command_deadline(options, interrupt, async {
            let mut batch = Vec::with_capacity(batch_options.max_rows);
            let mut batch_bytes = 0usize;
            let mut batch_start = 0usize;
            for (index, parameters) in parameter_sets.iter().enumerate() {
                active_index = Some(index);
                let encoded = build_sql_dta_with_inputs(
                    &statement.parameter_columns,
                    &value_parameter_inputs(parameters),
                )
                .map_err(|message| Db2Error::new(Db2ErrorKind::ParameterConversion, message))?;
                if !encoded.external_data.is_empty() {
                    if !batch.is_empty() {
                        active_index = Some(batch_start);
                        match self.execute_encoded_batch_chunk(&batch, batch_start).await {
                            Ok(outcome) => {
                                completed += outcome.completed;
                                rows_affected += outcome.rows_affected;
                                active_index = Some(index);
                            }
                            Err(failure) => {
                                completed += failure.completed;
                                rows_affected += failure.rows_affected;
                                active_index = Some(failure.failed_index);
                                return Err(failure.cause);
                            }
                        }
                        batch.clear();
                        batch_bytes = 0;
                    }
                    rows_affected += self
                        .execute_prepared_inner(statement, parameters, true, None, None)
                        .await?;
                    completed += 1;
                    active_index = None;
                    continue;
                }
                if !batch.is_empty()
                    && (batch.len() == batch_options.max_rows
                        || batch_bytes.saturating_add(encoded.sql_dta.len())
                            > batch_options.max_encoded_bytes)
                {
                    active_index = Some(batch_start);
                    match self.execute_encoded_batch_chunk(&batch, batch_start).await {
                        Ok(outcome) => {
                            completed += outcome.completed;
                            rows_affected += outcome.rows_affected;
                        }
                        Err(failure) => {
                            completed += failure.completed;
                            rows_affected += failure.rows_affected;
                            active_index = Some(failure.failed_index);
                            return Err(failure.cause);
                        }
                    }
                    batch.clear();
                    batch_bytes = 0;
                }
                if batch.is_empty() {
                    batch_start = index;
                }
                batch_bytes = batch_bytes.saturating_add(encoded.sql_dta.len());
                batch.push(encoded);
                active_index = None;
            }
            if !batch.is_empty() {
                active_index = Some(batch_start);
                match self.execute_encoded_batch_chunk(&batch, batch_start).await {
                    Ok(outcome) => {
                        completed += outcome.completed;
                        rows_affected += outcome.rows_affected;
                        active_index = None;
                    }
                    Err(failure) => {
                        completed += failure.completed;
                        rows_affected += failure.rows_affected;
                        active_index = Some(failure.failed_index);
                        return Err(failure.cause);
                    }
                }
            }
            if rows_affected != parameter_sets.len() as i64 {
                return Err(Db2Error::protocol(format!(
                    "Db2 confirmed {rows_affected} inserted rows for an import batch of {}; refusing unconfirmed progress",
                    parameter_sets.len()
                )));
            }
            Ok::<(), Db2Error>(())
        })
        .await;
        match result {
            Ok(()) => Ok(()),
            Err(cause) => {
                if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
                    self.closed = true;
                }
                Err(Db2BatchError::new(
                    cause,
                    active_index,
                    completed,
                    rows_affected,
                ))
            }
        }
    }

    /// Copies rows from a stream owned by another session into an explicitly mapped target.
    /// The destination must be in autocommit mode so this importer can own and finish its
    /// transactions without committing a caller-owned transaction. The fallible observer runs
    /// immediately before each commit: its snapshot includes source rows already read and only
    /// rows committed by earlier transactions. A pending partial `CommitEveryRows` batch is left
    /// uncommitted if source reading or row validation fails.
    pub async fn import_rows_from<F>(
        &mut self,
        source: &mut Db2RowReader<'_>,
        target: &Db2ImportTarget,
        policy: Db2ImportPolicy,
        before_commit: F,
    ) -> Result<Db2ImportProgress, Db2ImportError>
    where
        F: FnMut(Db2ImportProgress) -> Result<(), Db2Error>,
    {
        self.import_rows_with_options(
            source,
            target,
            Db2ImportOptions {
                policy,
                ..Db2ImportOptions::default()
            },
            before_commit,
        )
        .await
    }

    /// Copies rows with full import controls: periodic progress reports, an optional
    /// exclusive table lock, and a per-batch write deadline.
    ///
    /// When `notify_after_rows` is set, the observer runs each time the number of source
    /// rows read reaches a multiple of the interval, in addition to the pre-commit gate.
    /// Returning `Err` from the observer aborts the import without any automatic retry:
    /// the atomic transaction rolls back, while a `CommitEveryRows` import keeps its last
    /// confirmed checkpoint. This is also the cooperative cancellation hook — return an
    /// error from the observer to stop the import — complemented by `interrupt_handle()`
    /// for a hung batch write, which travels the SQLINTR path.
    pub async fn import_rows_with_options<F>(
        &mut self,
        source: &mut Db2RowReader<'_>,
        target: &Db2ImportTarget,
        options: Db2ImportOptions,
        mut before_commit: F,
    ) -> Result<Db2ImportProgress, Db2ImportError>
    where
        F: FnMut(Db2ImportProgress) -> Result<(), Db2Error>,
    {
        let mut progress = Db2ImportProgress::default();
        // Validate before any I/O: a bad notify interval or a table lock combined
        // with checkpoint commits must fail without touching the server.
        let options = options
            .validate()
            .map_err(|cause| Db2ImportError::new(cause, None, progress))?;
        if !self.auto_commit {
            return Err(Db2ImportError::new(
                Db2Error::other(
                    "DB2 import requires an autocommit destination session; caller transactions are not committed".into(),
                ),
                None,
                progress,
            ));
        }
        let (max_batch_rows, max_batch_bytes, commit_each_batch) = options
            .policy
            .limits()
            .map_err(|cause| Db2ImportError::new(cause, None, progress))?;
        // A positive import timeout overrides the session default command deadline
        // for every batch write below; zero keeps the session default.
        let mut batch_command_options = self.default_command_options();
        if options.command_timeout_ms != 0 {
            batch_command_options.timeout_ms = options.command_timeout_ms;
        }
        // Read one row before preparing the INSERT or opening a transaction. An empty
        // source is a true no-op: it must not require the target table to exist and must
        // not send BEGIN/COMMIT traffic to the destination.
        let mut pending_row = match source.next().await {
            Ok(Some(row)) => Some(row),
            Ok(None) => return Ok(progress),
            Err(cause) => {
                return Err(Db2ImportError::new(cause, Some(1), progress));
            }
        };
        self.pool_dirty.unknown = true;
        let statement = self
            .prepare(target.insert_sql())
            .await
            .map_err(|cause| Db2ImportError::new(cause, None, progress))?;
        if statement.parameter_columns.len() != target.columns.len() {
            return Err(Db2ImportError::new(
                Db2Error::other(format!(
                    "DB2 import INSERT described {} parameters for {} mapped columns",
                    statement.parameter_columns.len(),
                    target.columns.len()
                )),
                None,
                progress,
            ));
        }
        for (index, (column, actual)) in target
            .columns
            .iter()
            .zip(&statement.parameter_columns)
            .enumerate()
        {
            if let Some(expected) = column.expected_sql_type
                && actual.sql_type != expected
            {
                return Err(Db2ImportError::new(
                    Db2Error::other(format!(
                        "DB2 import target column {} at position {index} has SQL type {}, expected {expected}",
                        column.name, actual.sql_type
                    )),
                    None,
                    progress,
                ));
            }
        }

        let mut batch = ImportBatch::new(max_batch_rows, max_batch_bytes);
        // Source-row count of the last periodic observer report, used to suppress a
        // duplicate pre-commit report when the import ends on a notify interval.
        let mut last_notified_row: u64 = 0;
        if commit_each_batch {
            loop {
                let next_row = if let Some(row) = pending_row.take() {
                    Ok(Some(row))
                } else {
                    source.next().await
                };
                let next_row = match next_row {
                    Ok(row) => row,
                    Err(cause) => {
                        return Err(Db2ImportError::new(
                            cause,
                            Some(progress.source_rows_read.saturating_add(1)),
                            progress,
                        ));
                    }
                };
                let Some(row) = next_row else {
                    if !batch.is_empty() {
                        commit_import_batch(
                            self,
                            &mut batch,
                            &statement,
                            batch_command_options,
                            &mut progress,
                            &mut before_commit,
                            last_notified_row,
                        )
                        .await?;
                    }
                    return Ok(progress);
                };
                progress.source_rows_read = progress.source_rows_read.saturating_add(1);
                let absolute_row = progress.source_rows_read;
                if row.len() != target.columns.len() {
                    return Err(Db2ImportError::new(
                        Db2Error::other(format!(
                            "DB2 import source row {absolute_row} has {} values for {} mapped columns",
                            row.len(),
                            target.columns.len()
                        )),
                        Some(absolute_row),
                        progress,
                    ));
                }
                let row_bytes = encoded_row_size_estimate(&row)
                    .map_err(|cause| Db2ImportError::new(cause, Some(absolute_row), progress))?;
                if row.iter().any(|value| matches!(value, Db2Value::Lob(_)))
                    || row_has_materialized_import_lob(source, &row, &statement)
                {
                    // Consume/spool the current source row before flushing pending rows so a
                    // source-side failure leaves the partial checkpoint uncommitted.
                    let mut spools =
                        spool_import_lobs(source, &row, &statement)
                            .await
                            .map_err(|cause| {
                                Db2ImportError::new(cause, Some(absolute_row), progress)
                            })?;
                    if batch.should_flush_before(row_bytes) {
                        commit_import_batch(
                            self,
                            &mut batch,
                            &statement,
                            batch_command_options,
                            &mut progress,
                            &mut before_commit,
                            last_notified_row,
                        )
                        .await?;
                    }
                    let estimated_bytes = (batch.estimated_bytes() as u64)
                        .saturating_add(row_bytes as u64)
                        .saturating_add(progressive_import_spool_bytes(&spools));
                    commit_import_batch_with_lob_row(
                        self,
                        &mut batch,
                        &statement,
                        row,
                        &mut spools,
                        absolute_row,
                        estimated_bytes,
                        batch_command_options,
                        &mut progress,
                        &mut before_commit,
                        last_notified_row,
                    )
                    .await?;
                    continue;
                }
                if batch.should_flush_before(row_bytes) {
                    commit_import_batch(
                        self,
                        &mut batch,
                        &statement,
                        batch_command_options,
                        &mut progress,
                        &mut before_commit,
                        last_notified_row,
                    )
                    .await?;
                }
                batch.push(absolute_row, row, row_bytes);
                let flushed = batch.is_full();
                if flushed {
                    commit_import_batch(
                        self,
                        &mut batch,
                        &statement,
                        batch_command_options,
                        &mut progress,
                        &mut before_commit,
                        last_notified_row,
                    )
                    .await?;
                }
                // Periodic progress report, skipped when this row just flushed
                // through a commit gate with the identical snapshot. An observer
                // error aborts the import, keeping the last confirmed checkpoint
                // (no open transaction here).
                if !flushed
                    && let Some(interval) = options.notify_after_rows
                    && absolute_row % interval == 0
                {
                    last_notified_row = absolute_row;
                    if let Err(cause) = before_commit(progress) {
                        return Err(Db2ImportError::new(cause, Some(absolute_row), progress));
                    }
                }
            }
        }

        let mut transaction = self
            .begin_transaction()
            .map_err(|cause| Db2ImportError::new(cause, None, progress))?;
        if options.table_lock {
            let lock_sql = format!("LOCK TABLE {} IN EXCLUSIVE MODE", target.qualified_table());
            if let Err(cause) = transaction.execute(&lock_sql).await {
                let error = Db2ImportError::new(cause, None, progress);
                return Err(rollback_import_transaction(transaction, error).await);
            }
        }
        let mut total_estimated_bytes = 0u64;
        loop {
            let next_row = if let Some(row) = pending_row.take() {
                Ok(Some(row))
            } else {
                source.next().await
            };
            let next_row = match next_row {
                Ok(row) => row,
                Err(cause) => {
                    let error = Db2ImportError::new(
                        cause,
                        Some(progress.source_rows_read.saturating_add(1)),
                        progress,
                    );
                    return Err(rollback_import_transaction(transaction, error).await);
                }
            };
            let Some(row) = next_row else {
                if !batch.is_empty() {
                    total_estimated_bytes =
                        total_estimated_bytes.saturating_add(batch.estimated_bytes() as u64);
                    if let Err((cause, failed_row)) = write_import_batch(
                        &mut transaction,
                        &statement,
                        batch.take(),
                        batch_command_options,
                    )
                    .await
                    {
                        let error = Db2ImportError::new(cause, Some(failed_row), progress);
                        return Err(rollback_import_transaction(transaction, error).await);
                    }
                }
                break;
            };
            progress.source_rows_read = progress.source_rows_read.saturating_add(1);
            let absolute_row = progress.source_rows_read;
            // Periodic progress report: an observer error rolls the atomic
            // transaction back, mirroring the pre-commit gate below.
            if let Some(interval) = options.notify_after_rows
                && absolute_row % interval == 0
            {
                last_notified_row = absolute_row;
                if let Err(cause) = before_commit(progress) {
                    let error = Db2ImportError::new(cause, Some(absolute_row), progress);
                    return Err(rollback_import_transaction(transaction, error).await);
                }
            }
            if row.len() != target.columns.len() {
                let error = Db2ImportError::new(
                    Db2Error::other(format!(
                        "DB2 import source row {absolute_row} has {} values for {} mapped columns",
                        row.len(),
                        target.columns.len()
                    )),
                    Some(absolute_row),
                    progress,
                );
                return Err(rollback_import_transaction(transaction, error).await);
            }
            let row_bytes = match encoded_row_size_estimate(&row) {
                Ok(bytes) => bytes,
                Err(cause) => {
                    let error = Db2ImportError::new(cause, Some(absolute_row), progress);
                    return Err(rollback_import_transaction(transaction, error).await);
                }
            };
            if row.iter().any(|value| matches!(value, Db2Value::Lob(_)))
                || row_has_materialized_import_lob(source, &row, &statement)
            {
                let mut spools = match spool_import_lobs(source, &row, &statement).await {
                    Ok(spools) => spools,
                    Err(cause) => {
                        let error = Db2ImportError::new(cause, Some(absolute_row), progress);
                        return Err(rollback_import_transaction(transaction, error).await);
                    }
                };
                if !batch.is_empty() {
                    total_estimated_bytes =
                        total_estimated_bytes.saturating_add(batch.estimated_bytes() as u64);
                    if let Err((cause, failed_row)) = write_import_batch(
                        &mut transaction,
                        &statement,
                        batch.take(),
                        batch_command_options,
                    )
                    .await
                    {
                        let error = Db2ImportError::new(cause, Some(failed_row), progress);
                        return Err(rollback_import_transaction(transaction, error).await);
                    }
                }
                total_estimated_bytes = total_estimated_bytes
                    .saturating_add(row_bytes as u64)
                    .saturating_add(progressive_import_spool_bytes(&spools));
                if let Err(cause) = write_import_lob_row(
                    &mut transaction,
                    &statement,
                    &row,
                    &mut spools,
                    batch_command_options,
                )
                .await
                {
                    let error = Db2ImportError::new(cause, Some(absolute_row), progress);
                    return Err(rollback_import_transaction(transaction, error).await);
                }
                continue;
            }
            if batch.should_flush_before(row_bytes) {
                total_estimated_bytes =
                    total_estimated_bytes.saturating_add(batch.estimated_bytes() as u64);
                if let Err((cause, failed_row)) = write_import_batch(
                    &mut transaction,
                    &statement,
                    batch.take(),
                    batch_command_options,
                )
                .await
                {
                    let error = Db2ImportError::new(cause, Some(failed_row), progress);
                    return Err(rollback_import_transaction(transaction, error).await);
                }
            }
            batch.push(absolute_row, row, row_bytes);
            if batch.is_full() {
                total_estimated_bytes =
                    total_estimated_bytes.saturating_add(batch.estimated_bytes() as u64);
                if let Err((cause, failed_row)) = write_import_batch(
                    &mut transaction,
                    &statement,
                    batch.take(),
                    batch_command_options,
                )
                .await
                {
                    let error = Db2ImportError::new(cause, Some(failed_row), progress);
                    return Err(rollback_import_transaction(transaction, error).await);
                }
            }
        }
        if progress.source_rows_read > 0
            && progress.source_rows_read != last_notified_row
            && let Err(cause) = before_commit(progress)
        {
            let error = Db2ImportError::new(cause, None, progress);
            return Err(rollback_import_transaction(transaction, error).await);
        }
        if let Err(cause) = transaction.commit().await {
            return Err(Db2ImportError::new(cause, None, progress));
        }
        if progress.source_rows_read > 0 {
            progress.committed_rows = progress.source_rows_read;
            progress.committed_batches = 1;
            progress.estimated_committed_bytes = total_estimated_bytes;
        }
        Ok(progress)
    }

    /// Executes streamed parameter sets in order and returns confirmed partial progress on failure.
    pub async fn execute_batch_with_blob_streams(
        &mut self,
        sql: &str,
        parameter_sets: &mut [Vec<Db2InputParameter<'_>>],
    ) -> Result<i64, Db2BatchError> {
        self.execute_batch_with_blob_streams_and_options(
            sql,
            parameter_sets,
            self.default_command_options(),
        )
        .await
    }

    pub async fn execute_batch_with_blob_streams_and_options(
        &mut self,
        sql: &str,
        parameter_sets: &mut [Vec<Db2InputParameter<'_>>],
        options: Db2CommandOptions,
    ) -> Result<i64, Db2BatchError> {
        if parameter_sets.is_empty() {
            return Ok(0);
        }
        self.pool_dirty.unknown = true;
        let _operation = self.interrupt_coordinator.begin_operation().await;
        let interrupt = self.deadline_interrupt_handle();
        let mut completed = 0;
        let mut rows_affected = 0;
        let mut active_index = None;
        let result = run_with_command_deadline(options, interrupt, async {
            let statement = self.prepare_inner(sql).await?;
            for (index, parameters) in parameter_sets.iter_mut().enumerate() {
                active_index = Some(index);
                rows_affected += self
                    .execute_prepared_inputs_inner(&statement, parameters, true, None, None)
                    .await?;
                completed += 1;
                active_index = None;
            }
            Ok::<(), Db2Error>(())
        })
        .await;
        match result {
            Ok(()) => Ok(rows_affected),
            Err(cause) => {
                if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
                    self.closed = true;
                }
                Err(Db2BatchError::new(
                    cause,
                    active_index,
                    completed,
                    rows_affected,
                ))
            }
        }
    }

    pub async fn ping(&mut self) -> Result<(), Db2Error> {
        let _operation = self.interrupt_coordinator.begin_operation().await;
        self.control(ddm::pack_commit()).await
    }
    pub async fn commit(&mut self) -> Result<(), Db2Error> {
        let _operation = self.interrupt_coordinator.begin_operation().await;
        self.control(ddm::pack_commit()).await
    }
    pub async fn rollback(&mut self) -> Result<(), Db2Error> {
        let _operation = self.interrupt_coordinator.begin_operation().await;
        self.control(ddm::pack_rollback()).await
    }

    async fn read_identity_value(&mut self) -> Result<i64, Db2Error> {
        let result = self
            .query("SELECT IDENTITY_VAL_LOCAL() AS LAST_ID FROM SYSIBM.SYSDUMMY1")
            .await?;
        let value = result
            .rows
            .first()
            .and_then(|row| row.first())
            .ok_or_else(|| Db2Error::other("DB2 did not return an identity value".into()))?;
        match value {
            Db2Value::Integer(id) => Ok(*id),
            Db2Value::Decimal(text) | Db2Value::Text(text) => parse_identity_text(text),
            Db2Value::Bytes(bytes) => parse_identity_text(&String::from_utf8_lossy(bytes)),
            other => Err(Db2Error::other(format!(
                "DB2 returned an invalid identity value: {other:?}"
            ))),
        }
    }

    pub async fn close(&mut self) -> Result<(), Db2Error> {
        if self.closed {
            let _ = self.stream.shutdown().await;
            return Ok(());
        }
        self.closed = true;
        self.stream.shutdown().await.map_err(Into::into)
    }

    async fn handshake(&mut self) -> Result<(), Db2Error> {
        self.correlation_id = 1;
        let machine = std::env::var("HOSTNAME").unwrap_or_else(|_| "justybase-db2-client".into());
        let excsat = ddm::pack_excsat(
            "justybase-db2",
            &machine,
            "SQL12010",
            "justybase-db2",
            &ddm::default_manager_levels(),
        )?;
        self.send(&excsat, false, false, None).await?;
        let requested_mechanism = self.security_mechanism;
        let mut client_private_key: Option<zeroize::Zeroizing<[u8; 32]>> = None;
        let mut accsec_token: Vec<u8> = Vec::new();
        let mut gssapi_token: Vec<u8> = Vec::new();
        if requested_mechanism == 9 {
            let private = crate::security::generate_private_key().map_err(|error| {
                Db2Error::other(format!("DB2 EUSRIDPWD key generation failed: {error}"))
            })?;
            accsec_token = crate::security::calculate_public_key(&private)
                .map_err(|error| {
                    Db2Error::other(format!("DB2 EUSRIDPWD key generation failed: {error}"))
                })?
                .to_vec();
            client_private_key = Some(private);
        } else if requested_mechanism == 11 {
            gssapi_token = std::fs::read(&self.config.gssapi_token_file)?;
        }
        let security = security_enum(requested_mechanism)?;
        let accsec = ddm::pack_accsec(&self.database, security, &accsec_token)?;
        self.send(&accsec, false, true, None).await?;
        let negotiation = self.read_reply_chain().await?;
        let mut negotiated = self.security_mechanism;
        let mut security_token = Vec::new();
        for reply in negotiation {
            if reply.codepoint == cp::RDBNFNRM {
                return Err(Db2Error::new(
                    Db2ErrorKind::DatabaseNotFound,
                    "DB2 relational database was not found",
                ));
            }
            if reply.codepoint == cp::ACCSECRD {
                if let Some(value) = ddm::find_ddm_parameter(&reply.payload, cp::SECMEC)?
                    && value.len() >= 2
                {
                    negotiated = u16::from_be_bytes([value[0], value[1]]);
                }
                if let Some(value) = ddm::find_ddm_parameter(&reply.payload, cp::SECTKN)? {
                    security_token = value.to_vec();
                }
            }
        }
        if negotiated != self.security_mechanism {
            return Err(Db2Error::other(format!(
                "DB2 selected security mechanism {negotiated}, but this connection requested {}; mechanism changes are not allowed",
                self.security_mechanism
            )));
        }
        if negotiated == 3 {
            let secchk = ddm::pack_secchk(
                3,
                &security_token,
                &self.database,
                &self.config.user,
                self.config.password.as_bytes(),
                false,
                ddm::StringEncoding::Cp500,
            )?;
            self.send(&secchk, false, false, None).await?;
        } else if negotiated == 9 {
            let private = client_private_key.ok_or_else(|| {
                Db2Error::other(
                    "DB2 EUSRIDPWD requires a client Diffie-Hellman key pair".to_string(),
                )
            })?;
            if security_token.len() < 20 {
                return Err(Db2Error::protocol(
                    "DB2 EUSRIDPWD key exchange did not return a usable server security token",
                ));
            }
            let encrypted_userid = crate::security::encrypt(
                &encode_cp500(&self.config.user).map_err(Db2Error::other)?,
                &security_token,
                &private,
            )
            .map_err(|error| {
                Db2Error::other(format!("DB2 EUSRIDPWD encryption failed: {error}"))
            })?;
            let encrypted_password = crate::security::encrypt(
                &encode_cp500(&self.config.password).map_err(Db2Error::other)?,
                &security_token,
                &private,
            )
            .map_err(|error| {
                Db2Error::other(format!("DB2 EUSRIDPWD encryption failed: {error}"))
            })?;
            let secchk =
                ddm::pack_secchk_eusridpwd(&encrypted_userid, &encrypted_password, &self.database)?;
            self.send(&secchk, false, false, None).await?;
        } else if negotiated == 11 {
            let secchk =
                ddm::pack_secchk_kerberos(&gssapi_token, &self.database, &self.config.user)?;
            self.send(&secchk, false, false, None).await?;
        } else {
            return Err(Db2Error::other(format!(
                "DB2 negotiated security mechanism {negotiated}; mechanisms 3, 9, and 11 are currently implemented"
            )));
        }
        let accrdb = ddm::pack_accrdb("SQL12010", &self.database, ddm::StringEncoding::Cp500)?;
        self.send(&accrdb, false, true, None).await?;
        for reply in self.read_reply_chain().await? {
            if reply.codepoint == cp::SECCHKRM {
                if let Some(code) = ddm::find_ddm_parameter(&reply.payload, cp::SECCHKCD)?
                    .and_then(|value| value.first())
                    && *code != 0
                {
                    return Err(Db2Error::new(
                        Db2ErrorKind::AuthenticationFailed,
                        format!("DB2 authentication failed (SECCHKCD={code})"),
                    ));
                }
            } else if reply.codepoint == cp::ACCRDBRM {
                if let Some(token) = ddm::find_ddm_parameter(&reply.payload, cp::RDBINTTKN)?
                    && !token.is_empty()
                {
                    self.rdb_interrupt_token = Some(token.to_vec());
                }
            } else if reply.codepoint == cp::RDBAFLRM || reply.codepoint == cp::RDBATHRM {
                return Err(Db2Error::new(
                    Db2ErrorKind::AuthorizationFailed,
                    "DB2 authorization failed while connecting",
                ));
            } else if reply.codepoint == cp::SQLERRRM {
                return Err(server_error(&reply.payload));
            }
        }
        self.initialize_session(&machine).await
    }

    async fn initialize_session(&mut self, machine: &str) -> Result<(), Db2Error> {
        self.correlation_id = 1;
        let manager = ddm::pack_excsat_manager_levels(&[ManagerLevel {
            manager: cp::CCSIDMGR,
            level: 1208,
        }]);
        self.send(&manager, false, false, None).await?;
        let setup = ddm::pack_execute_sql_set(PACKAGE_ID, "", 1, &self.database);
        self.send(&setup, true, false, None).await?;
        let workstation = if self.config.client_wrkstn_name.is_empty() {
            machine.to_string()
        } else {
            self.config.client_wrkstn_name.clone()
        };
        let workstation = sanitize_client_register(&workstation);
        let escaped = escape_sql_value(&workstation);
        let sql = ddm::pack_sql_statement(&format!("SET CLIENT WRKSTNNAME '{escaped}'"));
        self.send(&sql, false, false, None).await?;
        let commit = ddm::pack_commit();
        self.send(&commit, false, true, None).await?;
        let _ = self.read_reply_chain().await?;
        self.client_info.workstation_name = workstation.clone();
        let target = Db2ClientInfo {
            application_name: self.config.client_appl_name.clone(),
            workstation_name: workstation,
            user_id: self.config.client_userid.clone(),
            accounting: self.config.client_acctng.clone(),
            correlation_token: self.config.client_corr_token.clone(),
        };
        self.update_client_info_inner(target).await?;
        self.base_client_info = self.client_info.clone();
        Ok(())
    }

    async fn update_client_info(&mut self, target: Db2ClientInfo) -> Result<(), Db2Error> {
        self.ensure_open()?;
        let result = run_with_command_deadline(
            self.default_command_options(),
            self.deadline_interrupt_handle(),
            self.update_client_info_inner(target),
        )
        .await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        result
    }

    async fn update_client_info_inner(&mut self, target: Db2ClientInfo) -> Result<(), Db2Error> {
        let target = sanitize_client_info(target);
        let commands = [
            (
                0u8,
                self.client_info.application_name.clone(),
                target.application_name.clone(),
                format!(
                    "SET CLIENT APPLNAME '{}'",
                    escape_sql_value(&target.application_name)
                ),
            ),
            (
                1u8,
                self.client_info.workstation_name.clone(),
                target.workstation_name.clone(),
                format!(
                    "SET CLIENT WRKSTNNAME '{}'",
                    escape_sql_value(&target.workstation_name)
                ),
            ),
            (
                2u8,
                self.client_info.user_id.clone(),
                target.user_id.clone(),
                format!("SET CLIENT USERID '{}'", escape_sql_value(&target.user_id)),
            ),
        ];
        for (index, current, value, sql) in commands {
            if current == value {
                continue;
            }
            self.execute_sql_set_inner(&sql).await?;
            match index {
                0 => self.client_info.application_name = value,
                1 => self.client_info.workstation_name = value,
                _ => self.client_info.user_id = value,
            }
        }

        if self.client_info.correlation_token != target.correlation_token {
            let value = escape_sql_value(&target.correlation_token);
            let mut last_error = None;
            for sql in [
                format!("SET CLIENT CORR_TOKEN '{value}'"),
                format!("SET CLIENT_CORR_TOKEN = '{value}'"),
                format!("SET CLIENT PROGRAMID '{value}'"),
            ] {
                match self.execute_sql_set_inner(&sql).await {
                    Ok(()) => {
                        self.client_info.correlation_token = target.correlation_token.clone();
                        last_error = None;
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            if let Some(error) = last_error {
                return Err(error);
            }
        }

        if self.client_info.accounting != target.accounting {
            let accounting = escape_sql_value(&target.accounting);
            self.execute_inner(&format!(
                "CALL SYSPROC.WLM_SET_CLIENT_INFO('', '', '', '{accounting}', NULL)"
            ))
            .await?;
            self.execute_sql_set_inner(&format!(
                "SET CLIENT APPLNAME '{}'",
                escape_sql_value(&target.application_name)
            ))
            .await?;
            self.execute_sql_set_inner(&format!(
                "SET CLIENT WRKSTNNAME '{}'",
                escape_sql_value(&target.workstation_name)
            ))
            .await?;
            self.execute_sql_set_inner(&format!(
                "SET CLIENT USERID '{}'",
                escape_sql_value(&target.user_id)
            ))
            .await?;
            self.client_info.accounting = target.accounting;
        }
        Ok(())
    }

    async fn switch_user_inner(
        &mut self,
        target: &str,
        password: Option<&str>,
        allow_fallback: bool,
    ) -> Result<(), Db2Error> {
        if target == self.current_user {
            return Ok(());
        }
        let mut last_error = None;
        for sql in [
            format!("SET SESSION AUTHORIZATION = \"{target}\""),
            format!("SET SESSION_USER = \"{target}\""),
        ] {
            match self.execute_inner(&sql).await {
                Ok(_) => {
                    self.current_user = target.to_string();
                    return Ok(());
                }
                Err(error) => last_error = Some(error),
            }
        }
        if !allow_fallback {
            return Err(last_error
                .unwrap_or_else(|| Db2Error::other("DB2 session user switch failed".into())));
        }
        // DRDA re-authentication fallback, mirroring go-db2: a standalone SECCHK
        // revalidates the new identity on the existing connection context.
        if self.security_mechanism != 3 {
            return Err(Db2Error::new(
                Db2ErrorKind::Unsupported,
                format!(
                    "DB2 user switch fallback requires security mechanism 3; current mechanism {} is not supported for mid-session re-authentication",
                    self.security_mechanism
                ),
            ));
        }
        self.correlation_id = 1;
        let password_bytes = password.map(str::as_bytes).unwrap_or(&[]);
        let secchk = ddm::pack_secchk(
            3,
            &[],
            &self.database,
            target,
            password_bytes,
            false,
            ddm::StringEncoding::Cp500,
        )?;
        self.send(&secchk, false, true, None).await?;
        let mut saw_secchk_rm = false;
        for reply in self.read_reply_chain().await? {
            if reply.codepoint == cp::SECCHKRM {
                saw_secchk_rm = true;
                if let Some(code) = ddm::find_ddm_parameter(&reply.payload, cp::SECCHKCD)?
                    .and_then(|value| value.first())
                    && *code != 0
                {
                    return Err(Db2Error::new(
                        Db2ErrorKind::AuthenticationFailed,
                        format!("DB2 user switch failed (SECCHKCD={code})"),
                    ));
                }
            } else if reply.codepoint == cp::SQLERRRM {
                return Err(server_error(&reply.payload));
            }
        }
        if !saw_secchk_rm {
            return Err(Db2Error::protocol(
                "DB2 user switch SECCHK response did not contain SECCHKRM",
            ));
        }
        self.current_user = target.to_string();
        Ok(())
    }

    async fn execute_sql_set_inner(&mut self, sql: &str) -> Result<(), Db2Error> {
        self.ensure_open()?;
        self.correlation_id = 1;
        let setup = ddm::pack_execute_sql_set(PACKAGE_ID, "", 1, &self.database);
        self.send(&setup, true, false, None).await?;
        let statement = ddm::pack_sql_statement(sql);
        self.send(&statement, false, !self.auto_commit, None)
            .await?;
        if self.auto_commit {
            let commit = ddm::pack_commit();
            self.send(&commit, false, true, None).await?;
        }
        for reply in self.read_reply_chain().await? {
            if reply.codepoint == cp::SQLCARD {
                let card = self.parse_sql_card_and_report_warning(&reply.payload)?;
                if card.sql_code < 0 {
                    return Err(sql_error(card.sql_code, &card.sql_state, &card.message));
                }
            } else if reply.codepoint == cp::SQLERRRM {
                return Err(server_error(&reply.payload));
            }
        }
        Ok(())
    }

    async fn query_inner(&mut self, sql: &str) -> Result<Db2QueryResult, Db2Error> {
        self.ensure_open()?;
        if is_call_statement(sql) {
            let statement = self.prepare_inner(sql).await?;
            let mut parameters = Vec::new();
            return Ok(self
                .execute_procedure_inputs_inner(&statement, &mut parameters, true, None)
                .await?
                .query_result);
        }
        self.correlation_id = 1;
        self.prepared_sql = None;
        let prepare = ddm::pack_prepare_sql_statement(
            PACKAGE_ID,
            CONSISTENCY_TOKEN,
            SECTION_NUMBER,
            &self.database,
        );
        self.send(&prepare, true, false, None).await?;
        let statement = ddm::pack_sql_statement(sql);
        self.send(&statement, false, false, None).await?;
        self.prepared_sql = Some(sql.to_string());
        let open = ddm::pack_open_query(
            PACKAGE_ID,
            CONSISTENCY_TOKEN,
            SECTION_NUMBER,
            &self.database,
            self.query_block_size,
        );
        self.send(&open, false, true, None).await?;
        let replies = self.read_reply_chain().await?;
        self.fetch_query(replies, &[]).await
    }

    async fn prepare_inner(&mut self, sql: &str) -> Result<Db2Statement, Db2Error> {
        self.ensure_open()?;
        self.correlation_id = 1;
        self.prepared_sql = None;
        let prepare = ddm::pack_prepare_sql_statement(
            PACKAGE_ID,
            CONSISTENCY_TOKEN,
            SECTION_NUMBER,
            &self.database,
        );
        self.send(&prepare, true, false, None).await?;
        let text = ddm::pack_sql_statement(sql);
        self.send(&text, false, false, None).await?;
        let describe = ddm::pack_describe_sql_statement(
            PACKAGE_ID,
            CONSISTENCY_TOKEN,
            SECTION_NUMBER,
            &self.database,
        );
        self.send(&describe, false, true, None).await?;
        let mut output_columns = Vec::new();
        let mut parameter_columns = Vec::new();
        for reply in self.read_reply_chain().await? {
            if reply.codepoint == cp::SQLDARD {
                let columns = ddm::parse_sql_dard(&reply.payload, true)?;
                if reply.payload.first() == Some(&0xff) {
                    parameter_columns = columns;
                } else {
                    output_columns = columns;
                }
            } else if reply.codepoint == cp::SQLCARD {
                let card = self.parse_sql_card_and_report_warning(&reply.payload)?;
                if card.sql_code < 0 {
                    return Err(sql_error(card.sql_code, &card.sql_state, &card.message));
                }
            } else if reply.codepoint == cp::SQLERRRM {
                return Err(server_error(&reply.payload));
            }
        }
        self.prepared_sql = Some(sql.to_string());
        Ok(Db2Statement {
            sql: sql.to_string(),
            output_columns,
            parameter_columns,
        })
    }

    async fn query_prepared_inputs_inner(
        &mut self,
        statement: &Db2Statement,
        parameters: &mut [Db2InputParameter<'_>],
        already_prepared: bool,
    ) -> Result<Db2QueryResult, Db2Error> {
        self.ensure_open()?;
        let refreshed =
            if !already_prepared && self.prepared_sql.as_deref() != Some(statement.sql.as_str()) {
                Some(self.prepare_inner(&statement.sql).await?)
            } else {
                None
            };
        let statement = refreshed.as_ref().unwrap_or(statement);
        if parameters.len() != statement.parameter_columns.len() {
            return Err(Db2Error::other(format!(
                "DB2 expected {} parameters, got {}",
                statement.parameter_columns.len(),
                parameters.len()
            )));
        }
        if is_call_statement(&statement.sql) {
            return Ok(self
                .execute_procedure_inputs_inner(statement, parameters, true, None)
                .await?
                .query_result);
        }
        if statement.parameter_columns.is_empty() {
            return self.query_inner(&statement.sql).await;
        }
        let encoded = build_sql_dta_with_inputs(&statement.parameter_columns, parameters)
            .map_err(|message| Db2Error::new(Db2ErrorKind::ParameterConversion, message))?;
        self.correlation_id = 1;
        let open = ddm::pack_open_query_with_params(
            PACKAGE_ID,
            CONSISTENCY_TOKEN,
            SECTION_NUMBER,
            &self.database,
            self.query_block_size,
        );
        self.send(&open, true, false, None).await?;
        self.send(
            &encoded.sql_dta,
            !encoded.external_data.is_empty(),
            encoded.external_data.is_empty(),
            None,
        )
        .await?;
        for (index, external) in encoded.external_data.iter().enumerate() {
            let last = index + 1 == encoded.external_data.len();
            self.send_external_data(external, parameters, !last, last)
                .await?;
        }
        let replies = self.read_reply_chain().await?;
        let result = self.fetch_query(replies, &statement.output_columns).await?;
        if self.auto_commit {
            self.correlation_id = 1;
            self.send(&ddm::pack_commit(), false, true, None).await?;
            let _ = self.read_reply_chain().await?;
        }
        Ok(result)
    }

    async fn execute_prepared_inner(
        &mut self,
        statement: &Db2Statement,
        parameters: &[Db2Value],
        already_prepared: bool,
        output_directions: Option<&[Db2ParameterDirection]>,
        output_parameters: Option<&mut [Db2Value]>,
    ) -> Result<i64, Db2Error> {
        let mut inputs = value_parameter_inputs(parameters);
        self.execute_prepared_inputs_inner(
            statement,
            &mut inputs,
            already_prepared,
            output_directions,
            output_parameters,
        )
        .await
    }

    async fn execute_prepared_inputs_inner(
        &mut self,
        statement: &Db2Statement,
        parameters: &mut [Db2InputParameter<'_>],
        already_prepared: bool,
        output_directions: Option<&[Db2ParameterDirection]>,
        output_parameters: Option<&mut [Db2Value]>,
    ) -> Result<i64, Db2Error> {
        self.ensure_open()?;
        let refreshed =
            if !already_prepared && self.prepared_sql.as_deref() != Some(statement.sql.as_str()) {
                Some(self.prepare_inner(&statement.sql).await?)
            } else {
                None
            };
        let statement = refreshed.as_ref().unwrap_or(statement);
        if parameters.len() != statement.parameter_columns.len() {
            return Err(Db2Error::other(format!(
                "DB2 expected {} parameters, got {}",
                statement.parameter_columns.len(),
                parameters.len()
            )));
        }
        if is_call_statement(&statement.sql) {
            let result = self
                .execute_procedure_inputs_inner(statement, parameters, true, output_directions)
                .await?;
            if let Some(outputs) = output_parameters {
                for (target, value) in outputs.iter_mut().zip(result.output_parameters.iter()) {
                    *target = value.clone();
                }
            }
            return Ok(result.rows_affected);
        }
        if statement.parameter_columns.is_empty() {
            return self.execute_inner(&statement.sql).await;
        }
        let encoded = build_sql_dta_with_inputs(&statement.parameter_columns, parameters)
            .map_err(|message| Db2Error::new(Db2ErrorKind::ParameterConversion, message))?;
        self.correlation_id = 1;
        let execute = ddm::pack_execute_sql_statement(
            PACKAGE_ID,
            CONSISTENCY_TOKEN,
            SECTION_NUMBER,
            &self.database,
        );
        if encoded.external_data.is_empty() {
            let commit = self.auto_commit.then(ddm::pack_commit);
            let requests = [
                dss::DssRequest {
                    payload: &execute,
                    correlation_id: None,
                    next_same_id: true,
                    last_packet: false,
                },
                dss::DssRequest {
                    payload: &encoded.sql_dta,
                    correlation_id: None,
                    next_same_id: false,
                    last_packet: !self.auto_commit,
                },
                dss::DssRequest {
                    payload: commit.as_deref().unwrap_or(&[]),
                    correlation_id: Some(1),
                    next_same_id: false,
                    last_packet: true,
                },
            ];
            self.send_dss_requests(&requests[..if self.auto_commit { 3 } else { 2 }])
                .await?;
        } else {
            self.send(&execute, true, false, None).await?;
            self.send(&encoded.sql_dta, true, false, None).await?;
            for (index, external) in encoded.external_data.iter().enumerate() {
                let last = index + 1 == encoded.external_data.len();
                self.send_external_data(external, parameters, !last, last && !self.auto_commit)
                    .await?;
            }
            if self.auto_commit {
                self.send(&ddm::pack_commit(), false, true, Some(1)).await?;
            }
        }
        let mut replies = self.read_reply_chain().await?;
        if !replies.is_empty() && replies.iter().all(|reply| reply.codepoint == cp::SQLDARD) {
            replies.extend(self.read_reply_chain().await?);
        }
        let mut affected = 0;
        let mut output_parameters = output_parameters;
        for reply in replies {
            if reply.codepoint == cp::SQLCARD {
                let card = self.parse_sql_card_and_report_warning(&reply.payload)?;
                if card.sql_code < 0 {
                    return Err(sql_error(card.sql_code, &card.sql_state, &card.message));
                }
                affected += card.rows_affected;
            } else if reply.codepoint == cp::SQLDTARD {
                if let (Some(directions), Some(outputs)) =
                    (output_directions, output_parameters.as_deref_mut())
                {
                    let (fields, values) =
                        ddm::parse_sql_dta_reply(&reply.payload).map_err(Db2Error::from)?;
                    if fields.is_empty() || values.is_empty() {
                        continue;
                    }
                    let mut row_data = values;
                    if row_data[0] != 0xff {
                        row_data.splice(0..0, [0xff, 0x00]);
                    }
                    let (rows, warnings) = decode_rows_with_warnings(&fields, &row_data)?;
                    self.report_warnings(warnings);
                    if let Some(decoded) = rows.first() {
                        for index in 0..outputs.len().min(decoded.len()).min(directions.len()) {
                            if directions[index] != Db2ParameterDirection::Input {
                                outputs[index] = decoded[index].clone();
                            }
                        }
                    }
                }
            } else if reply.codepoint == cp::SQLERRRM {
                return Err(server_error(&reply.payload));
            }
        }
        Ok(affected)
    }

    async fn execute_inner(&mut self, sql: &str) -> Result<i64, Db2Error> {
        self.ensure_open()?;
        if is_call_statement(sql) {
            let statement = self.prepare_inner(sql).await?;
            let mut parameters = Vec::new();
            return Ok(self
                .execute_procedure_inputs_inner(&statement, &mut parameters, true, None)
                .await?
                .rows_affected);
        }
        self.correlation_id = 1;
        let execute = ddm::pack_exec_sql_immediate(
            PACKAGE_ID,
            CONSISTENCY_TOKEN,
            SECTION_NUMBER,
            &self.database,
        );
        self.send(&execute, true, false, None).await?;
        let statement = ddm::pack_sql_statement(sql);
        self.send(&statement, false, !self.auto_commit, None)
            .await?;
        if self.auto_commit {
            let commit = ddm::pack_commit();
            self.send(&commit, false, true, None).await?;
        }
        let mut affected = 0;
        for reply in self.read_reply_chain().await? {
            if reply.codepoint == cp::SQLCARD {
                let card = self.parse_sql_card_and_report_warning(&reply.payload)?;
                if card.sql_code < 0 {
                    return Err(sql_error(card.sql_code, &card.sql_state, &card.message));
                }
                affected += card.rows_affected;
            } else if reply.codepoint == cp::SQLERRRM {
                return Err(server_error(&reply.payload));
            }
        }
        Ok(affected)
    }

    async fn execute_procedure_inputs_inner(
        &mut self,
        statement: &Db2Statement,
        parameters: &mut [Db2InputParameter<'_>],
        already_prepared: bool,
        output_directions: Option<&[Db2ParameterDirection]>,
    ) -> Result<ProcedureCallResult, Db2Error> {
        self.ensure_open()?;
        let refreshed =
            if !already_prepared && self.prepared_sql.as_deref() != Some(statement.sql.as_str()) {
                Some(self.prepare_inner(&statement.sql).await?)
            } else {
                None
            };
        let statement = refreshed.as_ref().unwrap_or(statement);
        if parameters.len() != statement.parameter_columns.len() {
            return Err(Db2Error::other(format!(
                "DB2 expected {} parameters, got {}",
                statement.parameter_columns.len(),
                parameters.len()
            )));
        }
        if let Some(directions) = output_directions
            && directions.len() != parameters.len()
        {
            return Err(Db2Error::other(
                "DB2 output parameter directions must match the prepared parameter count".into(),
            ));
        }

        let encoded = if parameters.is_empty() {
            None
        } else {
            Some(
                build_sql_dta_with_inputs(&statement.parameter_columns, parameters)
                    .map_err(|message| Db2Error::new(Db2ErrorKind::ParameterConversion, message))?,
            )
        };
        self.correlation_id = 1;
        let execute = ddm::pack_execute_sql_statement_for_result_sets(
            PACKAGE_ID,
            CONSISTENCY_TOKEN,
            SECTION_NUMBER,
            &self.database,
            u16::MAX,
            self.query_block_size,
            None,
            output_directions.is_some_and(|directions| {
                directions
                    .iter()
                    .any(|direction| *direction != Db2ParameterDirection::Input)
            }),
        );
        self.send(&execute, encoded.is_some(), encoded.is_none(), None)
            .await?;
        if let Some(encoded) = &encoded {
            self.send(
                &encoded.sql_dta,
                !encoded.external_data.is_empty(),
                encoded.external_data.is_empty(),
                None,
            )
            .await?;
            for (index, external) in encoded.external_data.iter().enumerate() {
                let last = index + 1 == encoded.external_data.len();
                self.send_external_data(external, parameters, !last, last)
                    .await?;
            }
        }

        let mut sections = Vec::new();
        let mut cursors: Vec<ProcedureCursorState> = Vec::new();
        let mut current_cursor: Option<usize> = None;
        let mut declared_count: Option<usize> = None;
        let mut output_parameters = vec![Db2Value::Null; parameters.len()];
        let mut output_sql_dta_reply: Option<Vec<u8>> = None;
        let mut output_external_data = Vec::new();
        let mut rows_affected = 0i64;
        let mut seen_initial_qrydta = false;
        let processed = async {
            for reply in self.read_reply_chain().await? {
                match reply.codepoint {
                    value if value == cp::RSLSETRM => sections = ddm::parse_result_set_sections(&reply.payload).map_err(Db2Error::from)?,
                    value if value == cp::SQLRSLRD => declared_count = Some(ddm::parse_sql_result_set_count(&reply.payload).map_err(Db2Error::from)?),
                    value if value == cp::SQLDTARD => {
                        if output_directions.is_some() { output_sql_dta_reply = Some(reply.payload.try_into_vec().map_err(Db2Error::from)?); }
                    }
                    value if value == cp::OPNQRYRM => {
                        if cursors.len() >= sections.len() {
                            return Err(Db2Error::other("DB2 opened more procedure cursors than RSLSETRM declared".into()));
                        }
                        let mut cursor = ProcedureCursorState {
                            section: sections[cursors.len()].clone(),
                            columns: Vec::new(),
                            fields: Vec::new(),
                            row_data: ReadBuffer::default(),
                            extdta: VecDeque::new(),
                            ended: false,
                            closed: false,
                            query_id: 0,
                            query_id_present: false,
                            next_correlation: reply.header.correlation_id,
                            buffered_rows: VecDeque::new(),
                            stalled_qrydta: Vec::new(),
                            stalled_qrydta_count: 0,
                        };
                        if let Some(id) = ddm::find_ddm_parameter(&reply.payload, cp::QRYINSID).map_err(Db2Error::from)?
                            && id.len() >= 8
                        {
                        cursor.query_id = u64::from_be_bytes(id[..8].try_into().unwrap());
                        cursor.query_id_present = true;
                        }
                        cursors.push(cursor);
                        current_cursor = Some(cursors.len() - 1);
                    }
                    value if value == cp::SQLCINRD => {
                        let Some(index) = current_cursor else { return Err(Db2Error::other("DB2 returned SQLCINRD without an open procedure cursor".into())); };
                        cursors[index].columns = ddm::parse_sql_cursor_info(&reply.payload, true).map_err(Db2Error::from)?;
                    }
                    value if value == cp::QRYDSC => {
                        let Some(index) = current_cursor else { return Err(Db2Error::other("DB2 returned QRYDSC without an open procedure cursor".into())); };
                        cursors[index].fields = ddm::parse_query_descriptor(&reply.payload).map_err(Db2Error::from)?;
                    }
                    value if value == cp::QRYDTA => {
                        let Some(index) = current_cursor else { return Err(Db2Error::other("DB2 returned QRYDTA without an open procedure cursor".into())); };
                        cursors[index].row_data.append(&reply.payload);
                        seen_initial_qrydta = true;
                    }
                    value if value == cp::EXTDTA => {
                        if let Some(index) = current_cursor {
                            if output_directions.is_some() && !seen_initial_qrydta {
                                // Db2 11.5.8 sends output-parameter EXTDTA after OPNQRYRM/SQLCINRD/QRYDSC
                                // but before any cursor QRYDTA. Route pre-data EXTDTA to outputs.
                                output_external_data
                                    .push(reply.payload.try_into_vec().map_err(Db2Error::from)?);
                            } else {
                                cursors[index]
                                    .extdta
                                    .push_back(reply.payload.try_into_vec().map_err(Db2Error::from)?);
                            }
                        } else if output_directions.is_some() {
                            output_external_data
                                .push(reply.payload.try_into_vec().map_err(Db2Error::from)?);
                        } else {
                            return Err(Db2Error::other("DB2 returned EXTDTA without an open procedure cursor".into()));
                        }
                    }
                    value if value == cp::ENDQRYRM => {
                        if let Some(index) = current_cursor { cursors[index].ended = true; }
                    }
                    value if value == cp::SQLCARD => {
                        let card = self.parse_sql_card_and_report_warning(&reply.payload)?;
                        if card.sql_code < 0 || card.sql_code == 464 {
                            return Err(sql_error(card.sql_code, &card.sql_state, &card.message));
                        }
                        rows_affected += card.rows_affected;
                        if card.sql_code == 100
                            && let Some(index) = current_cursor { cursors[index].ended = true; }
                    }
                    value if value == cp::SQLERRRM => return Err(server_error(&reply.payload)),
                    _ => (),
                }
            }

            if let Some(payload) = output_sql_dta_reply.as_deref() {
                let directions = output_directions.expect("SQLDTARD is collected only for output calls");
                let warnings = apply_procedure_output_parameters_limited(
                    payload,
                    directions,
                    &mut output_parameters,
                    &output_external_data,
                    self.config.protocol_limits.max_materialized_lob_size,
                )?;
                self.report_warnings(warnings);
            } else if !output_external_data.is_empty() {
                return Err(Db2Error::other("DB2 returned output-parameter EXTDTA without SQLDTARD metadata".into()));
            } else if let Some(directions) = output_directions {
                ensure_output_parameter_data_returned(directions, false)?;
            }

            if sections.is_empty() && cursors.is_empty() {
                if declared_count.is_some_and(|count| count != 0) {
                    return Err(Db2Error::other(format!("DB2 declared {} procedure result sets but returned no cursor package sections", declared_count.unwrap())));
                }
                if self.auto_commit { self.commit_procedure_call().await?; }
                return Ok(ProcedureCallResult {
                    query_result: Db2QueryResult { columns: Vec::new(), rows: Vec::new(), result_sets: Vec::new() },
                    output_parameters,
                    rows_affected,
                });
            }
            if declared_count != Some(sections.len()) || cursors.len() != sections.len() {
                return Err(Db2Error::other(format!(
                    "DB2 declared {:?} procedure result sets, returned {} package sections and opened {} cursors",
                    declared_count, sections.len(), cursors.len()
                )));
            }

            let mut result_sets = Vec::with_capacity(cursors.len());
            for cursor in &mut cursors {
                if !cursor.query_id_present {
                    return Err(Db2Error::other("DB2 left a procedure cursor open without its query instance ID".into()));
                }
                let mut fetch_count = 0usize;
                while !cursor.ended {
                    fetch_count += 1;
                    if fetch_count > 65_535 {
                        return Err(Db2Error::other("DB2 procedure cursor exceeded the supported fetch-block count".into()));
                    }
                    let fetch = ddm::pack_continue_procedure_result_set(&cursor.section, self.query_block_size, cursor.query_id).map_err(Db2Error::from)?;
                    self.send(&fetch, false, true, Some(cursor.next_correlation)).await?;
                    let replies = self.read_reply_chain().await?;
                    let mut cursor_warnings = Vec::new();
                    let last_qrydta =
                        consume_procedure_cursor_replies(replies, cursor, &mut cursor_warnings)?;
                    self.report_warnings(cursor_warnings);
                    if cursor.ended { break; }
                    let Some(last_data) = last_qrydta else {
                        return Err(Db2Error::other("DB2 CNTQRY returned no query data or end-of-query reply for a procedure cursor".into()));
                    };
                    if last_data.len() < maximum_procedure_query_data_payload(self.query_block_size) { break; }
                }

                let (mut rows, row_warnings) =
                    decode_rows_with_warnings(&cursor.fields, cursor.row_data.as_slice())?;
                self.report_warnings(row_warnings);
                let external_used = stitch_extdta_limited(
                    &cursor.fields,
                    &mut rows,
                    &cursor.extdta,
                    false,
                    self.config.protocol_limits.max_materialized_lob_size,
                )?;
                if external_used != cursor.extdta.len() {
                    return Err(Db2Error::other(
                        "DB2 returned procedure-cursor EXTDTA values that do not match the QRYDSC LOB descriptors".into(),
                    ));
                }
                self.resolve_progressive_lobs(
                    &mut rows,
                    &cursor.fields,
                    cursor.query_id,
                    cursor.next_correlation,
                )
                .await?;
                if has_unresolved_external_locator(&cursor.fields, &rows) {
                    return Err(Db2Error::other(
                        "DB2 ended a procedure cursor with unresolved external LOB data".into(),
                    ));
                }
                let close = ddm::pack_close_procedure_result_set(&cursor.section, cursor.query_id).map_err(Db2Error::from)?;
                self.send(&close, false, true, Some(cursor.next_correlation)).await?;
                let mut cursor_warnings = Vec::new();
                let _ = consume_procedure_cursor_replies(
                    self.read_reply_chain().await?,
                    cursor,
                    &mut cursor_warnings,
                )?;
                self.report_warnings(cursor_warnings);
                cursor.ended = true;
                cursor.closed = true;
                result_sets.push(Db2ResultSet { columns: cursor.columns.clone(), rows });
            }

            if self.auto_commit { self.commit_procedure_call().await?; }
            let first = result_sets.first().cloned().ok_or_else(|| Db2Error::other("DB2 returned procedure cursor metadata without a cursor".into()))?;
            Ok(ProcedureCallResult {
                query_result: Db2QueryResult { columns: first.columns, rows: first.rows, result_sets },
                output_parameters,
                rows_affected,
            })
        }.await;

        let server_confirmed_interrupt = matches!(
            &processed,
            Err(error)
                if error
                    .to_string()
                    .contains("SQLCODE=-952 SQLSTATE=57014")
        );
        if processed.is_err()
            && !server_confirmed_interrupt
            && (!sections.is_empty() || cursors.iter().any(|cursor| !cursor.ended))
        {
            self.closed = true;
        }
        processed
    }

    async fn commit_procedure_call(&mut self) -> Result<(), Db2Error> {
        self.correlation_id = 1;
        self.send(&ddm::pack_commit(), false, true, None).await?;
        for reply in self.read_reply_chain().await? {
            if reply.codepoint == cp::SQLCARD {
                let card = self.parse_sql_card_and_report_warning(&reply.payload)?;
                if card.sql_code < 0 {
                    self.closed = true;
                    return Err(sql_error(card.sql_code, &card.sql_state, &card.message));
                }
            } else if reply.codepoint == cp::SQLERRRM {
                self.closed = true;
                return Err(server_error(&reply.payload));
            }
        }
        Ok(())
    }

    async fn fetch_query(
        &mut self,
        replies: Vec<DssPacket>,
        described_columns: &[ColumnDescription],
    ) -> Result<Db2QueryResult, Db2Error> {
        let mut state = QueryStreamState {
            columns: described_columns.to_vec(),
            fields: Vec::new(),
            pending_row_data: ReadBuffer::default(),
            extdta: VecDeque::new(),
            open: false,
            ended: false,
            failed: false,
            query_id: 0,
            next_correlation: 1,
            current_row_generation: 0,
            current_row_lobs: VecDeque::new(),
            active_lob: None,
            max_materialized_lob_size: self.config.protocol_limits.max_materialized_lob_size,
        };
        let mut result = Db2QueryResult {
            columns: described_columns.to_vec(),
            rows: Vec::new(),
            result_sets: Vec::new(),
        };
        let mut materialized_result_bytes = 0usize;
        let mut append_rows = |rows: Vec<Vec<Db2Value>>,
                               result: &mut Db2QueryResult|
         -> Result<(), Db2Error> {
            for row in &rows {
                for value in row {
                    let bytes = match value {
                        Db2Value::Null => 1,
                        Db2Value::Text(text) | Db2Value::Decimal(text) => text.len(),
                        Db2Value::Bytes(data) => data.len(),
                        _ => 16,
                    };
                    if bytes > MAX_MATERIALIZED_RESULT_BYTES - materialized_result_bytes {
                        return Err(Db2Error::resource_limit(format!(
                            "DB2 materialized result exceeds the {MAX_MATERIALIZED_RESULT_BYTES}-byte limit"
                        )));
                    }
                    materialized_result_bytes += bytes;
                }
            }
            result.rows.extend(rows);
            Ok(())
        };
        let (mut rows, warnings) = consume_stream_replies(replies, &mut state)?;
        self.report_warnings(warnings);
        self.resolve_progressive_lobs(
            &mut rows,
            &state.fields,
            state.query_id,
            state.next_correlation,
        )
        .await?;
        append_rows(rows, &mut result)?;
        while state.open && !state.ended {
            let prior_pending = state.pending_row_data.len();
            let prior_external = state.extdta.len();
            let next = ddm::pack_continue_query(
                PACKAGE_ID,
                CONSISTENCY_TOKEN,
                SECTION_NUMBER,
                &self.database,
                self.query_block_size,
                state.query_id,
            );
            self.send(&next, false, true, Some(state.next_correlation))
                .await?;
            let replies = self.read_reply_chain().await?;
            let got_data = replies
                .iter()
                .any(|reply| matches!(reply.codepoint, cp::QRYDTA | cp::EXTDTA));
            let (mut rows, warnings) = consume_stream_replies(replies, &mut state)?;
            self.report_warnings(warnings);
            self.resolve_progressive_lobs(
                &mut rows,
                &state.fields,
                state.query_id,
                state.next_correlation,
            )
            .await?;
            append_rows(rows, &mut result)?;
            if !got_data
                && state.pending_row_data.len() == prior_pending
                && state.extdta.len() == prior_external
                && !state.ended
            {
                return Err(Db2Error::other(
                    "DB2 CNTQRY returned neither query data nor an end-of-query reply".into(),
                ));
            }
        }
        if !state.pending_row_data.is_empty()
            || !state.extdta.is_empty()
            || has_unresolved_external_locator(&state.fields, &result.rows)
        {
            return Err(Db2Error::other(
                "DB2 ended a query with incomplete row or external LOB data".into(),
            ));
        }
        result.columns = state.columns;
        Ok(result)
    }

    async fn resolve_progressive_lobs(
        &mut self,
        rows: &mut [Vec<Db2Value>],
        fields: &[FieldDescriptor],
        query_id: u64,
        correlation_id: u16,
    ) -> Result<(), Db2Error> {
        let result = self
            .resolve_progressive_lobs_inner(rows, fields, query_id, correlation_id)
            .await;
        if let Err(error) = &result {
            // A locator failure leaves the open cursor's position uncertain. Only
            // the server-confirmed -952 response proves that Db2 ended the request.
            if !is_confirmed_interrupt(error) {
                self.closed = true;
            }
        }
        result
    }

    async fn resolve_progressive_lobs_inner(
        &mut self,
        rows: &mut [Vec<Db2Value>],
        fields: &[FieldDescriptor],
        query_id: u64,
        correlation_id: u16,
    ) -> Result<(), Db2Error> {
        const CHUNK_LENGTH: u64 = 1024 * 1024;
        let max_materialized_length = self
            .config
            .protocol_limits
            .max_materialized_lob_size
            .min(i32::MAX as usize);

        for row in rows {
            for (column, field) in fields.iter().enumerate() {
                let Some(value) = row.get_mut(column) else {
                    continue;
                };
                let Db2Value::ProgressiveLobReference { length, reference } = value else {
                    continue;
                };
                let (length, reference) = (*length, *reference);
                if length == 0 {
                    *value = empty_lob_value(field.field_type);
                    continue;
                }

                let declared_length = usize::try_from(length).map_err(|_| {
                    Db2Error::resource_limit(
                        "DB2 progressive LOB length exceeds this platform's address space",
                    )
                })?;
                if declared_length > max_materialized_length {
                    return Err(Db2Error::resource_limit(format!(
                        "DB2 progressive LOB length {declared_length} exceeds the configured materialization limit of {max_materialized_length} bytes"
                    )));
                }

                let mut data = Vec::new();
                data.try_reserve_exact(declared_length).map_err(|error| {
                    Db2Error::resource_limit(format!(
                        "cannot reserve {declared_length} bytes for a DB2 progressive LOB: {error}"
                    ))
                })?;
                loop {
                    let request = ddm::pack_get_next_chunk(
                        PACKAGE_ID,
                        CONSISTENCY_TOKEN,
                        SECTION_NUMBER,
                        &self.database,
                        query_id,
                        reference,
                        CHUNK_LENGTH,
                    );
                    self.send(&request, false, true, Some(correlation_id))
                        .await?;
                    let replies = self.read_reply_chain().await?;
                    for reply in &replies {
                        if reply.codepoint == cp::SQLCARD {
                            let card = self.parse_sql_card_and_report_warning(&reply.payload)?;
                            if card.sql_code < 0 {
                                return Err(sql_error(
                                    card.sql_code,
                                    &card.sql_state,
                                    &card.message,
                                ));
                            }
                        } else if reply.codepoint == cp::SQLERRRM {
                            return Err(server_error(&reply.payload));
                        }
                    }
                    let chunk =
                        ddm::parse_progressive_chunk_reply(&replies).map_err(Db2Error::from)?;
                    let next_length =
                        data.len().checked_add(chunk.data.len()).ok_or_else(|| {
                            Db2Error::other("DB2 progressive LOB length overflow".into())
                        })?;
                    if next_length > max_materialized_length {
                        return Err(Db2Error::resource_limit(format!(
                            "DB2 progressive LOB exceeds the configured materialization limit of {max_materialized_length} bytes"
                        )));
                    }
                    data.try_reserve(chunk.data.len()).map_err(|error| {
                        Db2Error::resource_limit(format!(
                            "cannot grow the DB2 progressive LOB buffer: {error}"
                        ))
                    })?;
                    data.extend_from_slice(&chunk.data);
                    if chunk.end_of_reference || chunk.is_last_chunk {
                        break;
                    }
                    if chunk.data.is_empty() {
                        return Err(Db2Error::other(
                            "DB2 GETNXTCHK made no progress before the final chunk".into(),
                        ));
                    }
                }
                *value = decode_external_lob_value_limited(
                    field.field_type,
                    &data,
                    max_materialized_length,
                )?;
            }
        }
        Ok(())
    }

    async fn control(&mut self, payload: Vec<u8>) -> Result<(), Db2Error> {
        self.ensure_open()?;
        let result = run_with_command_deadline(
            self.default_command_options(),
            self.deadline_interrupt_handle(),
            async {
                self.correlation_id = 1;
                self.send(&payload, false, true, None).await?;
                for reply in self.read_reply_chain().await? {
                    if reply.codepoint == cp::SQLCARD {
                        let card = self.parse_sql_card_and_report_warning(&reply.payload)?;
                        if card.sql_code < 0 {
                            return Err(sql_error(card.sql_code, &card.sql_state, &card.message));
                        }
                    }
                }
                Ok::<(), Db2Error>(())
            },
        )
        .await;
        if self.interrupt_coordinator.poisoned.load(Ordering::Acquire) {
            self.closed = true;
        }
        result
    }

    async fn send(
        &mut self,
        payload: &[u8],
        next_same_id: bool,
        last_packet: bool,
        correlation: Option<u16>,
    ) -> Result<(), Db2Error> {
        self.send_dss_requests(&[dss::DssRequest {
            payload,
            correlation_id: correlation,
            next_same_id,
            last_packet,
        }])
        .await
    }

    async fn send_dss_requests(
        &mut self,
        requests: &[dss::DssRequest<'_>],
    ) -> Result<(), Db2Error> {
        self.ensure_open()?;
        if requests.is_empty() || requests.iter().any(|request| request.payload.len() < 4) {
            return Err(Db2Error::protocol(
                "DSS request chain is empty or contains a short DDM object",
            ));
        }
        // Mark before polling the vectored writer: cancellation may follow a partial write.
        self.interrupt_coordinator
            .exchange_pending
            .store(true, Ordering::Release);
        let mut io = ExchangeIoGuard::new(self.interrupt_coordinator.clone());
        let next_id =
            dss::write_request_dss_chain(&mut self.stream, requests, self.correlation_id).await?;
        io.completed = true;
        self.correlation_id = next_id;
        #[cfg(feature = "bench-internals")]
        self.interrupt_coordinator
            .request_batches_sent
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn send_external_data(
        &mut self,
        external: &EncodedExternalData,
        parameters: &mut [Db2InputParameter<'_>],
        next_same_id: bool,
        last_packet: bool,
    ) -> Result<(), Db2Error> {
        match external {
            EncodedExternalData::DdmObject(object) => {
                self.send(object, next_same_id, last_packet, None).await
            }
            EncodedExternalData::LobStream {
                parameter_index,
                length,
            } => {
                let Some(input) = parameters.get_mut(*parameter_index) else {
                    return Err(Db2Error::other(
                        "streamed LOB metadata no longer matches its parameter".into(),
                    ));
                };
                let source = match input {
                    Db2InputParameter::BlobStream { source, .. }
                    | Db2InputParameter::ClobStream { source, .. }
                    | Db2InputParameter::DbClobStream { source, .. }
                    | Db2InputParameter::XmlStream { source, .. } => &mut **source,
                    Db2InputParameter::Value(_) => {
                        return Err(Db2Error::other(
                            "streamed LOB metadata no longer matches its parameter".into(),
                        ));
                    }
                };
                let current_id = self.correlation_id;
                let mut io = ExchangeIoGuard::new(self.interrupt_coordinator.clone());
                match dss::write_external_lob_dss(
                    &mut self.stream,
                    source,
                    *length,
                    current_id,
                    next_same_id,
                    last_packet,
                )
                .await
                {
                    Ok(next_id) => {
                        io.completed = true;
                        self.correlation_id = next_id;
                        Ok(())
                    }
                    Err(error) => {
                        self.closed = true;
                        let _ = self.stream.shutdown().await;
                        Err(Db2Error::other(error.to_string()))
                    }
                }
            }
        }
    }

    async fn read_reply_chain(&mut self) -> Result<Vec<DssPacket>, Db2Error> {
        let mut io = ExchangeIoGuard::new(self.interrupt_coordinator.clone());
        let mut result = Vec::new();
        let mut reply_bytes = 0usize;
        let limits = self.config.protocol_limits.clone();
        loop {
            let packet = async {
                let mut streamed =
                    Db2DssStream::open(&mut self.stream, limits.max_ddm_object_size as u64).await?;
                let header = streamed.header;
                let codepoint = streamed.codepoint;
                let mut payload = Vec::new();
                while let Some(chunk) = streamed.next_chunk().await? {
                    payload.try_reserve(chunk.len()).map_err(|error| {
                        Db2Error::resource_limit(format!(
                            "cannot reserve {} bytes for a DB2 reply payload: {error}",
                            chunk.len()
                        ))
                    })?;
                    payload.extend_from_slice(&chunk);
                }
                Ok::<DssPacket, Db2Error>(DssPacket::new(header, codepoint, payload, false))
            }
            .await;
            let packet = match packet {
                Ok(packet) => packet,
                Err(error) => {
                    // The incremental reader may have consumed part of a packet.
                    // Its transport cannot safely be reused after any read failure.
                    self.closed = true;
                    return Err(error);
                }
            };
            let next_bytes = reply_bytes.checked_add(packet.payload.len());
            if result.len() >= limits.max_reply_chain_packets
                || next_bytes.is_none_or(|bytes| bytes > limits.max_reply_chain_bytes)
            {
                self.closed = true;
                return Err(Db2Error::protocol(format!(
                    "DB2 reply chain exceeds configured limits ({} packets, {} bytes)",
                    limits.max_reply_chain_packets, limits.max_reply_chain_bytes
                )));
            }
            if result.try_reserve(1).is_err() {
                self.closed = true;
                return Err(Db2Error::protocol(
                    "unable to reserve memory for DB2 reply chain",
                ));
            }
            reply_bytes = next_bytes.expect("checked above");
            let chained = packet.header.chained;
            result.push(packet);
            if !chained {
                io.completed = true;
                self.interrupt_coordinator
                    .exchange_pending
                    .store(false, Ordering::Release);
                if result.iter().any(|reply| reply.codepoint == cp::DTAMCHRM) {
                    return Err(Db2Error::other(
                        "DB2 server returned DTAMCHRM (SQLDTA descriptor and parameter data do not match)".into(),
                    ));
                }
                return Ok(result);
            }
        }
    }

    fn ensure_open(&self) -> Result<(), Db2Error> {
        if self.is_closed() {
            Err(Db2Error::connection_closed("DB2 connection is closed"))
        } else {
            Ok(())
        }
    }
}

struct BatchChunkOutcome {
    completed: usize,
    rows_affected: i64,
}

struct BatchChunkFailure {
    cause: Db2Error,
    failed_index: usize,
    completed: usize,
    rows_affected: i64,
}

fn timeout_duration(timeout_ms: i64) -> Duration {
    Duration::from_millis(if timeout_ms > 0 {
        timeout_ms as u64
    } else {
        30_000
    })
}

async fn write_import_batch(
    transaction: &mut Db2Transaction<'_>,
    statement: &Db2Statement,
    rows: Vec<(u64, Vec<Db2Value>)>,
    command_options: Db2CommandOptions,
) -> Result<(), (Db2Error, u64)> {
    let (absolute_rows, parameter_sets): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
    if let Err(error) = transaction
        .session
        .execute_prepared_batch_for_import(statement, &parameter_sets, command_options)
        .await
    {
        let failed_row = error
            .failed_parameter_set_index
            .and_then(|index| absolute_rows.get(index).copied())
            .or_else(|| absolute_rows.first().copied())
            .unwrap_or_default();
        return Err((error.cause, failed_row));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ImportLobKind {
    Blob,
    Clob,
    DbClob,
    Xml,
}

struct ImportLobSpool {
    // Keep the file handle before the path guard so it closes before TempPath unlinks it.
    file: tokio::fs::File,
    _path: TempPath,
    length: u64,
    kind: ImportLobKind,
    from_locator: bool,
    reserved: crate::resources::Db2ResourceReservation,
}

impl ImportLobSpool {
    fn new(kind: ImportLobKind) -> Result<Self, Db2Error> {
        let (file, path) = NamedTempFile::new()
            .map_err(|error| {
                Db2Error::other(format!(
                    "unable to create a private temporary file for DB2 LOB import: {error}"
                ))
            })?
            .into_parts();
        Ok(Self {
            file: tokio::fs::File::from_std(file),
            _path: path,
            length: 0,
            kind,
            from_locator: false,
            reserved: crate::resources::spool_budget().reservation(),
        })
    }

    async fn append(&mut self, bytes: &[u8]) -> Result<(), Db2Error> {
        if self.length.saturating_add(bytes.len() as u64) > crate::resources::MAX_STREAMED_LOB_BYTES
        {
            return Err(Db2Error::resource_limit(
                "Db2 LOB exceeds streamed-value limit",
            ));
        }
        self.reserved.grow(bytes.len() as u64)?;
        for chunk in bytes.chunks(crate::resources::CHUNK_BYTES) {
            self.file.write_all(chunk).await?;
        }
        self.length += bytes.len() as u64;
        crate::diagnostics::record("spool", Duration::ZERO, true, bytes.len() as u64);
        Ok(())
    }

    async fn rewind(&mut self) -> Result<(), Db2Error> {
        self.file.flush().await.map_err(|error| {
            Db2Error::other(format!(
                "unable to flush the temporary DB2 LOB spool: {error}"
            ))
        })?;
        self.file.seek(SeekFrom::Start(0)).await.map_err(|error| {
            Db2Error::other(format!(
                "unable to rewind the temporary DB2 LOB spool: {error}"
            ))
        })?;
        Ok(())
    }
}

#[derive(Default)]
struct Utf16BeTranscoder {
    pending_byte: Option<u8>,
    pending_high_surrogate: Option<u16>,
}

impl Utf16BeTranscoder {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<u8>, Db2Error> {
        let mut input = Vec::with_capacity(bytes.len() + usize::from(self.pending_byte.is_some()));
        if let Some(pending) = self.pending_byte.take() {
            input.push(pending);
        }
        input.extend_from_slice(bytes);
        if input.len() % 2 != 0 {
            self.pending_byte = input.pop();
        }

        let mut output = Vec::with_capacity(input.len().saturating_mul(2));
        for pair in input.as_chunks::<2>().0 {
            let unit = u16::from_be_bytes([pair[0], pair[1]]);
            let scalar = match self.pending_high_surrogate.take() {
                Some(high) if (0xdc00..=0xdfff).contains(&unit) => {
                    0x1_0000 + (((high as u32 - 0xd800) << 10) | (unit as u32 - 0xdc00))
                }
                Some(_) => {
                    return Err(Db2Error::other(
                        "DB2 progressive DBCLOB contains an invalid UTF-16 surrogate pair".into(),
                    ));
                }
                None if (0xd800..=0xdbff).contains(&unit) => {
                    self.pending_high_surrogate = Some(unit);
                    continue;
                }
                None if (0xdc00..=0xdfff).contains(&unit) => {
                    return Err(Db2Error::other(
                        "DB2 progressive DBCLOB contains an unpaired UTF-16 low surrogate".into(),
                    ));
                }
                None => unit as u32,
            };
            let character = char::from_u32(scalar).ok_or_else(|| {
                Db2Error::other("DB2 progressive DBCLOB contains invalid UTF-16".into())
            })?;
            let mut encoded = [0; 4];
            output.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
        }
        Ok(output)
    }

    fn finish(self) -> Result<(), Db2Error> {
        if self.pending_byte.is_some() {
            return Err(Db2Error::other(
                "DB2 progressive DBCLOB ended with an incomplete UTF-16 code unit".into(),
            ));
        }
        if self.pending_high_surrogate.is_some() {
            return Err(Db2Error::other(
                "DB2 progressive DBCLOB ended with an unpaired UTF-16 high surrogate".into(),
            ));
        }
        Ok(())
    }
}

fn import_lob_kind(sql_type: u8) -> Option<ImportLobKind> {
    if is_dbclob_type(sql_type) {
        Some(ImportLobKind::DbClob)
    } else if matches!(sql_type, 0xc6 | 0xc7) {
        Some(ImportLobKind::Xml)
    } else if is_clob_type(sql_type) {
        Some(ImportLobKind::Clob)
    } else if is_lob_type(sql_type) {
        Some(ImportLobKind::Blob)
    } else {
        None
    }
}

fn destination_lob_type(kind: ImportLobKind) -> u16 {
    match kind {
        ImportLobKind::Blob => 404,
        ImportLobKind::Clob => 408,
        ImportLobKind::DbClob => 412,
        ImportLobKind::Xml => 988,
    }
}

fn import_lob_kind_from_column_type(sql_type: u16) -> Option<ImportLobKind> {
    match sql_type & !1 {
        404 => Some(ImportLobKind::Blob),
        408 => Some(ImportLobKind::Clob),
        412 => Some(ImportLobKind::DbClob),
        988 => Some(ImportLobKind::Xml),
        _ => None,
    }
}

fn base_import_sql_type(sql_type: u16) -> u16 {
    sql_type & !1
}

fn row_has_materialized_import_lob(
    source: &Db2RowReader<'_>,
    row: &[Db2Value],
    statement: &Db2Statement,
) -> bool {
    row.iter().enumerate().any(|(index, value)| {
        let Some(kind) = source
            .columns()
            .get(index)
            .and_then(|column| import_lob_kind_from_column_type(column.sql_type))
        else {
            return false;
        };
        let Some(target_type) = statement
            .parameter_columns
            .get(index)
            .map(|column| base_import_sql_type(column.sql_type))
        else {
            return false;
        };
        if target_type != destination_lob_type(kind) {
            return false;
        }
        match (kind, value) {
            (ImportLobKind::Blob, Db2Value::Bytes(bytes)) => !bytes.is_empty(),
            (
                ImportLobKind::Clob | ImportLobKind::DbClob | ImportLobKind::Xml,
                Db2Value::Text(text),
            ) => !text.is_empty(),
            _ => false,
        }
    })
}

async fn spool_import_lobs(
    source: &mut Db2RowReader<'_>,
    row: &[Db2Value],
    statement: &Db2Statement,
) -> Result<Vec<Option<ImportLobSpool>>, Db2Error> {
    let mut spools = std::iter::repeat_with(|| None)
        .take(row.len())
        .collect::<Vec<Option<ImportLobSpool>>>();
    for (index, value) in row.iter().enumerate() {
        let target_type = statement
            .parameter_columns
            .get(index)
            .map(|column| base_import_sql_type(column.sql_type))
            .ok_or_else(|| Db2Error::other("DB2 import LOB has no target parameter".into()))?;
        if let Db2Value::Lob(locator) = value {
            let kind = import_lob_kind(locator.sql_type).ok_or_else(|| {
                Db2Error::other(format!(
                    "DB2 import does not recognize progressive LOB SQL type 0x{:02X}",
                    locator.sql_type
                ))
            })?;
            if target_type != destination_lob_type(kind) {
                return Err(Db2Error::other(format!(
                    "DB2 import source LOB SQL type 0x{:02X} requires destination SQL type {}, got {}",
                    locator.sql_type,
                    destination_lob_type(kind),
                    target_type
                )));
            }

            let expected_encoding = match kind {
                ImportLobKind::Blob => Db2LobEncoding::Binary,
                ImportLobKind::Clob | ImportLobKind::Xml => Db2LobEncoding::Utf8,
                ImportLobKind::DbClob => Db2LobEncoding::Utf16Be,
            };
            if locator.encoding != expected_encoding {
                return Err(Db2Error::other(format!(
                    "DB2 import progressive LOB type 0x{:02X} has unexpected {:?} encoding",
                    locator.sql_type, locator.encoding
                )));
            }

            let mut spool = ImportLobSpool::new(kind)?;
            spool.from_locator = true;
            let mut reader = source.open_lob(locator).await?;
            if kind == ImportLobKind::DbClob {
                let mut transcoder = Utf16BeTranscoder::default();
                while let Some(chunk) = reader.next_chunk().await? {
                    let utf8 = transcoder.push(&chunk)?;
                    spool.append(&utf8).await?;
                }
                transcoder.finish()?;
            } else {
                while let Some(chunk) = reader.next_chunk().await? {
                    spool.append(&chunk).await?;
                }
            }
            let _ = reader;
            spool.rewind().await?;
            spools[index] = Some(spool);
            continue;
        }

        let Some(kind) = source
            .columns()
            .get(index)
            .and_then(|column| import_lob_kind_from_column_type(column.sql_type))
        else {
            continue;
        };
        if target_type != destination_lob_type(kind) {
            continue;
        }
        let bytes = match (kind, value) {
            (ImportLobKind::Blob, Db2Value::Bytes(bytes)) => bytes.as_slice(),
            (
                ImportLobKind::Clob | ImportLobKind::DbClob | ImportLobKind::Xml,
                Db2Value::Text(text),
            ) => text.as_bytes(),
            _ => continue,
        };
        if bytes.is_empty() {
            // Empty materialized LOBs use the inline SQLDTA representation. A zero-length
            // EXTDTA stream is not treated consistently by Db2 for CLOB/DBCLOB input.
            continue;
        }
        let mut spool = ImportLobSpool::new(kind)?;
        spool.append(bytes).await?;
        spool.rewind().await?;
        spools[index] = Some(spool);
    }
    Ok(spools)
}

fn progressive_import_spool_bytes(spools: &[Option<ImportLobSpool>]) -> u64 {
    spools
        .iter()
        .flatten()
        .filter(|spool| spool.from_locator)
        .fold(0u64, |total, spool| total.saturating_add(spool.length))
}

async fn write_import_lob_row(
    transaction: &mut Db2Transaction<'_>,
    statement: &Db2Statement,
    row: &[Db2Value],
    spools: &mut [Option<ImportLobSpool>],
    command_options: Db2CommandOptions,
) -> Result<(), Db2Error> {
    if row.len() != spools.len() {
        return Err(Db2Error::other(
            "DB2 import LOB spool layout no longer matches its source row".into(),
        ));
    }
    let empty_blob = Db2Value::Bytes(Vec::new());
    let mut inputs = Vec::with_capacity(row.len());
    for (value, spool) in row.iter().zip(spools.iter_mut()) {
        let Some(spool) = spool else {
            if matches!(value, Db2Value::Lob(_)) {
                return Err(Db2Error::other(
                    "DB2 import progressive LOB was not spooled before row insertion".into(),
                ));
            }
            inputs.push(Db2InputParameter::Value(value));
            continue;
        };
        if spool.length == 0 && matches!(spool.kind, ImportLobKind::Blob) {
            inputs.push(Db2InputParameter::Value(&empty_blob));
            continue;
        }
        let source = &mut spool.file;
        let input = match spool.kind {
            ImportLobKind::Blob => Db2InputParameter::BlobStream {
                source,
                length: spool.length,
            },
            ImportLobKind::Clob => Db2InputParameter::ClobStream {
                source,
                length: spool.length,
            },
            ImportLobKind::DbClob => Db2InputParameter::DbClobStream {
                source,
                length: spool.length,
            },
            ImportLobKind::Xml => Db2InputParameter::XmlStream {
                source,
                length: spool.length,
            },
        };
        inputs.push(input);
    }
    let affected = transaction
        .session
        .execute_prepared_with_blob_streams_and_options(statement, &mut inputs, command_options)
        .await?;
    if affected != 1 {
        return Err(Db2Error::protocol(format!(
            "Db2 confirmed {affected} inserted rows; expected one for an import LOB row"
        )));
    }
    Ok(())
}

async fn rollback_import_transaction(
    transaction: Db2Transaction<'_>,
    mut error: Db2ImportError,
) -> Db2ImportError {
    if let Err(cleanup) = transaction.rollback().await {
        error.cause = error.cause.with_cleanup(cleanup);
    }
    error
}

async fn commit_import_batch<F>(
    destination: &mut Db2Session,
    batch: &mut ImportBatch,
    statement: &Db2Statement,
    command_options: Db2CommandOptions,
    progress: &mut Db2ImportProgress,
    before_commit: &mut F,
    last_notified_row: u64,
) -> Result<(), Db2ImportError>
where
    F: FnMut(Db2ImportProgress) -> Result<(), Db2Error>,
{
    let estimated_bytes = batch.estimated_bytes() as u64;
    let rows = batch.take();
    let Some((first_row, _)) = rows.first() else {
        return Ok(());
    };
    let committed_rows = rows.len() as u64;
    let transaction = destination
        .begin_transaction()
        .map_err(|cause| Db2ImportError::new(cause, Some(*first_row), *progress))?;
    let mut transaction = transaction;
    if let Err((cause, failed_row)) =
        write_import_batch(&mut transaction, statement, rows, command_options).await
    {
        let error = Db2ImportError::new(cause, Some(failed_row), *progress);
        return Err(rollback_import_transaction(transaction, error).await);
    }
    if progress.source_rows_read != last_notified_row
        && let Err(cause) = before_commit(*progress)
    {
        let error = Db2ImportError::new(cause, None, *progress);
        return Err(rollback_import_transaction(transaction, error).await);
    }
    if let Err(cause) = transaction.commit().await {
        return Err(Db2ImportError::new(cause, None, *progress));
    }
    progress.committed_rows = progress.committed_rows.saturating_add(committed_rows);
    progress.committed_batches = progress.committed_batches.saturating_add(1);
    progress.estimated_committed_bytes = progress
        .estimated_committed_bytes
        .saturating_add(estimated_bytes);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn commit_import_batch_with_lob_row<F>(
    destination: &mut Db2Session,
    batch: &mut ImportBatch,
    statement: &Db2Statement,
    row: Vec<Db2Value>,
    spools: &mut [Option<ImportLobSpool>],
    absolute_row: u64,
    estimated_bytes: u64,
    command_options: Db2CommandOptions,
    progress: &mut Db2ImportProgress,
    before_commit: &mut F,
    last_notified_row: u64,
) -> Result<(), Db2ImportError>
where
    F: FnMut(Db2ImportProgress) -> Result<(), Db2Error>,
{
    let rows = batch.take();
    let committed_rows = rows.len() as u64 + 1;
    let failed_row_if_begin_fails = rows
        .first()
        .map_or(absolute_row, |(first_row, _)| *first_row);
    let mut transaction = destination
        .begin_transaction()
        .map_err(|cause| Db2ImportError::new(cause, Some(failed_row_if_begin_fails), *progress))?;
    if let Err((cause, failed_row)) =
        write_import_batch(&mut transaction, statement, rows, command_options).await
    {
        let error = Db2ImportError::new(cause, Some(failed_row), *progress);
        return Err(rollback_import_transaction(transaction, error).await);
    }
    if let Err(cause) =
        write_import_lob_row(&mut transaction, statement, &row, spools, command_options).await
    {
        let error = Db2ImportError::new(cause, Some(absolute_row), *progress);
        return Err(rollback_import_transaction(transaction, error).await);
    }
    if progress.source_rows_read != last_notified_row
        && let Err(cause) = before_commit(*progress)
    {
        let error = Db2ImportError::new(cause, None, *progress);
        return Err(rollback_import_transaction(transaction, error).await);
    }
    if let Err(cause) = transaction.commit().await {
        return Err(Db2ImportError::new(cause, None, *progress));
    }
    progress.committed_rows = progress.committed_rows.saturating_add(committed_rows);
    progress.committed_batches = progress.committed_batches.saturating_add(1);
    progress.estimated_committed_bytes = progress
        .estimated_committed_bytes
        .saturating_add(estimated_bytes);
    Ok(())
}

fn escape_sql_value(value: &str) -> String {
    sanitize_client_register(value).replace('\'', "''")
}

fn sanitize_client_register(value: &str) -> String {
    value
        .chars()
        .filter(|character| !matches!(character, '\0' | '\r' | '\n'))
        .collect()
}

fn sanitize_client_info(info: Db2ClientInfo) -> Db2ClientInfo {
    Db2ClientInfo {
        application_name: sanitize_client_register(&info.application_name),
        workstation_name: sanitize_client_register(&info.workstation_name),
        user_id: sanitize_client_register(&info.user_id),
        accounting: sanitize_client_register(&info.accounting),
        correlation_token: sanitize_client_register(&info.correlation_token),
    }
}
fn validate_user_identifier(value: &str) -> Result<String, Db2Error> {
    let target = value.trim();
    if target.is_empty()
        || target.len() > 128
        || !target
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'#' | b'$'))
    {
        return Err(Db2Error::new(
            Db2ErrorKind::InvalidConfiguration,
            format!("invalid DB2 session user identifier {value:?}"),
        ));
    }
    Ok(target.to_string())
}

/// Quotes a schema identifier as a delimited Db2 identifier.
fn quote_identifier(value: &str) -> Result<String, Db2Error> {
    if value.is_empty() || value.len() > 128 || value.contains('\0') {
        return Err(Db2Error::other(format!("invalid DB2 identifier {value:?}")));
    }
    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}

fn security_enum(value: u16) -> Result<SecurityMechanism, Db2Error> {
    match value {
        3 => Ok(SecurityMechanism::USRIDPWD),
        4 => Ok(SecurityMechanism::USRIDONL),
        9 => Ok(SecurityMechanism::EUSRIDPWD),
        11 => Ok(SecurityMechanism::KERBEROS),
        _ => Err(Db2Error::protocol(format!(
            "unsupported DB2 security mechanism {value}"
        ))),
    }
}

/// Builds the trust store for server certificate validation. A configured CA file replaces the
/// system roots entirely, so only that CA can vouch for the Db2 server.
fn build_root_store(config: &Config) -> Result<RootCertStore, Db2Error> {
    let mut roots = RootCertStore::empty();
    if config.ssl_root_ca_path.is_empty() {
        for cert in load_native_certs().certs {
            roots.add(cert).map_err(|error| {
                Db2Error::other(format!("invalid system TLS certificate: {error}"))
            })?;
        }
        return Ok(roots);
    }
    let mut file = BufReader::new(File::open(&config.ssl_root_ca_path)?);
    let certificates = CertificateDer::pem_reader_iter(&mut file)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| Db2Error::other(format!("failed to read Db2 CA certificate: {error}")))?;
    if certificates.is_empty() {
        return Err(Db2Error::other(format!(
            "Db2 CA file contains no certificates: {}",
            config.ssl_root_ca_path
        )));
    }
    for cert in certificates {
        roots
            .add(cert)
            .map_err(|error| Db2Error::other(format!("invalid Db2 CA certificate: {error}")))?;
    }
    Ok(roots)
}

async fn connect_tls(
    socket: TcpStream,
    config: &Config,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, Db2Error> {
    let builder = ClientConfig::builder().with_root_certificates(build_root_store(config)?);
    let client = if !config.ssl_client_cert_path.is_empty() {
        let mut cert_file = BufReader::new(File::open(&config.ssl_client_cert_path)?);
        let certificates = CertificateDer::pem_reader_iter(&mut cert_file)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                Db2Error::other(format!("failed to read Db2 client certificate: {error}"))
            })?;
        let mut key_file = BufReader::new(File::open(&config.ssl_client_key_path)?);
        let key: PrivateKeyDer<'static> = PrivateKeyDer::pem_reader_iter(&mut key_file)
            .next()
            .ok_or_else(|| {
                Db2Error::other("Db2 client key file has no supported private key".into())
            })?
            .map_err(|error| Db2Error::other(format!("failed to read Db2 client key: {error}")))?;
        builder
            .with_client_auth_cert(certificates, key)
            .map_err(|error| Db2Error::other(format!("invalid Db2 client identity: {error}")))?
    } else {
        builder.with_no_client_auth()
    };
    let name = ServerName::try_from(config.host.clone())
        .map_err(|_| Db2Error::other("DB2 TLS hostname is invalid".into()))?;
    let connector = TlsConnector::from(std::sync::Arc::new(client));
    timeout(
        timeout_duration(config.timeout_ms),
        connector.connect(name, socket),
    )
    .await
    .map_err(|_| Db2Error::other("DB2 TLS handshake timed out".into()))?
    .map_err(|error| Db2Error::other(format!("DB2 TLS handshake failed: {error}")))
}

fn decode_procedure_cursor_available(
    cursor: &mut ProcedureCursorState,
    max_materialized_lob_size: usize,
) -> Result<Vec<crate::Db2Warning>, Db2Error> {
    let mut rows = std::mem::take(&mut cursor.buffered_rows)
        .into_iter()
        .collect::<Vec<_>>();
    let external_used = stitch_extdta_limited(
        &cursor.fields,
        &mut rows,
        &cursor.extdta,
        false,
        max_materialized_lob_size,
    )?;
    if external_used > 0 {
        cursor.extdta.drain(..external_used);
    }

    let (mut newly_decoded, consumed, warnings) =
        decode_rows_incremental(&cursor.fields, cursor.row_data.as_slice())?;
    if consumed > 0 {
        cursor.row_data.consume(consumed);
    }
    if let Some(terminal_len) = no_data_sqlca_len(cursor.row_data.as_slice()) {
        cursor.row_data.consume(terminal_len);
        cursor.ended = true;
    }
    rows.append(&mut newly_decoded);
    let external_used = stitch_extdta_limited(
        &cursor.fields,
        &mut rows,
        &cursor.extdta,
        false,
        max_materialized_lob_size,
    )?;
    if external_used > 0 {
        cursor.extdta.drain(..external_used);
    }
    cursor.buffered_rows = rows.into();
    Ok(warnings)
}

/// Returns the length of a complete terminal row SQLCA (+100 followed by a
/// null row indicator). Db2 LUW can append this marker directly after the last
/// QRYDTA row without ENDQRYRM or a separate SQLCARD.
fn no_data_sqlca_len(data: &[u8]) -> Option<usize> {
    if data.first().copied()? != 0 {
        return None;
    }
    let mut offset = 1usize;
    if data.len().saturating_sub(offset) < 18 {
        return None;
    }
    let code = i32::from_le_bytes(data[offset..offset + 4].try_into().ok()?);
    offset += 17;
    let x_group = *data.get(offset)?;
    offset += 1;
    if x_group == 0 {
        if data.len().saturating_sub(offset) < 35 {
            return None;
        }
        offset += 35;
        for _ in 0..3 {
            let length =
                u16::from_be_bytes(data.get(offset..offset + 2)?.try_into().ok()?) as usize;
            offset += 2;
            offset = offset.checked_add(length)?;
            if offset > data.len() {
                return None;
            }
        }
    } else if x_group != 0xff {
        return None;
    }
    if *data.get(offset)? != 0xff || code != 100 {
        return None;
    }
    offset += 1;
    if *data.get(offset)? != 0xff {
        return None;
    }
    Some(offset + 1)
}

fn consume_procedure_cursor_replies(
    replies: Vec<DssPacket>,
    cursor: &mut ProcedureCursorState,
    warnings: &mut Vec<crate::Db2Warning>,
) -> Result<Option<dss::DssPayload>, Db2Error> {
    let mut last_qrydta = None;
    for reply in replies {
        match reply.codepoint {
            value if value == cp::QRYDTA => {
                cursor.row_data.append(reply.payload.as_slice());
                last_qrydta = Some(reply.payload);
            }
            value if value == cp::EXTDTA => cursor
                .extdta
                .push_back(reply.payload.try_into_vec().map_err(Db2Error::from)?),
            value if value == cp::ENDQRYRM => cursor.ended = true,
            value if value == cp::OPNQRYRM => {
                cursor.next_correlation = reply.header.correlation_id;
                if let Some(id) =
                    ddm::find_ddm_parameter(&reply.payload, cp::QRYINSID).map_err(Db2Error::from)?
                    && id.len() >= 8
                {
                    cursor.query_id = u64::from_be_bytes(id[..8].try_into().unwrap());
                    cursor.query_id_present = true;
                }
            }
            value if value == cp::SQLCARD => {
                let card = ddm::parse_sql_card(&reply.payload, true)?;
                if card.sql_code < 0 || card.sql_code == 464 {
                    return Err(sql_error(card.sql_code, &card.sql_state, &card.message));
                }
                if card.sql_code == 100 {
                    cursor.ended = true;
                }
                if let Some(warning) = card.warning() {
                    warnings.push(warning);
                }
            }
            value if value == cp::SQLERRRM => return Err(server_error(&reply.payload)),
            _ => (),
        }
    }
    Ok(last_qrydta)
}

#[cfg(test)]
fn apply_procedure_output_parameters(
    payload: &[u8],
    directions: &[Db2ParameterDirection],
    output_parameters: &mut [Db2Value],
    external_data: &[Vec<u8>],
) -> Result<Vec<crate::Db2Warning>, Db2Error> {
    apply_procedure_output_parameters_limited(
        payload,
        directions,
        output_parameters,
        external_data,
        usize::MAX,
    )
}

fn apply_procedure_output_parameters_limited(
    payload: &[u8],
    directions: &[Db2ParameterDirection],
    output_parameters: &mut [Db2Value],
    external_data: &[Vec<u8>],
    max_materialized_lob_size: usize,
) -> Result<Vec<crate::Db2Warning>, Db2Error> {
    let (fields, values) = ddm::parse_sql_dta_reply(payload).map_err(Db2Error::from)?;
    if fields.is_empty() || values.is_empty() {
        if !external_data.is_empty() {
            return Err(Db2Error::other(
                "DB2 returned output-parameter EXTDTA without decodable SQLDTARD values".into(),
            ));
        }
        if payload.is_empty() {
            ensure_output_parameter_data_returned(directions, false)?;
            return Ok(Vec::new());
        }
        return Err(Db2Error::other(format!(
            "DB2 SQLDTARD lacked output descriptor/data (descriptor fields {}, data bytes {})",
            fields.len(),
            values.len()
        )));
    }
    let row_data = if values[0] == 0xff {
        values
    } else {
        [vec![0xff, 0], values].concat()
    };
    let (mut rows, warnings) = decode_rows_with_warnings(&fields, &row_data)?;
    if rows.is_empty() {
        if !external_data.is_empty() {
            return Err(Db2Error::other(
                "DB2 returned output-parameter EXTDTA without SQLDTARD rows".into(),
            ));
        }
        ensure_output_parameter_data_returned(directions, false)?;
        return Ok(warnings);
    }
    let external_used = stitch_extdta_limited(
        &fields,
        &mut rows,
        external_data,
        true,
        max_materialized_lob_size,
    )?;
    if external_used != external_data.len() || has_unresolved_external_locator(&fields, &rows) {
        return Err(Db2Error::other(
            "DB2 output-parameter EXTDTA values do not match SQLDTARD LOB descriptors".into(),
        ));
    }
    if let Some(decoded) = rows.first() {
        // A short SQLDTARD row would leave a requested output position at its
        // placeholder value without any error. Go skips those positions silently;
        // reject the reply instead of reporting a missing output as null.
        let last_requested = directions
            .iter()
            .enumerate()
            .filter(|(_, direction)| **direction != Db2ParameterDirection::Input)
            .map(|(index, _)| index)
            .next_back();
        if let Some(last_requested) = last_requested
            && last_requested >= decoded.len()
        {
            return Err(Db2Error::other(format!(
                "DB2 returned output-parameter SQLDTARD values for {} parameters, but position {} was requested as an output",
                decoded.len(),
                last_requested
            )));
        }
        for index in 0..output_parameters
            .len()
            .min(decoded.len())
            .min(directions.len())
        {
            if directions[index] != Db2ParameterDirection::Input {
                output_parameters[index] = decoded[index].clone();
            }
        }
    }
    Ok(warnings)
}

fn ensure_output_parameter_data_returned(
    directions: &[Db2ParameterDirection],
    data_returned: bool,
) -> Result<(), Db2Error> {
    if data_returned {
        return Ok(());
    }
    for (index, direction) in directions.iter().enumerate().rev() {
        if *direction == Db2ParameterDirection::Input {
            continue;
        }
        return Err(Db2Error::other(format!(
            "DB2 returned no SQLDTARD values for requested output parameter at position {index}"
        )));
    }
    Ok(())
}

fn maximum_procedure_query_data_payload(block_size: u32) -> usize {
    block_size.saturating_sub(if block_size > 0x7fff { 14 } else { 10 }) as usize
}

fn is_call_statement(sql: &str) -> bool {
    let mut rest = sql.strip_prefix('\u{feff}').unwrap_or(sql).trim_start();
    loop {
        if let Some(after_start) = rest.strip_prefix("/*") {
            let Some(end) = after_start.find("*/") else {
                return false;
            };
            rest = after_start[end + 2..].trim_start();
            continue;
        }
        if let Some(after_start) = rest.strip_prefix("--") {
            let newline = after_start.find(['\r', '\n']);
            let Some(end) = newline else {
                return false;
            };
            rest = after_start[end + 1..].trim_start();
            continue;
        }
        break;
    }
    let Some(keyword) = rest.get(..4) else {
        return false;
    };
    if !keyword.eq_ignore_ascii_case("CALL") {
        return false;
    }
    rest.get(4..)
        .and_then(|tail| tail.chars().next())
        .is_none_or(|next| next.is_whitespace() || next == '(')
}

fn consume_stream_replies(
    replies: Vec<DssPacket>,
    state: &mut QueryStreamState,
) -> Result<(Vec<Vec<Db2Value>>, Vec<crate::Db2Warning>), Db2Error> {
    let mut warnings = consume_stream_reply_metadata(replies, state)?;
    let (mut rows, consumed, row_warnings) =
        match decode_rows_incremental(&state.fields, state.pending_row_data.as_slice()) {
            Ok(result) => result,
            Err(error) => {
                state.failed = true;
                return Err(error);
            }
        };
    warnings.extend(row_warnings);
    state.pending_row_data.consume(consumed);
    let extdta_used = stitch_extdta_limited(
        &state.fields,
        &mut rows,
        &state.extdta,
        false,
        state.max_materialized_lob_size,
    )?;
    if extdta_used > 0 {
        state.extdta.drain(..extdta_used);
    }
    Ok((rows, warnings))
}

fn consume_stream_replies_flat(
    replies: Vec<DssPacket>,
    state: &mut QueryStreamState,
) -> Result<(FlatRowBatch, Vec<crate::Db2Warning>), Db2Error> {
    let mut warnings = consume_stream_reply_metadata(replies, state)?;
    let (mut rows, consumed, row_warnings) =
        match decode_rows_flat_incremental(&state.fields, state.pending_row_data.as_slice()) {
            Ok(result) => result,
            Err(error) => {
                state.failed = true;
                return Err(error);
            }
        };
    warnings.extend(row_warnings);
    state.pending_row_data.consume(consumed);
    let extdta_used = stitch_extdta_flat_limited(
        &state.fields,
        &mut rows,
        &state.extdta,
        false,
        state.max_materialized_lob_size,
    )?;
    if extdta_used > 0 {
        state.extdta.drain(..extdta_used);
    }
    Ok((rows, warnings))
}

fn consume_stream_reply_metadata(
    replies: Vec<DssPacket>,
    state: &mut QueryStreamState,
) -> Result<Vec<crate::Db2Warning>, Db2Error> {
    let mut warnings = Vec::new();
    for reply in replies {
        match reply.codepoint {
            value if value == cp::SQLDARD => {
                state.columns = ddm::parse_sql_dard(&reply.payload, true)?
            }
            value if value == cp::QRYDSC => {
                state.fields = ddm::parse_query_descriptor(&reply.payload)?
            }
            value if value == cp::QRYDTA => state.pending_row_data.append(&reply.payload),
            value if value == cp::EXTDTA => state
                .extdta
                .push_back(reply.payload.try_into_vec().map_err(Db2Error::from)?),
            value if value == cp::OPNQRYRM => {
                if let Some(id) = ddm::find_ddm_parameter(&reply.payload, cp::QRYINSID)?
                    && id.len() >= 8
                {
                    state.query_id = u64::from_be_bytes(id[..8].try_into().unwrap());
                }
                state.open = true;
                state.next_correlation = reply.header.correlation_id;
            }
            value if value == cp::ENDQRYRM => state.ended = true,
            value if value == cp::SQLCARD => {
                let card = ddm::parse_sql_card(&reply.payload, true)?;
                if card.sql_code < 0 {
                    state.ended = true;
                    state.failed = true;
                    return Err(sql_error(card.sql_code, &card.sql_state, &card.message));
                }
                if card.sql_code == 100 {
                    state.ended = true;
                }
                if let Some(warning) = card.warning() {
                    warnings.push(warning);
                }
            }
            value if value == cp::SQLERRRM => {
                state.ended = true;
                state.failed = true;
                return Err(server_error(&reply.payload));
            }
            _ => (),
        }
    }
    Ok(warnings)
}

fn decode_rows_with_warnings(
    fields: &[FieldDescriptor],
    data: &[u8],
) -> Result<(Vec<Vec<Db2Value>>, Vec<crate::Db2Warning>), Db2Error> {
    decode_rows_incremental(fields, data).map(|(rows, _, warnings)| (rows, warnings))
}

/// Decoded rows with the consumed byte count and collected warnings.
type DecodedRowsWithWarnings = (Vec<Vec<Db2Value>>, usize, Vec<crate::Db2Warning>);

fn decode_rows_incremental(
    fields: &[FieldDescriptor],
    data: &[u8],
) -> Result<DecodedRowsWithWarnings, Db2Error> {
    let (rows, consumed, warnings) =
        decode_rows_incremental_into(fields, data, NestedRowSink::default())?;
    Ok((rows.rows, consumed, warnings))
}

#[derive(Default)]
struct NestedRowSink {
    rows: Vec<Vec<Db2Value>>,
    current: Vec<Db2Value>,
}

impl RowValueSink for NestedRowSink {
    fn begin_row(&mut self, columns: usize) -> usize {
        self.current = Vec::with_capacity(columns);
        0
    }

    fn push_value(&mut self, value: Db2Value) {
        self.current.push(value);
    }

    fn rollback_row(&mut self, mark: usize) {
        self.current.truncate(mark);
    }

    fn finish_row(&mut self, _mark: usize) {
        self.rows.push(std::mem::take(&mut self.current));
    }
}

#[derive(Default)]
struct FlatRowBatch {
    values: Vec<Db2Value>,
    rows: usize,
    columns: usize,
    next_row: usize,
}

impl FlatRowBatch {
    #[cfg(any(test, feature = "bench-internals"))]
    fn row(&self, index: usize) -> Option<&[Db2Value]> {
        if index >= self.rows || self.columns == 0 {
            return None;
        }
        let start = index.checked_mul(self.columns)?;
        self.values.get(start..start.checked_add(self.columns)?)
    }

    fn row_mut(&mut self, index: usize) -> Option<&mut [Db2Value]> {
        if index >= self.rows || self.columns == 0 {
            return None;
        }
        let start = index.checked_mul(self.columns)?;
        self.values.get_mut(start..start.checked_add(self.columns)?)
    }
}

impl RowValueSink for FlatRowBatch {
    fn begin_row(&mut self, columns: usize) -> usize {
        if self.rows == 0 {
            self.columns = columns;
        } else {
            debug_assert_eq!(self.columns, columns);
        }
        self.values.len()
    }

    fn push_value(&mut self, value: Db2Value) {
        self.values.push(value);
    }

    fn rollback_row(&mut self, mark: usize) {
        self.values.truncate(mark);
    }

    fn finish_row(&mut self, _mark: usize) {
        self.rows += 1;
    }
}

#[derive(Default)]
struct RowBatchQueue {
    batches: VecDeque<FlatRowBatch>,
}

impl RowBatchQueue {
    fn from_batch(batch: FlatRowBatch) -> Self {
        let mut queue = Self::default();
        queue.push(batch);
        queue
    }

    #[cfg(test)]
    fn from_rows(rows: Vec<Vec<Db2Value>>) -> Self {
        let mut queue = Self::default();
        if let Some(first) = rows.first() {
            let columns = first.len();
            let mut batch = FlatRowBatch {
                values: Vec::with_capacity(rows.len().saturating_mul(columns)),
                rows: 0,
                columns,
                next_row: 0,
            };
            for row in rows {
                batch.values.extend(row);
                batch.rows += 1;
            }
            queue.push(batch);
        }
        queue
    }

    fn push(&mut self, batch: FlatRowBatch) {
        self.discard_consumed_front();
        if batch.next_row < batch.rows {
            self.batches.push_back(batch);
        }
    }

    fn has_next(&mut self) -> bool {
        self.discard_consumed_front();
        self.batches
            .front()
            .is_some_and(|batch| batch.next_row < batch.rows)
    }

    fn pop_owned_row(&mut self) -> Option<Vec<Db2Value>> {
        self.discard_consumed_front();
        let batch = self.batches.front_mut()?;
        let index = batch.next_row;
        let range_start = index.checked_mul(batch.columns)?;
        let range_end = range_start.checked_add(batch.columns)?;
        let row = batch
            .values
            .get_mut(range_start..range_end)?
            .iter_mut()
            .map(|value| std::mem::replace(value, Db2Value::Null))
            .collect();
        batch.next_row += 1;
        self.discard_consumed_front();
        Some(row)
    }

    fn next_ref(&mut self, state: &mut QueryStreamState) -> Option<&[Db2Value]> {
        self.discard_consumed_front();
        let batch = self.batches.front_mut()?;
        let index = batch.next_row;
        batch.next_row += 1;
        let row = batch.row_mut(index)?;
        expose_row_lobs_in_state(state, row);
        Some(row)
    }

    fn discard_consumed_front(&mut self) {
        while self
            .batches
            .front()
            .is_some_and(|batch| batch.next_row >= batch.rows)
        {
            self.batches.pop_front();
        }
    }
}

trait RowValueSink {
    fn begin_row(&mut self, columns: usize) -> usize;
    fn push_value(&mut self, value: Db2Value);
    fn rollback_row(&mut self, mark: usize);
    fn finish_row(&mut self, mark: usize);
}

fn decode_rows_flat_incremental(
    fields: &[FieldDescriptor],
    data: &[u8],
) -> Result<(FlatRowBatch, usize, Vec<crate::Db2Warning>), Db2Error> {
    let (rows, consumed, warnings) =
        decode_rows_incremental_into(fields, data, FlatRowBatch::default())?;
    Ok((rows, consumed, warnings))
}

fn decode_rows_incremental_into<S: RowValueSink>(
    fields: &[FieldDescriptor],
    data: &[u8],
    sink: S,
) -> Result<(S, usize, Vec<crate::Db2Warning>), Db2Error> {
    if fields.is_empty() || data.is_empty() {
        return Ok((sink, 0, Vec::new()));
    }
    let mut sink = sink;
    let mut warnings = Vec::new();
    let mut offset = 0usize;
    while offset + 2 <= data.len() {
        let row_start = offset;
        let sqlca = data[offset];
        offset += 1;
        let mut row_warning = None;
        if sqlca == 0xff {
            if data[offset] != 0 {
                return Ok((sink, row_start, warnings));
            }
            offset += 1;
        } else if sqlca == 0 {
            if data.len().saturating_sub(offset) < 18 {
                return Ok((sink, row_start, warnings));
            }
            let code = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            let state = String::from_utf8_lossy(&data[offset + 4..offset + 9]).to_string();
            offset += 17; // SQLCODE, SQLSTATE, SQLERRPROC; next is the SQLCAXGRP indicator.
            let x_group_indicator = data[offset];
            offset += 1;
            let mut warning_flags = String::new();
            let mut messages = Vec::new();
            if x_group_indicator == 0 {
                if data.len().saturating_sub(offset) < 35 {
                    return Ok((sink, row_start, warnings));
                }
                offset += 24; // SQLERRD(1..6).
                warning_flags = String::from_utf8_lossy(&data[offset..offset + 11]).to_string();
                offset += 11;
                let mut sqlca_strings = Vec::with_capacity(3);
                for _ in 0..3 {
                    if data.len().saturating_sub(offset) < 2 {
                        return Ok((sink, row_start, warnings));
                    }
                    let length = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
                    offset += 2;
                    if data.len().saturating_sub(offset) < length {
                        return Ok((sink, row_start, warnings));
                    }
                    sqlca_strings
                        .push(String::from_utf8_lossy(&data[offset..offset + length]).to_string());
                    offset += length;
                }
                for message in sqlca_strings.iter().skip(1) {
                    if !message.trim().is_empty() {
                        messages.push(message.clone());
                    }
                }
            } else if x_group_indicator != 0xff {
                return Err(Db2Error::protocol(format!(
                    "invalid SQLCAXGRP indicator 0x{x_group_indicator:02X} in query row"
                )));
            }

            if offset >= data.len() {
                return Ok((sink, row_start, warnings));
            }
            let diagnostic_indicator = data[offset];
            offset += 1;
            if diagnostic_indicator != 0xff {
                return Err(Db2Error::protocol(
                    "SQLDIAGGRP in a query row is not supported by this driver",
                ));
            }
            if code < 0 {
                return Err(sql_error(code, &state, "in query row"));
            }
            if offset >= data.len() || data[offset] != 0 {
                return Ok((sink, row_start, warnings));
            }
            offset += 1;
            if (code > 0 && code != 100)
                || state.starts_with("01")
                || warning_flags.starts_with('W')
            {
                row_warning = Some(crate::Db2Warning {
                    sql_code: code,
                    sql_state: state.clone(),
                    message: if messages.is_empty() {
                        format!("DB2 returned a row warning (SQLCODE={code}, SQLSTATE={state}).")
                    } else {
                        messages.join("\n")
                    },
                    warning_flags,
                });
            }
        } else {
            return Ok((sink, row_start, warnings));
        }
        let row_mark = sink.begin_row(fields.len());
        for field in fields {
            match decode_field(field, data, &mut offset) {
                Ok(value) => sink.push_value(value),
                Err(error) if error.message.starts_with("truncated DB2 query field") => {
                    sink.rollback_row(row_mark);
                    return Ok((sink, row_start, warnings));
                }
                Err(error) => return Err(error),
            }
        }
        sink.finish_row(row_mark);
        if let Some(warning) = row_warning {
            warnings.push(warning);
        }
    }
    Ok((sink, offset, warnings))
}

/// Benchmark hook for synthetic QRYDTA blocks. Available only with the
/// `bench-internals` feature so the public driver API remains unchanged.
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn benchmark_decode_rows(
    fields: &[FieldDescriptor],
    data: &[u8],
) -> Result<(usize, usize), Db2Error> {
    let (rows, consumed, _) = decode_rows_incremental(fields, data)?;
    Ok((rows.len(), data.len() - consumed))
}

/// Benchmark hook for the flat row-batch decoder.
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn benchmark_decode_rows_flat(
    fields: &[FieldDescriptor],
    data: &[u8],
) -> Result<(usize, usize), Db2Error> {
    let (rows, consumed, _) = decode_rows_flat_incremental(fields, data)?;
    let _ = rows.row(0).map(|row| row.len());
    Ok((rows.rows, data.len() - consumed))
}

/// Benchmark hook that follows the reusable-offset query buffer across arbitrary
/// transport fragment boundaries.
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn benchmark_decode_row_fragments(
    fields: &[FieldDescriptor],
    fragments: &[&[u8]],
) -> Result<(usize, usize), Db2Error> {
    let mut pending = ReadBuffer::default();
    let mut decoded_rows = 0;
    for fragment in fragments {
        pending.append(fragment);
        let (rows, consumed, _) = decode_rows_incremental(fields, pending.as_slice())?;
        decoded_rows += rows.len();
        pending.consume(consumed);
    }
    Ok((decoded_rows, pending.len()))
}

fn decode_field(
    field: &FieldDescriptor,
    data: &[u8],
    offset: &mut usize,
) -> Result<Db2Value, Db2Error> {
    if nullable_type(field.field_type) {
        require(data, *offset, 1)?;
        let indicator = data[*offset];
        *offset += 1;
        if matches!(indicator, 0xff | 0x80 | 0x7f) {
            return Ok(Db2Value::Null);
        }
    }
    let ps = field.precision_scale;
    let fixed_len = u16::from_be_bytes(ps) as usize;
    let value = match field.field_type {
        0x02 | 0x03 => {
            let bytes = take(data, offset, 4)?;
            Db2Value::Integer(i32::from_le_bytes(bytes.try_into().unwrap()) as i64)
        }
        0x04 | 0x05 => {
            let bytes = take(data, offset, 2)?;
            Db2Value::Integer(i16::from_le_bytes(bytes.try_into().unwrap()) as i64)
        }
        0x16 | 0x17 => {
            let bytes = take(data, offset, 8)?;
            Db2Value::Integer(i64::from_le_bytes(bytes.try_into().unwrap()))
        }
        0x06 | 0x07 => {
            let bytes = take(data, offset, 1)?;
            Db2Value::Integer((bytes[0] as i8) as i64)
        }
        0x0a | 0x0b => {
            let bytes = take(data, offset, 8)?;
            Db2Value::Float(f64::from_bits(u64::from_le_bytes(
                bytes.try_into().unwrap(),
            )))
        }
        0x0c | 0x0d => {
            let bytes = take(data, offset, 4)?;
            Db2Value::Float(f32::from_bits(u32::from_le_bytes(bytes.try_into().unwrap())) as f64)
        }
        0x30 | 0x31 | 0x3c | 0x3d => Db2Value::Text(read_utf8(data, offset, fixed_len)?),
        0x32 | 0x33 | 0x34 | 0x35 | 0x3e | 0x3f | 0x40 | 0x41 => {
            let length = read_length(data, offset)?;
            Db2Value::Text(read_utf8(data, offset, length)?)
        }
        0x36 | 0x37 => Db2Value::Text(read_utf16(data, offset, fixed_len * 2)?),
        0x38..=0x3b => {
            let length = read_length(data, offset)?;
            Db2Value::Text(read_utf16(data, offset, length * 2)?)
        }
        0x20..=0x25 => {
            let text = read_utf8(data, offset, fixed_len)?;
            let trimmed = text.trim();
            Db2Value::Text(if trimmed.len() == text.len() {
                text
            } else {
                trimmed.to_owned()
            })
        }
        0x0e | 0x0f => {
            let precision = ps[0] as usize;
            if !(1..=31).contains(&precision) {
                return Err(Db2Error::protocol(format!(
                    "invalid packed DECIMAL precision {precision}"
                )));
            }
            Db2Value::Decimal(decode_packed_decimal(
                take(data, offset, (precision + 2) / 2)?,
                ps[1],
            ))
        }
        0xba | 0xbb => Db2Value::Decimal(
            decode_dfp(take(
                data,
                offset,
                if ps[0] == 16 || ps[1] == 16 { 16 } else { 8 },
            )?)
            .map_err(Db2Error::from)?,
        ),
        0xbe | 0xbf => Db2Value::Boolean(
            take(data, offset, fixed_len)?
                .last()
                .is_some_and(|value| *value != 0),
        ),
        0x26 | 0x27 | 0x1e | 0x1f | 0xc0 | 0xc1 => {
            Db2Value::Bytes(take(data, offset, fixed_len & 0x7fff)?.to_vec())
        }
        0x28 | 0x29 | 0x2a | 0x2b | 0xc2 | 0xc3 => {
            let length = read_length(data, offset)?;
            Db2Value::Bytes(take(data, offset, length)?.to_vec())
        }
        // Db2 can describe direct, non-nullable DBCLOB expressions with 0xcc;
        // nullable table columns use 0xcd.
        0x10 | 0x11 | 0x18 | 0x19 | 0x1a | 0x1b | 0x1c | 0x1d | 0xcc | 0xcd | 0xc6 | 0xc7
        | 0xc8 | 0xc9 | 0xce | 0xcf | 0xf4 | 0xf5 | 0xf6 | 0xf7 | 0xf8 | 0xf9 => {
            let length = fixed_len & 0x7fff;
            let locator_length = if length == 0 { 4 } else { length };
            if ps[0] & 0x80 != 0 && ps[1] == 9 && locator_length == 9 {
                let locator = take(data, offset, locator_length)?;
                if locator[0] == 0x01 {
                    // Db2 can inline a short LOB in QRYDTA as a 9-byte
                    // length marker followed by its value. Marker 0x02 instead
                    // points to a separate EXTDTA object, stitched afterward.
                    let lob_length = u64::from_be_bytes(locator[1..9].try_into().unwrap());
                    let lob_length = if is_dbclob_type(field.field_type) {
                        lob_length.checked_mul(2).ok_or_else(|| {
                            Db2Error::other(
                                "DB2 inline DBCLOB length overflowed the DRDA length".into(),
                            )
                        })?
                    } else {
                        lob_length
                    };
                    let lob_length = usize::try_from(lob_length).map_err(|_| {
                        Db2Error::other(
                            "DB2 inline LOB length exceeds this platform's address space".into(),
                        )
                    })?;
                    if lob_length > crate::resources::MAX_MATERIALIZED_LOB_BYTES {
                        return Err(Db2Error::resource_limit(format!(
                            "DB2 inline LOB exceeds the {}-byte materialization limit",
                            crate::resources::MAX_MATERIALIZED_LOB_BYTES
                        )));
                    }
                    let value = take(data, offset, lob_length)?;
                    if is_dbclob_type(field.field_type) {
                        Db2Value::Text(decode_utf16_be(value))
                    } else if is_clob_type(field.field_type) {
                        Db2Value::Text(String::from_utf8_lossy(value).into_owned())
                    } else {
                        Db2Value::Bytes(value.to_vec())
                    }
                } else if locator[0] == 0x03 {
                    let chars = u64::from_be_bytes(locator[1..9].try_into().unwrap());
                    // Progressive DBCLOB advertises UTF-16 code units while its GETNXTCHK
                    // chunks are UTF-16BE bytes, so the byte budget is twice the locator
                    // value. CLOB/BLOB locators already advertise bytes.
                    let length = if is_dbclob_type(field.field_type) {
                        chars.saturating_mul(2)
                    } else {
                        chars
                    };
                    let reference = u64::from_be_bytes(take(data, offset, 8)?.try_into().unwrap());
                    Db2Value::ProgressiveLobReference { length, reference }
                } else {
                    Db2Value::Bytes(locator.to_vec())
                }
            } else {
                Db2Value::Bytes(take(data, offset, locator_length)?.to_vec())
            }
        }
        other => {
            return Err(Db2Error::protocol(format!(
                "unsupported DB2 query field type 0x{other:02X}"
            )));
        }
    };
    Ok(value)
}

fn decode_packed_decimal(bytes: &[u8], scale: u8) -> String {
    if bytes.is_empty() {
        return "0".into();
    }
    // Db2 DECIMAL precision is at most 31 digits. Keep unpacked digits on the
    // stack and allocate only the final returned String.
    let mut digits = [0u8; 64];
    let mut digit_count = 0;
    for byte in &bytes[..bytes.len() - 1] {
        digits[digit_count] = b'0' + (byte >> 4);
        digits[digit_count + 1] = b'0' + (byte & 0x0f);
        digit_count += 2;
    }
    let last = bytes[bytes.len() - 1];
    digits[digit_count] = b'0' + (last >> 4);
    digit_count += 1;
    let negative = matches!(last & 0x0f, 0x0b | 0x0d);
    let first = digits[..digit_count]
        .iter()
        .position(|digit| *digit != b'0')
        .unwrap_or(digit_count.saturating_sub(1));
    let significant = &digits[first..digit_count];
    let scale = scale as usize;
    let sign_len = usize::from(negative);
    let capacity = if scale == 0 {
        sign_len + significant.len()
    } else if significant.len() <= scale {
        sign_len + 2 + scale
    } else {
        sign_len + significant.len() + 1
    };
    let mut result = String::with_capacity(capacity);
    if negative {
        result.push('-');
    }
    if scale == 0 {
        result.push_str(std::str::from_utf8(significant).expect("packed decimal digits are ASCII"));
    } else if significant.len() <= scale {
        result.push_str("0.");
        result.extend(std::iter::repeat_n('0', scale - significant.len()));
        result.push_str(std::str::from_utf8(significant).expect("packed decimal digits are ASCII"));
    } else {
        let split = significant.len() - scale;
        result.push_str(
            std::str::from_utf8(&significant[..split]).expect("packed decimal digits are ASCII"),
        );
        result.push('.');
        result.push_str(
            std::str::from_utf8(&significant[split..]).expect("packed decimal digits are ASCII"),
        );
    }
    result
}

fn nullable_type(value: u8) -> bool {
    matches!(
        value,
        0x03 | 0x05
            | 0x07
            | 0x0b
            | 0x0d
            | 0x0f
            | 0x17
            | 0x19
            | 0x1b
            | 0x1d
            | 0x1f
            | 0x21
            | 0x23
            | 0x25
            | 0x27
            | 0x29
            | 0x2b
            | 0x31
            | 0x33
            | 0x35
            | 0x37
            | 0x39
            | 0x3b
            | 0x3d
            | 0x3f
            | 0x41
            | 0x43
            | 0xbb
            | 0xbf
            | 0xc1
            | 0xc3
            | 0xc7
            | 0xc9
            | 0xcd
            | 0xcf
            | 0xf5
            | 0xf7
            | 0xf9
    )
}

#[cfg(test)]
fn stitch_extdta(
    fields: &[FieldDescriptor],
    rows: &mut [Vec<Db2Value>],
    values: &[Vec<u8>],
    external_values_have_null_indicator: bool,
) -> usize {
    stitch_extdta_limited(
        fields,
        rows,
        values.iter(),
        external_values_have_null_indicator,
        usize::MAX,
    )
    .expect("unlimited EXTDTA materialization cannot exceed its configured size limit")
}

fn stitch_extdta_limited<'a>(
    fields: &[FieldDescriptor],
    rows: &mut [Vec<Db2Value>],
    values: impl IntoIterator<Item = &'a Vec<u8>>,
    external_values_have_null_indicator: bool,
    max_materialized_lob_size: usize,
) -> Result<usize, Db2Error> {
    let mut value_index = 0;
    let mut values = values.into_iter();
    for row in rows {
        value_index += stitch_extdta_row(
            fields,
            row,
            &mut values,
            external_values_have_null_indicator,
            max_materialized_lob_size,
        )?;
    }
    Ok(value_index)
}

fn stitch_extdta_flat_limited<'a>(
    fields: &[FieldDescriptor],
    rows: &mut FlatRowBatch,
    values: impl IntoIterator<Item = &'a Vec<u8>>,
    external_values_have_null_indicator: bool,
    max_materialized_lob_size: usize,
) -> Result<usize, Db2Error> {
    let mut value_index = 0;
    let mut values = values.into_iter();
    for index in 0..rows.rows {
        let row = rows
            .row_mut(index)
            .ok_or_else(|| Db2Error::protocol("flat DB2 row batch has inconsistent row layout"))?;
        value_index += stitch_extdta_row(
            fields,
            row,
            &mut values,
            external_values_have_null_indicator,
            max_materialized_lob_size,
        )?;
    }
    Ok(value_index)
}

fn stitch_extdta_row<'a>(
    fields: &[FieldDescriptor],
    row: &mut [Db2Value],
    values: &mut impl Iterator<Item = &'a Vec<u8>>,
    external_values_have_null_indicator: bool,
    max_materialized_lob_size: usize,
) -> Result<usize, Db2Error> {
    let mut value_index = 0;
    for (column, field) in fields.iter().enumerate() {
        if !is_lob_type(field.field_type) {
            continue;
        }
        let cell = row.get(column).ok_or_else(|| {
            Db2Error::protocol("decoded DB2 row has fewer values than its QRYDSC descriptor")
        })?;
        let Some(external_length) = external_lob_length(field, cell) else {
            continue;
        };
        if external_length == 0 {
            row[column] = empty_lob_value(field.field_type);
            continue;
        }
        let Some(raw) = values.next() else {
            continue;
        };
        value_index += 1;
        // SQLDTARD output EXTDTA always carries a nullable value indicator.
        // Query and procedure-cursor EXTDTA carry it only for a nullable LOB
        // descriptor; a NOT NULL LOB is sent verbatim, so its first byte may
        // legitimately be 0x00 and must not be stripped.
        let has_leading_null_indicator =
            external_values_have_null_indicator || is_nullable_lob_type(field.field_type);
        let data = if has_leading_null_indicator && raw.first() == Some(&0) {
            &raw[1..]
        } else {
            raw.as_slice()
        };
        row[column] =
            decode_external_lob_value_limited(field.field_type, data, max_materialized_lob_size)?;
    }
    Ok(value_index)
}

fn empty_lob_value(field_type: u8) -> Db2Value {
    if is_clob_type(field_type) {
        Db2Value::Text(String::new())
    } else {
        Db2Value::Bytes(Vec::new())
    }
}

fn decode_external_lob_value_limited(
    field_type: u8,
    data: &[u8],
    max_materialized_lob_size: usize,
) -> Result<Db2Value, Db2Error> {
    if data.len() > max_materialized_lob_size {
        return Err(Db2Error::resource_limit(format!(
            "DB2 LOB of {} bytes exceeds the configured materialization limit of {max_materialized_lob_size} bytes",
            data.len()
        )));
    }
    if is_dbclob_type(field_type) {
        Ok(Db2Value::Text(decode_utf16_be_limited(
            data,
            max_materialized_lob_size,
        )?))
    } else if is_clob_type(field_type) {
        Ok(Db2Value::Text(decode_utf8_lossy_limited(
            data,
            max_materialized_lob_size,
        )?))
    } else {
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(data.len()).map_err(|error| {
            Db2Error::resource_limit(format!("cannot allocate a DB2 LOB value: {error}"))
        })?;
        bytes.extend_from_slice(data);
        Ok(Db2Value::Bytes(bytes))
    }
}

fn decode_utf8_lossy_limited(
    data: &[u8],
    max_materialized_lob_size: usize,
) -> Result<String, Db2Error> {
    let mut output = String::new();
    let mut remaining = data;
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(text) => {
                push_lob_text_limited(&mut output, text, max_materialized_lob_size)?;
                break;
            }
            Err(error) => {
                let valid_length = error.valid_up_to();
                let invalid_length = error
                    .error_len()
                    .unwrap_or(remaining.len().saturating_sub(valid_length));
                let valid_prefix = std::str::from_utf8(&remaining[..valid_length])
                    .expect("Utf8Error::valid_up_to always identifies valid UTF-8");
                push_lob_text_limited(&mut output, valid_prefix, max_materialized_lob_size)?;
                push_lob_text_limited(&mut output, "\u{fffd}", max_materialized_lob_size)?;
                remaining = &remaining[valid_length + invalid_length..];
            }
        }
    }
    Ok(output)
}

fn push_lob_text_limited(
    output: &mut String,
    text: &str,
    max_materialized_lob_size: usize,
) -> Result<(), Db2Error> {
    let next_length = output
        .len()
        .checked_add(text.len())
        .ok_or_else(|| Db2Error::resource_limit("materialized DB2 LOB string length overflow"))?;
    if next_length > max_materialized_lob_size {
        return Err(Db2Error::resource_limit(format!(
            "materialized DB2 LOB string exceeds the configured limit of {max_materialized_lob_size} bytes"
        )));
    }
    output.try_reserve(text.len()).map_err(|error| {
        Db2Error::resource_limit(format!(
            "cannot grow a materialized DB2 LOB string: {error}"
        ))
    })?;
    output.push_str(text);
    Ok(())
}

fn decode_utf16_be_limited(
    data: &[u8],
    max_materialized_lob_size: usize,
) -> Result<String, Db2Error> {
    let units = || {
        data.as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
    };
    let mut decoded_length = 0usize;
    for decoded in std::char::decode_utf16(units()) {
        let character = decoded.unwrap_or(char::REPLACEMENT_CHARACTER);
        decoded_length = decoded_length
            .checked_add(character.len_utf8())
            .ok_or_else(|| {
                Db2Error::resource_limit("materialized DB2 LOB string length overflow")
            })?;
        if decoded_length > max_materialized_lob_size {
            return Err(Db2Error::resource_limit(format!(
                "materialized DB2 LOB string exceeds the configured limit of {max_materialized_lob_size} bytes"
            )));
        }
    }

    let mut output = String::new();
    output.try_reserve_exact(decoded_length).map_err(|error| {
        Db2Error::resource_limit(format!(
            "cannot allocate a materialized DB2 LOB string: {error}"
        ))
    })?;
    for decoded in std::char::decode_utf16(units()) {
        output.push(decoded.unwrap_or(char::REPLACEMENT_CHARACTER));
    }
    Ok(output)
}

fn external_lob_length(field: &FieldDescriptor, value: &Db2Value) -> Option<u64> {
    if !is_lob_type(field.field_type)
        || field.precision_scale[0] & 0x80 == 0
        || field.precision_scale[1] != 9
    {
        return None;
    }
    let Db2Value::Bytes(locator) = value else {
        return None;
    };
    if locator.len() != 9 || locator[0] != 0x02 {
        return None;
    }
    Some(u64::from_be_bytes(locator[1..9].try_into().unwrap()))
}

fn has_unresolved_external_locator(fields: &[FieldDescriptor], rows: &[Vec<Db2Value>]) -> bool {
    for row in rows {
        for (column, field) in fields.iter().enumerate() {
            let Some(value) = row.get(column) else {
                continue;
            };
            if let Some(length) = external_lob_length(field, value)
                && length != 0
            {
                return true;
            }
            if matches!(value, Db2Value::ProgressiveLobReference { .. }) {
                return true;
            }
        }
    }
    false
}

fn is_lob_type(value: u8) -> bool {
    matches!(
        value,
        0x10 | 0x11
            | 0x18
            | 0x19
            | 0x1a
            | 0x1b
            | 0x1c
            | 0x1d
            | 0xc6
            | 0xc7
            | 0xc8
            | 0xc9
            | 0xcc
            | 0xcd
            | 0xce
            | 0xcf
            | 0xf4
            | 0xf5
            | 0xf6
            | 0xf7
            | 0xf8
            | 0xf9
    )
}
/// Nullable LOB descriptors use the odd member of each type pair (e.g. 0xc9 BLOB, 0xcd DBCLOB).
fn is_nullable_lob_type(value: u8) -> bool {
    is_lob_type(value) && (value & 1) == 1
}
fn is_clob_type(value: u8) -> bool {
    matches!(
        value,
        0x1a | 0x1b
            | 0x1c
            | 0x1d
            | 0xc6
            | 0xc7
            | 0xcc
            | 0xcd
            | 0xce
            | 0xcf
            | 0xf6
            | 0xf7
            | 0xf8
            | 0xf9
    )
}
fn is_dbclob_type(value: u8) -> bool {
    matches!(value, 0x1c | 0x1d | 0xcc | 0xcd | 0xf8 | 0xf9)
}

fn decode_utf16_be(data: &[u8]) -> String {
    let units = data
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u16::from_be_bytes([pair[0], pair[1]]));
    let mut output = String::with_capacity(data.len() / 2);
    for decoded in std::char::decode_utf16(units) {
        output.push(decoded.unwrap_or(char::REPLACEMENT_CHARACTER));
    }
    output
}

fn require(data: &[u8], offset: usize, length: usize) -> Result<(), Db2Error> {
    if offset
        .checked_add(length)
        .is_none_or(|end| end > data.len())
    {
        Err(Db2Error::other("truncated DB2 query field".into()))
    } else {
        Ok(())
    }
}
fn take<'a>(data: &'a [u8], offset: &mut usize, length: usize) -> Result<&'a [u8], Db2Error> {
    require(data, *offset, length)?;
    let result = &data[*offset..*offset + length];
    *offset += length;
    Ok(result)
}
fn read_length(data: &[u8], offset: &mut usize) -> Result<usize, Db2Error> {
    Ok(u16::from_be_bytes(take(data, offset, 2)?.try_into().unwrap()) as usize)
}
fn read_utf8(data: &[u8], offset: &mut usize, length: usize) -> Result<String, Db2Error> {
    Ok(String::from_utf8_lossy(take(data, offset, length)?).to_string())
}
fn parse_identity_text(value: &str) -> Result<i64, Db2Error> {
    let integral = value.trim().split('.').next().unwrap_or_default();
    integral
        .parse()
        .map_err(|_| Db2Error::other(format!("DB2 returned an invalid identity value: {value}")))
}
fn read_utf16(data: &[u8], offset: &mut usize, length: usize) -> Result<String, Db2Error> {
    let bytes = take(data, offset, length)?;
    let units = bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u16::from_be_bytes([pair[0], pair[1]]));
    let mut text = String::with_capacity(bytes.len());
    for decoded in char::decode_utf16(units) {
        text.push(decoded.unwrap_or(char::REPLACEMENT_CHARACTER));
    }
    Ok(text)
}
fn sql_error(code: i32, state: &str, message: &str) -> Db2Error {
    Db2Error::sql(code, state, message)
}

fn is_confirmed_interrupt(error: &Db2Error) -> bool {
    error.sql_code == Some(-952)
}

fn server_error(payload: &[u8]) -> Db2Error {
    ddm::find_ddm_parameter(payload, cp::SRVDGN)
        .ok()
        .flatten()
        .map(|data| Db2Error::server_diagnostic(decode_server_diagnostic(data)))
        .unwrap_or_else(|| {
            Db2Error::new(Db2ErrorKind::ServerResponse, "DB2 server returned SQLERRRM")
        })
}

fn decode_server_diagnostic(data: &[u8]) -> String {
    std::str::from_utf8(data)
        .map(str::to_owned)
        .unwrap_or_else(|_| decode_cp500(data))
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::network::codepoint::CodePoint;
    use crate::network::dss::encode_reply_dss;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn dummy_session(auto_commit: bool) -> Db2Session {
        dummy_session_with_buffer(auto_commit, 1).0
    }

    fn dummy_session_with_buffer(
        auto_commit: bool,
        buffer_size: usize,
    ) -> (Db2Session, tokio::io::DuplexStream) {
        let (stream, peer) = tokio::io::duplex(buffer_size);
        let session = Db2Session {
            stream: Box::new(stream),
            pool_abort: None,
            config: Config::default(),
            database: "TESTDB".into(),
            security_mechanism: 3,
            query_block_size: 1024,
            correlation_id: 1,
            auto_commit,
            closed: false,
            prepared_sql: None,
            current_user: String::new(),
            base_user: String::new(),
            client_info: Db2ClientInfo::default(),
            base_client_info: Db2ClientInfo::default(),
            base_schema: None,
            base_isolation: None,
            current_schema: None,
            current_isolation: None,
            pool_baseline_captured: false,
            pool_dirty: SessionDirtyState::default(),
            warning_handler: None,
            rdb_interrupt_token: None,
            interrupt_coordinator: Arc::new(InterruptCoordinator::default()),
        };
        (session, peer)
    }

    #[cfg(feature = "bench-internals")]
    #[tokio::test]
    async fn benchmark_request_counter_counts_written_groups_not_ddm_objects() {
        let (mut session, _peer) = dummy_session_with_buffer(true, 1024);
        let one = ddm::pack_ddm_object(cp::EXCSAT, &[]);
        let two = ddm::pack_ddm_object(cp::EXCSAT, &[]);
        let three = ddm::pack_ddm_object(cp::EXCSAT, &[]);

        session.benchmark_reset_request_batches();
        session
            .send(&one, false, true, None)
            .await
            .expect("single request group should fit the mock transport");
        let requests = [
            dss::DssRequest {
                payload: &two,
                correlation_id: None,
                next_same_id: true,
                last_packet: false,
            },
            dss::DssRequest {
                payload: &three,
                correlation_id: None,
                next_same_id: false,
                last_packet: true,
            },
        ];
        session
            .send_dss_requests(&requests)
            .await
            .expect("chained objects should fit the mock transport");
        assert_eq!(session.benchmark_request_batches_sent(), 2);
    }

    async fn dummy_row_reader<'a>(
        session: &'a mut Db2Session,
        rows: Vec<Vec<Db2Value>>,
        ended: bool,
    ) -> Db2RowReader<'a> {
        let operation = session.interrupt_coordinator.begin_operation().await;
        Db2RowReader {
            session,
            state: QueryStreamState {
                columns: Vec::new(),
                fields: Vec::new(),
                pending_row_data: ReadBuffer::default(),
                extdta: VecDeque::new(),
                open: !ended,
                ended,
                failed: false,
                query_id: 0,
                next_correlation: 1,
                current_row_generation: 0,
                current_row_lobs: VecDeque::new(),
                active_lob: None,
                max_materialized_lob_size: usize::MAX,
            },
            rows: RowBatchQueue::from_rows(rows),
            completed: ended,
            commit_after_query: false,
            committed: false,
            _operation: operation,
            options: Db2CommandOptions::unlimited(),
            interrupt: None,
        }
    }

    #[tokio::test]
    async fn row_ref_borrows_text_and_bytes_until_the_reader_advances() {
        let mut session = dummy_session(true);
        let mut reader = dummy_row_reader(
            &mut session,
            vec![vec![
                Db2Value::Integer(7),
                Db2Value::Text("borrowed".into()),
                Db2Value::Bytes(vec![1, 2, 3]),
            ]],
            true,
        )
        .await;
        let row = reader.next_ref().await.unwrap().unwrap();
        assert_eq!(row.get(0), Some(Db2ValueRef::Integer(7)));
        assert_eq!(row.get(1), Some(Db2ValueRef::Text("borrowed")));
        assert_eq!(row.get(2), Some(Db2ValueRef::Bytes(&[1, 2, 3])));
        assert_eq!(row.iter().len(), 3);
        assert!(reader.next_ref().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn batch_rejects_limits_outside_the_correlation_id_range_before_io() {
        let mut session = dummy_session(true);
        let error = session
            .execute_batch_with_params_and_batch_options(
                "INSERT INTO T VALUES (?)",
                &[vec![Db2Value::Integer(1)]],
                Db2BatchOptions::new(i16::MAX as usize + 1, 1024),
            )
            .await
            .unwrap_err();
        assert_eq!(error.cause.kind, Db2ErrorKind::InvalidConfiguration);
        assert_eq!(error.failed_parameter_set_index, None);
        assert!(!session.is_closed());
    }

    async fn spawn_import_protocol_server(
        batches: Vec<(usize, cp::CodePoint)>,
        fail_on_insert: Option<usize>,
    ) -> (u16, tokio::task::JoinHandle<()>) {
        spawn_import_protocol_server_with_parameter(batches, fail_on_insert, 496, 4).await
    }

    async fn spawn_import_protocol_server_with_parameter(
        batches: Vec<(usize, cp::CodePoint)>,
        fail_on_insert: Option<usize>,
        parameter_sql_type: u16,
        parameter_length: u64,
    ) -> (u16, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            let prepare = read_chain(&mut socket).await;
            assert_eq!(
                prepare
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::DSCSQLSTT]
            );
            let mut parameter_dard = vec![0xff, 1, 1, 0, 0, 0, 0, 0];
            parameter_dard.extend_from_slice(&parameter_length.to_le_bytes());
            parameter_dard.extend_from_slice(&parameter_sql_type.to_le_bytes());
            parameter_dard.extend_from_slice(&[0, 0]);
            send(&mut socket, cp::SQLDARD, &parameter_dard, false).await;

            let mut observed_inserts = 0;
            for (insert_count, expected_control) in batches {
                let mut remaining_inserts = insert_count;
                let mut failed = false;
                while remaining_inserts > 0 {
                    let execute = read_chain(&mut socket).await;
                    assert_eq!(execute.len() % 2, 0);
                    let inserts_in_chain = execute.len() / 2;
                    assert!(inserts_in_chain > 0 && inserts_in_chain <= remaining_inserts);
                    assert!(execute.as_chunks::<2>().0.iter().all(|pair| {
                        pair[0].codepoint == cp::EXCSQLSTT && pair[1].codepoint == cp::SQLDTA
                    }));
                    for index in 0..inserts_in_chain {
                        observed_inserts += 1;
                        let sql_card = if fail_on_insert == Some(observed_inserts) {
                            failed = true;
                            test_sql_card_error(-803, "23505")
                        } else {
                            test_sql_card(1)
                        };
                        let is_last = index + 1 == inserts_in_chain || failed;
                        send(&mut socket, cp::SQLCARD, &sql_card, !is_last).await;
                        if failed {
                            break;
                        }
                    }
                    if failed {
                        break;
                    }
                    remaining_inserts -= inserts_in_chain;
                }
                if failed {
                    let rollback = read_chain(&mut socket).await;
                    assert_eq!(rollback[0].codepoint, cp::RDBRLLBCK);
                    send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
                    break;
                }
                let control = read_chain(&mut socket).await;
                assert_eq!(control[0].codepoint, expected_control);
                send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
            }
        });
        (port, server)
    }

    async fn connect_import_destination(port: u16) -> Db2Session {
        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        Db2Session::connect(config).await.unwrap()
    }

    fn one_column_import_target() -> Db2ImportTarget {
        Db2ImportTarget::new(
            Some("IMPORT_TEST"),
            "DESTINATION",
            vec![crate::Db2ImportColumn::new("ID").with_sql_type(496)],
        )
        .unwrap()
    }

    fn one_column_varchar_import_target() -> Db2ImportTarget {
        Db2ImportTarget::new(
            Some("IMPORT_TEST"),
            "DESTINATION",
            vec![crate::Db2ImportColumn::new("VALUE")],
        )
        .unwrap()
    }

    fn integer_import_rows(count: i64) -> Vec<Vec<Db2Value>> {
        (1..=count)
            .map(|value| vec![Db2Value::Integer(value)])
            .collect()
    }

    fn estimated_boundary_import_rows() -> Vec<Vec<Db2Value>> {
        [11, 12, 4, 4, 4, 13, 11, 4, 50]
            .into_iter()
            .map(|length| vec![Db2Value::Text("x".repeat(length))])
            .collect()
    }

    #[test]
    fn progressive_lob_import_types_match_drda_families_and_destination_types() {
        for sql_type in [0x10, 0x11, 0x18, 0x19, 0xc8, 0xc9, 0xf4, 0xf5] {
            assert_eq!(import_lob_kind(sql_type), Some(ImportLobKind::Blob));
        }
        for sql_type in [0x1a, 0x1b, 0xce, 0xcf, 0xf6, 0xf7] {
            assert_eq!(import_lob_kind(sql_type), Some(ImportLobKind::Clob));
        }
        for sql_type in [0x1c, 0x1d, 0xcc, 0xcd, 0xf8, 0xf9] {
            assert_eq!(import_lob_kind(sql_type), Some(ImportLobKind::DbClob));
        }
        for sql_type in [0xc6, 0xc7] {
            assert_eq!(import_lob_kind(sql_type), Some(ImportLobKind::Xml));
        }
        assert_eq!(destination_lob_type(ImportLobKind::Blob), 404);
        assert_eq!(destination_lob_type(ImportLobKind::Clob), 408);
        assert_eq!(destination_lob_type(ImportLobKind::DbClob), 412);
        assert_eq!(destination_lob_type(ImportLobKind::Xml), 988);
        for (sql_type, expected) in [
            (404, ImportLobKind::Blob),
            (405, ImportLobKind::Blob),
            (408, ImportLobKind::Clob),
            (409, ImportLobKind::Clob),
            (412, ImportLobKind::DbClob),
            (413, ImportLobKind::DbClob),
            (988, ImportLobKind::Xml),
            (989, ImportLobKind::Xml),
        ] {
            assert_eq!(import_lob_kind_from_column_type(sql_type), Some(expected));
        }
        assert_eq!(base_import_sql_type(989), 988);
        assert_eq!(import_lob_kind_from_column_type(448), None);
        assert_eq!(import_lob_kind(0x20), None);
    }

    #[test]
    fn client_info_normalization_strips_controls_from_every_register() {
        let normalized = sanitize_client_info(Db2ClientInfo {
            application_name: "app\r\nname".into(),
            workstation_name: "work\0station".into(),
            user_id: "user\r\nid".into(),
            accounting: "cost\ncenter".into(),
            correlation_token: "corr\rtoken".into(),
        });

        assert_eq!(normalized.application_name, "appname");
        assert_eq!(normalized.workstation_name, "workstation");
        assert_eq!(normalized.user_id, "userid");
        assert_eq!(normalized.accounting, "costcenter");
        assert_eq!(normalized.correlation_token, "corrtoken");
        assert_eq!(escape_sql_value("audit\r\n'app\0"), "audit''app");
    }

    #[tokio::test]
    async fn set_client_info_sends_sanitized_statement_and_tracks_the_same_value() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            let update = read_chain(&mut socket).await;
            assert_eq!(
                update
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            let statement = update
                .iter()
                .find(|packet| packet.codepoint == cp::SQLSTT)
                .unwrap();
            let payload = &statement.payload;
            let length = u32::from_be_bytes(payload[1..5].try_into().unwrap()) as usize;
            let sql = std::str::from_utf8(&payload[5..5 + length]).unwrap();
            assert_eq!(sql, "SET CLIENT APPLNAME 'audit''app'");
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 3_000;
        let mut session = Db2Session::connect(config).await.unwrap();
        session
            .set_client_info(Db2ClientInfo {
                application_name: "audit\r\n'app".into(),
                ..Db2ClientInfo::default()
            })
            .await
            .unwrap();
        assert_eq!(session.client_info().application_name, "audit'app");
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[test]
    fn progressive_lob_locator_uses_a_fixed_staging_estimate() {
        let row = [Db2Value::Lob(Db2LobValue {
            byte_length: 8 * 1024 * 1024,
            sql_type: 0xc9,
            encoding: Db2LobEncoding::Binary,
            reference: 7,
            row_generation: 2,
        })];
        assert_eq!(encoded_row_size_estimate(&row).unwrap(), 32);
    }

    #[test]
    fn dbclob_transcoder_handles_split_code_units_and_surrogate_pairs() {
        let text = "A🚀Zażółć 東京";
        let bytes = text
            .encode_utf16()
            .flat_map(u16::to_be_bytes)
            .collect::<Vec<_>>();
        let mut transcoder = Utf16BeTranscoder::default();
        let mut actual = transcoder.push(&bytes[..1]).unwrap();
        actual.extend(transcoder.push(&bytes[1..4]).unwrap());
        actual.extend(transcoder.push(&bytes[4..]).unwrap());
        transcoder.finish().unwrap();
        assert_eq!(actual, text.as_bytes());
    }

    #[test]
    fn dbclob_transcoder_rejects_incomplete_or_invalid_utf16() {
        let mut odd = Utf16BeTranscoder::default();
        odd.push(&[0]).unwrap();
        assert!(odd.finish().is_err());

        let mut low = Utf16BeTranscoder::default();
        assert!(low.push(&0xdc00u16.to_be_bytes()).is_err());

        let mut high_at_end = Utf16BeTranscoder::default();
        high_at_end.push(&0xd800u16.to_be_bytes()).unwrap();
        assert!(high_at_end.finish().is_err());

        let mut broken_pair = Utf16BeTranscoder::default();
        broken_pair.push(&0xd800u16.to_be_bytes()).unwrap();
        assert!(broken_pair.push(&[0x00, b'A']).is_err());
    }

    #[tokio::test]
    async fn progressive_lob_spool_rewinds_and_removes_its_private_tempfile() {
        let mut spool = ImportLobSpool::new(ImportLobKind::Blob).unwrap();
        let path = spool._path.to_path_buf();
        let expected = vec![0x5a; 128 * 1024];
        spool.file.write_all(&expected).await.unwrap();
        spool.length = expected.len() as u64;
        spool.rewind().await.unwrap();
        let mut actual = Vec::new();
        spool.file.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
        drop(spool);
        assert!(
            !path.exists(),
            "the temporary spool must be removed on drop"
        );
    }

    #[tokio::test]
    async fn import_refuses_to_commit_a_caller_owned_transaction() {
        let mut source_session = dummy_session(true);
        let source_operation = source_session.interrupt_coordinator.begin_operation().await;
        let mut source = Db2RowReader {
            session: &mut source_session,
            state: QueryStreamState {
                columns: Vec::new(),
                fields: Vec::new(),
                pending_row_data: ReadBuffer::default(),
                extdta: VecDeque::new(),
                open: false,
                ended: true,
                failed: false,
                query_id: 0,
                next_correlation: 1,
                current_row_generation: 0,
                current_row_lobs: VecDeque::new(),
                active_lob: None,
                max_materialized_lob_size: usize::MAX,
            },
            rows: RowBatchQueue::default(),
            completed: true,
            commit_after_query: false,
            committed: false,
            _operation: source_operation,
            options: Db2CommandOptions::unlimited(),
            interrupt: None,
        };
        let mut destination = dummy_session(false);
        let target = Db2ImportTarget::new(
            None,
            "T",
            vec![crate::Db2ImportColumn::new("ID").with_sql_type(496)],
        )
        .unwrap();
        let error = destination
            .import_rows_from(
                &mut source,
                &target,
                Db2ImportPolicy::Atomic {
                    max_batch_rows: 8,
                    max_batch_bytes: 1024,
                },
                |_| Ok(()),
            )
            .await
            .unwrap_err();
        assert!(
            error
                .cause
                .message
                .contains("caller transactions are not committed")
        );
        assert_eq!(error.progress, Db2ImportProgress::default());
        assert!(!destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        assert!(!source_session.closed);
    }

    #[tokio::test]
    async fn import_empty_source_is_a_noop_for_both_policies() {
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
            let mut source_session = dummy_session(true);
            let mut source = dummy_row_reader(&mut source_session, Vec::new(), true).await;
            let mut destination = dummy_session(true);
            let target = one_column_import_target();
            let mut observer_called = false;
            let progress = destination
                .import_rows_from(&mut source, &target, policy, |_| {
                    observer_called = true;
                    Ok(())
                })
                .await
                .unwrap();

            assert_eq!(progress, Db2ImportProgress::default());
            assert!(!observer_called);
            assert!(destination.auto_commit);
            assert!(!destination.closed);
            drop(source);
            assert!(!source_session.closed);
        }
    }

    #[tokio::test]
    async fn import_commits_bounded_batches_and_reports_confirmed_progress() {
        let (port, server) = spawn_import_protocol_server(
            vec![(2, cp::RDBCMM), (2, cp::RDBCMM), (1, cp::RDBCMM)],
            None,
        )
        .await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(5), true).await;
        let mut observed = Vec::new();
        let progress = destination
            .import_rows_from(
                &mut source,
                &one_column_import_target(),
                Db2ImportPolicy::CommitEveryRows {
                    rows: 4,
                    max_batch_bytes: 20,
                },
                |snapshot| {
                    observed.push(snapshot);
                    Ok(())
                },
            )
            .await
            .unwrap();

        assert_eq!(progress.source_rows_read, 5);
        assert_eq!(progress.committed_rows, 5);
        assert_eq!(progress.committed_batches, 3);
        assert_eq!(progress.estimated_committed_bytes, 50);
        assert_eq!(
            observed
                .iter()
                .map(|snapshot| (snapshot.source_rows_read, snapshot.committed_rows))
                .collect::<Vec<_>>(),
            vec![(2, 0), (4, 2), (5, 4)]
        );
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_commit_every_rows_combines_exact_row_and_byte_boundaries() {
        let (port, server) = spawn_import_protocol_server_with_parameter(
            vec![
                (2, cp::RDBCMM),
                (3, cp::RDBCMM),
                (1, cp::RDBCMM),
                (2, cp::RDBCMM),
                (1, cp::RDBCMM),
            ],
            None,
            449, // nullable VARCHAR
            128,
        )
        .await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let mut source =
            dummy_row_reader(&mut source_session, estimated_boundary_import_rows(), true).await;
        let mut observed = Vec::new();
        let progress = destination
            .import_rows_from(
                &mut source,
                &one_column_varchar_import_target(),
                Db2ImportPolicy::CommitEveryRows {
                    rows: 3,
                    max_batch_bytes: 39,
                },
                |snapshot| {
                    observed.push(snapshot);
                    Ok(())
                },
            )
            .await
            .unwrap();

        assert_eq!(progress.source_rows_read, 9);
        assert_eq!(progress.committed_rows, 9);
        assert_eq!(progress.committed_batches, 5);
        assert_eq!(progress.estimated_committed_bytes, 185);
        assert_eq!(
            observed
                .iter()
                .map(|snapshot| (
                    snapshot.source_rows_read,
                    snapshot.committed_rows,
                    snapshot.committed_batches,
                    snapshot.estimated_committed_bytes,
                ))
                .collect::<Vec<_>>(),
            vec![
                (2, 0, 0, 0),
                (5, 2, 1, 39),
                (7, 5, 2, 75),
                (9, 6, 3, 96),
                (9, 8, 4, 127),
            ]
        );
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_sends_a_single_row_that_exceeds_the_soft_byte_batch_limit() {
        for policy in [
            Db2ImportPolicy::Atomic {
                max_batch_rows: 8,
                max_batch_bytes: 1,
            },
            Db2ImportPolicy::CommitEveryRows {
                rows: 8,
                max_batch_bytes: 1,
            },
        ] {
            let (port, server) = spawn_import_protocol_server(vec![(1, cp::RDBCMM)], None).await;
            let mut destination = connect_import_destination(port).await;
            let mut source_session = dummy_session(true);
            let mut source =
                dummy_row_reader(&mut source_session, vec![vec![Db2Value::Integer(1)]], true).await;
            let progress = destination
                .import_rows_from(&mut source, &one_column_import_target(), policy, |_| Ok(()))
                .await
                .unwrap();

            assert_eq!(progress.source_rows_read, 1);
            assert_eq!(progress.committed_rows, 1);
            assert_eq!(progress.committed_batches, 1);
            assert_eq!(progress.estimated_committed_bytes, 10);
            drop(source);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn import_observer_failure_rolls_back_atomic_transaction() {
        let (port, server) = spawn_import_protocol_server(vec![(2, cp::RDBRLLBCK)], None).await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(2), true).await;
        let error = destination
            .import_rows_from(
                &mut source,
                &one_column_import_target(),
                Db2ImportPolicy::Atomic {
                    max_batch_rows: 1,
                    max_batch_bytes: 20,
                },
                |_| Err(Db2Error::other("observer stopped import".into())),
            )
            .await
            .unwrap_err();

        assert_eq!(error.failed_source_row, None);
        assert_eq!(error.progress.source_rows_read, 2);
        assert_eq!(error.progress.committed_rows, 0);
        assert_eq!(error.progress.committed_batches, 0);
        assert!(error.cause.message.contains("observer stopped import"));
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_commit_every_rows_observer_failure_preserves_prior_checkpoint() {
        let (port, server) =
            spawn_import_protocol_server(vec![(2, cp::RDBCMM), (2, cp::RDBRLLBCK)], None).await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(4), true).await;
        let mut callbacks = 0;
        let error = destination
            .import_rows_from(
                &mut source,
                &one_column_import_target(),
                Db2ImportPolicy::CommitEveryRows {
                    rows: 2,
                    max_batch_bytes: 100,
                },
                |_| {
                    callbacks += 1;
                    if callbacks == 2 {
                        Err(Db2Error::other("second checkpoint observer failed".into()))
                    } else {
                        Ok(())
                    }
                },
            )
            .await
            .unwrap_err();

        assert_eq!(error.failed_source_row, None);
        assert_eq!(error.progress.source_rows_read, 4);
        assert_eq!(error.progress.committed_rows, 2);
        assert_eq!(error.progress.committed_batches, 1);
        assert_eq!(error.progress.estimated_committed_bytes, 20);
        assert!(
            error
                .cause
                .message
                .contains("second checkpoint observer failed")
        );
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_with_options_reports_periodic_progress_between_commits() {
        let (port, server) = spawn_import_protocol_server(vec![(5, cp::RDBCMM)], None).await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(5), true).await;
        let mut observed = Vec::new();
        let progress = destination
            .import_rows_with_options(
                &mut source,
                &one_column_import_target(),
                crate::Db2ImportOptions {
                    policy: Db2ImportPolicy::Atomic {
                        max_batch_rows: 8,
                        max_batch_bytes: 1024,
                    },
                    notify_after_rows: Some(2),
                    ..crate::Db2ImportOptions::default()
                },
                |snapshot| {
                    observed.push((snapshot.source_rows_read, snapshot.committed_rows));
                    Ok(())
                },
            )
            .await
            .unwrap();

        assert_eq!(progress.source_rows_read, 5);
        assert_eq!(progress.committed_rows, 5);
        assert_eq!(progress.committed_batches, 1);
        assert_eq!(observed, vec![(2, 0), (4, 0), (5, 0)]);
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_with_options_deduplicates_final_periodic_and_precommit_report() {
        let (port, server) = spawn_import_protocol_server(vec![(4, cp::RDBCMM)], None).await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(4), true).await;
        let mut observed = Vec::new();
        let progress = destination
            .import_rows_with_options(
                &mut source,
                &one_column_import_target(),
                crate::Db2ImportOptions {
                    policy: Db2ImportPolicy::Atomic {
                        max_batch_rows: 8,
                        max_batch_bytes: 1024,
                    },
                    notify_after_rows: Some(2),
                    ..crate::Db2ImportOptions::default()
                },
                |snapshot| {
                    observed.push((snapshot.source_rows_read, snapshot.committed_rows));
                    Ok(())
                },
            )
            .await
            .unwrap();

        assert_eq!(progress.source_rows_read, 4);
        assert_eq!(progress.committed_rows, 4);
        // The interval lands exactly on the final row, so the pre-commit gate must
        // not repeat the report already delivered by the periodic hook.
        assert_eq!(observed, vec![(2, 0), (4, 0)]);
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_with_options_periodic_observer_abort_rolls_back_atomic() {
        let (port, server) = spawn_import_protocol_server(vec![(0, cp::RDBRLLBCK)], None).await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(5), true).await;
        let error = destination
            .import_rows_with_options(
                &mut source,
                &one_column_import_target(),
                crate::Db2ImportOptions {
                    policy: Db2ImportPolicy::Atomic {
                        max_batch_rows: 8,
                        max_batch_bytes: 1024,
                    },
                    notify_after_rows: Some(2),
                    ..crate::Db2ImportOptions::default()
                },
                |snapshot| {
                    if snapshot.source_rows_read == 4 {
                        Err(Db2Error::other("periodic observer stopped import".into()))
                    } else {
                        Ok(())
                    }
                },
            )
            .await
            .unwrap_err();

        assert_eq!(error.failed_source_row, Some(4));
        assert_eq!(error.progress.source_rows_read, 4);
        assert_eq!(error.progress.committed_rows, 0);
        assert_eq!(error.progress.committed_batches, 0);
        assert!(
            error
                .cause
                .message
                .contains("periodic observer stopped import")
        );
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_with_options_table_lock_with_checkpoint_fails_before_io() {
        let mut source_session = dummy_session(true);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(2), true).await;
        let mut destination = dummy_session(true);
        let error = destination
            .import_rows_with_options(
                &mut source,
                &one_column_import_target(),
                crate::Db2ImportOptions {
                    policy: Db2ImportPolicy::CommitEveryRows {
                        rows: 2,
                        max_batch_bytes: 100,
                    },
                    table_lock: true,
                    ..crate::Db2ImportOptions::default()
                },
                |_| Ok(()),
            )
            .await
            .unwrap_err();

        assert_eq!(error.failed_source_row, None);
        assert!(error.cause.message.contains("table lock requires"));
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        assert!(!source_session.closed);
    }

    #[tokio::test]
    async fn import_with_options_table_lock_runs_before_first_write() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            serve_basic_handshake(&mut socket).await;
            let prepare = read_chain(&mut socket).await;
            assert_eq!(
                prepare
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::DSCSQLSTT]
            );
            let mut parameter_dard = vec![0xff, 1, 1, 0, 0, 0, 0, 0];
            parameter_dard.extend_from_slice(&4u64.to_le_bytes());
            parameter_dard.extend_from_slice(&496u16.to_le_bytes());
            parameter_dard.extend_from_slice(&[0, 0]);
            send(&mut socket, cp::SQLDARD, &parameter_dard, false).await;
            // The exclusive lock runs inside the importer-owned transaction,
            // before any INSERT reaches the server.
            let lock = read_chain(&mut socket).await;
            assert_eq!(
                lock.iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSQLIMM, cp::SQLSTT]
            );
            let statement = lock
                .iter()
                .find(|packet| packet.codepoint == cp::SQLSTT)
                .expect("SQLSTT");
            assert_eq!(
                sql_statement_text(statement),
                "LOCK TABLE \"IMPORT_TEST\".\"DESTINATION\" IN EXCLUSIVE MODE"
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
            let mut remaining_inserts = 2;
            while remaining_inserts > 0 {
                let execute = read_chain(&mut socket).await;
                assert_eq!(execute.len() % 2, 0);
                let inserts_in_chain = execute.len() / 2;
                assert!(inserts_in_chain > 0 && inserts_in_chain <= remaining_inserts);
                assert!(execute.as_chunks::<2>().0.iter().all(|pair| {
                    pair[0].codepoint == cp::EXCSQLSTT && pair[1].codepoint == cp::SQLDTA
                }));
                for index in 0..inserts_in_chain {
                    let is_last = index + 1 == inserts_in_chain;
                    send(&mut socket, cp::SQLCARD, &test_sql_card(1), !is_last).await;
                }
                remaining_inserts -= inserts_in_chain;
            }
            let control = read_chain(&mut socket).await;
            assert_eq!(control[0].codepoint, cp::RDBCMM);
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
        });
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(2), true).await;
        let progress = destination
            .import_rows_with_options(
                &mut source,
                &one_column_import_target(),
                crate::Db2ImportOptions {
                    policy: Db2ImportPolicy::Atomic {
                        max_batch_rows: 8,
                        max_batch_bytes: 1024,
                    },
                    table_lock: true,
                    ..crate::Db2ImportOptions::default()
                },
                |_| Ok(()),
            )
            .await
            .unwrap();

        assert_eq!(progress.source_rows_read, 2);
        assert_eq!(progress.committed_rows, 2);
        assert_eq!(progress.committed_batches, 1);
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_source_transport_error_reports_next_absolute_row_and_rolls_back() {
        let (port, server) = spawn_import_protocol_server(vec![(2, cp::RDBRLLBCK)], None).await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(2), false).await;
        let error = destination
            .import_rows_from(
                &mut source,
                &one_column_import_target(),
                Db2ImportPolicy::Atomic {
                    max_batch_rows: 2,
                    max_batch_bytes: 100,
                },
                |_| Ok(()),
            )
            .await
            .unwrap_err();

        assert_eq!(error.failed_source_row, Some(3));
        assert_eq!(error.progress.source_rows_read, 2);
        assert_eq!(error.progress.committed_rows, 0);
        assert_eq!(error.cause.kind, Db2ErrorKind::Transport);
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_progressive_lob_peer_reset_preserves_checkpoint_and_discards_source() {
        let (port, server) = spawn_import_protocol_server_with_parameter(
            vec![(1, cp::RDBCMM)],
            None,
            405, // nullable BLOB
            300_000,
        )
        .await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let (source_stream, mut source_peer) = tokio::io::duplex(4096);
        source_session.stream = Box::new(source_stream);
        let mut source = dummy_row_reader(
            &mut source_session,
            vec![
                vec![Db2Value::Null],
                vec![Db2Value::ProgressiveLobReference {
                    length: 300_000,
                    reference: 0x1122_3344_5566_7788,
                }],
            ],
            true,
        )
        .await;
        source.state.fields = vec![ddm::FieldDescriptor {
            field_type: 0xc9,
            precision_scale: [0x80, 9],
        }];
        let source_peer_task = tokio::spawn(async move {
            let request = dss::read_dss(&mut source_peer)
                .await
                .expect("source must send GETNXTCHK for the current locator");
            assert_eq!(request.codepoint, cp::GETNXTCHK);
            drop(source_peer); // Simulate a source transport reset before EXTDTA arrives.
        });
        let target = Db2ImportTarget::new(
            Some("IMPORT_TEST"),
            "DESTINATION",
            vec![crate::Db2ImportColumn::new("B").with_sql_type(405)],
        )
        .unwrap();

        let error = destination
            .import_rows_from(
                &mut source,
                &target,
                Db2ImportPolicy::CommitEveryRows {
                    rows: 1,
                    max_batch_bytes: 1024,
                },
                |_| Ok(()),
            )
            .await
            .unwrap_err();

        assert_eq!(error.failed_source_row, Some(2));
        assert_eq!(error.progress.source_rows_read, 2);
        assert_eq!(error.progress.committed_rows, 1);
        assert_eq!(error.progress.committed_batches, 1);
        assert_eq!(error.progress.estimated_committed_bytes, 2);
        assert_eq!(error.cause.kind, Db2ErrorKind::Transport);
        assert!(
            source.session.closed,
            "uncertain progressive locator must discard its source session"
        );
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        source_peer_task.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_progressive_lob_confirmed_interrupt_preserves_checkpoint_and_keeps_source() {
        let (port, server) = spawn_import_protocol_server_with_parameter(
            vec![(1, cp::RDBCMM)],
            None,
            405, // nullable BLOB
            300_000,
        )
        .await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let (source_stream, mut source_peer) = tokio::io::duplex(4096);
        source_session.stream = Box::new(source_stream);
        let mut source = dummy_row_reader(
            &mut source_session,
            vec![
                vec![Db2Value::Null],
                vec![Db2Value::ProgressiveLobReference {
                    length: 300_000,
                    reference: 0x1122_3344_5566_7788,
                }],
            ],
            true,
        )
        .await;
        source.state.fields = vec![ddm::FieldDescriptor {
            field_type: 0xc9,
            precision_scale: [0x80, 9],
        }];
        let source_peer_task = tokio::spawn(async move {
            let request = dss::read_dss(&mut source_peer)
                .await
                .expect("source must send GETNXTCHK for the current locator");
            assert_eq!(request.codepoint, cp::GETNXTCHK);
            // A server-confirmed SQLINTR answer to the pending GETNXTCHK. Unlike a peer
            // reset, -952 proves the locator ended cleanly, so the source session must
            // stay reusable while the destination keeps its confirmed checkpoint.
            let card = ddm::pack_ddm_object(cp::SQLCARD, &test_sql_card_error(-952, "57014"));
            source_peer
                .write_all(&encode_reply_dss(&card, false, 1))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        let target = Db2ImportTarget::new(
            Some("IMPORT_TEST"),
            "DESTINATION",
            vec![crate::Db2ImportColumn::new("B").with_sql_type(405)],
        )
        .unwrap();

        let error = destination
            .import_rows_from(
                &mut source,
                &target,
                Db2ImportPolicy::CommitEveryRows {
                    rows: 1,
                    max_batch_bytes: 1024,
                },
                |_| Ok(()),
            )
            .await
            .unwrap_err();

        assert_eq!(error.failed_source_row, Some(2));
        assert_eq!(error.progress.source_rows_read, 2);
        assert_eq!(error.progress.committed_rows, 1);
        assert_eq!(error.progress.committed_batches, 1);
        assert_eq!(error.cause.sql_code, Some(-952));
        assert_eq!(error.cause.sql_state.as_deref(), Some("57014"));
        assert!(
            !source.session.closed,
            "a server-confirmed -952 GETNXTCHK must keep the source session reusable"
        );
        assert!(
            !source
                .session
                .interrupt_coordinator
                .poisoned
                .load(Ordering::Acquire)
        );
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        source_peer_task.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_commit_every_rows_source_error_preserves_confirmed_checkpoint() {
        let (port, server) = spawn_import_protocol_server(vec![(2, cp::RDBCMM)], None).await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(3), false).await;
        let error = destination
            .import_rows_from(
                &mut source,
                &one_column_import_target(),
                Db2ImportPolicy::CommitEveryRows {
                    rows: 2,
                    max_batch_bytes: 100,
                },
                |_| Ok(()),
            )
            .await
            .unwrap_err();

        assert_eq!(error.failed_source_row, Some(4));
        assert_eq!(error.progress.source_rows_read, 3);
        assert_eq!(error.progress.committed_rows, 2);
        assert_eq!(error.progress.committed_batches, 1);
        assert_eq!(error.progress.estimated_committed_bytes, 20);
        assert_eq!(error.cause.kind, Db2ErrorKind::Transport);
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_server_error_reports_failed_target_row_and_rolls_back_atomic_rows() {
        let (port, server) = spawn_import_protocol_server(vec![(2, cp::RDBRLLBCK)], Some(2)).await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(2), true).await;
        let error = destination
            .import_rows_from(
                &mut source,
                &one_column_import_target(),
                Db2ImportPolicy::Atomic {
                    max_batch_rows: 2,
                    max_batch_bytes: 100,
                },
                |_| Ok(()),
            )
            .await
            .unwrap_err();

        assert_eq!(error.failed_source_row, Some(2));
        assert_eq!(error.progress.source_rows_read, 2);
        assert_eq!(error.progress.committed_rows, 0);
        assert_eq!(error.cause.sql_code, Some(-803));
        assert_eq!(error.cause.sql_state.as_deref(), Some("23505"));
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_commit_every_rows_target_error_preserves_confirmed_checkpoint() {
        let (port, server) =
            spawn_import_protocol_server(vec![(2, cp::RDBCMM), (2, cp::RDBRLLBCK)], Some(4)).await;
        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(4), true).await;
        let error = destination
            .import_rows_from(
                &mut source,
                &one_column_import_target(),
                Db2ImportPolicy::CommitEveryRows {
                    rows: 2,
                    max_batch_bytes: 100,
                },
                |_| Ok(()),
            )
            .await
            .unwrap_err();

        assert_eq!(error.failed_source_row, Some(4));
        assert_eq!(error.progress.source_rows_read, 4);
        assert_eq!(error.progress.committed_rows, 2);
        assert_eq!(error.progress.committed_batches, 1);
        assert_eq!(error.progress.estimated_committed_bytes, 20);
        assert_eq!(error.cause.sql_code, Some(-803));
        assert_eq!(error.cause.sql_state.as_deref(), Some("23505"));
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn dropping_import_while_source_is_pending_preserves_confirmed_destination_checkpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (commit_tx, commit_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            let prepare = read_chain(&mut socket).await;
            assert_eq!(
                prepare
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::DSCSQLSTT]
            );
            let mut parameter_dard = vec![0xff, 1, 1, 0, 0, 0, 0, 0];
            parameter_dard.extend_from_slice(&4u64.to_le_bytes());
            parameter_dard.extend_from_slice(&496u16.to_le_bytes());
            parameter_dard.extend_from_slice(&[0, 0]);
            send(&mut socket, cp::SQLDARD, &parameter_dard, false).await;

            let execute = read_chain(&mut socket).await;
            assert_eq!(
                execute
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSQLSTT, cp::SQLDTA]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(1), false).await;
            let commit = read_chain(&mut socket).await;
            assert_eq!(commit[0].codepoint, cp::RDBCMM);
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
            let _ = commit_tx.send(());

            // A subsequent ping proves that cancellation happened between owned units of work,
            // after DB2 confirmed the first commit and before the next source row arrived.
            let ping = read_chain(&mut socket).await;
            assert_eq!(ping[0].codepoint, cp::RDBCMM);
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
        });

        let mut destination = connect_import_destination(port).await;
        let mut source_session = dummy_session(true);
        let (source_stream, mut source_peer) = tokio::io::duplex(4096);
        source_session.stream = Box::new(source_stream);
        let mut source = dummy_row_reader(&mut source_session, integer_import_rows(1), false).await;
        let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
        let pending_source = tokio::spawn(async move {
            let _request = dss::read_dss(&mut source_peer).await.unwrap();
            let _ = pending_tx.send(());
            std::future::pending::<()>().await;
        });
        let observer_called = Arc::new(AtomicBool::new(false));
        {
            let target = one_column_import_target();
            let observer_called_in_callback = Arc::clone(&observer_called);
            let import = destination.import_rows_from(
                &mut source,
                &target,
                Db2ImportPolicy::CommitEveryRows {
                    rows: 1,
                    max_batch_bytes: 100,
                },
                |_| {
                    observer_called_in_callback.store(true, Ordering::SeqCst);
                    Ok(())
                },
            );
            tokio::pin!(import);
            tokio::select! {
                signal = tokio::time::timeout(Duration::from_secs(2), commit_rx) => {
                    signal.expect("first destination checkpoint must commit")
                        .expect("mock server dropped checkpoint signal");
                },
                result = &mut import => panic!("import completed instead of waiting for source: {result:?}"),
            }
            tokio::select! {
                signal = tokio::time::timeout(Duration::from_secs(2), pending_rx) => {
                    signal.expect("import must advance to the pending source read")
                        .expect("mock source dropped its pending-read signal");
                },
                result = &mut import => panic!("import completed instead of waiting for source: {result:?}"),
            }
        }
        assert!(observer_called.load(Ordering::SeqCst));
        assert!(destination.auto_commit);
        assert!(!destination.closed);
        drop(source);
        assert!(
            source_session.closed,
            "the abandoned source reader is discarded"
        );

        destination.ping().await.unwrap();
        assert!(!destination.closed);
        pending_source.abort();
        server.await.unwrap();
    }

    #[test]
    fn procedure_output_reply_rejects_rows_shorter_than_the_requested_positions() {
        let descriptor = [9, 0x76, 0xd0, 0x04, 0, 2, 0x03, 0, 4];
        let values = [0xff, 0, 0x2a, 0, 0, 0, 0x64, 0, 0, 0];
        let payload = [
            ddm::pack_ddm_object(cp::FDODSC, &descriptor),
            ddm::pack_ddm_object(cp::FDODTA, &values),
        ]
        .concat();

        // The value row carries two values, so two output positions are complete.
        let mut covered = vec![Db2Value::Null; 2];
        apply_procedure_output_parameters(
            &payload,
            &[Db2ParameterDirection::Output, Db2ParameterDirection::Output],
            &mut covered,
            &[],
        )
        .unwrap();

        // Requesting a third position must fail instead of silently reporting the
        // missing output as its placeholder value.
        let mut short_row = vec![Db2Value::Null; 3];
        let error = apply_procedure_output_parameters(
            &payload,
            &[
                Db2ParameterDirection::Output,
                Db2ParameterDirection::Output,
                Db2ParameterDirection::Output,
            ],
            &mut short_row,
            &[],
        )
        .unwrap_err();
        assert!(
            error
                .message
                .contains("position 2 was requested as an output"),
            "{error}"
        );
    }

    #[test]
    fn procedure_output_reply_rejects_missing_or_empty_values_but_accepts_typed_null() {
        let output = [Db2ParameterDirection::Output];
        let mut output_value = [Db2Value::Null];
        let error =
            apply_procedure_output_parameters(&[], &output, &mut output_value, &[]).unwrap_err();
        assert!(error.message.contains("position 0"));
        assert!(ensure_output_parameter_data_returned(&output, false).is_err());

        let descriptor = [6, 0x76, 0xd0, 0x03, 0, 4];
        let row_without_values = [
            ddm::pack_ddm_object(cp::FDODSC, &descriptor),
            ddm::pack_ddm_object(cp::FDODTA, &[0xff, 0]),
        ]
        .concat();
        let mut missing_value = [Db2Value::Null];
        let error = apply_procedure_output_parameters(
            &row_without_values,
            &output,
            &mut missing_value,
            &[],
        )
        .unwrap_err();
        assert!(error.message.contains("position 0"));

        let input_only = [Db2ParameterDirection::Input];
        let mut input_value = [Db2Value::Null];
        apply_procedure_output_parameters(&[], &input_only, &mut input_value, &[]).unwrap();
        ensure_output_parameter_data_returned(&input_only, false).unwrap();

        // A real typed NULL has both an SQLDA descriptor and a nullable row value.
        let descriptor = [6, 0x76, 0xd0, 0x03, 0, 4];
        let values = [0xff, 0, 0xff];
        let payload = [
            ddm::pack_ddm_object(cp::FDODSC, &descriptor),
            ddm::pack_ddm_object(cp::FDODTA, &values),
        ]
        .concat();
        let mut typed_null = [Db2Value::Boolean(true)];
        apply_procedure_output_parameters(&payload, &output, &mut typed_null, &[]).unwrap();
        assert_eq!(typed_null, [Db2Value::Null]);
    }

    #[test]
    fn query_data_decodes_inline_blob_clob_dbclob_and_xml_values() {
        let fields = [0xc9, 0xcf, 0xcd, 0xc7]
            .into_iter()
            .map(|field_type| FieldDescriptor {
                field_type,
                precision_scale: [0x80, 9],
            })
            .collect::<Vec<_>>();
        let blob = [0, 0x7f, 0x80, 0xff];
        let clob = "CLOB café".as_bytes();
        let dbclob = "DBCLOB 東京"
            .encode_utf16()
            .flat_map(u16::to_be_bytes)
            .collect::<Vec<_>>();
        let xml = b"<root/>";
        let mut data = vec![0xff, 0];
        for (value, length) in [
            (&blob[..], blob.len() as u64),
            (clob, clob.len() as u64),
            (&dbclob[..], (dbclob.len() / 2) as u64),
            (xml, xml.len() as u64),
        ] {
            data.push(0);
            data.push(0x01);
            data.extend_from_slice(&length.to_be_bytes());
            data.extend_from_slice(value);
        }

        let (rows, consumed, warnings) = decode_rows_incremental(&fields, &data).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(consumed, data.len());
        assert_eq!(
            rows,
            vec![vec![
                Db2Value::Bytes(blob.to_vec()),
                Db2Value::Text("CLOB café".into()),
                Db2Value::Text("DBCLOB 東京".into()),
                Db2Value::Text("<root/>".into()),
            ]]
        );

        let (partial_rows, consumed, warnings) =
            decode_rows_incremental(&fields, &data[..data.len() - 3]).unwrap();
        assert!(warnings.is_empty());
        assert!(partial_rows.is_empty());
        assert_eq!(consumed, 0, "a split inline LOB row must be retained whole");
    }

    #[test]
    fn query_data_rejects_inline_lobs_above_materialization_budget() {
        let fields = [FieldDescriptor {
            field_type: 0xc8,
            precision_scale: [0x80, 9],
        }];
        let mut data = vec![0xff, 0, 0x01];
        data.extend_from_slice(&(16_u64 * 1024 * 1024 + 1).to_be_bytes());

        let error = decode_rows_incremental(&fields, &data).unwrap_err();

        assert_eq!(error.kind, Db2ErrorKind::ResourceLimit);
        assert!(error.message.contains("materialization limit"));
    }

    #[test]
    fn reusable_query_buffer_retains_only_the_unfinished_row_across_fragments() {
        let fields = [FieldDescriptor {
            field_type: 0x02,
            precision_scale: [0, 4],
        }];
        let mut first_fragment = vec![0xff, 0];
        first_fragment.extend_from_slice(&7i32.to_le_bytes());
        first_fragment.extend_from_slice(&[0xff, 0, 0x34, 0x12]);

        let mut buffer = ReadBuffer::default();
        buffer.append(&first_fragment);
        let (rows, consumed, warnings) =
            decode_rows_incremental(&fields, buffer.as_slice()).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(rows, vec![vec![Db2Value::Integer(7)]]);
        buffer.consume(consumed);
        assert_eq!(buffer.as_slice(), &[0xff, 0, 0x34, 0x12]);

        buffer.append(&[0, 0]);
        let (rows, consumed, warnings) =
            decode_rows_incremental(&fields, buffer.as_slice()).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(rows, vec![vec![Db2Value::Integer(0x1234)]]);
        buffer.consume(consumed);
        assert!(buffer.is_empty());
    }

    #[test]
    fn flat_row_decoder_matches_owned_rows_and_rolls_back_a_partial_tail() {
        let fields = [
            FieldDescriptor {
                field_type: 0x02,
                precision_scale: [0, 4],
            },
            FieldDescriptor {
                field_type: 0x40,
                precision_scale: [0, 0],
            },
        ];
        let mut data = vec![0xff, 0];
        data.extend_from_slice(&7i32.to_le_bytes());
        data.extend_from_slice(&5u16.to_be_bytes());
        data.extend_from_slice(b"first");
        data.extend_from_slice(&[0xff, 0]);
        data.extend_from_slice(&8i32.to_le_bytes());
        data.extend_from_slice(&5u16.to_be_bytes());
        data.extend_from_slice(b"x");

        let (nested, nested_consumed, nested_warnings) =
            decode_rows_incremental(&fields, &data).unwrap();
        let (flat, flat_consumed, flat_warnings) =
            decode_rows_flat_incremental(&fields, &data).unwrap();
        assert_eq!(nested_consumed, flat_consumed);
        assert_eq!(nested_warnings, flat_warnings);
        assert_eq!(flat.rows, nested.len());
        assert_eq!(flat.values.len(), nested.len() * fields.len());
        assert_eq!(flat.row(0), nested.first().map(Vec::as_slice));
        assert_eq!(flat.row(1), None);
    }

    #[test]
    fn reusable_query_buffer_compacts_consumed_prefix_amortized() {
        let mut buffer = ReadBuffer::default();
        let bytes = vec![0x5a; 160 * 1024];
        buffer.append(&bytes);
        buffer.consume(100 * 1024);

        assert_eq!(buffer.data.len(), 60 * 1024);
        assert_eq!(buffer.start, 0);
        assert_eq!(buffer.as_slice(), &bytes[100 * 1024..]);
    }

    #[test]
    fn query_rows_decode_full_sqlca_warnings_once_after_the_row_is_complete() {
        fn length_prefixed(value: &[u8]) -> Vec<u8> {
            [
                u16::try_from(value.len()).unwrap().to_be_bytes().to_vec(),
                value.to_vec(),
            ]
            .concat()
        }

        let fields = [FieldDescriptor {
            field_type: 0x03,
            precision_scale: [0, 4],
        }];
        let mut data = vec![0x00]; // SQLCAGRP is present.
        data.extend_from_slice(&0i32.to_le_bytes());
        data.extend_from_slice(b"01003");
        data.extend_from_slice(b"SQLRI6B1");
        data.push(0x00); // SQLCAXGRP is present.
        data.extend_from_slice(&[0; 24]); // SQLERRD(1..6).
        data.extend_from_slice(b"W W        ");
        data.extend_from_slice(&length_prefixed(b"TESTDB            "));
        data.extend_from_slice(&length_prefixed(b""));
        data.extend_from_slice(&length_prefixed(b""));
        data.push(0xff); // SQLDIAGGRP absent.
        data.push(0x00); // Row data follows.
        data.push(0x00); // Nullable INTEGER is not NULL.
        data.extend_from_slice(&40i32.to_le_bytes());
        let first_row_end = data.len();
        data.extend_from_slice(&[0xff, 0x00, 0x00]); // A normal row follows.
        data.extend_from_slice(&41i32.to_le_bytes());

        let (rows, consumed, warnings) = decode_rows_incremental(&fields, &data).unwrap();
        assert_eq!(consumed, data.len());
        assert_eq!(
            rows,
            vec![vec![Db2Value::Integer(40)], vec![Db2Value::Integer(41)]]
        );
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].sql_code, 0);
        assert_eq!(warnings[0].sql_state, "01003");
        assert!(warnings[0].warning_flags.starts_with('W'));

        let (partial_rows, partial_consumed, partial_warnings) =
            decode_rows_incremental(&fields, &data[..first_row_end - 1]).unwrap();
        assert!(partial_rows.is_empty());
        assert_eq!(partial_consumed, 0);
        assert!(
            partial_warnings.is_empty(),
            "a partial row must not emit its warning yet"
        );
    }

    #[test]
    fn terminal_no_data_sqlca_after_procedure_rows_is_consumed() {
        fn length_prefixed(value: &[u8]) -> Vec<u8> {
            [
                u16::try_from(value.len()).unwrap().to_be_bytes().to_vec(),
                value.to_vec(),
            ]
            .concat()
        }

        let fields = [FieldDescriptor {
            field_type: 0x03,
            precision_scale: [0, 4],
        }];
        let mut row_data = vec![0xff, 0x00, 0x00];
        row_data.extend_from_slice(&42i32.to_le_bytes());
        let terminal_start = row_data.len();
        let mut terminal = vec![0x00];
        terminal.extend_from_slice(&100i32.to_le_bytes());
        terminal.extend_from_slice(b"02000");
        terminal.extend_from_slice(b"SQLRI6B1");
        terminal.push(0x00); // SQLCAXGRP is present.
        terminal.extend_from_slice(&[0; 24]);
        terminal.extend_from_slice(b"           ");
        terminal.extend_from_slice(&length_prefixed(b"TESTDB            "));
        terminal.extend_from_slice(&length_prefixed(b""));
        terminal.extend_from_slice(&length_prefixed(b""));
        terminal.extend_from_slice(&[0xff, 0xff]); // no diagnostic or row follows.
        assert_eq!(terminal.len(), 80);
        row_data.extend_from_slice(&terminal);

        let (rows, consumed, warnings) = decode_rows_incremental(&fields, &row_data).unwrap();
        assert_eq!(rows, vec![vec![Db2Value::Integer(42)]]);
        assert_eq!(consumed, terminal_start);
        assert!(warnings.is_empty());
        assert_eq!(no_data_sqlca_len(&row_data[consumed..]), Some(80));

        let mut split = row_data[..terminal_start].to_vec();
        split.extend_from_slice(&[0xff]);
        let (partial, partial_consumed, _) = decode_rows_incremental(&fields, &split).unwrap();
        assert_eq!(partial, rows);
        assert_eq!(partial_consumed, terminal_start);
        assert_eq!(no_data_sqlca_len(&split[partial_consumed..]), None);
    }

    #[test]
    fn progressive_lob_query_field_keeps_length_and_opaque_reference_separate() {
        let fields = [FieldDescriptor {
            field_type: 0xc9,
            precision_scale: [0x80, 9],
        }];
        let mut data = vec![0xff, 0, 0, 0x03];
        data.extend_from_slice(&2_500_123_u64.to_be_bytes());
        data.extend_from_slice(&0xffff_ffff_e0b3_4079_u64.to_be_bytes());

        let (rows, consumed, warnings) = decode_rows_incremental(&fields, &data).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(consumed, data.len());
        assert_eq!(
            rows,
            vec![vec![Db2Value::ProgressiveLobReference {
                length: 2_500_123,
                reference: 0xffff_ffff_e0b3_4079,
            }]]
        );
        assert!(has_unresolved_external_locator(&fields, &rows));
    }

    #[test]
    fn empty_lob_locator_does_not_consume_the_next_xml_external_value() {
        fn locator(length: u64) -> Vec<u8> {
            [vec![0x02], length.to_be_bytes().to_vec()].concat()
        }

        let fields = [0xc9, 0xcf, 0xcd, 0xc7]
            .into_iter()
            .map(|field_type| FieldDescriptor {
                field_type,
                precision_scale: [0x80, 9],
            })
            .collect::<Vec<_>>();
        let mut rows = vec![
            vec![
                Db2Value::Bytes(locator(1)),
                Db2Value::Bytes(locator(1)),
                Db2Value::Bytes(locator(1)),
                Db2Value::Bytes(locator(u64::MAX)),
            ],
            vec![
                Db2Value::Null,
                Db2Value::Bytes(locator(0)),
                Db2Value::Null,
                Db2Value::Bytes(locator(u64::MAX)),
            ],
        ];
        let values = vec![
            vec![0, 0xaa],
            vec![0, b'C'],
            vec![0, 0, b'D'],
            b"\0<x/>".to_vec(),
            b"\0<empty/>".to_vec(),
        ];

        assert_eq!(
            stitch_extdta(&fields, &mut rows, &values, false),
            values.len()
        );
        // 0xc9 is the nullable BLOB descriptor, so its external value carries
        // the 0x00 nullable indicator and the single data byte remains 0xaa.
        assert_eq!(rows[0][0], Db2Value::Bytes(vec![0xaa]));
        assert_eq!(rows[0][1], Db2Value::Text("C".into()));
        assert_eq!(rows[0][2], Db2Value::Text("D".into()));
        assert_eq!(rows[0][3], Db2Value::Text("<x/>".into()));
        assert_eq!(rows[1][0], Db2Value::Null);
        assert_eq!(rows[1][1], Db2Value::Text(String::new()));
        assert_eq!(rows[1][2], Db2Value::Null);
        assert_eq!(rows[1][3], Db2Value::Text("<empty/>".into()));
        assert!(!has_unresolved_external_locator(&fields, &rows));
    }

    #[test]
    fn missing_external_lob_payload_leaves_an_unresolved_locator() {
        fn locator(length: u64) -> Vec<u8> {
            [vec![0x02], length.to_be_bytes().to_vec()].concat()
        }
        let fields = [0xc9, 0xcf]
            .into_iter()
            .map(|field_type| FieldDescriptor {
                field_type,
                precision_scale: [0x80, 9],
            })
            .collect::<Vec<_>>();

        let mut complete = vec![vec![
            Db2Value::Bytes(locator(1)),
            Db2Value::Bytes(locator(1)),
        ]];
        assert_eq!(
            stitch_extdta(
                &fields,
                &mut complete,
                &[vec![0, 0xaa], vec![0, b'C']],
                false
            ),
            2
        );
        assert!(!has_unresolved_external_locator(&fields, &complete));

        let mut missing = vec![vec![
            Db2Value::Bytes(locator(1)),
            Db2Value::Bytes(locator(1)),
        ]];
        assert_eq!(
            stitch_extdta(&fields, &mut missing, &[vec![0, 0xaa]], false),
            1
        );
        assert!(has_unresolved_external_locator(&fields, &missing));

        let mut empty = vec![vec![Db2Value::Bytes(locator(0)), Db2Value::Null]];
        assert_eq!(stitch_extdta(&fields, &mut empty, &[], false), 0);
        assert!(!has_unresolved_external_locator(&fields, &empty));

        let mut null_cell = vec![vec![Db2Value::Null, Db2Value::Bytes(locator(1))]];
        assert_eq!(
            stitch_extdta(&fields, &mut null_cell, &[vec![0, b'C']], false),
            1
        );
        assert!(!has_unresolved_external_locator(&fields, &null_cell));

        let mut output_parameter = vec![vec![Db2Value::Bytes(locator(1)), Db2Value::Null]];
        assert_eq!(
            stitch_extdta(&fields, &mut output_parameter, &[vec![0, 0xaa]], true),
            1
        );
        assert_eq!(output_parameter[0][0], Db2Value::Bytes(vec![0xaa]));
    }

    #[test]
    fn nullable_lob_descriptors_strip_the_indicator_while_not_null_preserve_leading_zero() {
        fn locator(length: u64) -> Vec<u8> {
            [vec![0x02], length.to_be_bytes().to_vec()].concat()
        }
        let field = |field_type: u8| FieldDescriptor {
            field_type,
            precision_scale: [0x80, 9],
        };

        let mut blob_not_null = vec![vec![Db2Value::Bytes(locator(3))]];
        assert_eq!(
            stitch_extdta(&[field(0xc8)], &mut blob_not_null, &[vec![0, 1, 2]], false),
            1
        );
        assert_eq!(blob_not_null[0][0], Db2Value::Bytes(vec![0, 1, 2]));

        let mut blob_nullable = vec![vec![Db2Value::Bytes(locator(3))]];
        assert_eq!(
            stitch_extdta(&[field(0xc9)], &mut blob_nullable, &[vec![0, 1, 2]], false),
            1
        );
        assert_eq!(blob_nullable[0][0], Db2Value::Bytes(vec![1, 2]));

        let mut clob_not_null = vec![vec![Db2Value::Bytes(locator(2))]];
        assert_eq!(
            stitch_extdta(&[field(0xce)], &mut clob_not_null, &[vec![0, b'a']], false),
            1
        );
        assert_eq!(clob_not_null[0][0], Db2Value::Text("\u{0}a".into()));

        let mut clob_nullable = vec![vec![Db2Value::Bytes(locator(2))]];
        assert_eq!(
            stitch_extdta(&[field(0xcf)], &mut clob_nullable, &[vec![0, b'a']], false),
            1
        );
        assert_eq!(clob_nullable[0][0], Db2Value::Text("a".into()));
    }

    #[test]
    fn materialized_lobs_obey_byte_limit_after_text_decoding() {
        let binary = decode_external_lob_value_limited(0xc9, &[0xaa, 0xbb], 2).unwrap();
        assert_eq!(binary, Db2Value::Bytes(vec![0xaa, 0xbb]));
        let binary_error = decode_external_lob_value_limited(0xc9, &[0xaa, 0xbb], 1).unwrap_err();
        assert_eq!(binary_error.kind, Db2ErrorKind::ResourceLimit);

        let clob = decode_external_lob_value_limited(0xc7, "é".as_bytes(), 2).unwrap();
        assert_eq!(clob, Db2Value::Text("é".into()));
        let invalid_utf8 = decode_external_lob_value_limited(0xc7, &[0xff], 2).unwrap_err();
        assert_eq!(invalid_utf8.kind, Db2ErrorKind::ResourceLimit);

        // U+FFFD takes three UTF-8 bytes even though its malformed UTF-16 source is two bytes.
        let invalid_utf16 = decode_external_lob_value_limited(0xf8, &[0xd8, 0x00], 2).unwrap_err();
        assert_eq!(invalid_utf16.kind, Db2ErrorKind::ResourceLimit);
        let valid_utf16 = decode_external_lob_value_limited(0xf8, &[0x00, 0x41], 2).unwrap();
        assert_eq!(valid_utf16, Db2Value::Text("A".into()));
    }

    #[test]
    fn extdta_stitching_rejects_payload_above_configured_materialization_limit() {
        fn locator(length: u64) -> Vec<u8> {
            [vec![0x02], length.to_be_bytes().to_vec()].concat()
        }
        let fields = [FieldDescriptor {
            field_type: 0xc9,
            precision_scale: [0x80, 9],
        }];
        let mut rows = vec![vec![Db2Value::Bytes(locator(2))]];
        let error =
            stitch_extdta_limited(&fields, &mut rows, &[vec![0xaa, 0xbb]], false, 1).unwrap_err();

        assert_eq!(error.kind, Db2ErrorKind::ResourceLimit);
        assert!(matches!(&rows[0][0], Db2Value::Bytes(_)));
    }

    #[test]
    fn server_diagnostics_decode_utf8_and_fall_back_to_cp500() {
        let diagnostic = "SQLERRP:SQLRI2D0 SQLCODE:-952 SQLSTATE:57014";
        let utf8 = ddm::pack_bytes(cp::SRVDGN, diagnostic.as_bytes());
        let error = server_error(&utf8);
        assert_eq!(error.to_string(), diagnostic);
        assert_eq!(error.kind, Db2ErrorKind::SqlServer);
        assert_eq!(error.sql_code, Some(-952));
        assert_eq!(error.sql_state.as_deref(), Some("57014"));

        let ebcdic = ddm::pack_bytes(
            cp::SRVDGN,
            &crate::converters::ebcdic::encode_cp500(
                "SQLERRP:SQLRI2D0 SQLCODE:-952 SQLSTATE:57014",
            )
            .unwrap(),
        );
        let error = server_error(&ebcdic);
        assert_eq!(error.kind, Db2ErrorKind::SqlServer);
        assert_eq!(error.sql_code, Some(-952));
        assert_eq!(error.sql_state.as_deref(), Some("57014"));
    }

    #[tokio::test]
    async fn handshake_classifies_database_authentication_and_authorization_rejections() {
        #[derive(Clone, Copy)]
        enum Rejection {
            DatabaseNotFound,
            Authentication,
            Authorization,
        }

        async fn connect_rejected(rejection: Rejection) -> Db2Error {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let initial = read_chain(&mut socket).await;
                assert_eq!(
                    initial
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSAT, cp::ACCSEC]
                );
                if matches!(rejection, Rejection::DatabaseNotFound) {
                    send(&mut socket, cp::RDBNFNRM, &[], false).await;
                    return;
                }

                send(
                    &mut socket,
                    cp::ACCSECRD,
                    &ddm::pack_u16(cp::SECMEC, 3),
                    false,
                )
                .await;
                let auth = read_chain(&mut socket).await;
                assert_eq!(
                    auth.iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::SECCHK, cp::ACCRDB]
                );
                if matches!(rejection, Rejection::Authentication) {
                    send(
                        &mut socket,
                        cp::SECCHKRM,
                        &ddm::pack_bytes(cp::SECCHKCD, &[1]),
                        false,
                    )
                    .await;
                    return;
                }
                send(
                    &mut socket,
                    cp::SECCHKRM,
                    &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                    true,
                )
                .await;
                send(&mut socket, cp::ACCRDBRM, &[], true).await;
                send(&mut socket, cp::RDBATHRM, &[], false).await;
            });

            let mut config = Config::default();
            config.host = "127.0.0.1".into();
            config.port = port;
            config.database = "TESTDB".into();
            config.user = "db2inst1".into();
            config.password = "test-secret".into();
            config.security_mechanism = 3;
            config.timeout_ms = 3_000;
            let error = Db2Session::connect(config)
                .await
                .err()
                .expect("mock server should reject connection");
            server.await.unwrap();
            assert!(!error.to_string().contains("test-secret"));
            error
        }

        for (rejection, expected_kind) in [
            (Rejection::DatabaseNotFound, Db2ErrorKind::DatabaseNotFound),
            (
                Rejection::Authentication,
                Db2ErrorKind::AuthenticationFailed,
            ),
            (Rejection::Authorization, Db2ErrorKind::AuthorizationFailed),
        ] {
            let error = connect_rejected(rejection).await;
            assert_eq!(error.kind, expected_kind, "{error}");
        }
    }

    #[tokio::test]
    async fn handshake_rejects_server_changing_the_requested_security_mechanism() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let initial = read_chain(&mut socket).await;
            assert_eq!(
                initial
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            let requested = ddm::find_ddm_parameter(&initial[1].payload, cp::SECMEC)
                .unwrap()
                .unwrap();
            assert_eq!(u16::from_be_bytes([requested[0], requested[1]]), 3);
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 4),
                false,
            )
            .await;
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(3), socket.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0,
                "the client must close rather than issue a fallback ACCSEC/SECCHK"
            );
        });

        let config = Config {
            host: "127.0.0.1".into(),
            port,
            database: "TESTDB".into(),
            user: "db2inst1".into(),
            password: "password".into(),
            security_mechanism: 3,
            ..Config::default()
        };
        let error = match Db2Session::connect(config).await {
            Ok(_) => panic!("a changed security mechanism must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("selected security mechanism 4"));
        server.await.unwrap();
    }

    #[test]
    fn closed_session_check_has_stable_connection_closed_kind() {
        let (stream, _peer) = tokio::io::duplex(1);
        let session = Db2Session {
            stream: Box::new(stream),
            pool_abort: None,
            config: Config::default(),
            database: String::new(),
            security_mechanism: 3,
            query_block_size: 65_535,
            correlation_id: 1,
            auto_commit: true,
            closed: true,
            prepared_sql: None,
            current_user: String::new(),
            base_user: String::new(),
            client_info: Db2ClientInfo::default(),
            base_client_info: Db2ClientInfo::default(),
            base_schema: None,
            base_isolation: None,
            current_schema: None,
            current_isolation: None,
            pool_baseline_captured: false,
            pool_dirty: SessionDirtyState::default(),
            warning_handler: None,
            rdb_interrupt_token: None,
            interrupt_coordinator: Arc::new(InterruptCoordinator::default()),
        };

        let error = session.ensure_open().unwrap_err();
        assert_eq!(error.kind, Db2ErrorKind::ConnectionClosed);
        assert_eq!(error.to_string(), "DB2 connection is closed");
    }

    #[tokio::test]
    async fn dropping_command_after_request_invalidates_session() {
        let (stream, mut peer) = tokio::io::duplex(4096);
        let mut session = Db2Session {
            stream: Box::new(stream),
            pool_abort: None,
            config: Config::default(),
            database: String::new(),
            security_mechanism: 3,
            query_block_size: 65_535,
            correlation_id: 1,
            auto_commit: true,
            closed: false,
            prepared_sql: None,
            current_user: String::new(),
            base_user: String::new(),
            client_info: Db2ClientInfo::default(),
            base_client_info: Db2ClientInfo::default(),
            base_schema: None,
            base_isolation: None,
            current_schema: None,
            current_isolation: None,
            pool_baseline_captured: false,
            pool_dirty: SessionDirtyState::default(),
            warning_handler: None,
            rdb_interrupt_token: None,
            interrupt_coordinator: Arc::new(InterruptCoordinator::default()),
        };
        let mut command = Box::pin(session.ping());
        tokio::select! {
            result = &mut command => panic!("command completed without a server reply: {result:?}"),
            request = dss::read_dss(&mut peer) => {
                assert_eq!(request.unwrap().codepoint, cp::RDBCMM);
            }
        }
        drop(command);
        assert!(
            session.is_closed(),
            "dropped request must not leave a reusable transport"
        );
        assert_eq!(
            session.ping().await.unwrap_err().kind,
            Db2ErrorKind::ConnectionClosed
        );
    }

    #[tokio::test]
    async fn dropping_partial_reply_poisoned_even_while_operation_guard_remains() {
        let (stream, mut peer) = tokio::io::duplex(4096);
        let mut session = Db2Session {
            stream: Box::new(stream),
            pool_abort: None,
            config: Config::default(),
            database: String::new(),
            security_mechanism: 3,
            query_block_size: 65_535,
            correlation_id: 1,
            auto_commit: true,
            closed: false,
            prepared_sql: None,
            current_user: String::new(),
            base_user: String::new(),
            client_info: Db2ClientInfo::default(),
            base_client_info: Db2ClientInfo::default(),
            base_schema: None,
            base_isolation: None,
            current_schema: None,
            current_isolation: None,
            pool_baseline_captured: false,
            pool_dirty: SessionDirtyState::default(),
            warning_handler: None,
            rdb_interrupt_token: None,
            interrupt_coordinator: Arc::new(InterruptCoordinator::default()),
        };
        let operation = session.interrupt_coordinator.begin_operation().await;
        session
            .send(&ddm::pack_commit(), false, true, None)
            .await
            .unwrap();
        dss::read_dss(&mut peer).await.unwrap();
        peer.write_all(&[0, 16]).await.unwrap();
        let mut reply = Box::pin(session.read_reply_chain());
        tokio::select! {
            result = &mut reply => panic!("partial DSS must stay pending: {result:?}"),
            _ = tokio::task::yield_now() => {},
        }
        drop(reply);
        assert!(session.is_closed());
        assert_eq!(
            session.ensure_open().unwrap_err().kind,
            Db2ErrorKind::ConnectionClosed
        );
        drop(operation);
    }

    #[tokio::test]
    async fn dropping_partial_request_write_invalidates_session() {
        let (stream, _peer) = tokio::io::duplex(1);
        let mut session = Db2Session {
            stream: Box::new(stream),
            pool_abort: None,
            config: Config::default(),
            database: String::new(),
            security_mechanism: 3,
            query_block_size: 65_535,
            correlation_id: 1,
            auto_commit: true,
            closed: false,
            prepared_sql: None,
            current_user: String::new(),
            base_user: String::new(),
            client_info: Db2ClientInfo::default(),
            base_client_info: Db2ClientInfo::default(),
            base_schema: None,
            base_isolation: None,
            current_schema: None,
            current_isolation: None,
            pool_baseline_captured: false,
            pool_dirty: SessionDirtyState::default(),
            warning_handler: None,
            rdb_interrupt_token: None,
            interrupt_coordinator: Arc::new(InterruptCoordinator::default()),
        };
        let mut write = Box::pin(session.send(&[0; 128], false, true, None));
        tokio::select! {
            result = &mut write => panic!("small duplex must keep write pending: {result:?}"),
            _ = tokio::task::yield_now() => {},
        }
        drop(write);
        assert!(session.is_closed());
    }

    #[tokio::test]
    async fn prepared_autocommit_writes_one_exact_execute_sql_dta_commit_chain() {
        let (stream, mut peer) = tokio::io::duplex(4096);
        let mut session = dummy_session(true);
        session.stream = Box::new(stream);
        let statement = Db2Statement {
            sql: "INSERT INTO T VALUES (?)".into(),
            output_columns: Vec::new(),
            parameter_columns: vec![ColumnDescription {
                name: "P1".into(),
                sql_type: 497,
                length: 4,
                precision: 0,
                scale: 0,
                nullable: true,
            }],
        };
        let values = [Db2Value::Integer(42)];
        let mut parameters = value_parameter_inputs(&values);
        let mut execution = Box::pin(session.execute_prepared_inputs_inner(
            &statement,
            &mut parameters,
            true,
            None,
            None,
        ));
        let mut packets = Vec::new();
        for _ in 0..3 {
            let packet = tokio::select! {
                result = &mut execution => panic!("execution completed without a server reply: {result:?}"),
                packet = dss::read_dss(&mut peer) => packet.unwrap(),
            };
            packets.push(packet);
        }
        assert_eq!(
            packets
                .iter()
                .map(|packet| packet.codepoint)
                .collect::<Vec<_>>(),
            vec![cp::EXCSQLSTT, cp::SQLDTA, cp::RDBCMM]
        );
        assert!(packets[0].header.chained && packets[0].header.same_id);
        assert!(packets[1].header.chained && !packets[1].header.same_id);
        assert!(!packets[2].header.chained && !packets[2].header.same_id);
        assert!(
            packets
                .iter()
                .all(|packet| packet.header.correlation_id == 1)
        );
        drop(execution);
        assert!(
            session.is_closed(),
            "cancellation after the chain must poison it"
        );
    }

    #[tokio::test]
    async fn interrupt_coordinator_ignores_idle_and_duplicate_interrupts() {
        let coordinator = Arc::new(InterruptCoordinator::default());
        assert!(coordinator.begin_interrupt().is_none());

        let operation = coordinator.begin_operation().await;
        let interrupt = coordinator.begin_interrupt().unwrap();
        assert!(coordinator.begin_interrupt().is_none());
        drop(operation);
        assert_eq!(coordinator.state.load(Ordering::Acquire), INTERRUPT_PENDING);
        drop(interrupt);
        assert_eq!(coordinator.state.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn guard_drop_before_wire_keeps_session_but_unconfirmed_interrupt_poisoned() {
        let coordinator = Arc::new(InterruptCoordinator::default());
        drop(coordinator.begin_operation().await);
        assert!(!coordinator.poisoned.load(Ordering::Acquire));
        let operation = coordinator.begin_operation().await;
        let interrupt = coordinator.begin_interrupt().unwrap();
        coordinator.interrupt_sent.store(true, Ordering::Release);
        drop(interrupt);
        assert!(coordinator.poisoned.load(Ordering::Acquire));
        drop(operation);
        assert_eq!(coordinator.state.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn next_operation_waits_until_interrupt_exchange_finishes() {
        let coordinator = Arc::new(InterruptCoordinator::default());
        let operation = coordinator.begin_operation().await;
        let interrupt = coordinator.begin_interrupt().unwrap();
        let next_operation = coordinator.begin_operation();
        tokio::pin!(next_operation);
        assert!(
            timeout(Duration::from_millis(10), &mut next_operation)
                .await
                .is_err()
        );

        drop(operation);
        assert_eq!(coordinator.state.load(Ordering::Acquire), INTERRUPT_PENDING);
        drop(interrupt);
        let _next = timeout(Duration::from_millis(100), &mut next_operation)
            .await
            .unwrap();
        assert_eq!(coordinator.state.load(Ordering::Acquire), INTERRUPT_ACTIVE);
    }

    #[test]
    fn packed_decimal_retains_precision_and_sign() {
        assert_eq!(decode_packed_decimal(&[0x12, 0x50, 0x0c], 2), "125.00");
        assert_eq!(decode_packed_decimal(&[0x00, 0x12, 0x5d], 2), "-1.25");
    }

    #[test]
    fn malformed_packed_decimal_precision_fails_without_panicking() {
        let fields = [FieldDescriptor {
            field_type: 0x0e,
            precision_scale: [u8::MAX, 0],
        }];
        let error = decode_rows_incremental(&fields, &[0xff, 0, 0]).unwrap_err();
        assert_eq!(error.kind, Db2ErrorKind::Protocol);
        assert!(error.to_string().contains("packed DECIMAL precision 255"));
    }

    #[test]
    fn session_user_identifier_validation_rejects_sql_injection() {
        assert_eq!(validate_user_identifier(" tenant_1 ").unwrap(), "tenant_1");
        assert!(validate_user_identifier("alice\"; DROP TABLE T").is_err());
        assert!(validate_user_identifier("").is_err());
    }

    #[test]
    fn pool_schema_identifier_quoting_preserves_delimited_whitespace_and_quotes() {
        assert_eq!(
            quote_identifier(" Mixed \"Schema\" ").unwrap(),
            "\" Mixed \"\"Schema\"\" \""
        );
        assert!(quote_identifier("").is_err());
        assert!(quote_identifier("SCHEMA\0NAME").is_err());
    }

    #[tokio::test]
    async fn mock_server_completes_handshake_exec_and_query() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let initial = read_chain(&mut socket).await;
            assert_eq!(
                initial.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;

            let auth = read_chain(&mut socket).await;
            assert_eq!(
                auth.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;

            let setup = read_chain(&mut socket).await;
            assert_eq!(
                setup.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::RDBCMM);
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            let execute = read_chain(&mut socket).await;
            assert_eq!(
                execute.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSQLIMM, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(1), false).await;

            let query = read_chain(&mut socket).await;
            assert_eq!(
                query.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(&mut socket, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], true).await;
            let mut id = vec![0; 8];
            id.copy_from_slice(&1u64.to_be_bytes());
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &id),
                true,
            )
            .await;
            send(&mut socket, cp::QRYDTA, &[0xff, 0, 42, 0, 0, 0], true).await;
            send(&mut socket, cp::ENDQRYRM, &[], false).await;

            let prepare = read_chain(&mut socket).await;
            assert_eq!(
                prepare.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::DSCSQLSTT]
            );
            let mut parameter_dard = vec![0xff, 1, 1, 0, 0, 0, 0, 0];
            parameter_dard.extend_from_slice(&4u64.to_le_bytes());
            parameter_dard.extend_from_slice(&497u16.to_le_bytes());
            parameter_dard.extend_from_slice(&[0, 0]);
            send(&mut socket, cp::SQLDARD, &parameter_dard, false).await;

            let parameter_query = read_chain(&mut socket).await;
            assert_eq!(
                parameter_query
                    .iter()
                    .map(|p| p.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::OPNQRY, cp::SQLDTA]
            );
            assert_eq!(
                parameter_query[1].payload,
                vec![
                    0x00, 0x10, 0x00, 0x10, 0x06, 0x76, 0xd0, 0x03, 0x00, 0x04, 0x06, 0x71, 0xe4,
                    0xd0, 0x00, 0x01, 0x00, 0x0a, 0x14, 0x7a, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00,
                ]
            );
            send(&mut socket, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], true).await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &id),
                true,
            )
            .await;
            send(&mut socket, cp::QRYDTA, &[0xff, 0, 42, 0, 0, 0], true).await;
            send(&mut socket, cp::ENDQRYRM, &[], false).await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::RDBCMM);
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        let mut session = Db2Session::connect(config).await.unwrap();
        session.ping().await.unwrap();
        assert_eq!(session.execute("VALUES 1").await.unwrap(), 1);
        let result = session.query("VALUES CAST(42 AS INTEGER)").await.unwrap();
        assert!(result.columns.is_empty());
        assert_eq!(result.rows, vec![vec![Db2Value::Integer(42)]]);
        let statement = session.prepare("VALUES CAST(? AS INTEGER)").await.unwrap();
        assert_eq!(statement.parameter_columns.len(), 1);
        let parameter_result = session
            .query_prepared(&statement, &[Db2Value::Integer(42)])
            .await
            .unwrap();
        assert_eq!(parameter_result.rows, vec![vec![Db2Value::Integer(42)]]);
        session.close().await.unwrap();
        server.await.unwrap();
    }

    fn sectkn_fields(payload: &[u8]) -> Vec<Vec<u8>> {
        let mut tokens = Vec::new();
        let mut position = 0;
        while position + 4 <= payload.len() {
            let length = u16::from_be_bytes([payload[position], payload[position + 1]]) as usize;
            if length < 4 || position + length > payload.len() {
                break;
            }
            let codepoint = u16::from_be_bytes([payload[position + 2], payload[position + 3]]);
            if codepoint == cp::SECTKN.0 {
                tokens.push(payload[position + 4..position + length].to_vec());
            }
            position += length;
        }
        tokens
    }

    fn secmec9_from_hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
            .collect()
    }

    fn secmec9_des_decrypt(key: &[u8], iv: &[u8], ciphertext: &[u8]) -> Vec<u8> {
        use des::cipher::{Block, BlockDecrypt, KeyInit};
        let cipher = des::Des::new_from_slice(key).unwrap();
        let mut previous = [0u8; 8];
        previous.copy_from_slice(iv);
        let mut output = Vec::new();
        for chunk in ciphertext.as_chunks::<8>().0 {
            let mut block = Block::<des::Des>::clone_from_slice(chunk);
            let incoming = block;
            cipher.decrypt_block(&mut block);
            for (index, byte) in block.iter_mut().enumerate() {
                *byte ^= previous[index];
            }
            previous.copy_from_slice(&incoming);
            output.extend_from_slice(&block);
        }
        let padding = *output.last().unwrap() as usize;
        output.truncate(output.len() - padding);
        output
    }

    #[tokio::test]
    async fn secmec9_handshake_exchanges_keys_in_accsec_and_encrypts_userid_and_password() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_private: [u8; 32] =
            secmec9_from_hex("1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90a")
                .try_into()
                .unwrap();
        let server_public = crate::security::calculate_public_key(&server_private)
            .unwrap()
            .to_vec();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let initial = read_chain(&mut socket).await;
            assert_eq!(
                initial.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            let accsec = ddm::parse_ddm_parameters(&initial[1].payload).unwrap();
            assert_eq!(
                u16::from_be_bytes([accsec[&cp::SECMEC][0], accsec[&cp::SECMEC][1]]),
                9
            );
            let client_public = accsec[&cp::SECTKN].clone();
            assert_eq!(client_public.len(), 32);

            let mut accsec_rd = ddm::pack_u16(cp::SECMEC, 9);
            accsec_rd.extend_from_slice(&ddm::pack_bytes(cp::SECTKN, &server_public));
            send(&mut socket, cp::ACCSECRD, &accsec_rd, false).await;

            let auth = read_chain(&mut socket).await;
            assert_eq!(
                auth.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            let secchk = ddm::parse_ddm_parameters(&auth[0].payload).unwrap();
            assert_eq!(
                u16::from_be_bytes([secchk[&cp::SECMEC][0], secchk[&cp::SECMEC][1]]),
                9
            );
            // Db2 LUW carries both encrypted values as SECTKN fields, not USRID/PASSWORD.
            let tokens = sectkn_fields(&auth[0].payload);
            assert_eq!(tokens.len(), 2);
            let session_key =
                crate::security::calculate_session_key(&client_public, &server_private).unwrap();
            let userid =
                secmec9_des_decrypt(&session_key[12..20], &server_public[12..20], &tokens[0]);
            let password =
                secmec9_des_decrypt(&session_key[12..20], &server_public[12..20], &tokens[1]);
            assert_eq!(decode_cp500(&userid), "db2inst1");
            assert_eq!(decode_cp500(&password), "secret");

            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(
                &mut socket,
                cp::ACCRDBRM,
                &ddm::pack_bytes(cp::RDBINTTKN, &[4, 28, 167, 50]),
                false,
            )
            .await;

            let setup = read_chain(&mut socket).await;
            assert_eq!(
                setup.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "secret".into();
        config.security_mechanism = 9;
        let mut session = Db2Session::connect(config).await.unwrap();
        session.close().await.unwrap();
        server.await.unwrap();
    }

    fn tls_fixture(name: &str) -> String {
        format!("{}/tests/fixtures/tls/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    async fn tls_read_chain<S>(socket: &mut S) -> Vec<DssPacket>
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        let mut result = Vec::new();
        loop {
            let packet = dss::read_dss(socket).await.unwrap();
            let chained = packet.header.chained;
            result.push(packet);
            if !chained {
                return result;
            }
        }
    }

    async fn tls_send<S>(socket: &mut S, codepoint: CodePoint, payload: &[u8], chained: bool)
    where
        S: tokio::io::AsyncWrite + Unpin,
    {
        use tokio::io::AsyncWriteExt;
        let object = ddm::pack_ddm_object(codepoint, payload);
        socket
            .write_all(&dss::encode_reply_dss(&object, chained, 1))
            .await
            .unwrap();
    }

    fn mutual_tls_acceptor() -> tokio_rustls::TlsAcceptor {
        use std::io::BufReader;
        use std::sync::Arc;
        use tokio_rustls::rustls;
        let ca = CertificateDer::pem_reader_iter(&mut BufReader::new(
            std::fs::File::open(tls_fixture("ca.pem")).unwrap(),
        ))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        for certificate in ca {
            roots.add(certificate).unwrap();
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        let server_certs = CertificateDer::pem_reader_iter(&mut BufReader::new(
            std::fs::File::open(tls_fixture("server.pem")).unwrap(),
        ))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let server_key = PrivateKeyDer::pem_reader_iter(&mut BufReader::new(
            std::fs::File::open(tls_fixture("server-key.pem")).unwrap(),
        ))
        .next()
        .unwrap()
        .unwrap();
        let config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(server_certs, server_key)
            .unwrap();
        tokio_rustls::TlsAcceptor::from(Arc::new(config))
    }

    async fn serve_mutual_tls_setup<S>(socket: &mut S)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let initial = tls_read_chain(socket).await;
        assert_eq!(
            initial.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
            vec![cp::EXCSAT, cp::ACCSEC]
        );
        tls_send(socket, cp::ACCSECRD, &ddm::pack_u16(cp::SECMEC, 3), false).await;

        let auth = tls_read_chain(socket).await;
        assert_eq!(
            auth.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
            vec![cp::SECCHK, cp::ACCRDB]
        );
        tls_send(
            socket,
            cp::SECCHKRM,
            &ddm::pack_bytes(cp::SECCHKCD, &[0]),
            true,
        )
        .await;
        tls_send(
            socket,
            cp::ACCRDBRM,
            &ddm::pack_bytes(cp::RDBINTTKN, &[4, 28, 167, 50]),
            false,
        )
        .await;

        let setup = tls_read_chain(socket).await;
        assert_eq!(
            setup.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
            vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
        );
        tls_send(socket, cp::SQLCARD, &test_sql_card(0), false).await;
    }

    #[test]
    fn configured_ca_file_replaces_system_roots() {
        let mut config = Config::default();
        config.ssl_root_ca_path = tls_fixture("ca.pem");
        let roots = build_root_store(&config).unwrap();
        assert_eq!(roots.roots.len(), 1);
    }

    #[test]
    fn ca_file_without_certificates_is_rejected() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut config = Config::default();
        config.ssl_root_ca_path = file.path().to_string_lossy().into_owned();
        let error = build_root_store(&config).unwrap_err();
        assert!(
            error.to_string().contains("contains no certificates"),
            "{error}"
        );
    }

    fn mutual_tls_config(port: u16, client_cert: Option<&str>, client_key: Option<&str>) -> Config {
        let mut config = Config::default();
        config.host = "localhost".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "secret".into();
        config.use_ssl = true;
        config.ssl_root_ca_path = tls_fixture("ca.pem");
        config.ssl_client_cert_path = client_cert.map(tls_fixture).unwrap_or_default();
        config.ssl_client_key_path = client_key.map(tls_fixture).unwrap_or_default();
        config
    }

    #[tokio::test]
    async fn mutual_tls_with_trusted_client_certificate_completes_the_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = mutual_tls_acceptor();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(socket).await.unwrap();
            serve_mutual_tls_setup(&mut tls).await;
            let _ = tls.into_inner();
        });

        let mut session = Db2Session::connect(mutual_tls_config(
            port,
            Some("client.pem"),
            Some("client-key.pem"),
        ))
        .await
        .unwrap();
        assert!(!session.is_closed());
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn mutual_tls_rejects_a_client_without_a_certificate() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = mutual_tls_acceptor();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = acceptor.accept(socket).await;
        });

        let result = Db2Session::connect(mutual_tls_config(port, None, None)).await;
        assert!(result.is_err());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn mutual_tls_rejects_a_client_certificate_from_an_untrusted_ca() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = mutual_tls_acceptor();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = acceptor.accept(socket).await;
        });

        let result = Db2Session::connect(mutual_tls_config(
            port,
            Some("other-client.pem"),
            Some("other-client-key.pem"),
        ))
        .await;
        assert!(result.is_err());
        server.await.unwrap();
    }

    async fn serve_failing_session_set(socket: &mut tokio::net::TcpStream) {
        let chain = read_chain(socket).await;
        assert_eq!(
            chain.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
            vec![cp::EXCSQLIMM, cp::SQLSTT, cp::RDBCMM]
        );
        send(
            socket,
            cp::SQLCARD,
            &test_sql_card_error(-551, "42501"),
            false,
        )
        .await;
    }

    async fn serve_switch_secchk(
        socket: &mut tokio::net::TcpStream,
    ) -> (u16, String, Option<Vec<u8>>) {
        let chain = read_chain(socket).await;
        assert_eq!(
            chain.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
            vec![cp::SECCHK]
        );
        let params = ddm::parse_ddm_parameters(&chain[0].payload).unwrap();
        (
            u16::from_be_bytes([params[&cp::SECMEC][0], params[&cp::SECMEC][1]]),
            decode_cp500(&params[&cp::USRID]),
            params.get(&cp::PASSWORD).cloned(),
        )
    }

    #[tokio::test]
    async fn switch_user_falls_back_to_standalone_secchk_with_password() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let initial = read_chain(&mut socket).await;
            assert_eq!(
                initial.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            let auth = read_chain(&mut socket).await;
            assert_eq!(
                auth.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;
            let setup = read_chain(&mut socket).await;
            assert_eq!(
                setup.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            serve_failing_session_set(&mut socket).await;
            serve_failing_session_set(&mut socket).await;
            let (mechanism, user, password) = serve_switch_secchk(&mut socket).await;
            assert_eq!(mechanism, 3);
            assert_eq!(user, "db2test2");
            assert_eq!(decode_cp500(password.as_deref().unwrap()), "secret");
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                false,
            )
            .await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "secret".into();
        config.security_mechanism = 3;
        let mut session = Db2Session::connect(config).await.unwrap();
        session
            .switch_user_with_password("db2test2", "secret")
            .await
            .unwrap();
        assert_eq!(session.current_session_user(), "db2test2");
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn switch_user_without_password_attempts_fallback_and_surfaces_secchkcd() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let initial = read_chain(&mut socket).await;
            assert_eq!(
                initial.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            let auth = read_chain(&mut socket).await;
            assert_eq!(
                auth.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;
            let setup = read_chain(&mut socket).await;
            assert_eq!(
                setup.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            serve_failing_session_set(&mut socket).await;
            serve_failing_session_set(&mut socket).await;
            let (_, _, password) = serve_switch_secchk(&mut socket).await;
            assert!(password.is_none());
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[4]),
                false,
            )
            .await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "secret".into();
        config.security_mechanism = 3;
        let mut session = Db2Session::connect(config).await.unwrap();
        let error = session.switch_user("db2test2").await.unwrap_err();
        assert_eq!(error.kind, Db2ErrorKind::AuthenticationFailed);
        assert!(error.to_string().contains("SECCHKCD=4"));
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn switch_user_fallback_fails_closed_for_mechanism9() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let initial = read_chain(&mut socket).await;
            assert_eq!(
                initial.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            let auth = read_chain(&mut socket).await;
            assert_eq!(
                auth.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;
            let setup = read_chain(&mut socket).await;
            assert_eq!(
                setup.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            serve_failing_session_set(&mut socket).await;
            serve_failing_session_set(&mut socket).await;
            let mut buffer = [0u8; 1];
            let read = tokio::time::timeout(
                std::time::Duration::from_millis(300),
                socket.read(&mut buffer),
            )
            .await;
            assert!(
                read.is_err() || matches!(read, Ok(Ok(0))),
                "mechanism 9 must not send a plaintext-password SECCHK fallback"
            );
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "secret".into();
        config.security_mechanism = 3;
        let mut session = Db2Session::connect(config).await.unwrap();
        // The tests module can reach the private field to model a mechanism-9 session
        // without repeating the full DH handshake here.
        session.security_mechanism = 9;
        let error = session
            .switch_user_with_password("db2test2", "secret")
            .await
            .unwrap_err();
        assert_eq!(error.kind, Db2ErrorKind::Unsupported);
        assert!(error.to_string().contains("mid-session re-authentication"));
        server.await.unwrap();
    }

    async fn serve_basic_handshake(socket: &mut tokio::net::TcpStream) {
        let initial = read_chain(socket).await;
        assert_eq!(
            initial.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
            vec![cp::EXCSAT, cp::ACCSEC]
        );
        send(socket, cp::ACCSECRD, &ddm::pack_u16(cp::SECMEC, 3), false).await;
        let auth = read_chain(socket).await;
        assert_eq!(
            auth.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
            vec![cp::SECCHK, cp::ACCRDB]
        );
        send(
            socket,
            cp::SECCHKRM,
            &ddm::pack_bytes(cp::SECCHKCD, &[0]),
            true,
        )
        .await;
        send(socket, cp::ACCRDBRM, &[], false).await;
        let setup = read_chain(socket).await;
        assert_eq!(
            setup.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
            vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
        );
        send(socket, cp::SQLCARD, &test_sql_card(0), false).await;
    }

    fn sql_statement_text(packet: &DssPacket) -> String {
        let payload = packet.payload.as_slice();
        let length = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]) as usize;
        String::from_utf8_lossy(&payload[5..5 + length]).to_string()
    }

    async fn serve_execute_statement(
        socket: &mut tokio::net::TcpStream,
        expected_sql: &str,
        codepoint: CodePoint,
    ) {
        let chain = read_chain(socket).await;
        assert_eq!(
            chain.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
            vec![codepoint, cp::SQLSTT, cp::RDBCMM]
        );
        let statement = chain
            .iter()
            .find(|packet| packet.codepoint == cp::SQLSTT)
            .expect("SQLSTT");
        assert_eq!(sql_statement_text(statement), expected_sql);
        send(socket, cp::SQLCARD, &test_sql_card(0), false).await;
    }

    #[tokio::test]
    async fn per_command_context_switches_user_only_when_it_differs() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            serve_basic_handshake(&mut socket).await;
            serve_execute_statement(
                &mut socket,
                "SET SESSION AUTHORIZATION = \"BORROWER\"",
                cp::EXCSQLIMM,
            )
            .await;
            serve_execute_statement(&mut socket, "VALUES 1", cp::EXCSQLIMM).await;
            // The second command targets the same user, so no switch statement is sent.
            serve_execute_statement(&mut socket, "VALUES 1", cp::EXCSQLIMM).await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "secret".into();
        config.security_mechanism = 3;
        let mut session = Db2Session::connect(config).await.unwrap();
        let context = Db2CommandContext::default().with_user("BORROWER");
        session
            .execute_with_context("VALUES 1", &context)
            .await
            .unwrap();
        session
            .execute_with_context("VALUES 1", &context)
            .await
            .unwrap();
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn per_command_context_applies_client_info_only_when_it_differs() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            serve_basic_handshake(&mut socket).await;
            serve_execute_statement(&mut socket, "SET CLIENT APPLNAME 'svc'", cp::EXCSQLSET).await;
            serve_execute_statement(&mut socket, "VALUES 1", cp::EXCSQLIMM).await;
            // The register is already set, so the second command sends nothing extra.
            serve_execute_statement(&mut socket, "VALUES 1", cp::EXCSQLIMM).await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "secret".into();
        config.security_mechanism = 3;
        let mut session = Db2Session::connect(config).await.unwrap();
        let context = Db2CommandContext::default().with_client_info(Db2ClientInfo {
            application_name: "svc".into(),
            ..Db2ClientInfo::default()
        });
        session
            .execute_with_context("VALUES 1", &context)
            .await
            .unwrap();
        session
            .execute_with_context("VALUES 1", &context)
            .await
            .unwrap();
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn per_command_context_rejects_an_invalid_user_before_the_network() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            serve_basic_handshake(&mut socket).await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "secret".into();
        config.security_mechanism = 3;
        let mut session = Db2Session::connect(config).await.unwrap();
        let context = Db2CommandContext::default().with_user("bad\"; DROP TABLE T");
        let error = session
            .execute_with_context("VALUES 1", &context)
            .await
            .unwrap_err();
        assert_eq!(error.kind, Db2ErrorKind::InvalidConfiguration);
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn kerberos_mechanism_requires_a_token_file_before_connecting() {
        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = 1;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "secret".into();
        config.security_mechanism = 11;
        let error = match Db2Session::connect(config).await {
            Ok(_) => panic!("connect with mechanism 11 and no token file must fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind, Db2ErrorKind::InvalidConfiguration);
        assert!(error.to_string().contains("gssapi_token_file"));
    }

    #[tokio::test]
    async fn kerberos_handshake_sends_the_token_file_bytes_in_secchk() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let token = vec![0x60, 0x82, 1, 2, 3, 4, 5, 6, 7, 8];
        let token_path =
            std::env::temp_dir().join(format!("db2-gssapi-token-{}.bin", std::process::id()));
        std::fs::write(&token_path, &token).unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let initial = read_chain(&mut socket).await;
            assert_eq!(
                initial.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            let accsec = ddm::parse_ddm_parameters(&initial[1].payload).unwrap();
            assert_eq!(
                u16::from_be_bytes([accsec[&cp::SECMEC][0], accsec[&cp::SECMEC][1]]),
                11
            );
            // No key exchange token is expected: the caller supplies the token.
            assert!(!accsec.contains_key(&cp::SECTKN));

            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 11),
                false,
            )
            .await;

            let auth = read_chain(&mut socket).await;
            assert_eq!(
                auth.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            let secchk = ddm::parse_ddm_parameters(&auth[0].payload).unwrap();
            assert_eq!(secchk[&cp::SECTKN], token);
            assert_eq!(decode_cp500(&secchk[&cp::USRID]), "db2inst1");
            assert!(!secchk.contains_key(&cp::PASSWORD));

            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(
                &mut socket,
                cp::ACCRDBRM,
                &ddm::pack_bytes(cp::RDBINTTKN, &[4, 28, 167, 50]),
                false,
            )
            .await;

            let setup = read_chain(&mut socket).await;
            assert_eq!(
                setup.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "secret".into();
        config.security_mechanism = 11;
        config.gssapi_token_file = token_path.to_string_lossy().into_owned();
        let mut session = Db2Session::connect(config).await.unwrap();
        session.close().await.unwrap();
        server.await.unwrap();
        std::fs::remove_file(&token_path).ok();
    }

    #[test]
    fn pack_secchk_kerberos_carries_the_token_without_a_password() {
        let token = [0x60, 0x82, 1, 2, 3];
        let secchk = ddm::pack_secchk_kerberos(&token, "TESTDB", "db2inst1").unwrap();
        let params = ddm::parse_ddm_parameters(&secchk[4..]).unwrap();
        assert_eq!(
            u16::from_be_bytes([params[&cp::SECMEC][0], params[&cp::SECMEC][1]]),
            11
        );
        assert_eq!(params[&cp::SECTKN], token);
        assert_eq!(decode_cp500(&params[&cp::USRID]), "db2inst1");
        assert!(!params.contains_key(&cp::PASSWORD));
    }

    #[tokio::test]
    async fn streamed_local_parameter_conversion_error_keeps_session_reusable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let initial = read_chain(&mut socket).await;
            assert_eq!(
                initial
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;

            let auth = read_chain(&mut socket).await;
            assert_eq!(
                auth.iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;

            let setup = read_chain(&mut socket).await;
            assert_eq!(
                setup
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::RDBCMM);
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            let prepare = read_chain(&mut socket).await;
            assert_eq!(
                prepare
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::DSCSQLSTT]
            );
            let mut parameter_dard = vec![0xff, 1, 1, 0, 0, 0, 0, 0];
            parameter_dard.extend_from_slice(&2u64.to_le_bytes());
            parameter_dard.extend_from_slice(&501u16.to_le_bytes());
            parameter_dard.extend_from_slice(&[0, 0]);
            assert_eq!(
                ddm::parse_sql_dard(&parameter_dard, true).unwrap()[0].sql_type,
                501
            );
            send(&mut socket, cp::SQLDARD, &parameter_dard, false).await;

            let query = read_chain(&mut socket).await;
            assert_eq!(
                query
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY],
                "only the follow-up recovery query should send OPNQRY"
            );
            let id = 1u64.to_be_bytes();
            send(&mut socket, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], true).await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &id),
                true,
            )
            .await;
            send(&mut socket, cp::QRYDTA, &[0xff, 0, 29, 0, 0, 0], true).await;
            send(&mut socket, cp::ENDQRYRM, &[], false).await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        let mut session = Db2Session::connect(config).await.unwrap();
        session.ping().await.unwrap();

        let error = session
            .query_stream_with_params(
                "VALUES CAST(? AS SMALLINT)",
                &[Db2Value::Bytes(b"123".to_vec())],
            )
            .await
            .err()
            .expect("unsupported binary-to-smallint conversion should fail locally");
        assert_eq!(error.kind, Db2ErrorKind::ParameterConversion, "{error}");
        assert!(
            !session.is_closed(),
            "local parameter validation happens before query execution"
        );
        assert_eq!(
            session.query("VALUES 29").await.unwrap().rows,
            vec![vec![Db2Value::Integer(29)]]
        );

        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn progressive_lob_chunk_without_progress_fails_and_closes_the_session() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(
                &mut socket,
                cp::QRYDSC,
                &[6, 0x76, 0xd0, 0xc9, 0x80, 9],
                true,
            )
            .await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                false,
            )
            .await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
            let mut row = vec![0xff, 0, 0, 0x03];
            row.extend_from_slice(&1_u64.to_be_bytes());
            row.extend_from_slice(&0x1122_3344_5566_7788_u64.to_be_bytes());
            send(&mut socket, cp::QRYDTA, &row, false).await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::GETNXTCHK);
            send(&mut socket, cp::EXTDTA, &[0], true).await;
            send(
                &mut socket,
                cp::ENDDTARM,
                &ddm::pack_u16(cp::SVRCOD, 0),
                false,
            )
            .await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 3_000;
        let mut session = Db2Session::connect(config).await.unwrap();

        let mut rows = session
            .query_stream("SELECT B FROM PROGRESSIVE_LOB_FIXTURE")
            .await
            .unwrap();
        let row = rows
            .next()
            .await
            .unwrap()
            .expect("locator row should be yielded");
        let Db2Value::Lob(locator) = &row[0] else {
            panic!("expected a progressive LOB locator, got {row:?}");
        };
        let mut lob = rows.open_lob(locator).await.unwrap();
        let error = lob.next_chunk().await.unwrap_err();

        assert!(
            error
                .message
                .contains("GETNXTCHK made no progress before the final chunk"),
            "{error}"
        );
        let _ = lob;
        drop(rows);
        assert!(session.is_closed());
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn progressive_lob_peer_reset_invalidates_the_session() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(
                &mut socket,
                cp::QRYDSC,
                &[6, 0x76, 0xd0, 0xc9, 0x80, 9],
                true,
            )
            .await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                false,
            )
            .await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
            let mut row = vec![0xff, 0, 0, 0x03];
            row.extend_from_slice(&300_000_u64.to_be_bytes());
            row.extend_from_slice(&0x1122_3344_5566_7788_u64.to_be_bytes());
            send(&mut socket, cp::QRYDTA, &row, false).await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::GETNXTCHK);
            // Model a peer reset before the first chunk arrives.
            drop(socket);
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 3_000;
        let mut session = Db2Session::connect(config).await.unwrap();

        let mut rows = session
            .query_stream("SELECT B FROM PROGRESSIVE_LOB_FIXTURE")
            .await
            .unwrap();
        let row = rows
            .next()
            .await
            .unwrap()
            .expect("locator row should be yielded");
        let Db2Value::Lob(locator) = &row[0] else {
            panic!("expected a progressive LOB locator, got {row:?}");
        };
        let mut lob = rows.open_lob(locator).await.unwrap();
        let error = lob.next_chunk().await.unwrap_err();

        assert!(!error.message.is_empty(), "{error}");
        let _ = lob;
        drop(rows);
        assert!(
            session.is_closed(),
            "a peer reset during GETNXTCHK leaves the locator uncertain"
        );
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reply_chain_packet_limit_fails_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            // 4097 tiny replies, all chained except the last, exceed the 4096-packet cap.
            for index in 0..=4096usize {
                send(&mut socket, cp::SQLCARD, &test_sql_card(0), index < 4096).await;
            }
            drop(socket);
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 5_000;
        let mut session = Db2Session::connect(config).await.unwrap();

        let error = match session
            .query_stream("SELECT B FROM EXCESSIVE_CHAIN_FIXTURE")
            .await
        {
            Ok(_) => panic!("expected the reply-chain limit to reject the query"),
            Err(error) => error,
        };
        assert!(
            error
                .message
                .contains("reply chain exceeds configured limits"),
            "{error}"
        );
        assert!(
            session.is_closed(),
            "an oversized reply chain must not leave a reusable session"
        );
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn progressive_lob_row_is_lazy_and_streams_chunks_in_locator_order() {
        use tokio::io::AsyncReadExt;

        let binary = (0..70_000)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let text = "Zażółć gęślą jaźń – 日本語".as_bytes().to_vec();
        let binary_for_server = binary.clone();
        let text_for_server = text.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(
                &mut socket,
                cp::QRYDSC,
                &[
                    6, 0x76, 0xd0, 0xc9, 0x80, 9, 6, 0x76, 0xd0, 0xc6, 0x80, 9, 6, 0x76, 0xd0,
                    0xf6, 0x80, 9,
                ],
                true,
            )
            .await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                false,
            )
            .await;

            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
            let mut row = vec![0xff, 0, 0, 0x03];
            row.extend_from_slice(&70_000_u64.to_be_bytes());
            row.extend_from_slice(&0x1122_3344_5566_7788_u64.to_be_bytes());
            row.push(0x03);
            row.extend_from_slice(&(text_for_server.len() as u64).to_be_bytes());
            row.extend_from_slice(&0x8877_6655_4433_2211_u64.to_be_bytes());
            row.push(0x03);
            row.extend_from_slice(&0_u64.to_be_bytes());
            row.extend_from_slice(&0x0102_0304_0506_0708_u64.to_be_bytes());
            send(&mut socket, cp::QRYDTA, &row, true).await;
            send(&mut socket, cp::ENDQRYRM, &[], false).await;

            // All locator requests must preserve the row's column order.
            for (expected_reference, bytes) in [
                (0x1122_3344_5566_7788_u64, binary_for_server.as_slice()),
                (0x8877_6655_4433_2211_u64, text_for_server.as_slice()),
            ] {
                let mut offset = 0;
                while offset < bytes.len() {
                    let request = read_chain(&mut socket).await;
                    assert_eq!(request[0].codepoint, cp::GETNXTCHK);
                    let parameters = ddm::parse_ddm_parameters(&request[0].payload).unwrap();
                    assert_eq!(
                        u64::from_be_bytes(parameters[&cp::GETNXTREF][..8].try_into().unwrap()),
                        expected_reference
                    );
                    assert_eq!(
                        u64::from_be_bytes(parameters[&cp::GETNXTLEN][..8].try_into().unwrap()),
                        65_536
                    );
                    let end = (offset + 65_536).min(bytes.len());
                    let final_chunk = end == bytes.len();
                    let mut external = vec![0];
                    external.extend_from_slice(&bytes[offset..end]);
                    send(&mut socket, cp::EXTDTA, &external, true).await;
                    let flags = if final_chunk {
                        [
                            ddm::pack_bytes(cp::ENDREF, &[0xf1]),
                            ddm::pack_bytes(cp::LSTCHK, &[0xf1]),
                        ]
                        .concat()
                    } else {
                        [
                            ddm::pack_bytes(cp::ENDREF, &[0xf0]),
                            ddm::pack_bytes(cp::LSTCHK, &[0xf0]),
                        ]
                        .concat()
                    };
                    send(
                        &mut socket,
                        cp::ENDDTARM,
                        &[ddm::pack_u16(cp::SVRCOD, 0), flags].concat(),
                        false,
                    )
                    .await;
                    offset = end;
                }
            }

            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(&mut socket, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], true).await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                true,
            )
            .await;
            send(&mut socket, cp::QRYDTA, &[0xff, 0, 29, 0, 0, 0], true).await;
            send(&mut socket, cp::ENDQRYRM, &[], false).await;
            let mut byte = [0_u8; 1];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 3_000;
        let mut session = Db2Session::connect(config).await.unwrap();
        let mut reader = session
            .query_stream("SELECT B, C FROM PROGRESSIVE_LOBS")
            .await
            .unwrap();
        let row = reader
            .next()
            .await
            .unwrap()
            .expect("locator row should be yielded");
        let (Db2Value::Lob(binary_lob), Db2Value::Lob(text_lob), Db2Value::Lob(empty_lob)) =
            (&row[0], &row[1], &row[2])
        else {
            panic!("expected progressive LOB values, got {row:?}");
        };
        assert_eq!(binary_lob.byte_length, binary.len() as u64);
        assert_eq!(binary_lob.sql_type, 0xc9);
        assert_eq!(binary_lob.encoding, Db2LobEncoding::Binary);
        assert_eq!(text_lob.byte_length, text.len() as u64);
        assert_eq!(text_lob.sql_type, 0xc6);
        assert_eq!(text_lob.encoding, Db2LobEncoding::Utf8);
        assert_eq!(empty_lob.byte_length, 0);
        assert_eq!(empty_lob.sql_type, 0xf6);
        assert_eq!(empty_lob.encoding, Db2LobEncoding::Utf8);

        // No GETNXTCHK was needed to yield the row; open_lob fetches lazily.
        let mut binary_reader = reader.open_lob(binary_lob).await.unwrap();
        let first_binary_chunk = binary_reader.next_chunk().await.unwrap().unwrap();
        assert_eq!(first_binary_chunk.len(), 65_536);
        assert_eq!(first_binary_chunk, binary[..65_536]);
        // Dropping a partially read handle is safe: opening the next locator drains the rest.
        let _ = binary_reader;

        let mut text_reader = reader.open_lob(text_lob).await.unwrap();
        let mut text_readback = Vec::new();
        while let Some(chunk) = text_reader.next_chunk().await.unwrap() {
            text_readback.extend_from_slice(&chunk);
        }
        assert_eq!(text_readback, text);
        let _ = text_reader;
        let mut empty_reader = reader.open_lob(empty_lob).await.unwrap();
        assert_eq!(empty_reader.next_chunk().await.unwrap(), None);
        let _ = empty_reader;
        assert_eq!(reader.next().await.unwrap(), None);
        drop(reader);
        assert_eq!(
            session.query("VALUES 29").await.unwrap().rows,
            vec![vec![Db2Value::Integer(29)]]
        );
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn early_row_stream_close_drains_same_and_next_block_lobs_and_drop_invalidates_incomplete_reader()
     {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let same_block_locator_observed = Arc::new(AtomicBool::new(false));
        let server_locator_observed = same_block_locator_observed.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(
                &mut socket,
                cp::QRYDSC,
                &[6, 0x76, 0xd0, 0x02, 0, 4, 6, 0x76, 0xd0, 0xc9, 0x80, 9],
                true,
            )
            .await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                false,
            )
            .await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
            send(
                &mut socket,
                cp::QRYDTA,
                &[0xff, 0, 42, 0, 0, 0, 0xff],
                false,
            )
            .await;

            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
            let mut locator_row = vec![0xff, 0, 43, 0, 0, 0, 0, 0x03];
            locator_row.extend_from_slice(&1_u64.to_be_bytes());
            locator_row.extend_from_slice(&0x1122_3344_5566_7788_u64.to_be_bytes());
            send(&mut socket, cp::QRYDTA, &locator_row, false).await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::GETNXTCHK);
            send(&mut socket, cp::EXTDTA, &[0, 0xaa], true).await;
            send(
                &mut socket,
                cp::ENDDTARM,
                &[
                    ddm::pack_u16(cp::SVRCOD, 0),
                    ddm::pack_bytes(cp::ENDREF, &[0xf1]),
                    ddm::pack_bytes(cp::LSTCHK, &[0xf1]),
                ]
                .concat(),
                false,
            )
            .await;

            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
            send(&mut socket, cp::ENDQRYRM, &[], false).await;

            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(&mut socket, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], true).await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                false,
            )
            .await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
            send(&mut socket, cp::QRYDTA, &[0xff, 0, 29, 0, 0, 0], true).await;
            send(&mut socket, cp::ENDQRYRM, &[], false).await;

            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(
                &mut socket,
                cp::QRYDSC,
                &[6, 0x76, 0xd0, 0x02, 0, 4, 6, 0x76, 0xd0, 0xc9, 0x80, 9],
                true,
            )
            .await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                false,
            )
            .await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
            let mut same_block_rows = vec![0xff, 0, 42, 0, 0, 0, 0xff];
            same_block_rows.extend_from_slice(&locator_row);
            send(&mut socket, cp::QRYDTA, &same_block_rows, false).await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::GETNXTCHK);
            server_locator_observed.store(true, Ordering::SeqCst);
            send(&mut socket, cp::EXTDTA, &[0, 0xaa], true).await;
            send(
                &mut socket,
                cp::ENDDTARM,
                &[
                    ddm::pack_u16(cp::SVRCOD, 0),
                    ddm::pack_bytes(cp::ENDREF, &[0xf1]),
                    ddm::pack_bytes(cp::LSTCHK, &[0xf1]),
                ]
                .concat(),
                false,
            )
            .await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
            send(&mut socket, cp::ENDQRYRM, &[], false).await;

            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(&mut socket, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], true).await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                false,
            )
            .await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
            send(&mut socket, cp::QRYDTA, &[0xff, 0, 30, 0, 0, 0], true).await;
            send(&mut socket, cp::ENDQRYRM, &[], false).await;

            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(&mut socket, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], true).await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                false,
            )
            .await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
            send(&mut socket, cp::QRYDTA, &[0xff, 0, 7, 0, 0, 0], false).await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 3_000;
        let mut session = Db2Session::connect(config).await.unwrap();

        let mut reader = session
            .query_stream("SELECT ID, B FROM EARLY_EXIT_LOB_FIXTURE")
            .await
            .unwrap();
        assert_eq!(
            reader.next().await.unwrap(),
            Some(vec![Db2Value::Integer(42), Db2Value::Null])
        );
        reader.close().await.unwrap();
        assert!(!session.is_closed());
        assert_eq!(
            session.query("VALUES 29").await.unwrap().rows,
            vec![vec![Db2Value::Integer(29)]]
        );

        let mut same_block = session
            .query_stream("SELECT ID, B FROM EARLY_EXIT_LOB_FIXTURE")
            .await
            .unwrap();
        assert_eq!(
            same_block.next().await.unwrap(),
            Some(vec![Db2Value::Integer(42), Db2Value::Null])
        );
        assert!(!same_block_locator_observed.load(Ordering::SeqCst));
        same_block.close().await.unwrap();
        assert!(same_block_locator_observed.load(Ordering::SeqCst));
        assert!(!session.is_closed());
        assert_eq!(
            session.query("VALUES 30").await.unwrap().rows,
            vec![vec![Db2Value::Integer(30)]]
        );

        let mut unfinished = session.query_stream("VALUES 7").await.unwrap();
        assert_eq!(
            unfinished.next().await.unwrap(),
            Some(vec![Db2Value::Integer(7)])
        );
        drop(unfinished);
        assert!(
            session.is_closed(),
            "Drop cannot asynchronously drain an open cursor"
        );
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn transport_failure_during_text_lob_stream_invalidates_session() {
        const DATA_LENGTH: u64 = 16_000_000;
        for (sql_type, type_name) in [(409u16, "CLOB"), (413, "DBCLOB"), (989, "XML")] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                assert_eq!(
                    read_chain(&mut socket)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSAT, cp::ACCSEC]
                );
                send(
                    &mut socket,
                    cp::ACCSECRD,
                    &ddm::pack_u16(cp::SECMEC, 3),
                    false,
                )
                .await;
                assert_eq!(
                    read_chain(&mut socket)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::SECCHK, cp::ACCRDB]
                );
                send(
                    &mut socket,
                    cp::SECCHKRM,
                    &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                    true,
                )
                .await;
                send(&mut socket, cp::ACCRDBRM, &[], false).await;
                assert_eq!(
                    read_chain(&mut socket)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
                );
                send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

                assert_eq!(
                    read_chain(&mut socket)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::PRPSQLSTT, cp::SQLSTT, cp::DSCSQLSTT]
                );
                let mut parameter_dard = vec![0xff, 1, 1, 0, 0, 0, 0, 0];
                parameter_dard.extend_from_slice(&DATA_LENGTH.to_le_bytes());
                parameter_dard.extend_from_slice(&sql_type.to_le_bytes());
                parameter_dard.extend_from_slice(&[0, 0]);
                send(&mut socket, cp::SQLDARD, &parameter_dard, false).await;

                assert_eq!(
                    dss::read_dss(&mut socket).await.unwrap().codepoint,
                    cp::OPNQRY
                );
                assert_eq!(
                    dss::read_dss(&mut socket).await.unwrap().codepoint,
                    cp::SQLDTA
                );

                // Consume one partial EXTDTA DSS segment, then reset the socket so the next
                // bounded segment write fails while the driver is still sending the value.
                let mut header = [0; 6];
                tokio::io::AsyncReadExt::read_exact(&mut socket, &mut header)
                    .await
                    .unwrap();
                let segment_length = (u16::from_be_bytes([header[0], header[1]]) & 0x7fff) as usize;
                let mut body = vec![0; segment_length - header.len()];
                tokio::io::AsyncReadExt::read_exact(&mut socket, &mut body)
                    .await
                    .unwrap();
                assert_eq!(u16::from_be_bytes([body[2], body[3]]), cp::EXTDTA.0);
                #[allow(deprecated)]
                socket.set_linger(Some(Duration::ZERO)).unwrap();
            });

            let mut config = Config::default();
            config.host = "127.0.0.1".into();
            config.port = port;
            config.database = "TESTDB".into();
            config.user = "db2inst1".into();
            config.password = "password".into();
            config.security_mechanism = 3;
            config.timeout_ms = 5_000;
            let mut session = Db2Session::connect(config).await.unwrap();
            let bytes_sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut source = InfinitePatternReader(bytes_sent.clone());
            let mut parameters = [match sql_type {
                409 => Db2InputParameter::ClobStream {
                    source: &mut source,
                    length: DATA_LENGTH,
                },
                413 => Db2InputParameter::DbClobStream {
                    source: &mut source,
                    length: DATA_LENGTH,
                },
                _ => Db2InputParameter::XmlStream {
                    source: &mut source,
                    length: DATA_LENGTH,
                },
            }];
            let sql = format!("VALUES CAST(? AS {type_name}({DATA_LENGTH}))");
            let result = session
                .query_stream_with_blob_streams(&sql, &mut parameters)
                .await;
            assert!(result.is_err(), "{type_name} stream unexpectedly succeeded");
            drop(result);
            assert!(
                session.is_closed(),
                "{type_name} stream left a broken session open"
            );
            assert!(
                bytes_sent.load(Ordering::Acquire) < DATA_LENGTH as usize,
                "{type_name} source was fully consumed despite a mid-send TCP reset"
            );
            session.close().await.unwrap();
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn transaction_commit_and_rollback_errors_invalidate_the_session() {
        for commit in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let expected = if commit { cp::RDBCMM } else { cp::RDBRLLBCK };
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let initial = read_chain(&mut socket).await;
                assert_eq!(
                    initial.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                    vec![cp::EXCSAT, cp::ACCSEC]
                );
                send(
                    &mut socket,
                    cp::ACCSECRD,
                    &ddm::pack_u16(cp::SECMEC, 3),
                    false,
                )
                .await;
                let auth = read_chain(&mut socket).await;
                assert_eq!(
                    auth.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                    vec![cp::SECCHK, cp::ACCRDB]
                );
                send(
                    &mut socket,
                    cp::SECCHKRM,
                    &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                    true,
                )
                .await;
                send(&mut socket, cp::ACCRDBRM, &[], false).await;
                let setup = read_chain(&mut socket).await;
                assert_eq!(
                    setup.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                    vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
                );
                send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
                let control = read_chain(&mut socket).await;
                assert_eq!(control.len(), 1);
                assert_eq!(control[0].codepoint, expected);
                send(
                    &mut socket,
                    cp::SQLCARD,
                    &test_sql_card_error(-911, "40001"),
                    false,
                )
                .await;
                let mut byte = [0_u8; 1];
                let read = tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    tokio::io::AsyncReadExt::read(&mut socket, &mut byte),
                )
                .await
                .expect("closing an invalidated session should release the socket")
                .unwrap();
                assert_eq!(read, 0);
            });

            let mut config = Config::default();
            config.host = "127.0.0.1".into();
            config.port = port;
            config.database = "TESTDB".into();
            config.user = "db2inst1".into();
            config.password = "password".into();
            config.security_mechanism = 3;
            let mut session = Db2Session::connect(config).await.unwrap();
            let transaction = session.begin_transaction().unwrap();
            let result = if commit {
                transaction.commit().await
            } else {
                transaction.rollback().await
            };
            assert!(result.unwrap_err().to_string().contains("SQLCODE=-911"));
            assert!(session.is_closed());
            session.close().await.unwrap();
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn transaction_control_transport_failure_invalidates_the_session() {
        for commit in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let expected = if commit { cp::RDBCMM } else { cp::RDBRLLBCK };
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let initial = read_chain(&mut socket).await;
                assert_eq!(
                    initial
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSAT, cp::ACCSEC]
                );
                send(
                    &mut socket,
                    cp::ACCSECRD,
                    &ddm::pack_u16(cp::SECMEC, 3),
                    false,
                )
                .await;
                let auth = read_chain(&mut socket).await;
                assert_eq!(
                    auth.iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::SECCHK, cp::ACCRDB]
                );
                send(
                    &mut socket,
                    cp::SECCHKRM,
                    &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                    true,
                )
                .await;
                send(&mut socket, cp::ACCRDBRM, &[], false).await;
                let setup = read_chain(&mut socket).await;
                assert_eq!(
                    setup
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
                );
                send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
                let control = read_chain(&mut socket).await;
                assert_eq!(control.len(), 1);
                assert_eq!(control[0].codepoint, expected);
                socket.shutdown().await.unwrap();
            });

            let mut config = Config::default();
            config.host = "127.0.0.1".into();
            config.port = port;
            config.database = "TESTDB".into();
            config.user = "db2inst1".into();
            config.password = "password".into();
            config.security_mechanism = 3;
            let mut session = Db2Session::connect(config).await.unwrap();
            let transaction = session.begin_transaction().unwrap();
            let result = if commit {
                transaction.commit().await
            } else {
                transaction.rollback().await
            };
            assert!(result.is_err());
            assert!(session.is_closed());
            session.close().await.unwrap();
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn interrupt_handle_cancels_active_commit_and_rollback_control() {
        let interrupt_token = [0x04, 0x1c, 0xa7, 0x32];
        for commit in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let expected_control = if commit { cp::RDBCMM } else { cp::RDBRLLBCK };
            let (control_started, wait_for_control) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (mut main, _) = listener.accept().await.unwrap();
                assert_eq!(
                    read_chain(&mut main)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSAT, cp::ACCSEC]
                );
                send(
                    &mut main,
                    cp::ACCSECRD,
                    &ddm::pack_u16(cp::SECMEC, 3),
                    false,
                )
                .await;
                assert_eq!(
                    read_chain(&mut main)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::SECCHK, cp::ACCRDB]
                );
                send(
                    &mut main,
                    cp::SECCHKRM,
                    &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                    true,
                )
                .await;
                send(
                    &mut main,
                    cp::ACCRDBRM,
                    &ddm::pack_bytes(cp::RDBINTTKN, &interrupt_token),
                    false,
                )
                .await;
                assert_eq!(
                    read_chain(&mut main)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
                );
                send(&mut main, cp::SQLCARD, &test_sql_card(0), false).await;
                assert_eq!(read_chain(&mut main).await[0].codepoint, expected_control);
                control_started.send(()).unwrap();

                let (mut auxiliary, _) = listener.accept().await.unwrap();
                assert_eq!(
                    read_chain(&mut auxiliary)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSAT]
                );
                send(&mut auxiliary, cp::EXCSATRD, &[], false).await;
                let interrupt = read_chain(&mut auxiliary).await;
                assert_eq!(interrupt.len(), 1);
                assert_eq!(interrupt[0].codepoint, cp::INTRDBRQS);
                assert_eq!(
                    ddm::parse_ddm_parameters(&interrupt[0].payload)
                        .unwrap()
                        .get(&cp::RDBINTTKN)
                        .unwrap(),
                    &interrupt_token
                );
                send(
                    &mut auxiliary,
                    cp::CMDCMPRM,
                    &ddm::pack_u16(cp::SVRCOD, 0),
                    false,
                )
                .await;
                send(
                    &mut main,
                    cp::SQLCARD,
                    &test_sql_card_error(-952, "57014"),
                    false,
                )
                .await;
                let mut byte = [0_u8; 1];
                let read = tokio::time::timeout(
                    Duration::from_secs(3),
                    tokio::io::AsyncReadExt::read(&mut main, &mut byte),
                )
                .await
                .expect("closing the invalidated transaction session should release its socket")
                .unwrap();
                assert_eq!(read, 0);
            });

            let mut config = Config::default();
            config.host = "127.0.0.1".into();
            config.port = port;
            config.database = "TESTDB".into();
            config.user = "db2inst1".into();
            config.password = "password".into();
            config.security_mechanism = 3;
            config.timeout_ms = 3_000;
            let mut session = Db2Session::connect(config).await.unwrap();
            let handle = session.interrupt_handle().unwrap();
            let transaction = session.begin_transaction().unwrap();
            let operation = async {
                if commit {
                    transaction.commit().await
                } else {
                    transaction.rollback().await
                }
            };
            let interrupt = async {
                wait_for_control.await.unwrap();
                handle.interrupt().await.unwrap();
            };
            let (result, ()) = tokio::join!(operation, interrupt);
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("SQLCODE=-952 SQLSTATE=57014")
            );
            assert!(session.is_closed());
            session.close().await.unwrap();
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn interrupt_handle_cancels_pending_progressive_lob_chunk_and_reuses_session() {
        let interrupt_token = [0x04, 0x1c, 0xa7, 0x32];
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (get_next_started, wait_for_get_next) = tokio::sync::oneshot::channel();
        let (allow_completion, wait_for_completion) = tokio::sync::oneshot::channel();
        let (stream_get_next_started, wait_for_stream_get_next) = tokio::sync::oneshot::channel();
        let (allow_stream_completion, wait_for_stream_completion) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut main, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut main)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut main,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut main)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut main,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(
                &mut main,
                cp::ACCRDBRM,
                &ddm::pack_bytes(cp::RDBINTTKN, &interrupt_token),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut main)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut main, cp::SQLCARD, &test_sql_card(0), false).await;

            let query = read_chain(&mut main).await;
            assert_eq!(
                query
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            assert_eq!(
                ddm::parse_ddm_parameters(&query[2].payload)
                    .unwrap()
                    .get(&cp::DYNDTAFMT)
                    .unwrap(),
                &[0xf1]
            );
            send(&mut main, cp::QRYDSC, &[6, 0x76, 0xd0, 0xc9, 0x80, 9], true).await;
            send(
                &mut main,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                false,
            )
            .await;
            assert_eq!(read_chain(&mut main).await[0].codepoint, cp::CNTQRY);
            let mut row = vec![0xff, 0, 0, 0x03];
            row.extend_from_slice(&1_u64.to_be_bytes());
            row.extend_from_slice(&0x1122_3344_5566_7788_u64.to_be_bytes());
            send(&mut main, cp::QRYDTA, &row, false).await;
            assert_eq!(read_chain(&mut main).await[0].codepoint, cp::GETNXTCHK);
            get_next_started.send(()).unwrap();

            let (mut auxiliary, _) =
                tokio::time::timeout(Duration::from_secs(3), listener.accept())
                    .await
                    .expect("interrupt connection should arrive while GETNXTCHK is pending")
                    .unwrap();
            assert_eq!(
                read_chain(&mut auxiliary)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT]
            );
            send(&mut auxiliary, cp::EXCSATRD, &[], false).await;
            let interrupt = read_chain(&mut auxiliary).await;
            assert_eq!(interrupt.len(), 1);
            assert_eq!(interrupt[0].codepoint, cp::INTRDBRQS);
            assert_eq!(
                ddm::parse_ddm_parameters(&interrupt[0].payload)
                    .unwrap()
                    .get(&cp::RDBINTTKN)
                    .unwrap(),
                &interrupt_token
            );
            send(
                &mut auxiliary,
                cp::CMDCMPRM,
                &ddm::pack_u16(cp::SVRCOD, 0),
                false,
            )
            .await;
            wait_for_completion
                .await
                .expect("test should release the canceled query response");
            send(
                &mut main,
                cp::SQLCARD,
                &test_sql_card_error(-952, "57014"),
                false,
            )
            .await;

            let reuse_query = read_chain(&mut main).await;
            assert_eq!(
                reuse_query
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(&mut main, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], true).await;
            send(
                &mut main,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                true,
            )
            .await;
            send(&mut main, cp::QRYDTA, &[0xff, 0, 29, 0, 0, 0], true).await;
            send(&mut main, cp::ENDQRYRM, &[], false).await;

            let stream_query = read_chain(&mut main).await;
            assert_eq!(
                stream_query
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(&mut main, cp::QRYDSC, &[6, 0x76, 0xd0, 0xc9, 0x80, 9], true).await;
            send(
                &mut main,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                false,
            )
            .await;
            assert_eq!(read_chain(&mut main).await[0].codepoint, cp::CNTQRY);
            let mut stream_row = vec![0xff, 0, 0, 0x03];
            stream_row.extend_from_slice(&1_u64.to_be_bytes());
            stream_row.extend_from_slice(&0x8877_6655_4433_2211_u64.to_be_bytes());
            send(&mut main, cp::QRYDTA, &stream_row, false).await;
            assert_eq!(read_chain(&mut main).await[0].codepoint, cp::GETNXTCHK);
            stream_get_next_started.send(()).unwrap();

            let (mut stream_auxiliary, _) =
                tokio::time::timeout(Duration::from_secs(3), listener.accept())
                    .await
                    .expect("stream interrupt connection should arrive while GETNXTCHK is pending")
                    .unwrap();
            assert_eq!(
                read_chain(&mut stream_auxiliary)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT]
            );
            send(&mut stream_auxiliary, cp::EXCSATRD, &[], false).await;
            let stream_interrupt = read_chain(&mut stream_auxiliary).await;
            assert_eq!(stream_interrupt.len(), 1);
            assert_eq!(stream_interrupt[0].codepoint, cp::INTRDBRQS);
            assert_eq!(
                ddm::parse_ddm_parameters(&stream_interrupt[0].payload)
                    .unwrap()
                    .get(&cp::RDBINTTKN)
                    .unwrap(),
                &interrupt_token
            );
            send(
                &mut stream_auxiliary,
                cp::CMDCMPRM,
                &ddm::pack_u16(cp::SVRCOD, 0),
                false,
            )
            .await;
            wait_for_stream_completion
                .await
                .expect("test should release the canceled stream response");
            send(
                &mut main,
                cp::SQLCARD,
                &test_sql_card_error(-952, "57014"),
                false,
            )
            .await;

            let stream_reuse_query = read_chain(&mut main).await;
            assert_eq!(
                stream_reuse_query
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(&mut main, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], true).await;
            send(
                &mut main,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &[0; 8]),
                true,
            )
            .await;
            send(&mut main, cp::QRYDTA, &[0xff, 0, 29, 0, 0, 0], true).await;
            send(&mut main, cp::ENDQRYRM, &[], false).await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 5_000;
        let session = Arc::new(tokio::sync::Mutex::new(
            Db2Session::connect(config).await.unwrap(),
        ));
        let interrupt_handle = session.lock().await.interrupt_handle().unwrap();
        let query_session = session.clone();
        let query = tokio::spawn(async move {
            query_session
                .lock()
                .await
                .query("SELECT B FROM INTERRUPTED_PROGRESSIVE_LOB_FIXTURE")
                .await
        });
        wait_for_get_next.await.unwrap();
        interrupt_handle.interrupt().await.unwrap();
        assert!(
            !query.is_finished(),
            "the mock server withholds the command response until after interrupt acknowledgment"
        );
        allow_completion.send(()).unwrap();
        let query_result = query.await.unwrap();
        let error = query_result.unwrap_err();
        assert_eq!(error.sql_code, Some(-952));
        assert_eq!(error.sql_state.as_deref(), Some("57014"));

        let mut db = session.lock().await;
        assert!(
            !db.is_closed(),
            "a server-confirmed SQLCODE -952 must preserve the session"
        );
        assert_eq!(
            db.query("VALUES 29").await.unwrap().rows,
            vec![vec![Db2Value::Integer(29)]]
        );

        drop(db);
        let stream_interrupt_handle = session.lock().await.interrupt_handle().unwrap();
        let stream_session = session.clone();
        let stream_query = tokio::spawn(async move {
            let mut db = stream_session.lock().await;
            let mut reader = db
                .query_stream("SELECT B FROM INTERRUPTED_PROGRESSIVE_LOB_FIXTURE")
                .await
                .unwrap();

            async {
                let row = reader.next().await?.expect("locator row should be yielded");
                let Db2Value::Lob(locator) = &row[0] else {
                    panic!("expected a lazy progressive LOB value: {row:?}");
                };
                let mut lob = reader.open_lob(locator).await?;
                let _ = lob.next_chunk().await?;
                let _ = lob;
                reader.next().await
            }
            .await
        });
        wait_for_stream_get_next.await.unwrap();
        stream_interrupt_handle.interrupt().await.unwrap();
        assert!(
            !stream_query.is_finished(),
            "the mock server withholds the row-stream response until after interrupt acknowledgment"
        );
        allow_stream_completion.send(()).unwrap();
        let stream_result = stream_query.await.unwrap();
        let stream_error = stream_result.unwrap_err();
        assert_eq!(stream_error.sql_code, Some(-952));
        assert_eq!(stream_error.sql_state.as_deref(), Some("57014"));

        let mut db = session.lock().await;
        assert!(
            !db.is_closed(),
            "a server-confirmed SQLCODE -952 during row-stream GETNXTCHK must preserve the session"
        );
        assert_eq!(
            db.query("VALUES 29").await.unwrap().rows,
            vec![vec![Db2Value::Integer(29)]]
        );
        db.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn procedure_cursor_fetches_again_after_an_exact_full_block() {
        use tokio::io::AsyncReadExt;

        const ROW_COUNT: usize = 169;
        const EXACT_QUERY_DATA_LENGTH: usize = 1_014;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut socket,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut socket,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut socket, cp::ACCRDBRM, &[], false).await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::DSCSQLSTT]
            );
            let mut parameter_dard = vec![0xff, 1, 1, 0, 0, 0, 0, 0];
            parameter_dard.extend_from_slice(&4u64.to_le_bytes());
            parameter_dard.extend_from_slice(&497u16.to_le_bytes());
            parameter_dard.extend_from_slice(&[0, 0]);
            send(&mut socket, cp::SQLDARD, &parameter_dard, false).await;
            assert_eq!(
                read_chain(&mut socket)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSQLSTT, cp::SQLDTA]
            );

            let mut section_payload = Vec::new();
            section_payload.extend_from_slice(&fixed_ascii("TESTDB", 18));
            section_payload.extend_from_slice(b"NULLID            ");
            section_payload.extend_from_slice(&fixed_ascii("SYSSH200", 18));
            section_payload.extend_from_slice(b"SYSLVL01");
            section_payload.extend_from_slice(&65_u16.to_be_bytes());
            let section = ddm::pack_ddm_object(cp::PKGNAMCSN, &section_payload);
            let mut empty_section_payload = section_payload.clone();
            empty_section_payload[62..64].copy_from_slice(&66_u16.to_be_bytes());
            let empty_section = ddm::pack_ddm_object(cp::PKGNAMCSN, &empty_section_payload);
            let sections = [section, empty_section].concat();
            let package_list = ddm::pack_ddm_object(cp::PKGSNLST, &sections);
            send(&mut socket, cp::RSLSETRM, &package_list, true).await;
            let mut summary = vec![2, 0];
            summary.extend_from_slice(&[0; 24]);
            send(&mut socket, cp::SQLRSLRD, &summary, true).await;
            let output_descriptor = [6, 0x76, 0xd0, 0x03, 0, 4];
            // SQLDTARD's SQLDA INTEGER descriptor is nullable (type 0x03), so
            // FDODTA includes a separate NULL indicator before the 4-byte value.
            let output_values = [0xff, 0, 0, 42, 0, 0, 0];
            let output_data = [
                ddm::pack_ddm_object(cp::FDODSC, &output_descriptor),
                ddm::pack_ddm_object(cp::FDODTA, &output_values),
            ]
            .concat();
            let (test_output_fields, test_output_bytes) =
                ddm::parse_sql_dta_reply(&output_data).unwrap();
            assert_eq!(test_output_fields.len(), 1);
            assert_eq!(test_output_bytes, output_values);
            let mut test_output = [Db2Value::Null];
            apply_procedure_output_parameters(
                &output_data,
                &[Db2ParameterDirection::Output],
                &mut test_output,
                &[],
            )
            .unwrap();
            assert_eq!(test_output, [Db2Value::Integer(42)]);
            send(&mut socket, cp::SQLDTARD, &output_data, true).await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &123_u64.to_be_bytes()),
                true,
            )
            .await;
            send(
                &mut socket,
                cp::SQLCINRD,
                &procedure_integer_cursor_info(),
                true,
            )
            .await;
            send(&mut socket, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], true).await;
            send(
                &mut socket,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &456_u64.to_be_bytes()),
                true,
            )
            .await;
            send(
                &mut socket,
                cp::SQLCINRD,
                &procedure_integer_cursor_info(),
                true,
            )
            .await;
            send(&mut socket, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], true).await;
            send(&mut socket, cp::ENDQRYRM, &[], false).await;

            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
            let mut row_data = vec![0; EXACT_QUERY_DATA_LENGTH];
            for index in 0..ROW_COUNT {
                let offset = index * 6;
                row_data[offset] = 0xff;
                row_data[offset + 1] = 0;
                row_data[offset + 2..offset + 6]
                    .copy_from_slice(&((index + 1) as i32).to_le_bytes());
            }
            assert_eq!(row_data.len(), EXACT_QUERY_DATA_LENGTH);
            send(&mut socket, cp::QRYDTA, &row_data, false).await;

            assert_eq!(
                read_chain(&mut socket).await[0].codepoint,
                cp::CNTQRY,
                "an exact-full-block QRYDTA must be followed by another CNTQRY"
            );
            send(&mut socket, cp::ENDQRYRM, &[], false).await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CLSQRY);
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CLSQRY);
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::RDBCMM);
            send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;
            let mut byte = [0_u8; 1];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 3_000;
        config.block_size = 1_024;
        let mut session = Db2Session::connect(config).await.unwrap();

        let statement = session.prepare("CALL TESTPROC(?)").await.unwrap();
        let mut reader = session
            .call_procedure_stream(
                &statement,
                &[Db2Parameter {
                    value: Db2Value::Integer(0),
                    direction: Db2ParameterDirection::Output,
                }],
            )
            .await
            .unwrap();
        assert_eq!(reader.result_set_index(), Some(0));
        assert_eq!(reader.columns().len(), 1);
        assert_eq!(
            reader.next().await.unwrap(),
            Some(vec![Db2Value::Integer(1)])
        );
        assert!(reader.output_parameters().is_none());
        // Advancing early must drain the remaining rows and close this cursor before
        // opening the next result set.
        assert!(reader.next_result_set().await.unwrap());
        assert_eq!(reader.result_set_index(), Some(1));
        assert_eq!(reader.columns().len(), 1);
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.output_parameters().is_none());
        assert!(!reader.next_result_set().await.unwrap());
        assert_eq!(
            reader.output_parameters(),
            Some(&[Db2Value::Integer(42)][..])
        );
        let terminal = reader.finish().await.unwrap();
        assert_eq!(terminal.output_parameters, vec![Db2Value::Integer(42)]);
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn procedure_cursor_fetch_and_close_failures_invalidate_the_session() {
        use tokio::io::AsyncReadExt;

        for (server_error, close_failure, stalled) in [
            (true, false, false),
            (false, false, false),
            (true, true, false),
            (false, false, true),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                assert_eq!(
                    read_chain(&mut socket)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSAT, cp::ACCSEC]
                );
                send(
                    &mut socket,
                    cp::ACCSECRD,
                    &ddm::pack_u16(cp::SECMEC, 3),
                    false,
                )
                .await;
                assert_eq!(
                    read_chain(&mut socket)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::SECCHK, cp::ACCRDB]
                );
                send(
                    &mut socket,
                    cp::SECCHKRM,
                    &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                    true,
                )
                .await;
                send(&mut socket, cp::ACCRDBRM, &[], false).await;
                assert_eq!(
                    read_chain(&mut socket)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
                );
                send(&mut socket, cp::SQLCARD, &test_sql_card(0), false).await;

                assert_eq!(
                    read_chain(&mut socket)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::PRPSQLSTT, cp::SQLSTT, cp::DSCSQLSTT]
                );
                send(&mut socket, cp::SQLDARD, &[], false).await;
                assert_eq!(
                    read_chain(&mut socket)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSQLSTT]
                );

                let mut section_payload = Vec::new();
                section_payload.extend_from_slice(&fixed_ascii("TESTDB", 18));
                section_payload.extend_from_slice(b"NULLID            ");
                section_payload.extend_from_slice(&fixed_ascii("SYSSH200", 18));
                section_payload.extend_from_slice(b"SYSLVL01");
                section_payload.extend_from_slice(&65_u16.to_be_bytes());
                let section = ddm::pack_ddm_object(cp::PKGNAMCSN, &section_payload);
                let package_list = ddm::pack_ddm_object(cp::PKGSNLST, &section);
                send(&mut socket, cp::RSLSETRM, &package_list, true).await;
                let mut summary = vec![1, 0];
                summary.extend_from_slice(&[0; 12]);
                send(&mut socket, cp::SQLRSLRD, &summary, true).await;
                send(
                    &mut socket,
                    cp::OPNQRYRM,
                    &ddm::pack_bytes(cp::QRYINSID, &123_u64.to_be_bytes()),
                    true,
                )
                .await;
                send(
                    &mut socket,
                    cp::SQLCINRD,
                    &procedure_integer_cursor_info(),
                    true,
                )
                .await;
                send(&mut socket, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], false).await;

                assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
                if stalled {
                    let fragment = [0xff, 0xff];
                    send(&mut socket, cp::QRYDTA, &fragment, false).await;
                    for _ in 1..3 {
                        assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CNTQRY);
                        send(&mut socket, cp::QRYDTA, &fragment, false).await;
                    }
                } else if close_failure {
                    send(&mut socket, cp::QRYDTA, &[0xff, 0, 42, 0, 0, 0], true).await;
                    send(&mut socket, cp::ENDQRYRM, &[], false).await;
                    assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::CLSQRY);
                }
                if !stalled {
                    let card = if server_error {
                        test_sql_card_error(-911, "40001")
                    } else {
                        test_sql_card(0)
                    };
                    send(&mut socket, cp::SQLCARD, &card, false).await;
                }
                let mut byte = [0_u8; 1];
                assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
            });

            let mut config = Config::default();
            config.host = "127.0.0.1".into();
            config.port = port;
            config.database = "TESTDB".into();
            config.user = "db2inst1".into();
            config.password = "password".into();
            config.security_mechanism = 3;
            config.timeout_ms = 3_000;
            let mut session = Db2Session::connect(config).await.unwrap();

            let statement = session.prepare("CALL TESTPROC()").await.unwrap();
            let mut reader = session
                .call_procedure_stream(&statement, &[])
                .await
                .unwrap();
            if close_failure {
                assert_eq!(
                    reader.next().await.unwrap(),
                    Some(vec![Db2Value::Integer(42)])
                );
            }
            let error = reader.next().await.unwrap_err();
            if stalled {
                assert!(
                    error
                        .to_string()
                        .contains("repeated a non-progressing procedure QRYDTA fragment"),
                    "{error}"
                );
            } else if server_error {
                assert!(error.to_string().contains("SQLCODE=-911"), "{error}");
                assert_eq!(error.kind, Db2ErrorKind::SqlServer);
                assert_eq!(error.sql_code, Some(-911));
                assert_eq!(error.sql_state.as_deref(), Some("40001"));
            } else {
                assert!(
                    error
                        .to_string()
                        .contains("neither cursor data nor an end-of-query reply"),
                    "{error}"
                );
            }
            drop(reader);
            assert!(
                session.is_closed(),
                "a failed procedure-cursor fetch must not leave a reusable session"
            );
            let _ = session.close().await;
            server.await.unwrap();
        }
    }

    fn fixed_ascii(value: &str, length: usize) -> Vec<u8> {
        let mut bytes = value.as_bytes().to_vec();
        bytes.resize(length, b' ');
        bytes
    }

    fn procedure_integer_cursor_info() -> Vec<u8> {
        fn fdoca_string_pair(output: &mut Vec<u8>, value: &str) {
            output.extend_from_slice(&[0, 0]);
            output.extend_from_slice(&(value.len() as u16).to_be_bytes());
            output.extend_from_slice(value.as_bytes());
        }

        let mut output = vec![
            0xff, 1, 0, 10, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0xf1, 1, 0, 0, 0, 0,
        ];
        fdoca_string_pair(&mut output, "VALUE");
        fdoca_string_pair(&mut output, "VALUE");
        fdoca_string_pair(&mut output, "");
        output.extend_from_slice(&[0xff, 0xff]);
        output
    }

    async fn read_chain(socket: &mut tokio::net::TcpStream) -> Vec<DssPacket> {
        let mut result = Vec::new();
        loop {
            let packet = dss::read_dss(socket).await.unwrap();
            let chained = packet.header.chained;
            result.push(packet);
            if !chained {
                return result;
            }
        }
    }

    #[tokio::test]
    async fn command_deadline_sends_sqlintr_awaits_minus_952_and_reuses_session() {
        let token = [0x04, 0x1c, 0xa7, 0x32];
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut main, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut main)
                    .await
                    .iter()
                    .map(|p| p.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut main,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut main)
                    .await
                    .iter()
                    .map(|p| p.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut main,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(
                &mut main,
                cp::ACCRDBRM,
                &ddm::pack_bytes(cp::RDBINTTKN, &token),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut main)
                    .await
                    .iter()
                    .map(|p| p.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut main, cp::SQLCARD, &test_sql_card(0), false).await;

            let command = read_chain(&mut main).await;
            assert_eq!(
                command.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSQLIMM, cp::SQLSTT, cp::RDBCMM]
            );
            let (mut auxiliary, _) =
                tokio::time::timeout(Duration::from_secs(2), listener.accept())
                    .await
                    .expect("SQLINTR should use an auxiliary connection")
                    .unwrap();
            assert_eq!(read_chain(&mut auxiliary).await[0].codepoint, cp::EXCSAT);
            send(&mut auxiliary, cp::EXCSATRD, &[], false).await;
            let interrupt = read_chain(&mut auxiliary).await;
            assert_eq!(interrupt.len(), 1);
            assert_eq!(interrupt[0].codepoint, cp::INTRDBRQS);
            assert_eq!(
                ddm::parse_ddm_parameters(&interrupt[0].payload)
                    .unwrap()
                    .get(&cp::RDBINTTKN)
                    .unwrap(),
                &token
            );
            send(
                &mut auxiliary,
                cp::CMDCMPRM,
                &ddm::pack_u16(cp::SVRCOD, 0),
                false,
            )
            .await;
            send(
                &mut main,
                cp::SQLCARD,
                &test_sql_card_error(-952, "57014"),
                false,
            )
            .await;

            let reuse = read_chain(&mut main).await;
            assert_eq!(
                reuse.iter().map(|p| p.codepoint).collect::<Vec<_>>(),
                vec![cp::EXCSQLIMM, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut main, cp::SQLCARD, &test_sql_card(1), false).await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 2_000;
        let mut session = Db2Session::connect(config).await.unwrap();
        let error = session
            .execute_with_options("VALUES 1", Db2CommandOptions::new(30, 1_000))
            .await
            .unwrap_err();
        assert_eq!(error.sql_code, Some(-952), "{error}");
        assert!(
            !session.is_closed(),
            "a complete -952 reply synchronizes the session"
        );
        assert_eq!(session.execute("VALUES 2").await.unwrap(), 1);
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn dropping_unfinished_procedure_reader_invalidates_its_session() {
        let (stream, _server) = tokio::io::duplex(1);
        let coordinator = Arc::new(InterruptCoordinator::default());
        let operation = coordinator.begin_operation().await;
        let mut session = Db2Session {
            stream: Box::new(stream),
            pool_abort: None,
            config: Config::default(),
            database: "TESTDB".into(),
            security_mechanism: 3,
            query_block_size: 1024,
            correlation_id: 1,
            auto_commit: true,
            closed: false,
            prepared_sql: Some("CALL TESTPROC()".into()),
            current_user: String::new(),
            base_user: String::new(),
            client_info: Db2ClientInfo::default(),
            base_client_info: Db2ClientInfo::default(),
            base_schema: None,
            base_isolation: None,
            current_schema: None,
            current_isolation: None,
            pool_baseline_captured: false,
            pool_dirty: SessionDirtyState::default(),
            warning_handler: None,
            rdb_interrupt_token: None,
            interrupt_coordinator: coordinator,
        };
        let reader = Db2ProcedureReader {
            session: &mut session,
            cursors: Vec::new(),
            current_cursor: 0,
            rows_affected: 0,
            output_parameters: Vec::new(),
            options: Db2CommandOptions::unlimited(),
            interrupt: None,
            _operation: operation,
            completed: false,
            cancelled: false,
            terminal_result: None,
        };
        drop(reader);
        assert!(session.is_closed());
    }

    #[tokio::test]
    async fn procedure_stream_deadline_cancels_cursor_and_preserves_session_on_minus_952() {
        let token = [0x03, 0x1a, 0x94, 0x21];
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut main, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut main)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            send(
                &mut main,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut main)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut main,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(
                &mut main,
                cp::ACCRDBRM,
                &ddm::pack_bytes(cp::RDBINTTKN, &token),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut main)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut main, cp::SQLCARD, &test_sql_card(0), false).await;

            assert_eq!(
                read_chain(&mut main)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::DSCSQLSTT]
            );
            send(&mut main, cp::SQLDARD, &[], false).await;

            assert_eq!(
                read_chain(&mut main)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSQLSTT]
            );
            let mut section_payload = Vec::new();
            section_payload.extend_from_slice(&fixed_ascii("TESTDB", 18));
            section_payload.extend_from_slice(b"NULLID            ");
            section_payload.extend_from_slice(&fixed_ascii("SYSSH200", 18));
            section_payload.extend_from_slice(b"SYSLVL01");
            section_payload.extend_from_slice(&65_u16.to_be_bytes());
            let section = ddm::pack_ddm_object(cp::PKGNAMCSN, &section_payload);
            send(
                &mut main,
                cp::RSLSETRM,
                &ddm::pack_ddm_object(cp::PKGSNLST, &section),
                true,
            )
            .await;
            let mut summary = vec![1, 0];
            summary.extend_from_slice(&[0; 24]);
            send(&mut main, cp::SQLRSLRD, &summary, true).await;
            send(
                &mut main,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &123_u64.to_be_bytes()),
                true,
            )
            .await;
            send(
                &mut main,
                cp::SQLCINRD,
                &procedure_integer_cursor_info(),
                true,
            )
            .await;
            send(&mut main, cp::QRYDSC, &[6, 0x76, 0xd0, 0x02, 0, 4], false).await;

            assert_eq!(read_chain(&mut main).await[0].codepoint, cp::CNTQRY);
            let (mut auxiliary, _) =
                tokio::time::timeout(Duration::from_secs(2), listener.accept())
                    .await
                    .expect("procedure deadline should open a SQLINTR connection")
                    .unwrap();
            assert_eq!(read_chain(&mut auxiliary).await[0].codepoint, cp::EXCSAT);
            send(&mut auxiliary, cp::EXCSATRD, &[], false).await;
            let interrupt = read_chain(&mut auxiliary).await;
            assert_eq!(interrupt.len(), 1);
            assert_eq!(interrupt[0].codepoint, cp::INTRDBRQS);
            assert_eq!(
                ddm::parse_ddm_parameters(&interrupt[0].payload)
                    .unwrap()
                    .get(&cp::RDBINTTKN)
                    .unwrap(),
                &token
            );
            send(
                &mut auxiliary,
                cp::CMDCMPRM,
                &ddm::pack_u16(cp::SVRCOD, 0),
                false,
            )
            .await;
            send(
                &mut main,
                cp::SQLCARD,
                &test_sql_card_error(-952, "57014"),
                false,
            )
            .await;

            let reuse = read_chain(&mut main).await;
            assert_eq!(
                reuse
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSQLIMM, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut main, cp::SQLCARD, &test_sql_card(1), false).await;
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 2_000;
        let mut session = Db2Session::connect(config).await.unwrap();
        let statement = session.prepare("CALL TESTPROC()").await.unwrap();
        let mut reader = session
            .call_procedure_stream_with_options(
                &statement,
                &[],
                Db2CommandOptions::new(500, 1_000).with_interrupt_ack_timeout_ms(1_000),
            )
            .await
            .unwrap();
        let error = reader.next().await.unwrap_err();
        assert_eq!(error.sql_code, Some(-952), "{error}");
        drop(reader);
        assert!(!session.is_closed(), "a confirmed SQLCODE -952 is reusable");
        assert_eq!(session.execute("VALUES 2").await.unwrap(), 1);
        session.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn zero_command_timeout_waits_for_completion_without_sqlintr() {
        let started = tokio::time::Instant::now();
        let result = run_with_command_deadline(Db2CommandOptions::unlimited(), None, async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<_, Db2Error>(42)
        })
        .await
        .unwrap();
        assert_eq!(result, 42);
        assert!(started.elapsed() >= Duration::from_millis(20));
    }

    #[tokio::test]
    async fn deadline_controller_discards_on_ack_rejection_or_ack_timeout() {
        for reject_ack in [true, false] {
            let coordinator = Arc::new(InterruptCoordinator::default());
            let command = std::future::pending::<Result<(), Db2Error>>();
            let interrupt = async move {
                if reject_ack {
                    Err(Db2Error::other("mock SQLINTR rejected".into()))
                } else {
                    std::future::pending::<Result<(), Db2Error>>().await
                }
            };
            let result = run_with_deadline_controller(
                Db2CommandOptions::new(5, 10).with_interrupt_ack_timeout_ms(10),
                coordinator.clone(),
                Duration::from_millis(10),
                command,
                || interrupt,
            )
            .await;
            assert!(result.is_err());
            assert!(coordinator.poisoned.load(Ordering::Acquire));
        }
    }

    #[tokio::test]
    async fn deadline_controller_discards_when_original_reply_does_not_settle() {
        let coordinator = Arc::new(InterruptCoordinator::default());
        let result = run_with_deadline_controller(
            Db2CommandOptions::new(5, 10).with_interrupt_ack_timeout_ms(10),
            coordinator.clone(),
            Duration::from_millis(10),
            std::future::pending::<Result<(), Db2Error>>(),
            || async { Ok(()) },
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("original command did not complete")
        );
        assert!(coordinator.poisoned.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn deadline_controller_preserves_natural_completion_race_result() {
        let coordinator = Arc::new(InterruptCoordinator::default());
        let result = run_with_deadline_controller(
            Db2CommandOptions::new(5, 50).with_interrupt_ack_timeout_ms(50),
            coordinator.clone(),
            Duration::from_millis(50),
            async {
                tokio::time::sleep(Duration::from_millis(15)).await;
                Ok::<_, Db2Error>(7)
            },
            || async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            },
        )
        .await;
        assert_eq!(result.unwrap(), 7);
        assert!(!coordinator.poisoned.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn deadline_controller_keeps_batch_progress_when_active_set_times_out() {
        let coordinator = Arc::new(InterruptCoordinator::default());
        let mut completed = 0;
        let mut rows_affected = 0;
        let mut active_set = None;
        let result = run_with_deadline_controller(
            Db2CommandOptions::new(5, 10).with_interrupt_ack_timeout_ms(10),
            coordinator,
            Duration::from_millis(10),
            async {
                completed = 1;
                rows_affected = 3;
                active_set = Some(1);
                std::future::pending::<()>().await;
                Ok::<_, Db2Error>(())
            },
            || async { Err(Db2Error::other("mock SQLINTR rejected".into())) },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(completed, 1);
        assert_eq!(rows_affected, 3);
        assert_eq!(active_set, Some(1));
    }

    struct InfinitePatternReader(Arc<std::sync::atomic::AtomicUsize>);

    impl AsyncRead for InfinitePatternReader {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
            buffer: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let count = buffer.remaining().min(16_384);
            let mut chunk = vec![0; count];
            for (index, byte) in chunk.iter_mut().enumerate() {
                *byte = ((index * 37 + 11) % 251) as u8;
            }
            buffer.put_slice(&chunk);
            self.0.fetch_add(count, Ordering::AcqRel);
            std::task::Poll::Ready(Ok(()))
        }
    }

    async fn send(
        socket: &mut tokio::net::TcpStream,
        codepoint: CodePoint,
        payload: &[u8],
        chained: bool,
    ) {
        let object = ddm::pack_ddm_object(codepoint, payload);
        socket
            .write_all(&encode_reply_dss(&object, chained, 1))
            .await
            .unwrap();
    }

    fn test_sql_card(affected: i32) -> [u8; 54] {
        test_sql_card_with(0, "00000", affected)
    }

    fn test_sql_card_error(code: i32, state: &str) -> [u8; 54] {
        test_sql_card_with(code, state, 0)
    }

    fn test_sql_card_with(code: i32, state: &str, affected: i32) -> [u8; 54] {
        let mut result = [0; 54];
        result[1..5].copy_from_slice(&code.to_le_bytes());
        result[5..10].copy_from_slice(state.as_bytes());
        result[27..31].copy_from_slice(&affected.to_le_bytes());
        result
    }
}
