use crate::admin::{ADMIN_COMMAND_CALL_SQL, normalize_admin_command};
use crate::config::{Config, parse_dsn};
use crate::error::{Db2BatchError, Db2Error};
use crate::network::session::{Db2InputParameter, Db2QueryResult, Db2Session, Db2Value};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};

struct PoolInner {
    config: Config,
    reset_policy: PoolResetPolicy,
    idle: Mutex<Vec<IdleSession>>,
    options: Db2PoolOptions,
    active: Mutex<HashMap<usize, crate::network::session::PoolAbort>>,
    waiters: AtomicUsize,
    reaper_started: AtomicBool,
    close_gate: AsyncMutex<()>,
    semaphore: Arc<Semaphore>,
    closed: AtomicBool,
    session_count: AtomicUsize,
}

/// Bounds physical sessions, admission and retained connection lifetime.
#[derive(Clone, Debug)]
pub struct Db2PoolOptions {
    pub max_connections: usize,
    pub max_waiters: usize,
    pub acquire_timeout: Duration,
    pub connect_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_lifetime: Duration,
    pub reap_interval: Duration,
}
impl Default for Db2PoolOptions {
    fn default() -> Self {
        Self {
            max_connections: 10,
            max_waiters: 100,
            acquire_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_secs(1800),
            reap_interval: Duration::from_secs(30),
        }
    }
}
struct IdleSession {
    session: Db2Session,
    born: Instant,
    returned: Instant,
}
struct WaitingGuard(Arc<PoolInner>);
impl Drop for WaitingGuard {
    fn drop(&mut self) {
        self.0.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Controls how pooled sessions handle state changes made through arbitrary SQL.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PoolResetPolicy {
    /// Arbitrary SQL requires server-state validation before the session is reused.
    #[default]
    Strict,
    /// Trust the typed state APIs and skip reset for arbitrary SQL. Applications must
    /// not change session registers through raw SQL when choosing this policy.
    Tracked,
}

// Owns accounting while an idle physical session is removed for async reset.
// A cancelled acquire drops its local session and must also remove that count.
struct SessionCountGuard {
    inner: Arc<PoolInner>,
    armed: bool,
}
impl SessionCountGuard {
    fn new(inner: Arc<PoolInner>) -> Self {
        Self { inner, armed: true }
    }
    fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for SessionCountGuard {
    fn drop(&mut self) {
        if self.armed {
            self.inner.session_count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// A bounded async pool. A `Db2PooledSession` holds one slot until it is dropped.
#[derive(Clone)]
pub struct Db2Pool {
    inner: Arc<PoolInner>,
}

impl Db2Pool {
    pub fn connect_dsn(dsn: &str, max_connections: usize) -> Result<Self, Db2Error> {
        Self::new(parse_dsn(dsn)?, max_connections)
    }

    pub fn new(config: Config, max_connections: usize) -> Result<Self, Db2Error> {
        Self::new_with_reset_policy(config, max_connections, PoolResetPolicy::Strict)
    }

    pub fn new_with_reset_policy(
        config: Config,
        max_connections: usize,
        reset_policy: PoolResetPolicy,
    ) -> Result<Self, Db2Error> {
        Self::new_with_options(
            config,
            Db2PoolOptions {
                max_connections,
                ..Default::default()
            },
            reset_policy,
        )
    }

    pub fn new_with_options(
        config: Config,
        options: Db2PoolOptions,
        reset_policy: PoolResetPolicy,
    ) -> Result<Self, Db2Error> {
        if options.max_connections == 0
            || options.max_connections > Semaphore::MAX_PERMITS
            || options.max_waiters == 0
            || options.acquire_timeout.is_zero()
            || options.connect_timeout.is_zero()
            || options.idle_timeout.is_zero()
            || options.max_lifetime.is_zero()
            || options.reap_interval.is_zero()
        {
            return Err(Db2Error::new(
                crate::Db2ErrorKind::InvalidConfiguration,
                "Db2 pool capacities must be positive integers and intervals must be positive and supported by the runtime",
            ));
        }
        Ok(Self {
            inner: Arc::new(PoolInner {
                config,
                reset_policy,
                idle: Mutex::new(Vec::new()),
                close_gate: AsyncMutex::new(()),
                semaphore: Arc::new(Semaphore::new(options.max_connections)),
                options,
                active: Mutex::new(HashMap::new()),
                waiters: AtomicUsize::new(0),
                reaper_started: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                session_count: AtomicUsize::new(0),
            }),
        })
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }
    pub fn session_count(&self) -> usize {
        self.inner.session_count.load(Ordering::Acquire)
    }
    pub fn idle_session_count(&self) -> usize {
        self.inner.idle.lock().map(|idle| idle.len()).unwrap_or(0)
    }

    pub fn waiting_count(&self) -> usize {
        self.inner.waiters.load(Ordering::Acquire)
    }

    fn start_reaper(&self) {
        if self.inner.reaper_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let weak = Arc::downgrade(&self.inner);
        let period = self.inner.options.reap_interval;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(period).await;
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                if inner.closed.load(Ordering::Acquire) {
                    return;
                }
                let expired = {
                    let Ok(mut idle) = inner.idle.lock() else {
                        return;
                    };
                    let mut expired = Vec::new();
                    let mut i = 0;
                    while i < idle.len() {
                        if idle[i].returned.elapsed() >= inner.options.idle_timeout
                            || idle[i].born.elapsed() >= inner.options.max_lifetime
                        {
                            expired.push(idle.swap_remove(i));
                        } else {
                            i += 1;
                        }
                    }
                    inner
                        .session_count
                        .fetch_sub(expired.len(), Ordering::AcqRel);
                    expired
                };
                for mut entry in expired {
                    let _ = entry.session.close().await;
                }
            }
        });
    }

    pub async fn acquire(&self) -> Result<Db2PooledSession, Db2Error> {
        let started = Instant::now();
        let result = self.acquire_inner().await;
        crate::diagnostics::record("acquire", started.elapsed(), result.is_ok(), 0);
        result
    }

    async fn acquire_inner(&self) -> Result<Db2PooledSession, Db2Error> {
        if self.is_closed() {
            return Err(Db2Error::connection_closed("Db2 pool is closed"));
        }
        self.start_reaper();
        let permit = match self.inner.semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(Db2Error::connection_closed("Db2 pool is closed"));
            }
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                if self
                    .inner
                    .waiters
                    .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                        (count < self.inner.options.max_waiters).then_some(count + 1)
                    })
                    .is_err()
                {
                    return Err(Db2Error::new(
                        crate::Db2ErrorKind::PoolExhausted,
                        "Db2 pool waiting queue is full",
                    ));
                }
                let _waiting = WaitingGuard(self.inner.clone());
                tokio::time::timeout(
                    self.inner.options.acquire_timeout,
                    self.inner.semaphore.clone().acquire_owned(),
                )
                .await
                .map_err(|_| {
                    Db2Error::new(
                        crate::Db2ErrorKind::PoolTimeout,
                        "Db2 pool acquisition timed out",
                    )
                })?
                .map_err(|_| Db2Error::connection_closed("Db2 pool is closed"))?
            }
        };
        if self.is_closed() {
            return Err(Db2Error::connection_closed("Db2 pool is closed"));
        }
        let entry = self
            .inner
            .idle
            .lock()
            .map_err(|_| Db2Error::other("Db2 pool state is poisoned".into()))?
            .pop();
        let mut born = Instant::now();
        let mut session = match entry {
            Some(entry)
                if !entry.session.is_closed()
                    && entry.born.elapsed() < self.inner.options.max_lifetime
                    && entry.returned.elapsed() < self.inner.options.idle_timeout =>
            {
                born = entry.born;
                let idle_elapsed = entry.returned.elapsed();
                let mut session = entry.session;
                let mut count = SessionCountGuard::new(self.inner.clone());
                if self.inner.reset_policy == PoolResetPolicy::Tracked {
                    session.clear_unknown_pool_dirty();
                }
                let reset = if idle_elapsed > Duration::from_secs(60) {
                    match session.query("VALUES 1").await {
                        Ok(_) => session.reset_for_pool().await,
                        Err(error) => Err(error),
                    }
                } else {
                    session.reset_for_pool().await
                };
                if reset.is_ok() {
                    count.disarm();
                    session
                } else {
                    let _ = session.close().await;
                    drop(count);
                    if self.is_closed() {
                        return Err(Db2Error::connection_closed("Db2 pool is closed"));
                    }
                    born = Instant::now();
                    self.connect_reserved().await?
                }
            }
            Some(entry) => {
                let mut session = entry.session;
                let count = SessionCountGuard::new(self.inner.clone());
                let _ = session.close().await;
                drop(count);
                if self.is_closed() {
                    return Err(Db2Error::connection_closed("Db2 pool is closed"));
                }
                self.connect_reserved().await?
            }
            None => self.connect_reserved().await?,
        };
        if self.is_closed() {
            let count = SessionCountGuard::new(self.inner.clone());
            let mut session = session;
            let _ = session.close().await;
            drop(count);
            return Err(Db2Error::connection_closed("Db2 pool is closed"));
        }
        let abort = session.install_pool_abort();
        self.inner
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(abort.id(), abort.clone());
        Ok(Db2PooledSession {
            session: Some(session),
            inner: self.inner.clone(),
            _permit: permit,
            born,
            abort,
        })
    }

    pub async fn query(&self, sql: &str) -> Result<Db2QueryResult, Db2Error> {
        let mut session = self.acquire().await?;
        let started = Instant::now();
        let result = session.query(sql).await;
        crate::diagnostics::record("execute", started.elapsed(), result.is_ok(), 0);
        match result {
            Ok(result) => Ok(result),
            Err(error) => Err(Self::attach_cleanup(&mut session, error).await),
        }
    }

    pub async fn query_with_params(
        &self,
        sql: &str,
        parameters: &[Db2Value],
    ) -> Result<Db2QueryResult, Db2Error> {
        let mut session = self.acquire().await?;
        match session.query_with_params(sql, parameters).await {
            Ok(result) => Ok(result),
            Err(error) => Err(Self::attach_cleanup(&mut session, error).await),
        }
    }

    pub async fn query_with_blob_streams(
        &self,
        sql: &str,
        parameters: &mut [Db2InputParameter<'_>],
    ) -> Result<Db2QueryResult, Db2Error> {
        let mut session = self.acquire().await?;
        match session.query_with_blob_streams(sql, parameters).await {
            Ok(result) => Ok(result),
            Err(error) => Err(Self::attach_cleanup(&mut session, error).await),
        }
    }

    pub async fn execute(&self, sql: &str) -> Result<i64, Db2Error> {
        let mut session = self.acquire().await?;
        let started = Instant::now();
        let result = session.execute(sql).await;
        crate::diagnostics::record("execute", started.elapsed(), result.is_ok(), 0);
        match result {
            Ok(result) => Ok(result),
            Err(error) => Err(Self::attach_cleanup(&mut session, error).await),
        }
    }

    pub async fn execute_with_params(
        &self,
        sql: &str,
        parameters: &[Db2Value],
    ) -> Result<i64, Db2Error> {
        let mut session = self.acquire().await?;
        match session.execute_with_params(sql, parameters).await {
            Ok(result) => Ok(result),
            Err(error) => Err(Self::attach_cleanup(&mut session, error).await),
        }
    }

    /// Executes one Db2 administrative command through a leased pool session.
    pub async fn execute_admin_command(&self, command: &str) -> Result<i64, Db2Error> {
        let normalized =
            normalize_admin_command(command).map_err(|error| Db2Error::other(error.into()))?;
        self.execute_with_params(
            ADMIN_COMMAND_CALL_SQL,
            &[Db2Value::Text(normalized.to_owned())],
        )
        .await
    }

    pub async fn execute_with_blob_streams(
        &self,
        sql: &str,
        parameters: &mut [Db2InputParameter<'_>],
    ) -> Result<i64, Db2Error> {
        let mut session = self.acquire().await?;
        match session.execute_with_blob_streams(sql, parameters).await {
            Ok(result) => Ok(result),
            Err(error) => Err(Self::attach_cleanup(&mut session, error).await),
        }
    }

    pub async fn execute_batch_with_params(
        &self,
        sql: &str,
        parameter_sets: &[Vec<Db2Value>],
    ) -> Result<i64, Db2BatchError> {
        if parameter_sets.is_empty() {
            return Ok(0);
        }
        let mut session = self.acquire().await?;
        match session.execute_batch_with_params(sql, parameter_sets).await {
            Ok(result) => Ok(result),
            Err(error) => Err(Self::attach_batch_cleanup(&mut session, error).await),
        }
    }

    pub async fn execute_batch_with_blob_streams(
        &self,
        sql: &str,
        parameter_sets: &mut [Vec<Db2InputParameter<'_>>],
    ) -> Result<i64, Db2BatchError> {
        if parameter_sets.is_empty() {
            return Ok(0);
        }
        let mut session = self.acquire().await?;
        match session
            .execute_batch_with_blob_streams(sql, parameter_sets)
            .await
        {
            Ok(result) => Ok(result),
            Err(error) => Err(Self::attach_batch_cleanup(&mut session, error).await),
        }
    }

    /// Stops admission; after the deadline physically closes retained active transports.
    pub async fn shutdown(&self, budget: Duration) -> Result<(), Db2Error> {
        self.close().await?;
        let until = tokio::time::Instant::now() + budget;
        loop {
            if self
                .inner
                .active
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty()
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= until {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        for abort in self
            .inner
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
        {
            abort.abort();
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<(), Db2Error> {
        let _close_guard = self.inner.close_gate.lock().await;
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.inner.semaphore.close();
        let sessions = {
            let mut idle = self
                .inner
                .idle
                .lock()
                .map_err(|_| Db2Error::other("Db2 pool state is poisoned".into()))?;
            std::mem::take(&mut *idle)
        };
        // Remove all detached sessions from accounting before async shutdown.
        // Dropping close() also drops the remaining Vec without leaking counts.
        self.inner
            .session_count
            .fetch_sub(sessions.len(), Ordering::AcqRel);
        let mut close_error = None;
        for mut entry in sessions {
            if let Err(error) = entry.session.close().await {
                close_error.get_or_insert(error);
            }
        }
        close_error.map_or(Ok(()), Err)
    }

    async fn connect_reserved(&self) -> Result<Db2Session, Db2Error> {
        let started = Instant::now();
        let result = self.connect_reserved_inner().await;
        crate::diagnostics::record("connect", started.elapsed(), result.is_ok(), 0);
        result
    }
    async fn connect_reserved_inner(&self) -> Result<Db2Session, Db2Error> {
        let mut session = tokio::time::timeout(
            self.inner.options.connect_timeout,
            Db2Session::connect(self.inner.config.clone()),
        )
        .await
        .map_err(|_| {
            Db2Error::new(
                crate::Db2ErrorKind::Transport,
                "Db2 pool connection timed out",
            )
        })??;
        self.inner.session_count.fetch_add(1, Ordering::AcqRel);
        // Keep the count owned while baseline capture is doing async protocol I/O.
        // If acquire is cancelled here, dropping the session closes its transport and
        // this guard releases the reserved pool slot.
        let mut count = SessionCountGuard::new(self.inner.clone());
        if self.is_closed() {
            let _ = session.close().await;
            return Err(Db2Error::connection_closed("Db2 pool is closed"));
        }
        // Record CURRENT SCHEMA/ISOLATION before the session reaches its first
        // caller. A baseline read failure discards the session without a lease.
        if let Err(error) = session.capture_pool_baseline().await {
            let _ = session.close().await;
            return Err(error);
        }
        count.disarm();
        Ok(session)
    }

    /// Runs the pool reset after a failed operation so a cleanup failure can be
    /// attached to the operation error instead of masking it. Uncertain DML is
    /// never retried automatically. A session the operation already closed needs
    /// no reset; the next acquire discards it.
    async fn attach_cleanup(session: &mut Db2Session, error: Db2Error) -> Db2Error {
        if session.is_closed() {
            return error;
        }
        match session.reset_for_pool().await {
            Ok(()) => error,
            Err(reset_error) => error.with_cleanup(reset_error),
        }
    }

    async fn attach_batch_cleanup(session: &mut Db2Session, error: Db2BatchError) -> Db2BatchError {
        if session.is_closed() {
            return error;
        }
        match session.reset_for_pool().await {
            Ok(()) => error,
            Err(reset_error) => Db2BatchError {
                cause: error.cause.with_cleanup(reset_error),
                ..error
            },
        }
    }
}

pub struct Db2PooledSession {
    session: Option<Db2Session>,
    inner: Arc<PoolInner>,
    _permit: OwnedSemaphorePermit,
    born: Instant,
    abort: crate::network::session::PoolAbort,
}

impl Deref for Db2PooledSession {
    type Target = Db2Session;
    fn deref(&self) -> &Self::Target {
        self.session
            .as_ref()
            .expect("pooled session is present until drop")
    }
}

impl DerefMut for Db2PooledSession {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.session
            .as_mut()
            .expect("pooled session is present until drop")
    }
}

impl Db2PooledSession {
    /// Resets tracked session state and returns the physical session to the pool now.
    /// Dropping the future during reset drops the transport instead of returning an
    /// uncertain session to the idle list.
    pub async fn release(mut self) -> Result<(), Db2Error> {
        let Some(mut session) = self.session.take() else {
            return Ok(());
        };
        let mut count = SessionCountGuard::new(self.inner.clone());
        session.clear_warning_handler();
        if self.inner.closed.load(Ordering::Acquire)
            || session.is_closed()
            || self.born.elapsed() >= self.inner.options.max_lifetime
        {
            let _ = session.close().await;
            return Ok(());
        }
        if self.inner.reset_policy == PoolResetPolicy::Tracked {
            session.clear_unknown_pool_dirty();
        }
        if let Err(error) = session.reset_for_pool().await {
            let _ = session.close().await;
            return Err(error);
        }
        let mut idle = match self.inner.idle.lock() {
            Ok(idle) => idle,
            Err(_) => {
                let _ = session.close().await;
                return Err(Db2Error::other("Db2 pool state is poisoned".into()));
            }
        };
        if self.inner.closed.load(Ordering::Acquire) {
            drop(idle);
            let _ = session.close().await;
            return Err(Db2Error::connection_closed("Db2 pool is closed"));
        }
        idle.push(IdleSession {
            session,
            born: self.born,
            returned: Instant::now(),
        });
        count.disarm();
        Ok(())
    }
}

impl Drop for Db2PooledSession {
    fn drop(&mut self) {
        self.inner
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.abort.id());
        let Some(mut session) = self.session.take() else {
            return;
        };
        // A callback may carry caller-specific state; never retain it for the next lease.
        session.clear_warning_handler();
        if self.inner.closed.load(Ordering::Acquire)
            || session.is_closed()
            || self.born.elapsed() >= self.inner.options.max_lifetime
        {
            self.inner.session_count.fetch_sub(1, Ordering::AcqRel);
            drop(session);
            return;
        }
        if let Ok(mut idle) = self.inner.idle.lock() {
            if self.inner.closed.load(Ordering::Acquire) {
                self.inner.session_count.fetch_sub(1, Ordering::AcqRel);
                drop(session);
            } else {
                idle.push(IdleSession {
                    session,
                    born: self.born,
                    returned: Instant::now(),
                });
            }
        } else {
            self.inner.session_count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::Db2ErrorKind;
    use crate::network::{codepoint as cp, ddm, dss};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn admission_limits_timeout_and_cancel_without_socket() {
        let pool = Db2Pool::new_with_options(
            Config::default(),
            Db2PoolOptions {
                max_connections: 1,
                max_waiters: 1,
                acquire_timeout: Duration::from_millis(20),
                ..Default::default()
            },
            PoolResetPolicy::Strict,
        )
        .unwrap();
        let permit = pool.inner.semaphore.clone().try_acquire_owned().unwrap();
        let queued_pool = pool.clone();
        let queued = tokio::spawn(async move { queued_pool.acquire().await });
        while pool.waiting_count() == 0 {
            tokio::task::yield_now().await;
        }
        let error = match pool.acquire().await {
            Err(error) => error,
            Ok(_) => panic!("queue should be full"),
        };
        assert_eq!(error.kind, Db2ErrorKind::PoolExhausted);
        let error = match queued.await.unwrap() {
            Err(error) => error,
            Ok(_) => panic!("must time out"),
        };
        assert_eq!(error.kind, Db2ErrorKind::PoolTimeout);
        assert_eq!(pool.waiting_count(), 0);
        let queued_pool = pool.clone();
        let queued = tokio::spawn(async move { queued_pool.acquire().await });
        while pool.waiting_count() == 0 {
            tokio::task::yield_now().await;
        }
        queued.abort();
        let _ = queued.await;
        assert_eq!(pool.waiting_count(), 0);
        drop(permit);
        assert_eq!(pool.inner.semaphore.available_permits(), 1);
        pool.close().await.unwrap();
    }

    #[test]
    fn pool_rejects_zero_capacity() {
        let error = match Db2Pool::new(Config::default(), 0) {
            Ok(_) => panic!("zero capacity must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("positive integer"));
        assert_eq!(error.kind, Db2ErrorKind::InvalidConfiguration);
    }

    #[tokio::test]
    async fn explicit_release_skips_validation_for_a_clean_session() {
        use tokio::io::AsyncReadExt;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let mut socket = accept_initialized_pool_session(&listener).await;
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
        let pool = Db2Pool::new(config, 1).unwrap();
        let lease = pool.acquire().await.unwrap();
        lease.release().await.unwrap();
        assert_eq!(pool.idle_session_count(), 1);
        pool.close().await.unwrap();
        assert_eq!(pool.session_count(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn explicit_release_resets_only_typed_dirty_registers() {
        use tokio::io::AsyncReadExt;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let mut socket = accept_initialized_pool_session(&listener).await;
            for expected_sql in [
                b"SET CURRENT SCHEMA \"TENANT\"".as_slice(),
                b"SET CURRENT ISOLATION UR".as_slice(),
                b"SET CURRENT SCHEMA \"DB2INST1\"".as_slice(),
                b"SET CURRENT ISOLATION CS".as_slice(),
            ] {
                let request = read_chain(&mut socket).await;
                assert_eq!(
                    request
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
                );
                assert!(
                    request[1]
                        .payload
                        .windows(expected_sql.len())
                        .any(|window| { window == expected_sql })
                );
                send(&mut socket, cp::SQLCARD, &test_sql_card(), false).await;
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
        let pool = Db2Pool::new(config, 1).unwrap();
        let mut lease = pool.acquire().await.unwrap();
        lease.set_current_schema("TENANT").await.unwrap();
        lease
            .set_current_isolation(crate::network::session::Db2IsolationLevel::UncommittedRead)
            .await
            .unwrap();
        lease.release().await.unwrap();
        assert_eq!(pool.idle_session_count(), 1);
        pool.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tracked_pool_policy_skips_validation_after_arbitrary_sql() {
        use tokio::io::AsyncReadExt;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let mut socket = accept_initialized_pool_session(&listener).await;
            let request = read_chain(&mut socket).await;
            assert_eq!(
                request
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSQLIMM, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(), false).await;
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
        let pool = Db2Pool::new_with_reset_policy(config, 1, PoolResetPolicy::Tracked).unwrap();
        let mut lease = pool.acquire().await.unwrap();
        lease
            .execute("SET CURRENT SCHEMA \"TENANT\"")
            .await
            .unwrap();
        lease.release().await.unwrap();
        assert_eq!(pool.idle_session_count(), 1);
        pool.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn dropping_acquire_during_reset_releases_count_and_capacity() {
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (started_tx, started_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut first = accept_initialized_pool_session(&listener).await;
            assert_eq!(read_chain(&mut first).await[0].codepoint, cp::RDBRLLBCK);
            started_tx.send(()).unwrap();
            let mut byte = [0];
            assert_eq!(first.read(&mut byte).await.unwrap(), 0);
            let mut replacement = accept_initialized_pool_session(&listener).await;
            assert_eq!(replacement.read(&mut byte).await.unwrap(), 0);
        });
        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        let pool = Db2Pool::new(config, 1).unwrap();
        let mut lease = pool.acquire().await.unwrap();
        lease.set_auto_commit(false);
        drop(lease);
        let mut acquire = Box::pin(pool.acquire());
        tokio::select! {
            result = &mut acquire => panic!("reset finished without reply: {}", result.is_ok()),
            result = started_rx => result.unwrap(),
        }
        drop(acquire);
        assert_eq!(pool.session_count(), 0);
        assert_eq!(pool.idle_session_count(), 0);
        let replacement = timeout(Duration::from_secs(2), pool.acquire())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pool.session_count(), 1);
        drop(replacement);
        pool.close().await.unwrap();
        server.await.unwrap();
        assert_eq!(pool.session_count(), 0);
    }

    #[tokio::test]
    async fn dropping_acquire_during_handshake_closes_transport_without_reserving_count() {
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (started_tx, started_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            read_chain(&mut first).await;
            started_tx.send(()).unwrap();
            let mut byte = [0];
            assert_eq!(first.read(&mut byte).await.unwrap(), 0);
            let mut replacement = accept_initialized_pool_session(&listener).await;
            assert_eq!(replacement.read(&mut byte).await.unwrap(), 0);
        });
        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        let pool = Db2Pool::new(config, 1).unwrap();
        let mut acquire = Box::pin(pool.acquire());
        tokio::select! {
            result = &mut acquire => panic!("handshake completed without reply: {}", result.is_ok()),
            result = started_rx => result.unwrap(),
        }
        drop(acquire);
        assert_eq!(pool.session_count(), 0);
        let replacement = timeout(Duration::from_secs(2), pool.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(replacement);
        pool.close().await.unwrap();
        server.await.unwrap();
        assert_eq!(pool.session_count(), 0);
    }

    #[tokio::test]
    async fn dropping_acquire_during_pool_baseline_read_releases_count_and_slot() {
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (started_tx, started_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut first = accept_until_pool_baseline_query(&listener).await;
            started_tx.send(()).unwrap();
            let mut byte = [0];
            assert_eq!(first.read(&mut byte).await.unwrap(), 0);
            let mut replacement = accept_initialized_pool_session(&listener).await;
            assert_eq!(replacement.read(&mut byte).await.unwrap(), 0);
        });
        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        let pool = Db2Pool::new(config, 1).unwrap();

        let acquire_pool = pool.clone();
        let acquire = tokio::spawn(async move { acquire_pool.acquire().await });
        timeout(Duration::from_secs(3), started_rx)
            .await
            .expect("pool should begin baseline capture after handshake")
            .unwrap();
        assert_eq!(pool.session_count(), 1);
        acquire.abort();
        match acquire.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(Ok(_)) => panic!("cancelled baseline acquisition unexpectedly returned a lease"),
            Ok(Err(_)) => panic!("cancelled baseline acquisition unexpectedly returned an error"),
        }
        assert_eq!(pool.session_count(), 0);
        assert_eq!(pool.idle_session_count(), 0);

        let replacement = timeout(Duration::from_secs(2), pool.acquire())
            .await
            .expect("the released pool slot should allow a replacement connection")
            .unwrap();
        assert_eq!(pool.session_count(), 1);
        drop(replacement);
        pool.close().await.unwrap();
        server.await.unwrap();
        assert_eq!(pool.session_count(), 0);
    }

    #[tokio::test]
    async fn concurrent_pool_close_calls_are_idempotent_and_drain_idle_sessions() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(serve_connection_only(listener));
        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 5_000;
        let pool = Db2Pool::new(config, 1).unwrap();
        let lease = pool.acquire().await.unwrap();
        drop(lease);
        assert_eq!(pool.idle_session_count(), 1);

        let first_pool = pool.clone();
        let first_close = tokio::spawn(async move { first_pool.close().await });
        let second_pool = pool.clone();
        let second_close = tokio::spawn(async move { second_pool.close().await });
        first_close.await.unwrap().unwrap();
        second_close.await.unwrap().unwrap();

        assert!(pool.is_closed());
        assert_eq!(pool.session_count(), 0);
        assert_eq!(pool.idle_session_count(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn close_rejects_waiters_and_keeps_active_lease_until_drop() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(serve_connection_only(listener));
        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 5_000;
        let pool = Db2Pool::new(config, 1).unwrap();
        let lease = pool.acquire().await.unwrap();
        assert_eq!(pool.session_count(), 1);

        let waiter_pool = pool.clone();
        let mut waiter = tokio::spawn(async move { waiter_pool.acquire().await });
        assert!(
            timeout(Duration::from_millis(20), &mut waiter)
                .await
                .is_err()
        );

        pool.close().await.unwrap();
        let result = timeout(Duration::from_millis(100), &mut waiter)
            .await
            .expect("pool close should wake waiting acquisitions")
            .unwrap();
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("closed pool unexpectedly returned a session to a waiter"),
        };
        assert!(error.to_string().contains("pool is closed"));
        assert_eq!(error.kind, Db2ErrorKind::ConnectionClosed);
        assert_eq!(
            pool.session_count(),
            1,
            "an active lease remains owned until it is dropped"
        );

        drop(lease);
        assert_eq!(pool.session_count(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn aborting_queued_acquire_does_not_interrupt_active_lease_or_leak_capacity() {
        use tokio::sync::oneshot;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(serve_connection_only(listener));
        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 5_000;
        let pool = Db2Pool::new(config, 1).unwrap();
        let lease = pool.acquire().await.unwrap();
        let (queued_tx, queued_rx) = oneshot::channel();
        let waiter_pool = pool.clone();
        let mut waiter = tokio::spawn(async move {
            queued_tx.send(()).unwrap();
            waiter_pool.acquire().await
        });
        timeout(Duration::from_secs(1), queued_rx)
            .await
            .expect("waiter task should start")
            .unwrap();
        assert!(
            timeout(Duration::from_millis(20), &mut waiter)
                .await
                .is_err()
        );

        waiter.abort();
        match waiter.await {
            Ok(_) => panic!("an aborted acquire task must not complete successfully"),
            Err(error) => assert!(error.is_cancelled()),
        }
        assert!(
            !lease.is_closed(),
            "aborting a waiter must not affect the checked-out session"
        );
        assert!(!pool.is_closed());
        assert_eq!(pool.session_count(), 1);
        assert_eq!(pool.idle_session_count(), 0);

        drop(lease);
        assert_eq!(pool.session_count(), 1);
        assert_eq!(pool.idle_session_count(), 1);
        pool.close().await.unwrap();
        assert_eq!(pool.session_count(), 0);
        assert_eq!(pool.idle_session_count(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn repeated_pool_authentication_failures_release_capacity_to_waiters() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        const FAILED_CONNECTIONS: usize = 3;
        let server = tokio::spawn(serve_pool_open_failures_then_success(
            listener,
            FAILED_CONNECTIONS,
        ));
        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 5_000;
        let pool = Db2Pool::new(config, 1).unwrap();

        let callers = (0..=FAILED_CONNECTIONS)
            .map(|_| {
                let pool = pool.clone();
                tokio::spawn(async move {
                    timeout(Duration::from_secs(5), pool.acquire())
                        .await
                        .expect("a queued acquisition must proceed after a failed connection")
                })
            })
            .collect::<Vec<_>>();
        let mut failures = 0;
        let mut leases = Vec::new();
        for caller in callers {
            match timeout(Duration::from_secs(6), caller)
                .await
                .expect("every queued acquisition must finish")
                .unwrap()
            {
                Ok(lease) => leases.push(lease),
                Err(error) => {
                    assert_eq!(error.kind, Db2ErrorKind::AuthenticationFailed);
                    failures += 1;
                }
            }
        }

        assert_eq!(failures, FAILED_CONNECTIONS);
        assert_eq!(
            leases.len(),
            1,
            "capacity must serve a successful later retry"
        );
        assert_eq!(pool.session_count(), 1);
        assert_eq!(pool.idle_session_count(), 0);

        pool.close().await.unwrap();
        drop(leases.pop());
        assert_eq!(pool.session_count(), 0);
        assert_eq!(pool.idle_session_count(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn close_drains_connection_that_finishes_opening_after_close() {
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (handshake_started_tx, handshake_started_rx) = oneshot::channel();
        let (continue_handshake_tx, continue_handshake_rx) = oneshot::channel();
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
            handshake_started_tx.send(()).unwrap();
            continue_handshake_rx.await.unwrap();

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
            send(&mut socket, cp::SQLCARD, &test_sql_card(), false).await;
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
        config.timeout_ms = 5_000;
        let pool = Db2Pool::new(config, 1).unwrap();
        let opening_pool = pool.clone();
        let mut opening = tokio::spawn(async move { opening_pool.acquire().await });

        timeout(Duration::from_secs(3), handshake_started_rx)
            .await
            .expect("connection should begin its handshake")
            .unwrap();
        pool.close().await.unwrap();
        continue_handshake_tx.send(()).unwrap();

        let result = timeout(Duration::from_secs(5), &mut opening)
            .await
            .expect("connection completion after pool close should return")
            .unwrap();
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("a connection finishing after pool close was leased"),
        };
        assert!(error.to_string().contains("pool is closed"));
        assert_eq!(error.kind, Db2ErrorKind::ConnectionClosed);
        assert_eq!(pool.session_count(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn close_during_session_reset_discards_the_acquiring_connection() {
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (reset_started_tx, reset_started_rx) = oneshot::channel();
        let (continue_reset_tx, continue_reset_rx) = oneshot::channel();
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
            send(&mut socket, cp::SQLCARD, &test_sql_card(), false).await;
            serve_pool_baseline(&mut socket).await;

            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::RDBRLLBCK);
            reset_started_tx.send(()).unwrap();
            continue_reset_rx.await.unwrap();
            send(&mut socket, cp::SQLCARD, &test_sql_card(), false).await;
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
        config.timeout_ms = 5_000;
        let pool = Db2Pool::new(config, 1).unwrap();
        let mut lease = pool.acquire().await.unwrap();
        assert_eq!(pool.session_count(), 1);
        lease.set_auto_commit(false);
        drop(lease);

        let acquiring_pool = pool.clone();
        let mut acquiring = tokio::spawn(async move { acquiring_pool.acquire().await });
        timeout(Duration::from_secs(3), reset_started_rx)
            .await
            .expect("pool should begin resetting the idle session")
            .unwrap();
        pool.close().await.unwrap();
        continue_reset_tx.send(()).unwrap();

        let result = timeout(Duration::from_secs(3), &mut acquiring)
            .await
            .expect("an acquisition resetting during close should finish")
            .unwrap();
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("pool returned a session after close raced with reset"),
        };
        assert!(error.to_string().contains("pool is closed"));
        assert_eq!(error.kind, Db2ErrorKind::ConnectionClosed);
        assert_eq!(pool.session_count(), 0);
        assert_eq!(pool.idle_session_count(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn close_during_failed_reset_does_not_connect_a_replacement() {
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (reset_started_tx, reset_started_rx) = oneshot::channel();
        let (continue_reset_tx, continue_reset_rx) = oneshot::channel();
        let (replacement_tx, replacement_rx) = oneshot::channel();
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
            send(&mut socket, cp::SQLCARD, &test_sql_card(), false).await;
            serve_pool_baseline(&mut socket).await;

            assert_eq!(read_chain(&mut socket).await[0].codepoint, cp::RDBRLLBCK);
            reset_started_tx.send(()).unwrap();
            continue_reset_rx.await.unwrap();
            send(
                &mut socket,
                cp::SQLCARD,
                &test_sql_card_error(-911, "40001"),
                false,
            )
            .await;

            let replacement = timeout(Duration::from_secs(1), listener.accept()).await;
            if let Ok(Ok((mut replacement, _))) = replacement {
                let _ = replacement.shutdown().await;
                replacement_tx.send(true).unwrap();
            } else {
                replacement_tx.send(false).unwrap();
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
        config.timeout_ms = 1_000;
        let pool = Db2Pool::new(config, 1).unwrap();
        let mut lease = pool.acquire().await.unwrap();
        lease.set_auto_commit(false);
        drop(lease);

        let acquiring_pool = pool.clone();
        let mut acquiring = tokio::spawn(async move { acquiring_pool.acquire().await });
        timeout(Duration::from_secs(3), reset_started_rx)
            .await
            .expect("pool should begin resetting the idle transaction")
            .unwrap();
        pool.close().await.unwrap();
        continue_reset_tx.send(()).unwrap();

        let result = timeout(Duration::from_secs(3), &mut acquiring)
            .await
            .expect("acquisition should end after reset fails during close")
            .unwrap();
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("pool returned a session after close raced with failed reset"),
        };
        assert!(error.to_string().contains("pool is closed"), "{error}");
        assert_eq!(error.kind, Db2ErrorKind::ConnectionClosed);
        assert!(
            !replacement_rx.await.unwrap(),
            "closed pool started a replacement connection"
        );
        assert_eq!(pool.session_count(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn close_during_replacement_handshake_after_failed_reset_discards_session_and_waiters() {
        use tokio::io::AsyncReadExt;
        use tokio::sync::oneshot;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (reset_started_tx, reset_started_rx) = oneshot::channel();
        let (continue_reset_tx, continue_reset_rx) = oneshot::channel();
        let (replacement_started_tx, replacement_started_rx) = oneshot::channel();
        let (continue_replacement_tx, continue_replacement_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut original = accept_initialized_pool_session(&listener).await;
            assert_eq!(read_chain(&mut original).await[0].codepoint, cp::RDBRLLBCK);
            reset_started_tx.send(()).unwrap();
            continue_reset_rx.await.unwrap();
            send(
                &mut original,
                cp::SQLCARD,
                &test_sql_card_error(-911, "40001"),
                false,
            )
            .await;
            let mut byte = [0_u8; 1];
            assert_eq!(original.read(&mut byte).await.unwrap(), 0);
            drop(original);

            let (mut replacement, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_chain(&mut replacement)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::ACCSEC]
            );
            replacement_started_tx.send(()).unwrap();
            continue_replacement_rx.await.unwrap();
            send(
                &mut replacement,
                cp::ACCSECRD,
                &ddm::pack_u16(cp::SECMEC, 3),
                false,
            )
            .await;
            assert_eq!(
                read_chain(&mut replacement)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::SECCHK, cp::ACCRDB]
            );
            send(
                &mut replacement,
                cp::SECCHKRM,
                &ddm::pack_bytes(cp::SECCHKCD, &[0]),
                true,
            )
            .await;
            send(&mut replacement, cp::ACCRDBRM, &[], false).await;
            assert_eq!(
                read_chain(&mut replacement)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSAT, cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut replacement, cp::SQLCARD, &test_sql_card(), false).await;
            let mut byte = [0_u8; 1];
            assert_eq!(replacement.read(&mut byte).await.unwrap(), 0);
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 5_000;
        let pool = Db2Pool::new(config, 1).unwrap();
        let mut lease = pool.acquire().await.unwrap();
        lease.set_auto_commit(false);
        drop(lease);

        let opening_pool = pool.clone();
        let mut opening = tokio::spawn(async move { opening_pool.acquire().await });
        timeout(Duration::from_secs(3), reset_started_rx)
            .await
            .expect("replacement acquire should first reset the idle session")
            .unwrap();
        continue_reset_tx.send(()).unwrap();
        timeout(Duration::from_secs(3), replacement_started_rx)
            .await
            .expect("a failed reset should start opening its replacement")
            .unwrap();

        let queued_pool = pool.clone();
        let mut queued = tokio::spawn(async move { queued_pool.acquire().await });
        tokio::task::yield_now().await;
        pool.close().await.unwrap();
        let queued_result = timeout(Duration::from_secs(1), &mut queued)
            .await
            .expect("close should reject the waiter behind the replacement handshake")
            .unwrap();
        let queued_error = match queued_result {
            Err(error) => error,
            Ok(_) => panic!("closed pool unexpectedly leased a queued session"),
        };
        assert_eq!(queued_error.kind, Db2ErrorKind::ConnectionClosed);

        continue_replacement_tx.send(()).unwrap();
        let opening_result = timeout(Duration::from_secs(5), &mut opening)
            .await
            .expect("replacement handshake completion after close should return")
            .unwrap();
        let opening_error = match opening_result {
            Err(error) => error,
            Ok(_) => panic!("replacement session was leased after pool close"),
        };
        assert_eq!(opening_error.kind, Db2ErrorKind::ConnectionClosed);
        assert_eq!(pool.session_count(), 0);
        assert_eq!(pool.idle_session_count(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn denied_session_authorization_reset_discards_session_and_next_acquire_uses_replacement()
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;

            let mut original = accept_initialized_pool_session(&listener).await;
            assert_eq!(
                read_chain(&mut original)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSQLIMM, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut original, cp::SQLCARD, &test_sql_card(), false).await;

            // Raw SQL can switch identity without changing the driver's Rust-side cache.
            serve_pool_state(&mut original, ["TENANT", "DB2INST1", "CS", "", "", "", ""]).await;
            assert_eq!(
                read_chain(&mut original)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSQLIMM, cp::SQLSTT, cp::RDBCMM]
            );
            send(
                &mut original,
                cp::SQLCARD,
                &test_sql_card_error(-911, "40001"),
                false,
            )
            .await;
            let mut byte = [0_u8; 1];
            assert_eq!(original.read(&mut byte).await.unwrap(), 0);
            drop(original);

            let mut replacement = accept_initialized_pool_session(&listener).await;
            assert_eq!(
                read_chain(&mut replacement)
                    .await
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
            );
            send(
                &mut replacement,
                cp::QRYDSC,
                &[6, 0x76, 0xd0, 0x02, 0, 4],
                true,
            )
            .await;
            send(
                &mut replacement,
                cp::OPNQRYRM,
                &ddm::pack_bytes(cp::QRYINSID, &1_u64.to_be_bytes()),
                true,
            )
            .await;
            send(&mut replacement, cp::QRYDTA, &[0xff, 0, 42, 0, 0, 0], true).await;
            send(&mut replacement, cp::ENDQRYRM, &[], false).await;
            assert_eq!(replacement.read(&mut byte).await.unwrap(), 0);
        });

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 5_000;
        let pool = Db2Pool::new(config, 1).unwrap();

        let mut original = pool.acquire().await.unwrap();
        original
            .execute("SET SESSION AUTHORIZATION \"TENANT\"")
            .await
            .unwrap();
        drop(original);
        assert_eq!(pool.session_count(), 1);
        assert_eq!(pool.idle_session_count(), 1);

        let mut replacement = timeout(Duration::from_secs(5), pool.acquire())
            .await
            .expect("acquire should replace the session whose reset failed")
            .unwrap();
        assert_eq!(pool.session_count(), 1);
        assert_eq!(pool.idle_session_count(), 0);
        let result = replacement
            .query("VALUES CAST(42 AS INTEGER)")
            .await
            .unwrap();
        assert_eq!(result.rows, vec![vec![Db2Value::Integer(42)]]);

        drop(replacement);
        pool.close().await.unwrap();
        assert_eq!(pool.session_count(), 0);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn session_authorization_reset_uses_actual_server_state_and_reuses_same_session() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;

            let mut socket = accept_initialized_pool_session_with_baseline(
                &listener,
                ["POOLBASE", "DB2INST1", "CS", "", "", "", ""],
            )
            .await;
            let initial_identity_change = read_chain(&mut socket).await;
            assert_eq!(
                initial_identity_change
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSQLIMM, cp::SQLSTT, cp::RDBCMM]
            );
            send(&mut socket, cp::SQLCARD, &test_sql_card(), false).await;

            // This is the observed server state, independent of Rust's identity cache.
            serve_pool_state(
                &mut socket,
                ["TENANT", "DB2INST1", "CS", "temporary", "", "", ""],
            )
            .await;
            let reset = read_chain(&mut socket).await;
            assert_eq!(
                reset
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSQLIMM, cp::SQLSTT, cp::RDBCMM]
            );
            assert!(reset.iter().any(|packet| {
                packet
                    .payload
                    .windows(b"SET SESSION AUTHORIZATION = \"POOLBASE\"".len())
                    .any(|window| window == b"SET SESSION AUTHORIZATION = \"POOLBASE\"")
            }));
            send(&mut socket, cp::SQLCARD, &test_sql_card(), false).await;

            let reset_client_register = read_chain(&mut socket).await;
            assert_eq!(
                reset_client_register
                    .iter()
                    .map(|packet| packet.codepoint)
                    .collect::<Vec<_>>(),
                vec![cp::EXCSQLSET, cp::SQLSTT, cp::RDBCMM]
            );
            assert!(reset_client_register.iter().any(|packet| {
                packet
                    .payload
                    .windows(b"SET CLIENT APPLNAME ''".len())
                    .any(|window| window == b"SET CLIENT APPLNAME ''")
            }));
            send(&mut socket, cp::SQLCARD, &test_sql_card(), false).await;

            // The same socket is leased again, then the next probe confirms baseline state.
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
                &ddm::pack_bytes(cp::QRYINSID, &1_u64.to_be_bytes()),
                true,
            )
            .await;
            send(&mut socket, cp::QRYDTA, &[0xff, 0, 42, 0, 0, 0], true).await;
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
        config.timeout_ms = 5_000;
        let pool = Db2Pool::new(config, 1).unwrap();

        let mut lease = pool.acquire().await.unwrap();
        lease
            .execute("SET SESSION AUTHORIZATION \"TENANT\"")
            .await
            .unwrap();
        drop(lease);

        let mut lease = pool.acquire().await.unwrap();
        let result = lease.query("VALUES CAST(42 AS INTEGER)").await.unwrap();
        assert_eq!(result.rows, vec![vec![Db2Value::Integer(42)]]);
        drop(lease);
        pool.close().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn failed_reset_and_replacement_transport_and_auth_errors_recover_multiple_waiters() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        const FAILED_REPLACEMENTS: usize = 3;
        let (reset_started_tx, reset_started_rx) = tokio::sync::oneshot::channel();
        let (continue_reset_tx, continue_reset_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            serve_pool_reset_failure_with_queued_transport_and_auth_failures(
                listener,
                FAILED_REPLACEMENTS,
                reset_started_tx,
                continue_reset_rx,
            ),
        );

        let mut config = Config::default();
        config.host = "127.0.0.1".into();
        config.port = port;
        config.database = "TESTDB".into();
        config.user = "db2inst1".into();
        config.password = "password".into();
        config.security_mechanism = 3;
        config.timeout_ms = 5_000;
        let pool = Db2Pool::new(config, 1).unwrap();

        let mut original = pool.acquire().await.unwrap();
        original
            .execute("SET SESSION AUTHORIZATION \"TENANT\"")
            .await
            .unwrap();
        drop(original);
        assert_eq!(pool.session_count(), 1);
        assert_eq!(pool.idle_session_count(), 1);

        let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
        let callers = (0..=FAILED_REPLACEMENTS)
            .map(|_| {
                let pool = pool.clone();
                let ready = ready_tx.clone();
                tokio::spawn(async move {
                    // Sending before acquire lets the test know this task is about to
                    // poll acquire; with the current-thread test runtime it then parks
                    // on the semaphore before the test task resumes.
                    ready.send(()).unwrap();
                    timeout(Duration::from_secs(6), async move {
                        let mut lease = pool.acquire().await?;
                        lease.query("VALUES CAST(42 AS INTEGER)").await
                    })
                    .await
                    .expect("every waiter must finish after reset recovery")
                })
            })
            .collect::<Vec<_>>();
        drop(ready_tx);

        timeout(Duration::from_secs(3), reset_started_rx)
            .await
            .expect("first waiter should begin resetting the idle session")
            .unwrap();
        for _ in 0..callers.len() {
            timeout(Duration::from_secs(3), ready_rx.recv())
                .await
                .expect("every caller should reach the pool acquisition")
                .expect("ready channel remains open until all callers have started");
        }
        assert_eq!(pool.session_count(), 1);
        assert_eq!(pool.idle_session_count(), 0);
        continue_reset_tx.send(()).unwrap();

        let outcomes = timeout(Duration::from_secs(10), async {
            let mut outcomes = Vec::with_capacity(callers.len());
            for caller in callers {
                outcomes.push(caller.await.unwrap());
            }
            outcomes
        })
        .await
        .expect("reset failure and queued open attempts must not strand waiters");
        let transport_failures = outcomes
            .iter()
            .filter(|outcome| {
                outcome
                    .as_ref()
                    .is_err_and(|error| error.kind == Db2ErrorKind::Transport)
            })
            .count();
        assert_eq!(transport_failures, 2, "waiter outcomes: {outcomes:?}");
        let authentication_failures = outcomes
            .iter()
            .filter(|outcome| {
                outcome
                    .as_ref()
                    .is_err_and(|error| error.kind == Db2ErrorKind::AuthenticationFailed)
            })
            .count();
        assert_eq!(authentication_failures, 1);
        let successes = outcomes
            .iter()
            .filter_map(|outcome| outcome.as_ref().ok())
            .collect::<Vec<_>>();
        assert_eq!(successes.len(), 1);
        assert_eq!(successes[0].rows, vec![vec![Db2Value::Integer(42)]]);
        assert_eq!(pool.session_count(), 1);
        assert_eq!(pool.idle_session_count(), 1);

        pool.close().await.unwrap();
        assert_eq!(pool.session_count(), 0);
        assert_eq!(pool.idle_session_count(), 0);
        server.await.unwrap();
    }

    async fn accept_initialized_pool_session(listener: &TcpListener) -> tokio::net::TcpStream {
        accept_initialized_pool_session_with_baseline(
            listener,
            ["DB2INST1", "DB2INST1", "CS", "", "", "", ""],
        )
        .await
    }

    async fn accept_initialized_pool_session_with_baseline(
        listener: &TcpListener,
        baseline: [&str; 7],
    ) -> tokio::net::TcpStream {
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
        send(&mut socket, cp::SQLCARD, &test_sql_card(), false).await;
        serve_pool_state(&mut socket, baseline).await;
        socket
    }

    /// Opens and initializes a socket, then stops while the pool's first baseline query is in flight.
    async fn accept_until_pool_baseline_query(listener: &TcpListener) -> tokio::net::TcpStream {
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
        send(&mut socket, cp::SQLCARD, &test_sql_card(), false).await;
        assert_eq!(
            read_chain(&mut socket)
                .await
                .iter()
                .map(|packet| packet.codepoint)
                .collect::<Vec<_>>(),
            vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
        );
        socket
    }

    /// Answers the pool's combined actual server-state probe.
    async fn serve_pool_baseline(socket: &mut tokio::net::TcpStream) {
        serve_pool_state(socket, ["DB2INST1", "DB2INST1", "CS", "", "", "", ""]).await;
    }

    async fn serve_pool_state(socket: &mut tokio::net::TcpStream, values: [&str; 7]) {
        assert_eq!(
            read_chain(socket)
                .await
                .iter()
                .map(|packet| packet.codepoint)
                .collect::<Vec<_>>(),
            vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
        );
        let mut descriptor = vec![24, 0x76, 0xd0];
        for _ in 0..7 {
            descriptor.extend_from_slice(&[0x41, 0, 128]);
        }
        send(socket, cp::QRYDSC, &descriptor, true).await;
        send(
            socket,
            cp::OPNQRYRM,
            &ddm::pack_bytes(cp::QRYINSID, &1_u64.to_be_bytes()),
            true,
        )
        .await;
        let mut data = vec![0xff, 0];
        for value in values {
            data.push(0); // nullable VARCHAR: non-null
            data.extend_from_slice(&(value.len() as u16).to_be_bytes());
            data.extend_from_slice(value.as_bytes());
        }
        send(socket, cp::QRYDTA, &data, true).await;
        send(socket, cp::ENDQRYRM, &[], false).await;
    }

    async fn serve_pool_open_failures_then_success(
        listener: TcpListener,
        failed_connections: usize,
    ) {
        use tokio::io::AsyncReadExt;

        for _ in 0..failed_connections {
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
                &ddm::pack_bytes(cp::SECCHKCD, &[8]),
                false,
            )
            .await;
        }

        let mut socket = accept_initialized_pool_session(&listener).await;
        let mut byte = [0_u8; 1];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    }

    async fn serve_pool_reset_failure_with_queued_transport_and_auth_failures(
        listener: TcpListener,
        failed_replacements: usize,
        reset_started: tokio::sync::oneshot::Sender<()>,
        continue_reset: tokio::sync::oneshot::Receiver<()>,
    ) {
        use tokio::io::AsyncReadExt;

        let mut original = accept_initialized_pool_session(&listener).await;
        assert_eq!(
            read_chain(&mut original)
                .await
                .iter()
                .map(|packet| packet.codepoint)
                .collect::<Vec<_>>(),
            vec![cp::EXCSQLIMM, cp::SQLSTT, cp::RDBCMM]
        );
        send(&mut original, cp::SQLCARD, &test_sql_card(), false).await;
        serve_pool_state(&mut original, ["TENANT", "DB2INST1", "CS", "", "", "", ""]).await;
        assert_eq!(
            read_chain(&mut original)
                .await
                .iter()
                .map(|packet| packet.codepoint)
                .collect::<Vec<_>>(),
            vec![cp::EXCSQLIMM, cp::SQLSTT, cp::RDBCMM]
        );
        reset_started.send(()).unwrap();
        continue_reset.await.unwrap();
        send(
            &mut original,
            cp::SQLCARD,
            &test_sql_card_error(-911, "40001"),
            false,
        )
        .await;
        let mut byte = [0_u8; 1];
        assert_eq!(original.read(&mut byte).await.unwrap(), 0);
        drop(original);

        for attempt in 0..failed_replacements {
            if attempt < 2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                assert_eq!(
                    read_chain(&mut socket)
                        .await
                        .iter()
                        .map(|packet| packet.codepoint)
                        .collect::<Vec<_>>(),
                    vec![cp::EXCSAT, cp::ACCSEC]
                );
                drop(socket);
                continue;
            }
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
                &ddm::pack_bytes(cp::SECCHKCD, &[8]),
                false,
            )
            .await;
        }

        let mut replacement = accept_initialized_pool_session(&listener).await;
        assert_eq!(
            read_chain(&mut replacement)
                .await
                .iter()
                .map(|packet| packet.codepoint)
                .collect::<Vec<_>>(),
            vec![cp::PRPSQLSTT, cp::SQLSTT, cp::OPNQRY]
        );
        send(
            &mut replacement,
            cp::QRYDSC,
            &[6, 0x76, 0xd0, 0x02, 0, 4],
            true,
        )
        .await;
        send(
            &mut replacement,
            cp::OPNQRYRM,
            &ddm::pack_bytes(cp::QRYINSID, &1_u64.to_be_bytes()),
            true,
        )
        .await;
        send(&mut replacement, cp::QRYDTA, &[0xff, 0, 42, 0, 0, 0], true).await;
        send(&mut replacement, cp::ENDQRYRM, &[], false).await;
        assert_eq!(replacement.read(&mut byte).await.unwrap(), 0);
    }

    async fn serve_connection_only(listener: TcpListener) {
        use tokio::io::AsyncReadExt;
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
        send(&mut socket, cp::SQLCARD, &test_sql_card(), false).await;
        serve_pool_baseline(&mut socket).await;
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    }

    async fn read_chain(socket: &mut tokio::net::TcpStream) -> Vec<dss::DssPacket> {
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

    async fn send(
        socket: &mut tokio::net::TcpStream,
        codepoint: cp::CodePoint,
        payload: &[u8],
        chained: bool,
    ) {
        let object = ddm::pack_ddm_object(codepoint, payload);
        socket
            .write_all(&dss::encode_reply_dss(&object, chained, 1))
            .await
            .unwrap();
    }

    fn test_sql_card() -> [u8; 54] {
        let mut result = [0; 54];
        result[5..10].copy_from_slice(b"00000");
        result
    }

    fn test_sql_card_error(code: i32, state: &str) -> [u8; 54] {
        let mut result = [0; 54];
        result[1..5].copy_from_slice(&code.to_le_bytes());
        result[5..10].copy_from_slice(state.as_bytes());
        result
    }
}
