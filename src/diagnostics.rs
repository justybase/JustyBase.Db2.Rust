//! Metadata-only events; observers and exporters are owned by the application.
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Db2Diagnostic {
    pub operation: &'static str,
    pub duration: Duration,
    pub success: bool,
    pub bytes: u64,
}
type Observer = Arc<dyn Fn(&Db2Diagnostic) + Send + Sync>;
static OBSERVERS: OnceLock<Mutex<Vec<(u64, Observer)>>> = OnceLock::new();
static NEXT: AtomicU64 = AtomicU64::new(1);

/// Dropping the subscription removes the observer and its captured application state.
pub struct Db2DiagnosticSubscription(u64);
pub fn observe_db2_diagnostics(
    observer: impl Fn(&Db2Diagnostic) + Send + Sync + 'static,
) -> Db2DiagnosticSubscription {
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    OBSERVERS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .push((id, Arc::new(observer)));
    Db2DiagnosticSubscription(id)
}
impl Drop for Db2DiagnosticSubscription {
    fn drop(&mut self) {
        if let Some(observers) = OBSERVERS.get() {
            observers
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .retain(|(id, _)| *id != self.0);
        }
    }
}
pub(crate) fn record(operation: &'static str, duration: Duration, success: bool, bytes: u64) {
    let Some(observers) = OBSERVERS.get() else {
        return;
    };
    let snapshot = observers
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
        .map(|(_, observer)| observer.clone())
        .collect::<Vec<_>>();
    let event = Db2Diagnostic {
        operation,
        duration,
        success,
        bytes,
    };
    for observer in snapshot {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(&event)));
    }
}
