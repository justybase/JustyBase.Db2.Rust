//! Shared process budgets. Reservations precede writes and are released with ownership.
use crate::Db2Error;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};
pub const CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_STREAMED_LOB_BYTES: u64 = 1024 * 1024 * 1024;
pub const MAX_MATERIALIZED_LOB_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_MATERIALIZED_RESULT_BYTES: usize = 32 * 1024 * 1024;

pub struct Db2ResourceBudget {
    limit: u64,
    used: AtomicU64,
}
impl Db2ResourceBudget {
    pub fn new(limit: u64) -> Result<Arc<Self>, Db2Error> {
        if limit == 0 {
            return Err(Db2Error::new(
                crate::Db2ErrorKind::InvalidConfiguration,
                "Resource budget must be positive",
            ));
        }
        Ok(Arc::new(Self {
            limit,
            used: AtomicU64::new(0),
        }))
    }
    pub fn used_bytes(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }
    pub fn reservation(self: &Arc<Self>) -> Db2ResourceReservation {
        Db2ResourceReservation {
            budget: self.clone(),
            bytes: 0,
        }
    }
}
pub struct Db2ResourceReservation {
    budget: Arc<Db2ResourceBudget>,
    bytes: u64,
}
impl Db2ResourceReservation {
    pub fn grow(&mut self, bytes: u64) -> Result<(), Db2Error> {
        self.budget
            .used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.budget.limit)
            })
            .map_err(|_| Db2Error::resource_limit("Db2 process spool quota exceeded"))?;
        self.bytes += bytes;
        Ok(())
    }
}
impl Drop for Db2ResourceReservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
pub(crate) fn spool_budget() -> &'static Arc<Db2ResourceBudget> {
    static BUDGET: OnceLock<Arc<Db2ResourceBudget>> = OnceLock::new();
    BUDGET
        .get_or_init(|| Db2ResourceBudget::new(10 * 1024 * 1024 * 1024).expect("positive constant"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_quota_rejects_before_write_and_releases_exactly_once() {
        let budget = Db2ResourceBudget::new(10).unwrap();
        let mut first = budget.reservation();
        let mut second = budget.reservation();
        first.grow(7).unwrap();
        second.grow(3).unwrap();
        assert_eq!(
            first.grow(1).unwrap_err().kind,
            crate::Db2ErrorKind::ResourceLimit
        );
        assert_eq!(budget.used_bytes(), 10);
        drop(first);
        assert_eq!(budget.used_bytes(), 3);
        second.grow(7).unwrap();
        drop(second);
        assert_eq!(budget.used_bytes(), 0);
    }
}
