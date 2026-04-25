//! Replay arm for `RecordType::SurrogateAlloc`: monotonically advance
//! the in-memory `SurrogateRegistry` hwm.

use nodedb_wal::record::SurrogateAllocPayload;

use crate::control::surrogate::SurrogateRegistryHandle;

pub fn apply_surrogate_alloc(
    payload: &[u8],
    registry: &SurrogateRegistryHandle,
) -> crate::Result<()> {
    let parsed = SurrogateAllocPayload::from_bytes(payload).map_err(crate::Error::Wal)?;
    let guard = registry.write().map_err(|_| crate::Error::Internal {
        detail: "surrogate registry lock poisoned during WAL replay".into(),
    })?;
    guard.restore_hwm(parsed.hi)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use nodedb_types::Surrogate;

    use super::*;
    use crate::control::surrogate::SurrogateRegistry;

    #[test]
    fn alloc_advances_registry() {
        let reg: SurrogateRegistryHandle = Arc::new(RwLock::new(SurrogateRegistry::new()));
        let payload = SurrogateAllocPayload::new(42).to_bytes();
        apply_surrogate_alloc(&payload, &reg).unwrap();
        let next = reg.read().unwrap().current_hwm();
        assert_eq!(next, 42);
    }

    #[test]
    fn alloc_does_not_lower_hwm() {
        let reg: SurrogateRegistryHandle =
            Arc::new(RwLock::new(SurrogateRegistry::from_persisted_hwm(100)));
        let payload = SurrogateAllocPayload::new(50).to_bytes();
        let err = apply_surrogate_alloc(&payload, &reg).unwrap_err();
        assert!(matches!(err, crate::Error::Storage { .. }));
    }

    #[test]
    fn alloc_idempotent_at_same_hwm() {
        let reg: SurrogateRegistryHandle =
            Arc::new(RwLock::new(SurrogateRegistry::from_persisted_hwm(7)));
        let payload = SurrogateAllocPayload::new(7).to_bytes();
        apply_surrogate_alloc(&payload, &reg).unwrap();
        let s = reg.read().unwrap().alloc_one().unwrap();
        assert_eq!(s, Surrogate::new(8));
    }
}
