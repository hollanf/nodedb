use std::path::Path;

use tracing::info;

use super::core::WalManager;

impl WalManager {
    /// Open with encryption key loaded from a file.
    pub fn open_encrypted(
        path: &Path,
        use_direct_io: bool,
        key_path: &Path,
    ) -> crate::Result<Self> {
        let key =
            nodedb_wal::crypto::WalEncryptionKey::from_file(key_path).map_err(crate::Error::Wal)?;
        let ring = nodedb_wal::crypto::KeyRing::new(key);
        let mut mgr = Self::open(path, use_direct_io)?;
        {
            let mut wal = mgr.wal.lock().unwrap_or_else(|p| p.into_inner());
            wal.set_encryption_ring(ring.clone())
                .map_err(crate::Error::Wal)?;
        }
        mgr.encryption_ring = Some(ring);
        info!(key_path = %key_path.display(), "WAL encryption enabled");
        Ok(mgr)
    }

    /// Open with key rotation: current key + previous key for dual-key reads.
    ///
    /// New writes use `current_key_path`. Reads try current first, then previous.
    /// Once all old WAL segments are compacted, remove the previous key.
    pub fn open_encrypted_rotating(
        path: &Path,
        use_direct_io: bool,
        current_key_path: &Path,
        previous_key_path: &Path,
    ) -> crate::Result<Self> {
        let current = nodedb_wal::crypto::WalEncryptionKey::from_file(current_key_path)
            .map_err(crate::Error::Wal)?;
        let previous = nodedb_wal::crypto::WalEncryptionKey::from_file(previous_key_path)
            .map_err(crate::Error::Wal)?;
        let ring = nodedb_wal::crypto::KeyRing::with_previous(current, previous);
        let mut mgr = Self::open(path, use_direct_io)?;
        {
            let mut wal = mgr.wal.lock().unwrap_or_else(|p| p.into_inner());
            wal.set_encryption_ring(ring.clone())
                .map_err(crate::Error::Wal)?;
        }
        mgr.encryption_ring = Some(ring);
        info!(
            current_key = %current_key_path.display(),
            previous_key = %previous_key_path.display(),
            "WAL encryption enabled with key rotation"
        );
        Ok(mgr)
    }

    /// Rotate the encryption key at runtime without downtime.
    ///
    /// The new key becomes the current key for all future writes.
    /// The old current key becomes the previous key for dual-key reads.
    /// Returns an error if the WAL has already written records to the active
    /// segment — in that case, roll to a new segment first.
    pub fn rotate_key(&self, new_key_path: &Path) -> crate::Result<()> {
        let new_key = nodedb_wal::crypto::WalEncryptionKey::from_file(new_key_path)
            .map_err(crate::Error::Wal)?;

        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let new_ring = if let Some(ring) = wal.encryption_ring() {
            nodedb_wal::crypto::KeyRing::with_previous(new_key, ring.current().clone())
        } else {
            nodedb_wal::crypto::KeyRing::new(new_key)
        };

        wal.set_encryption_ring(new_ring)
            .map_err(crate::Error::Wal)?;
        info!(new_key = %new_key_path.display(), "WAL encryption key rotated");
        Ok(())
    }

    /// Get the current encryption key (if configured). Used for backup encryption.
    pub fn encryption_key(&self) -> Option<&nodedb_wal::crypto::WalEncryptionKey> {
        self.encryption_ring.as_ref().map(|r| r.current())
    }

    /// Get the key ring (if configured). Used for dual-key decryption during replay.
    pub fn encryption_ring(&self) -> Option<&nodedb_wal::crypto::KeyRing> {
        self.encryption_ring.as_ref()
    }

    /// Set the encryption key ring. All subsequent records will be encrypted.
    ///
    /// Must be called before any records are written to the active segment.
    pub fn set_encryption_ring(&mut self, ring: nodedb_wal::crypto::KeyRing) -> crate::Result<()> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.set_encryption_ring(ring.clone())
            .map_err(crate::Error::Wal)?;
        self.encryption_ring = Some(ring);
        Ok(())
    }
}
