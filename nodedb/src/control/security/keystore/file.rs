//! `File` key provider — load a raw 32-byte key from a local file.
//!
//! The simplest backend: the file must contain exactly 32 bytes.
//! Suitable for development or environments where an external secrets manager
//! delivers the key file (e.g. a Kubernetes Secret mounted as a file).
//!
//! # Security checks
//!
//! Before reading any bytes the loader calls [`check_key_file`]:
//! - Symlinks are rejected (TOCTOU / path-traversal risk).
//! - Non-regular files are rejected.
//! - On Unix: group- or world-readable files (`mode & 0o077 != 0`) are rejected.
//! - On Unix: files not owned by the current process UID are rejected.

use std::path::PathBuf;

use zeroize::Zeroizing;

use crate::Result;

use super::KeyProvider;
use super::key_file_security::check_key_file;

/// Loads the plaintext 32-byte key from a local file.
pub struct FileKeyProvider {
    pub(super) key_path: PathBuf,
}

#[async_trait::async_trait]
impl KeyProvider for FileKeyProvider {
    async fn unwrap_key(&self) -> Result<Zeroizing<[u8; 32]>> {
        load_key_file(&self.key_path)
    }

    async fn rotate(&self) -> Result<Zeroizing<[u8; 32]>> {
        // Rotation for file backend means the file has been updated externally;
        // re-read it.
        load_key_file(&self.key_path)
    }
}

fn load_key_file(path: &std::path::Path) -> Result<Zeroizing<[u8; 32]>> {
    check_key_file(path)?;

    let bytes = std::fs::read(path).map_err(|e| crate::Error::Encryption {
        detail: format!("failed to read key file {}: {e}", path.display()),
    })?;
    if bytes.len() != 32 {
        return Err(crate::Error::Encryption {
            detail: format!(
                "key file {} must contain exactly 32 bytes, got {}",
                path.display(),
                bytes.len()
            ),
        });
    }
    let mut key = Zeroizing::new([0u8; 32]);
    key.copy_from_slice(&bytes);
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;

    /// Create a regular file with given mode (Unix) or default perms (non-Unix).
    #[cfg(unix)]
    fn make_key_file(
        dir: &std::path::Path,
        name: &str,
        mode: u32,
        content: &[u8],
    ) -> std::path::PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content).unwrap();
        drop(f);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn file_roundtrip_0o600() {
        let dir = tempfile::tempdir().unwrap();
        let expected = [0x42u8; 32];
        let path = make_key_file(dir.path(), "key.bin", 0o600, &expected);

        let provider = FileKeyProvider { key_path: path };
        let key = provider.unwrap_key().await.unwrap();
        assert_eq!(*key, expected);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn file_roundtrip_0o400() {
        let dir = tempfile::tempdir().unwrap();
        let expected = [0x11u8; 32];
        let path = make_key_file(dir.path(), "key.bin", 0o400, &expected);

        let provider = FileKeyProvider { key_path: path };
        let key = provider.unwrap_key().await.unwrap();
        assert_eq!(*key, expected);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn file_0o644_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = make_key_file(dir.path(), "key.bin", 0o644, &[0x42u8; 32]);

        let provider = FileKeyProvider { key_path: path };
        let err = provider.unwrap_key().await.unwrap_err();
        let detail = format!("{err:?}");
        assert!(
            detail.contains("insecure") || detail.contains("644"),
            "expected insecure-permissions error, got: {detail}"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn symlink_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let target = make_key_file(dir.path(), "target.bin", 0o600, &[0x42u8; 32]);
        let link = dir.path().join("link.bin");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let provider = FileKeyProvider { key_path: link };
        let err = provider.unwrap_key().await.unwrap_err();
        let detail = format!("{err:?}");
        assert!(
            detail.contains("symlink"),
            "expected symlink rejection, got: {detail}"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn file_wrong_size_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = make_key_file(dir.path(), "short.bin", 0o600, &[0u8; 16]);

        let provider = FileKeyProvider { key_path: path };
        assert!(provider.unwrap_key().await.is_err());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn file_rotate_rereads() {
        let dir = tempfile::tempdir().unwrap();
        let key1 = [0x11u8; 32];
        let path = make_key_file(dir.path(), "key.bin", 0o600, &key1);
        let provider = FileKeyProvider {
            key_path: path.clone(),
        };
        assert_eq!(*provider.unwrap_key().await.unwrap(), key1);

        // Update the file and rotate.
        // Must restore write permission temporarily to overwrite it.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let key2 = [0x22u8; 32];
        std::fs::write(&path, key2).unwrap();
        // Set back to 0o600 (write already set).
        let rotated = provider.rotate().await.unwrap();
        assert_eq!(*rotated, key2);
        assert_ne!(key1, key2);
    }

    #[tokio::test]
    async fn zeroizing_return_type_compiles() {
        // Confirms the trait return type is Zeroizing<[u8; 32]> and that
        // it can be dereferenced as &[u8; 32].
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("k.bin");
            std::fs::write(&path, [0xAAu8; 32]).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            let provider = FileKeyProvider { key_path: path };
            let key: Zeroizing<[u8; 32]> = provider.unwrap_key().await.unwrap();
            // Verify we can dereference and read the value.
            assert_eq!((*key)[0], 0xAA);
            // Dropping `key` here triggers the Zeroizing wipe.
        }
    }
}
