//! `File` key provider — load a raw 32-byte key from a local file.
//!
//! The simplest backend: the file must contain exactly 32 bytes.
//! Suitable for development or environments where an external secrets manager
//! delivers the key file (e.g. a Kubernetes Secret mounted as a file).

use std::path::PathBuf;

use crate::Result;

use super::KeyProvider;

/// Loads the plaintext 32-byte key from a local file.
pub struct FileKeyProvider {
    pub(super) key_path: PathBuf,
}

#[async_trait::async_trait]
impl KeyProvider for FileKeyProvider {
    async fn unwrap_key(&self) -> Result<[u8; 32]> {
        load_key_file(&self.key_path)
    }

    async fn rotate(&self) -> Result<[u8; 32]> {
        // Rotation for file backend means the file has been updated externally;
        // re-read it.
        load_key_file(&self.key_path)
    }
}

fn load_key_file(path: &std::path::Path) -> Result<[u8; 32]> {
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
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[tokio::test]
    async fn file_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.bin");
        let expected = [0x42u8; 32];
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&expected)
            .unwrap();

        let provider = FileKeyProvider { key_path: path };
        let key = provider.unwrap_key().await.unwrap();
        assert_eq!(key, expected);
    }

    #[tokio::test]
    async fn file_wrong_size_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.bin");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&[0u8; 16])
            .unwrap();

        let provider = FileKeyProvider { key_path: path };
        assert!(provider.unwrap_key().await.is_err());
    }

    #[tokio::test]
    async fn file_rotate_rereads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.bin");

        let key1 = [0x11u8; 32];
        std::fs::write(&path, key1).unwrap();
        let provider = FileKeyProvider {
            key_path: path.clone(),
        };
        assert_eq!(provider.unwrap_key().await.unwrap(), key1);

        // Update the file and rotate.
        let key2 = [0x22u8; 32];
        std::fs::write(&path, key2).unwrap();
        let rotated = provider.rotate().await.unwrap();
        assert_eq!(rotated, key2);
        assert_ne!(key1, key2);
    }
}
