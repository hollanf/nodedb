//! AWS KMS key provider.
//!
//! ## Flow
//!
//! On first init the operator encrypts the plaintext DEK:
//! ```bash
//! aws kms encrypt --key-id <key_id> --plaintext fileb://<32-byte-file> \
//!   --query CiphertextBlob --output text | base64 -d > .wal-key.ciphertext
//! ```
//!
//! At startup NodeDB reads the ciphertext blob from `ciphertext_blob_path`,
//! calls KMS `Decrypt`, and holds the plaintext 32-byte key in mlocked memory.
//!
//! AWS credentials are resolved via the standard chain:
//! - `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars
//! - `~/.aws/credentials` file
//! - EC2/ECS instance metadata
//! - IAM role attached to the instance
//!
//! Key rotation: generate a new random 32-byte DEK, call KMS `Encrypt` to
//! produce a new ciphertext blob, persist it to disk, then return the new key.
//!
//! # Security checks
//!
//! `ciphertext_blob_path` is passed through [`check_key_file`] before being
//! read. Symlinks, world/group-readable files, and files owned by a different
//! UID are rejected.

use std::path::PathBuf;

use aws_sdk_kms::Client as KmsClient;
use aws_sdk_kms::primitives::Blob;
use tracing::info;
use zeroize::Zeroizing;

use crate::Result;

use super::KeyProvider;
use super::key_file_security::check_key_file;

/// AWS KMS key provider.
pub struct AwsKmsProvider {
    key_id: String,
    ciphertext_blob_path: PathBuf,
    client: KmsClient,
}

impl AwsKmsProvider {
    /// Create a new provider. Resolves AWS credentials via the standard chain.
    pub async fn new(
        key_id: String,
        region: String,
        ciphertext_blob_path: PathBuf,
    ) -> Result<Self> {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_kms::config::Region::new(region))
            .load()
            .await;
        let client = KmsClient::new(&config);
        Ok(Self {
            key_id,
            ciphertext_blob_path,
            client,
        })
    }

    fn read_ciphertext_blob(&self) -> Result<Vec<u8>> {
        check_key_file(&self.ciphertext_blob_path)?;
        std::fs::read(&self.ciphertext_blob_path).map_err(|e| crate::Error::Encryption {
            detail: format!(
                "failed to read KMS ciphertext blob from {}: {e}",
                self.ciphertext_blob_path.display()
            ),
        })
    }

    fn write_ciphertext_blob(&self, blob: &[u8]) -> Result<()> {
        std::fs::write(&self.ciphertext_blob_path, blob).map_err(|e| crate::Error::Encryption {
            detail: format!(
                "failed to write KMS ciphertext blob to {}: {e}",
                self.ciphertext_blob_path.display()
            ),
        })
    }
}

#[async_trait::async_trait]
impl KeyProvider for AwsKmsProvider {
    async fn unwrap_key(&self) -> Result<Zeroizing<[u8; 32]>> {
        let ciphertext = self.read_ciphertext_blob()?;

        let resp = self
            .client
            .decrypt()
            .key_id(&self.key_id)
            .ciphertext_blob(Blob::new(ciphertext))
            .send()
            .await
            .map_err(|e| crate::Error::Encryption {
                detail: format!("AWS KMS Decrypt failed: {e}"),
            })?;

        let plaintext_blob = resp.plaintext.ok_or_else(|| crate::Error::Encryption {
            detail: "AWS KMS Decrypt returned no plaintext".into(),
        })?;

        let bytes = plaintext_blob.into_inner();
        if bytes.len() != 32 {
            return Err(crate::Error::Encryption {
                detail: format!(
                    "AWS KMS Decrypt returned {} bytes, expected 32",
                    bytes.len()
                ),
            });
        }

        let mut key = Zeroizing::new([0u8; 32]);
        key.copy_from_slice(&bytes);
        info!(
            key_id = %self.key_id,
            "AWS KMS key decrypted successfully"
        );
        Ok(key)
    }

    async fn rotate(&self) -> Result<Zeroizing<[u8; 32]>> {
        // Generate a new random 32-byte DEK.
        let mut new_dek = Zeroizing::new([0u8; 32]);
        getrandom::fill(new_dek.as_mut()).map_err(|e| crate::Error::Encryption {
            detail: format!("failed to generate new DEK for KMS rotation: {e}"),
        })?;

        // Encrypt the new DEK with KMS.
        let resp = self
            .client
            .encrypt()
            .key_id(&self.key_id)
            .plaintext(Blob::new(new_dek.as_slice().to_vec()))
            .send()
            .await
            .map_err(|e| crate::Error::Encryption {
                detail: format!("AWS KMS Encrypt failed during rotation: {e}"),
            })?;

        let ciphertext_blob = resp
            .ciphertext_blob
            .ok_or_else(|| crate::Error::Encryption {
                detail: "AWS KMS Encrypt returned no ciphertext blob".into(),
            })?;

        // Persist the new ciphertext blob before returning the key.
        self.write_ciphertext_blob(ciphertext_blob.as_ref())?;
        info!(
            key_id = %self.key_id,
            "AWS KMS key rotated successfully"
        );
        Ok(new_dek)
    }
}

#[cfg(test)]
mod tests {
    // AWS KMS tests use a mock HTTP server since the real SDK requires credentials.
    // We verify the happy path and auth error path by intercepting the AWS
    // endpoint via an HTTP mock that speaks the KMS JSON protocol shape.

    use super::*;
    use axum::{Router, routing::post};
    use base64::Engine as _;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;

    fn spawn_mock_kms(port: u16, auth_ok: bool) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let app = Router::new().route(
                "/",
                post(
                    move |headers: axum::http::HeaderMap, _body: axum::body::Bytes| async move {
                        // Check for X-Amz-Target to distinguish Decrypt vs Encrypt.
                        let target = headers
                            .get("X-Amz-Target")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_owned();

                        if !auth_ok {
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                axum::Json(serde_json::json!({
                                    "__type": "AccessDeniedException",
                                    "message": "Access denied"
                                })),
                            );
                        }

                        if target.contains("Decrypt") {
                            let plaintext_b64 =
                                base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);
                            (
                                axum::http::StatusCode::OK,
                                axum::Json(serde_json::json!({
                                    "KeyId": "arn:aws:kms:us-east-1:123:key/fake",
                                    "Plaintext": plaintext_b64,
                                })),
                            )
                        } else {
                            // Encrypt response.
                            let ct_b64 =
                                base64::engine::general_purpose::STANDARD.encode([0xABu8; 64]);
                            (
                                axum::http::StatusCode::OK,
                                axum::Json(serde_json::json!({
                                    "KeyId": "arn:aws:kms:us-east-1:123:key/fake",
                                    "CiphertextBlob": ct_b64,
                                })),
                            )
                        }
                    },
                ),
            );
            let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
                .await
                .unwrap();
            axum::serve(listener, app).await.unwrap();
        })
    }

    async fn make_provider_with_endpoint(
        port: u16,
        ciphertext_blob_path: PathBuf,
    ) -> AwsKmsProvider {
        // Override the KMS endpoint to point at the mock.
        let endpoint = format!("http://127.0.0.1:{port}");
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_kms::config::Region::new("us-east-1"))
            .endpoint_url(&endpoint)
            // Provide dummy credentials so the SDK doesn't fail credential resolution.
            .credentials_provider(aws_sdk_kms::config::Credentials::new(
                "AKID", "SECRET", None, None, "test",
            ))
            .load()
            .await;
        let client = KmsClient::new(&config);
        AwsKmsProvider {
            key_id: "arn:aws:kms:us-east-1:123:key/fake".into(),
            ciphertext_blob_path,
            client,
        }
    }

    /// Create a file with secure Unix permissions (0o600).
    #[cfg(unix)]
    fn write_secure(path: &std::path::Path, content: &[u8]) {
        use std::io::Write as _;
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(content).unwrap();
        drop(f);
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn kms_decrypt_happy_path() {
        let port = 18301u16;
        let _srv = spawn_mock_kms(port, true);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let dir = tempfile::tempdir().unwrap();
        let blob_path = dir.path().join("ct.bin");
        write_secure(&blob_path, &[0xFFu8; 64]);

        let provider = make_provider_with_endpoint(port, blob_path).await;
        let key = provider.unwrap_key().await.unwrap();
        assert_eq!(*key, [0x42u8; 32]);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn kms_auth_error_path() {
        let port = 18302u16;
        let _srv = spawn_mock_kms(port, false);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let dir = tempfile::tempdir().unwrap();
        let blob_path = dir.path().join("ct.bin");
        write_secure(&blob_path, &[0xFFu8; 64]);

        let provider = make_provider_with_endpoint(port, blob_path).await;
        let err = provider.unwrap_key().await.unwrap_err();
        let detail = format!("{err:?}");
        assert!(
            detail.contains("KMS") || detail.contains("Access") || detail.contains("decrypt"),
            "expected KMS error, got: {detail}"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn kms_insecure_ciphertext_blob_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let blob_path = dir.path().join("ct.bin");

        use std::io::Write as _;
        // Insecure permissions (0o644).
        let mut f = std::fs::File::create(&blob_path).unwrap();
        f.write_all(&[0xFFu8; 64]).unwrap();
        drop(f);
        std::fs::set_permissions(&blob_path, std::fs::Permissions::from_mode(0o644)).unwrap();

        // Use a dummy port — the check fires before any network call.
        let provider = make_provider_with_endpoint(19999, blob_path).await;
        let err = provider.unwrap_key().await.unwrap_err();
        let detail = format!("{err:?}");
        assert!(
            detail.contains("insecure") || detail.contains("644"),
            "expected insecure-permissions error, got: {detail}"
        );
    }
}
