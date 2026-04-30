//! Shared key-file security checks applied before any key material is read.
//!
//! All key-file loaders (File provider, Vault token/ciphertext, AWS KMS
//! ciphertext, WAL master-key) call [`check_key_file`] before opening
//! the file. The function enforces:
//!
//! - **No symlinks** — symlinks are an attack surface (path traversal /
//!   TOCTOU). The check uses `symlink_metadata` so it inspects the link
//!   itself, not its target.
//! - **Regular file** — devices, FIFOs, and directories are rejected.
//! - **Unix mode bits** — any group or world access bit (`mode & 0o077`)
//!   is rejected. Key files must be `0o400` or `0o600`.
//! - **Owner UID** — the file's owner must match the current process UID.
//!   A key file owned by a different user is a misconfiguration.
//!
//! On Windows the permission and owner checks are skipped (Windows uses
//! ACLs, not POSIX mode bits; enforcing a specific ACL shape here would
//! require the `windows-acl` or `winapi` crate, which is out of scope).
//! The symlink check still applies on Windows.

use std::path::Path;

use crate::Result;

/// Verify that `path` is safe to use as a key file.
///
/// Checks (Unix): not a symlink, regular file, mode `& 0o077 == 0`, owner == process UID.
/// Checks (Windows): not a symlink, regular file (no mode/owner checks).
///
/// Returns `Err(Error::Encryption { … })` on any violation; `Ok(())` otherwise.
pub(crate) fn check_key_file(path: &Path) -> Result<()> {
    // --- symlink check (all platforms) -----------------------------------
    // Use `symlink_metadata` so we inspect the link entry itself, not its
    // target. This is the only metadata call that is accurate for symlinks.
    let symlink_meta = std::fs::symlink_metadata(path).map_err(|e| crate::Error::Encryption {
        detail: format!("cannot stat key file {}: {e}", path.display()),
    })?;

    if symlink_meta.file_type().is_symlink() {
        return Err(crate::Error::Encryption {
            detail: format!(
                "key file {} is a symlink, which is not permitted (path traversal / TOCTOU risk)",
                path.display()
            ),
        });
    }

    if !symlink_meta.is_file() {
        return Err(crate::Error::Encryption {
            detail: format!(
                "key file {} is not a regular file (file type: {:?})",
                path.display(),
                symlink_meta.file_type()
            ),
        });
    }

    // --- Unix-only: mode bits + owner UID --------------------------------
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        let mode = symlink_meta.mode();
        // Reject if any group or world bit is set.
        if mode & 0o077 != 0 {
            return Err(crate::Error::Encryption {
                detail: format!(
                    "key file {} has insecure permissions: 0o{:03o} \
                     (must be 0o400 or 0o600 — no group or world access)",
                    path.display(),
                    mode & 0o777,
                ),
            });
        }

        // Reject if the file is owned by a different user.
        let file_uid = symlink_meta.uid();
        // SAFETY: geteuid() is always safe to call; it has no preconditions.
        let process_uid = unsafe { libc::geteuid() };
        if file_uid != process_uid {
            return Err(crate::Error::Encryption {
                detail: format!(
                    "key file {} is owned by UID {} but the process runs as UID {} \
                     — key files must be owned by the server process user",
                    path.display(),
                    file_uid,
                    process_uid,
                ),
            });
        }
    }

    // On Windows: ACL-based permission enforcement is not implemented.
    // Windows filesystems use Access Control Lists (not POSIX mode bits),
    // and adding a correct ACL check requires the windows-acl or winapi crate,
    // which is not a current workspace dependency. Operators running NodeDB on
    // Windows must ensure key files are ACL-restricted manually.
    //
    // The symlink check above still applies on all platforms.

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt as _;

    /// Create a regular file in `dir` with the given Unix mode and 32 bytes of content.
    fn make_file_with_mode(dir: &std::path::Path, name: &str, mode: u32) -> std::path::PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&[0x42u8; 32]).unwrap();
        drop(f);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    #[test]
    #[cfg(unix)]
    fn mode_0o644_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = make_file_with_mode(dir.path(), "key.bin", 0o644);
        let err = check_key_file(&path).unwrap_err();
        let detail = format!("{err:?}");
        // Error message must contain the actual octal mode.
        assert!(
            detail.contains("644") || detail.contains("0o644") || detail.contains("insecure"),
            "expected insecure-permissions error, got: {detail}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn mode_0o640_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = make_file_with_mode(dir.path(), "key.bin", 0o640);
        let err = check_key_file(&path).unwrap_err();
        let detail = format!("{err:?}");
        assert!(
            detail.contains("insecure") || detail.contains("640"),
            "expected insecure-permissions error, got: {detail}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn mode_0o600_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let path = make_file_with_mode(dir.path(), "key.bin", 0o600);
        // Must be owned by the current process — tempfile ensures this.
        check_key_file(&path).expect("0o600 owned by self must be accepted");
    }

    #[test]
    #[cfg(unix)]
    fn mode_0o400_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let path = make_file_with_mode(dir.path(), "key.bin", 0o400);
        check_key_file(&path).expect("0o400 owned by self must be accepted");
    }

    #[test]
    #[cfg(unix)]
    fn symlink_to_0o600_file_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let target = make_file_with_mode(dir.path(), "target.bin", 0o600);
        let link = dir.path().join("link.bin");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let err = check_key_file(&link).unwrap_err();
        let detail = format!("{err:?}");
        assert!(
            detail.contains("symlink"),
            "expected symlink rejection, got: {detail}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn nonexistent_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.bin");
        assert!(check_key_file(&path).is_err());
    }

    // Owner-UID check:
    // Testing the owner-mismatch case in unit tests would require either
    // root privileges (to chown) or a mock of geteuid(), neither of which
    // is feasible in standard cargo tests. The runtime check is always
    // active; the only unverified case is "file owned by a different UID",
    // which requires manual verification or a privileged integration test.
}
