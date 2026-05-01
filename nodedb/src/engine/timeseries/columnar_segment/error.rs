#[derive(Debug)]
pub enum SegmentError {
    Io(String),
    Corrupt(String),
    /// A file was encrypted (`SEGT`) but no KEK was provided.
    MissingKek,
    /// A KEK was provided but the file is plaintext (no `SEGT` preamble).
    UnexpectedPlaintext,
    /// AES-256-GCM encryption failed.
    EncryptionFailed(String),
    /// AES-256-GCM decryption or authentication failed.
    DecryptionFailed(String),
}

impl std::fmt::Display for SegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(msg) => write!(f, "segment I/O error: {msg}"),
            Self::Corrupt(msg) => write!(f, "segment corrupt: {msg}"),
            Self::MissingKek => write!(f, "segment is encrypted (SEGT) but no KEK was provided"),
            Self::UnexpectedPlaintext => write!(
                f,
                "KEK configured but segment file is plaintext (no SEGT preamble)"
            ),
            Self::EncryptionFailed(msg) => write!(f, "segment encryption failed: {msg}"),
            Self::DecryptionFailed(msg) => write!(f, "segment decryption failed: {msg}"),
        }
    }
}

impl std::error::Error for SegmentError {}
