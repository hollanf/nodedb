//! Multi-provider JWKS registry: routes JWT tokens to the correct provider,
//! fetches keys on demand, and validates signatures.

use std::sync::Arc;

use tracing::{debug, warn};

use crate::config::auth::{JwtAuthConfig, JwtProviderConfig};
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use crate::control::security::jwt::{JwtClaims, JwtError};
use crate::control::security::util::base64_url_decode;
use crate::types::TenantId;

use super::cache::JwksCache;
use super::key::verify_signature;

/// Multi-provider JWKS registry.
///
/// Manages providers, caches keys, and validates JWT tokens.
/// Lives on the Control Plane (Send + Sync).
pub struct JwksRegistry {
    providers: Vec<JwtProviderConfig>,
    cache: Arc<JwksCache>,
    config: JwtAuthConfig,
    /// Background refresh task handle.
    _refresh_handle: Option<tokio::task::JoinHandle<()>>,
}

impl JwksRegistry {
    /// Create and initialize the registry.
    ///
    /// Fetches JWKS from all providers on startup, loads disk cache as fallback,
    /// and spawns the periodic refresh task.
    pub async fn init(config: JwtAuthConfig) -> Self {
        let cache = Arc::new(JwksCache::new(config.jwks_cache_path.clone()));

        // Load disk cache first (offline fallback).
        cache.load_from_disk();

        // Fetch from all providers (best-effort — failures use disk cache).
        for provider in &config.providers {
            super::fetch::fetch_and_cache(&provider.name, &provider.jwks_url, &cache).await;
        }

        // Spawn periodic refresh.
        let refresh_handle = if !config.providers.is_empty() {
            let pairs: Vec<(String, String)> = config
                .providers
                .iter()
                .map(|p| (p.name.clone(), p.jwks_url.clone()))
                .collect();
            Some(super::fetch::spawn_refresh_task(
                pairs,
                cache.clone(),
                config.jwks_refresh_secs,
            ))
        } else {
            None
        };

        Self {
            providers: config.providers.clone(),
            cache,
            config,
            _refresh_handle: refresh_handle,
        }
    }

    /// Validate a JWT token using JWKS.
    ///
    /// Flow:
    /// 1. Decode header (unverified) → extract `kid` and `alg`.
    /// 2. Decode payload (unverified) → extract `iss`.
    /// 3. Match `iss` to a provider → find cached key by `kid`.
    /// 4. If `kid` not in cache → on-demand re-fetch (rate-limited).
    /// 5. Verify signature using the matched key.
    /// 6. Validate `exp`, `nbf`, `iss`, `aud`.
    /// 7. Return `AuthenticatedIdentity`.
    pub async fn validate(&self, token: &str) -> Result<AuthenticatedIdentity, JwtError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(JwtError::MalformedToken);
        }

        // 1. Decode header.
        let header = decode_jwt_header(parts[0])?;
        let kid = header.kid.as_deref().unwrap_or("");
        let alg = &header.alg;

        // Check algorithm is allowed.
        if alg == "none" {
            return Err(JwtError::UnsupportedAlgorithm);
        }
        if !self.config.allowed_algorithms.is_empty()
            && !self.config.allowed_algorithms.iter().any(|a| a == alg)
        {
            return Err(JwtError::UnsupportedAlgorithm);
        }

        // 2. Decode payload (unverified, for issuer routing).
        let payload_bytes = base64_url_decode(parts[1]).ok_or(JwtError::DecodingError)?;
        let claims: JwtClaims =
            serde_json::from_slice(&payload_bytes).map_err(|_| JwtError::InvalidClaims)?;

        // 3. Find provider by issuer.
        let provider = self.find_provider(&claims.iss)?;

        // 4. Look up key by kid.
        let key = match self.cache.get(&provider.name, kid) {
            Some(k) => k,
            None => {
                // Unknown kid → on-demand re-fetch (rate-limited).
                self.refetch_for_unknown_kid(provider, kid).await?
            }
        };

        // Verify algorithm matches key.
        if key.algorithm != *alg {
            // HMAC-when-RSA-expected attack prevention.
            warn!(
                expected = %key.algorithm,
                actual = %alg,
                kid = %kid,
                "JWT algorithm mismatch — possible algorithm confusion attack"
            );
            return Err(JwtError::UnsupportedAlgorithm);
        }

        // 5. Verify signature.
        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let signature = base64_url_decode(parts[2]).ok_or(JwtError::DecodingError)?;

        if !verify_signature(&key, signing_input.as_bytes(), &signature) {
            return Err(JwtError::InvalidSignature);
        }

        // 6. Validate time claims.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if claims.exp > 0 && now > claims.exp + self.config.clock_skew_secs {
            return Err(JwtError::Expired);
        }
        if claims.nbf > 0 && now + self.config.clock_skew_secs < claims.nbf {
            return Err(JwtError::NotYetValid);
        }

        // Validate issuer.
        if !provider.issuer.is_empty() && claims.iss != provider.issuer {
            return Err(JwtError::InvalidIssuer);
        }

        // Validate audience.
        if !provider.audience.is_empty() && claims.aud != provider.audience {
            return Err(JwtError::InvalidAudience);
        }

        // 7. Build identity.
        let roles: Vec<Role> = claims
            .roles
            .iter()
            .map(|r| r.parse::<Role>().unwrap_or(Role::Custom(r.clone())))
            .collect();

        let username = if claims.sub.is_empty() {
            format!("jwt_user_{}", claims.user_id)
        } else {
            claims.sub.clone()
        };

        debug!(
            username = %username,
            tenant_id = claims.tenant_id,
            provider = %provider.name,
            kid = %kid,
            "JWKS JWT validated"
        );

        Ok(AuthenticatedIdentity {
            user_id: claims.user_id,
            username,
            tenant_id: TenantId::new(claims.tenant_id),
            auth_method: AuthMethod::ApiKey,
            roles,
            is_superuser: claims.is_superuser,
        })
    }

    /// Decode JWT claims without signature verification (for AuthContext building).
    pub fn decode_claims(&self, token: &str) -> Result<JwtClaims, JwtError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(JwtError::MalformedToken);
        }
        let payload_bytes = base64_url_decode(parts[1]).ok_or(JwtError::DecodingError)?;
        serde_json::from_slice(&payload_bytes).map_err(|_| JwtError::InvalidClaims)
    }

    /// Check if any providers are configured.
    pub fn is_configured(&self) -> bool {
        !self.providers.is_empty()
    }

    /// Find the provider matching a token's issuer.
    fn find_provider(&self, issuer: &str) -> Result<&JwtProviderConfig, JwtError> {
        // Exact match on issuer.
        if let Some(p) = self
            .providers
            .iter()
            .find(|p| !p.issuer.is_empty() && p.issuer == issuer)
        {
            return Ok(p);
        }

        // If only one provider is configured, use it regardless of issuer.
        if self.providers.len() == 1 {
            return Ok(&self.providers[0]);
        }

        // Ambiguous: multiple providers, no issuer match.
        Err(JwtError::InvalidIssuer)
    }

    /// On-demand re-fetch for unknown `kid`.
    async fn refetch_for_unknown_kid(
        &self,
        provider: &JwtProviderConfig,
        kid: &str,
    ) -> Result<super::key::VerificationKey, JwtError> {
        if !self
            .cache
            .can_refetch(&provider.name, self.config.jwks_min_refetch_secs)
        {
            warn!(
                provider = %provider.name,
                kid = %kid,
                "unknown kid — re-fetch rate-limited"
            );
            return Err(JwtError::InvalidSignature);
        }

        self.cache.mark_refetch_attempted(&provider.name);
        super::fetch::fetch_and_cache(&provider.name, &provider.jwks_url, &self.cache).await;

        self.cache
            .get(&provider.name, kid)
            .ok_or(JwtError::InvalidSignature)
    }
}

// ── JWT Header Parsing ──────────────────────────────────────────────────

#[derive(Debug, serde::Deserialize)]
struct JwtHeader {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
}

fn decode_jwt_header(encoded: &str) -> Result<JwtHeader, JwtError> {
    let bytes = base64_url_decode(encoded).ok_or(JwtError::DecodingError)?;
    serde_json::from_slice(&bytes).map_err(|_| JwtError::InvalidClaims)
}
