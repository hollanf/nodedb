//! Alert notification dispatch: TOPIC, WEBHOOK, INSERT INTO.
//!
//! Called by the eval loop when a hysteresis transition occurs (Fired or Recovered).
//! Runs on the Event Plane (Send + Sync, Tokio). NEVER does storage I/O directly.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::core::StatementExecutor;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::hysteresis::HysteresisTransition;
use super::types::{AlertDef, AlertEvent, NotifyTarget};

/// Dispatch notifications for an alert transition.
///
/// Sends to all configured targets (TOPIC, WEBHOOK, INSERT INTO).
/// Errors are logged but do not prevent other targets from being notified.
pub async fn dispatch_notifications(
    state: &Arc<SharedState>,
    alert: &AlertDef,
    group_key: &str,
    value: f64,
    transition: HysteresisTransition,
    now_ms: u64,
) {
    let status_str = match transition {
        HysteresisTransition::Fired => "ACTIVE",
        HysteresisTransition::Recovered => "CLEARED",
        HysteresisTransition::NoChange => return, // No notification needed.
    };

    let event = AlertEvent {
        alert_name: alert.name.clone(),
        group_key: group_key.to_string(),
        severity: alert.severity.clone(),
        status: status_str.to_string(),
        value,
        threshold: alert.condition.threshold,
        timestamp_ms: now_ms,
        collection: alert.collection.clone(),
    };

    for target in &alert.notify_targets {
        match target {
            NotifyTarget::Topic { name } => {
                notify_topic(state, alert.tenant_id, name, &event);
            }
            NotifyTarget::Webhook { url } => {
                let timeout = Duration::from_secs(state.tuning.scheduler.webhook_timeout_secs);
                notify_webhook_with_client(state.http_client(), url, &event, timeout).await;
            }
            NotifyTarget::InsertInto { table, columns } => {
                notify_insert(state, alert.tenant_id, &alert.owner, table, columns, &event).await;
            }
        }
    }
}

/// Publish alert event to a CDC topic.
fn notify_topic(state: &SharedState, tenant_id: u64, topic_name: &str, event: &AlertEvent) {
    let payload = match sonic_rs::to_string(event) {
        Ok(p) => p,
        Err(e) => {
            warn!(alert = event.alert_name, error = %e, "failed to serialize alert event");
            return;
        }
    };

    match crate::event::topic::publish::publish_to_topic(state, tenant_id, topic_name, &payload) {
        Ok(seq) => {
            info!(
                alert = event.alert_name,
                topic = topic_name,
                seq,
                status = event.status,
                "alert event published to topic"
            );
        }
        Err(e) => {
            warn!(
                alert = event.alert_name,
                topic = topic_name,
                error = %e,
                "failed to publish alert event to topic"
            );
        }
    }
}

/// HTTP POST alert event to a webhook URL using a shared `reqwest::Client`.
///
/// Retries with exponential backoff (3 attempts, 100ms base). Reusing the
/// client avoids re-building the connection pool / TLS session cache per
/// notification — `CREATE ALERT` rules firing at high rate would otherwise
/// cause a SYN flood and TLS-handshake-dominated CPU.
pub async fn notify_webhook_with_client(
    client: &reqwest::Client,
    url: &str,
    event: &AlertEvent,
    per_request_timeout: Duration,
) {
    let body = match sonic_rs::to_string(event) {
        Ok(b) => b,
        Err(e) => {
            warn!(alert = event.alert_name, error = %e, "failed to serialize alert event");
            return;
        }
    };

    let max_retries = 3u32;
    for attempt in 0..max_retries {
        match client
            .post(url)
            .header("Content-Type", "application/json")
            .timeout(per_request_timeout)
            .body(body.clone())
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                info!(
                    alert = event.alert_name,
                    url,
                    status = event.status,
                    "alert webhook delivered"
                );
                return;
            }
            Ok(resp) if resp.status().is_client_error() && resp.status().as_u16() != 429 => {
                // 4xx (except 429) is permanent failure.
                warn!(
                    alert = event.alert_name,
                    url,
                    status_code = resp.status().as_u16(),
                    "alert webhook permanently rejected"
                );
                return;
            }
            Ok(resp) => {
                warn!(
                    alert = event.alert_name,
                    url,
                    status_code = resp.status().as_u16(),
                    attempt,
                    "alert webhook delivery failed, retrying"
                );
            }
            Err(e) => {
                warn!(
                    alert = event.alert_name,
                    url,
                    attempt,
                    error = %e,
                    "alert webhook delivery error, retrying"
                );
            }
        }

        // Exponential backoff: 100ms, 200ms, 400ms.
        let backoff = Duration::from_millis(100 * (1 << attempt));
        tokio::time::sleep(backoff).await;
    }

    warn!(
        alert = event.alert_name,
        url, "alert webhook delivery failed after all retries"
    );
}

/// INSERT alert event into a history table via StatementExecutor.
async fn notify_insert(
    state: &SharedState,
    tenant_id: u64,
    owner: &str,
    table: &str,
    columns: &[String],
    event: &AlertEvent,
) {
    // Build INSERT statement.
    let col_list = if columns.is_empty() {
        "alert_name, group_key, severity, status, value, threshold, timestamp_ms".to_string()
    } else {
        columns.join(", ")
    };

    let values = format!(
        "'{}', '{}', '{}', '{}', {}, {}, {}",
        escape_sql(&event.alert_name),
        escape_sql(&event.group_key),
        escape_sql(&event.severity),
        escape_sql(&event.status),
        event.value,
        event.threshold,
        event.timestamp_ms,
    );

    let sql = format!("BEGIN INSERT INTO {table} ({col_list}) VALUES ({values}); END");

    let identity = alert_identity(TenantId::new(tenant_id), owner);
    let block = match crate::control::planner::procedural::parse_block(&sql) {
        Ok(b) => b,
        Err(e) => {
            warn!(
                alert = event.alert_name,
                table,
                error = %e,
                "failed to parse INSERT statement for alert history"
            );
            return;
        }
    };

    let executor = StatementExecutor::new(state, identity, TenantId::new(tenant_id), 0);
    let bindings = RowBindings::empty();

    if let Err(e) = executor.execute_block(&block, &bindings).await {
        warn!(
            alert = event.alert_name,
            table,
            error = %e,
            "failed to INSERT alert event into history table"
        );
    } else {
        info!(
            alert = event.alert_name,
            table,
            status = event.status,
            "alert event inserted into history table"
        );
    }
}

/// Create a system identity for alert notification execution.
fn alert_identity(tenant_id: TenantId, owner: &str) -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: 0,
        username: owner.to_string(),
        tenant_id,
        auth_method: AuthMethod::Trust,
        roles: vec![Role::Superuser],
        is_superuser: true,
    }
}

/// SQL string escaping for single-quoted literals.
///
/// Escapes single quotes (SQL standard) and backslashes (PostgreSQL extension).
/// Null bytes are stripped since they are invalid in SQL string literals.
fn escape_sql(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\'', "''")
        .replace('\0', "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_sql_strings() {
        assert_eq!(escape_sql("it's"), "it''s");
        assert_eq!(escape_sql("no quotes"), "no quotes");
        assert_eq!(escape_sql("a'b'c"), "a''b''c");
        assert_eq!(escape_sql("back\\slash"), "back\\\\slash");
        assert_eq!(escape_sql("null\0byte"), "nullbyte");
    }
}
