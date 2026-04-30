pub mod alter;
pub mod create;
pub mod drop;
pub mod show;

pub use alter::alter_alert;
pub use create::{CreateAlertRequest, create_alert};
pub use drop::drop_alert;
pub use show::{show_alert_status, show_alerts};

/// CRDT collection name for alert rule sync between Origin and Lite.
const ALERT_RULES_CRDT_COLLECTION: &str = "_alert_rules";
