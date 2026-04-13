//! `CREATE / DROP / SHOW RLS POLICY` handlers.
//!
//! Supports both legacy static predicates and rich `$auth.*` predicates:
//!
//! ```sql
//! -- Legacy (static):
//! CREATE RLS POLICY <name> ON <collection> FOR <read|write|all>
//!     USING (<field> <op> <value>) [TENANT <id>]
//!
//! -- Rich (with $auth session variables, set ops, composites):
//! CREATE RLS POLICY <name> ON <collection> FOR <read|write|all>
//!     USING (user_id = $auth.id OR $auth.roles CONTAINS 'admin')
//!     [RESTRICTIVE] [TENANT <id>]
//!
//! DROP RLS POLICY <name> ON <collection> [TENANT <id>]
//!
//! SHOW RLS POLICIES [ON <collection>] [TENANT <id>]
//! ```
//!
//! `create_rls_policy` parses the statement into a `ParsedRlsCreate`,
//! validates it, and proposes `CatalogEntry::PutRlsPolicy` through
//! the metadata raft group. Every node's applier installs the
//! resulting `RlsPolicy` into its local `state.rls` cache via
//! `install_replicated_policy`. `drop_rls_policy` uses the matching
//! `DeleteRlsPolicy` variant.

pub mod create;
pub mod drop;
pub mod parse;
pub mod show;

pub use create::create_rls_policy;
pub use drop::drop_rls_policy;
pub use show::show_rls_policies;
