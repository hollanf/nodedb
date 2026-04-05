//! Schedule metadata operations for the system catalog.

use super::types::{SCHEDULES, SystemCatalog, catalog_err};
use crate::event::scheduler::ScheduleDef;

impl SystemCatalog {
    /// Store a schedule definition.
    ///
    /// Key format: `"{tenant_id}:{schedule_name}"`.
    pub fn put_schedule(&self, def: &ScheduleDef) -> crate::Result<()> {
        let key = schedule_key(def.tenant_id, &def.name);
        let bytes =
            zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize schedule", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(SCHEDULES)
                .map_err(|e| catalog_err("open schedules", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert schedule", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Delete a schedule. Returns true if it existed.
    pub fn delete_schedule(&self, tenant_id: u32, name: &str) -> crate::Result<bool> {
        let key = schedule_key(tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(SCHEDULES)
                .map_err(|e| catalog_err("open schedules", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete schedule", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load all schedules (all tenants).
    pub fn load_all_schedules(&self) -> crate::Result<Vec<ScheduleDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SCHEDULES)
            .map_err(|e| catalog_err("open schedules", e))?;

        let mut schedules = Vec::new();
        let mut range = table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range schedules", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(def) = zerompk::from_msgpack::<ScheduleDef>(value.value()) {
                schedules.push(def);
            }
        }
        Ok(schedules)
    }
}

fn schedule_key(tenant_id: u32, name: &str) -> String {
    format!("{tenant_id}:{name}")
}

#[cfg(test)]
mod tests {
    use crate::control::security::catalog::types::SystemCatalog;
    use crate::event::scheduler::types::*;

    fn make_catalog() -> SystemCatalog {
        let dir = tempfile::tempdir().unwrap();
        SystemCatalog::open(&dir.path().join("system.redb")).unwrap()
    }

    #[test]
    fn put_and_load() {
        let cat = make_catalog();
        let def = ScheduleDef {
            tenant_id: 1,
            name: "cleanup".into(),
            cron_expr: "0 0 * * *".into(),
            body_sql: "BEGIN DELETE FROM old_data; END".into(),
            scope: ScheduleScope::Normal,
            missed_policy: MissedPolicy::Skip,
            allow_overlap: true,
            enabled: true,
            target_collection: None,
            owner: "admin".into(),
            created_at: 1000,
        };
        cat.put_schedule(&def).unwrap();

        let all = cat.load_all_schedules().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "cleanup");
        assert_eq!(all[0].cron_expr, "0 0 * * *");
    }

    #[test]
    fn delete_schedule() {
        let cat = make_catalog();
        let def = ScheduleDef {
            tenant_id: 1,
            name: "s1".into(),
            cron_expr: "* * * * *".into(),
            body_sql: "BEGIN RETURN; END".into(),
            scope: ScheduleScope::Local,
            missed_policy: MissedPolicy::default(),
            allow_overlap: false,
            enabled: true,
            target_collection: None,
            owner: "admin".into(),
            created_at: 0,
        };
        cat.put_schedule(&def).unwrap();
        assert!(cat.delete_schedule(1, "s1").unwrap());
        assert!(!cat.delete_schedule(1, "s1").unwrap());
    }
}
