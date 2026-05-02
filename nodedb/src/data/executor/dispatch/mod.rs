//! Main execute() dispatch: matches on PhysicalPlan variant and delegates
//! to the appropriate per-engine sub-dispatcher.

pub mod array;
pub mod bitmap;
pub mod columnar;
pub mod crdt;
pub mod document;
pub mod graph;
pub mod kv;
pub mod meta;
pub mod meta_retention;
pub mod query;
pub mod spatial;
pub mod text;
pub mod timeseries;
pub mod vector;

use crate::bridge::envelope::Response;
use crate::bridge::physical_plan::PhysicalPlan;

use super::core_loop::CoreLoop;
use super::task::ExecutionTask;

impl CoreLoop {
    /// Execute a physical plan. Dispatches to the appropriate sub-dispatcher.
    pub(in crate::data::executor) fn execute(&mut self, task: &ExecutionTask) -> Response {
        self.execute_plan(task, task.plan())
    }

    /// Execute an arbitrary physical plan (used for inline sub-plans in multi-way joins).
    pub(in crate::data::executor) fn execute_plan(
        &mut self,
        task: &ExecutionTask,
        plan: &PhysicalPlan,
    ) -> Response {
        let tid = task.request.tenant_id.as_u64();
        match plan {
            PhysicalPlan::Document(op) => self.dispatch_document(task, op),
            PhysicalPlan::Vector(op) => self.dispatch_vector(task, op),
            PhysicalPlan::Crdt(op) => self.dispatch_crdt(task, op),
            PhysicalPlan::Graph(op) => self.dispatch_graph(task, op),
            PhysicalPlan::Text(op) => self.dispatch_text(task, op),
            PhysicalPlan::Array(op) => self.dispatch_array(task, op),
            PhysicalPlan::Query(op) => self.dispatch_query(task, tid, op),
            PhysicalPlan::Meta(op) => self.dispatch_meta(task, tid, op),
            PhysicalPlan::Columnar(op) => self.dispatch_columnar(task, op),
            PhysicalPlan::Timeseries(op) => self.dispatch_timeseries(task, op),
            PhysicalPlan::Spatial(op) => self.dispatch_spatial(task, tid, op),
            PhysicalPlan::Kv(op) => self.dispatch_kv(task, tid, op),

            // ClusterArray variants are handled exclusively on the Control Plane.
            // They must never reach the Data Plane dispatcher.
            PhysicalPlan::ClusterArray(_) => {
                unreachable!("ClusterArray plans must not be dispatched to the Data Plane")
            }
        }
    }
}
