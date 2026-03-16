use crate::bridge::envelope::{PhysicalPlan, Request};
use crate::types::RequestId;

/// An execution task on a Data Plane core.
///
/// Wraps a `Request` envelope with execution state tracking.
/// This type is `!Send` — it lives and dies on a single core.
pub struct ExecutionTask {
    /// The original request envelope.
    pub request: Request,

    /// Current execution state.
    pub state: TaskState,
}

/// Lifecycle states for a Data Plane task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Queued, waiting for core capacity.
    Pending,
    /// Actively executing (io_uring submitted, SIMD running, etc.)
    Running,
    /// Completed successfully — response ready to send back.
    Completed,
    /// Cancelled by Control Plane.
    Cancelled,
    /// Failed with error.
    Failed,
}

impl ExecutionTask {
    pub fn new(request: Request) -> Self {
        Self {
            request,
            state: TaskState::Pending,
        }
    }

    pub fn request_id(&self) -> RequestId {
        self.request.request_id
    }

    pub fn plan(&self) -> &PhysicalPlan {
        &self.request.plan
    }

    pub fn is_expired(&self) -> bool {
        std::time::Instant::now() > self.request.deadline
    }
}
