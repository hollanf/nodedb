use crate::error::Result;
use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};

/// Trait for Raft network transport.
///
/// The `nodedb-cluster` crate implements this over nexar's QUIC/RDMA
/// transport. Tests use an in-memory channel-based implementation.
pub trait RaftTransport: Send + Sync {
    /// Send AppendEntries RPC to a peer and await response.
    fn append_entries(
        &self,
        target: u64,
        req: AppendEntriesRequest,
    ) -> impl std::future::Future<Output = Result<AppendEntriesResponse>> + Send;

    /// Send RequestVote RPC to a peer and await response.
    fn request_vote(
        &self,
        target: u64,
        req: RequestVoteRequest,
    ) -> impl std::future::Future<Output = Result<RequestVoteResponse>> + Send;

    /// Send InstallSnapshot RPC to a peer and await response.
    fn install_snapshot(
        &self,
        target: u64,
        req: InstallSnapshotRequest,
    ) -> impl std::future::Future<Output = Result<InstallSnapshotResponse>> + Send;
}
