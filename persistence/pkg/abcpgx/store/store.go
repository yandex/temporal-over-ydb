package store

import (
	"context"

	p "go.temporal.io/server/common/persistence"
)

type HistoryStore interface {
	p.Closeable
	AppendHistoryNodes(ctx context.Context, request *p.InternalAppendHistoryNodesRequest) error
	DeleteHistoryNodes(ctx context.Context, request *p.InternalDeleteHistoryNodesRequest) error
	ForkHistoryBranch(ctx context.Context, request *p.InternalForkHistoryBranchRequest) error
	DeleteHistoryBranch(ctx context.Context, request *p.InternalDeleteHistoryBranchRequest) error
	ReadHistoryBranch(
		ctx context.Context,
		request *p.InternalReadHistoryBranchRequest,
	) (*p.InternalReadHistoryBranchResponse, error)
	GetHistoryTreeContainingBranch(
		ctx context.Context,
		request *p.InternalGetHistoryTreeContainingBranchRequest,
	) (*p.InternalGetHistoryTreeContainingBranchResponse, error)
	GetAllHistoryTreeBranches(
		ctx context.Context,
		request *p.GetAllHistoryTreeBranchesRequest,
	) (*p.InternalGetAllHistoryTreeBranchesResponse, error)
}

// BatchHistoryStore optionally preserves store-native batched history appends.
type BatchHistoryStore interface {
	HistoryStore
	AppendHistoryNodesForCreateWorkflowExecutionRequest(
		ctx context.Context,
		request *p.InternalCreateWorkflowExecutionRequest,
	) (int, int, error)
	AppendHistoryNodesForUpdateWorkflowExecutionRequest(
		ctx context.Context,
		request *p.InternalUpdateWorkflowExecutionRequest,
	) (int, int, error)
	AppendHistoryNodesForConflictResolveWorkflowExecutionRequest(
		ctx context.Context,
		request *p.InternalConflictResolveWorkflowExecutionRequest,
	) (int, int, error)
}

type BatchExecutionStore interface {
	p.ExecutionStore
	BatchHistoryStore
}
