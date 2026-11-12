package store

import (
	"context"

	p "go.temporal.io/server/common/persistence"
)

type batchHistoryAdapter struct{ p.ExecutionStore }

// NewBatchExecutionStore preserves native batches when available and otherwise
// appends history nodes through the standard persistence interface.
func NewBatchExecutionStore(store p.ExecutionStore) BatchExecutionStore {
	if batch, ok := store.(BatchExecutionStore); ok {
		return batch
	}
	return batchHistoryAdapter{store}
}

func (s batchHistoryAdapter) appendAll(
	ctx context.Context,
	requests ...[]*p.InternalAppendHistoryNodesRequest,
) (int, int, error) {
	var nodeCount, treeCount int
	for _, batch := range requests {
		for _, request := range batch {
			if err := s.AppendHistoryNodes(ctx, request); err != nil {
				return 0, 0, err
			}
			nodeCount++
			if request.IsNewBranch {
				treeCount++
			}
		}
	}
	return nodeCount, treeCount, nil
}

func (s batchHistoryAdapter) AppendHistoryNodesForCreateWorkflowExecutionRequest(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (int, int, error) {
	return s.appendAll(ctx, request.NewWorkflowNewEvents)
}

func (s batchHistoryAdapter) AppendHistoryNodesForUpdateWorkflowExecutionRequest(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) (int, int, error) {
	return s.appendAll(ctx, request.NewWorkflowNewEvents, request.UpdateWorkflowNewEvents)
}

func (s batchHistoryAdapter) AppendHistoryNodesForConflictResolveWorkflowExecutionRequest(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) (int, int, error) {
	return s.appendAll(ctx,
		request.CurrentWorkflowEventsNewEvents,
		request.ResetWorkflowEventsNewEvents,
		request.NewWorkflowEventsNewEvents)
}

var _ BatchExecutionStore = batchHistoryAdapter{}
