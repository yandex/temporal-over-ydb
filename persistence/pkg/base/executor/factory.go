package executor

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/tasks"
)

type CurrentRunIDAndLastWriteVersion struct {
	LastWriteVersion int64
	CurrentRunID     primitives.UUID
}

type Query[S any] interface {
	ToStmt(prefix string, conditionID primitives.UUID) S
}

type TransactionFactory interface {
	NewTransaction(shardID int32) Transaction
}

type EventsCache interface {
	Invalidate(ctx context.Context, shardID int32) error
	Put(ctx context.Context, shardID int32, tasks ...map[tasks.Category][]p.InternalHistoryTask) error
}

type Transaction interface {
	AssertShard(lockForUpdate bool, rangeIDEqualTo int64)
	AssertCurrentWorkflow(
		lockForUpdate bool, namespaceID primitives.UUID, workflowID string,
		currentRunIDNotEqualTo primitives.UUID, currentRunIDEqualTo primitives.UUID,
		mustNotExist bool, currentRunIDAndLastWriteVersionEqualTo *CurrentRunIDAndLastWriteVersion,
	)
	AssertWorkflowExecution(
		lockForUpdate bool, namespaceID primitives.UUID, workflowID string, runID primitives.UUID,
		recordVersionEqualTo *int64, mustNotExist bool,
	)

	UpsertShard(rangeID int64, shardInfo *commonpb.DataBlob)
	UpsertCurrentWorkflow(
		namespaceID primitives.UUID, workflowID string, currentRunID primitives.UUID,
		executionStateBlob *commonpb.DataBlob, lastWriteVersion int64, state enums.WorkflowExecutionState)
	HandleWorkflowSnapshot(workflowSnapshot *p.InternalWorkflowSnapshot)
	HandleWorkflowMutation(workflowMutation *p.InternalWorkflowMutation)
	InsertHistoryTasks(insertTasks map[tasks.Category][]p.InternalHistoryTask)
	// UpsertHistoryTasks is used for migration purposes
	UpsertHistoryTasks(tasks.Category, []p.InternalHistoryTask) error

	DeleteBufferedEvents(namespaceID primitives.UUID, workflowID string, runID primitives.UUID)
	DeleteStateItems(namespaceID primitives.UUID, workflowID string, runID primitives.UUID)

	Execute(ctx context.Context) error
}
