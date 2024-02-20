package persistencelite

import (
	"context"

	"github.com/yandex/temporal-over-ydb/persistencelite/conn"
	"github.com/yandex/temporal-over-ydb/xydb"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
)

type (
	ExecutionStore struct {
		baseStore     p.ExecutionStore
		ydb           *xydb.Client
		rqliteFactory conn.RqliteFactory

		logger log.Logger

		*MutableStateStore
		*MutableStateTaskStore
		*HistoryStore
	}
)

var _ p.ExecutionStore = (*ExecutionStore)(nil)

func NewExecutionStore(
	baseStore p.ExecutionStore,
	shardCount int32,
	rqliteFactory conn.RqliteFactory,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *ExecutionStore {
	mss := NewMutableStateStore(rqliteFactory, logger)
	msts := NewMutableStateTaskStore(baseStore, rqliteFactory, logger)
	hs := NewHistoryStore(rqliteFactory, shardCount, logger)
	return &ExecutionStore{
		baseStore:             baseStore,
		rqliteFactory:         rqliteFactory,
		logger:                logger,
		MutableStateStore:     mss,
		MutableStateTaskStore: msts,
		HistoryStore:          hs,
	}
}

func (d *ExecutionStore) CreateWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (*p.InternalCreateWorkflowExecutionResponse, error) {
	return d.MutableStateStore.createWorkflowExecution(ctx, request)
}

func (d *ExecutionStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) error {
	return d.MutableStateStore.updateWorkflowExecution(ctx, request)
}

func (d *ExecutionStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) error {
	return d.MutableStateStore.conflictResolveWorkflowExecution(ctx, request)
}

func (d *ExecutionStore) GetName() string {
	return persistenceName
}

func (d *ExecutionStore) Close() {
}
