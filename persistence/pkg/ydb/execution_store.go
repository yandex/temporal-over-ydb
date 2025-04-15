package ydb

import (
	"context"
	"strconv"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/cache"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

type (
	ExecutionStore struct {
		metricsHandler     metrics.Handler
		enableDebugMetrics bool
		*HistoryStore
		*MutableStateStore
		*MutableStateTaskStore
	}
)

var _ p.ExecutionStore = (*ExecutionStore)(nil)

func NewExecutionStore(
	client *conn.Client,
	logger log.Logger,
	metricsHandler metrics.Handler,
	taskCacheFactory cache.TaskCacheFactory,
) *ExecutionStore {
	eventsCache := cache.NewEventsCache(taskCacheFactory)
	return &ExecutionStore{
		metricsHandler:        metricsHandler,
		enableDebugMetrics:    true, // TODO
		HistoryStore:          NewHistoryStore(client, logger),
		MutableStateStore:     NewMutableStateStore(client, logger, eventsCache),
		MutableStateTaskStore: NewMutableStateTaskStore(client, logger, eventsCache, taskCacheFactory),
	}
}

func (d *ExecutionStore) CreateWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (resp *p.InternalCreateWorkflowExecutionResponse, err error) {
	defer func() {
		if err != nil {
			err = conn.ConvertToTemporalError("CreateWorkflowExecution", err)
		}
	}()
	startTime := time.Now().UTC()
	nodeCount, treeCount, err := d.HistoryStore.AppendHistoryNodesForCreateWorkflowExecutionRequest(ctx, request)
	if err != nil {
		return nil, err
	}
	handler := d.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceCreateWorkflowExecutionScope))

	if d.enableDebugMetrics {
		latency := time.Since(startTime)
		handler.WithTags(
			metrics.StringTag("tree_count", strconv.Itoa(treeCount)),
			metrics.StringTag("node_count", strconv.Itoa(nodeCount)),
		).Timer("stage1").Record(latency)
	}

	startTime = time.Now().UTC()
	resp, err = d.MutableStateStore.createWorkflowExecution(ctx, request)
	if d.enableDebugMetrics {
		latency := time.Since(startTime)
		handler.Timer("stage2").Record(latency)
	}
	return resp, err
}

func (d *ExecutionStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) (err error) {
	defer func() {
		if err != nil {
			err = conn.ConvertToTemporalError("UpdateWorkflowExecution", err)
		}
	}()
	startTime := time.Now().UTC()
	nodeCount, treeCount, err := d.HistoryStore.AppendHistoryNodesForUpdateWorkflowExecutionRequest(ctx, request)
	if err != nil {
		return err
	}
	handler := d.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceUpdateWorkflowExecutionScope))

	if d.enableDebugMetrics {
		latency := time.Since(startTime)
		handler.WithTags(
			metrics.StringTag("tree_count", strconv.Itoa(treeCount)),
			metrics.StringTag("node_count", strconv.Itoa(nodeCount)),
		).Timer("stage1").Record(latency)
	}

	startTime = time.Now().UTC()
	err = d.MutableStateStore.updateWorkflowExecution(ctx, request)
	if d.enableDebugMetrics {
		latency := time.Since(startTime)
		handler.Timer("stage2").Record(latency)
	}
	return err
}

func (d *ExecutionStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) (err error) {
	defer func() {
		if err != nil {
			err = conn.ConvertToTemporalError("ConflictResolveWorkflowExecution", err)
		}
	}()
	startTime := time.Now().UTC()
	nodeCount, treeCount, err := d.HistoryStore.AppendHistoryNodesForConflictResolveWorkflowExecutionRequest(ctx, request)
	if err != nil {
		return err
	}
	handler := d.metricsHandler.WithTags(metrics.OperationTag(metrics.PersistenceConflictResolveWorkflowExecutionScope))

	if d.enableDebugMetrics {
		latency := time.Since(startTime)
		handler.WithTags(
			metrics.StringTag("tree_count", strconv.Itoa(treeCount)),
			metrics.StringTag("node_count", strconv.Itoa(nodeCount)),
		).Timer("stage1").Record(latency)
	}

	startTime = time.Now().UTC()
	err = d.MutableStateStore.conflictResolveWorkflowExecution(ctx, request)
	if d.enableDebugMetrics {
		latency := time.Since(startTime)
		handler.Timer("stage2").Record(latency)
	}
	return err
}

func (d *ExecutionStore) GetName() string {
	return ydbPersistenceName
}

func (d *ExecutionStore) Close() {

}
