package persistence

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
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
	client *xydb.Client,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *ExecutionStore {
	return &ExecutionStore{
		metricsHandler:        metricsHandler,
		enableDebugMetrics:    true, // TODO
		HistoryStore:          NewHistoryStore(client, logger),
		MutableStateStore:     NewMutableStateStore(client, logger),
		MutableStateTaskStore: NewMutableStateTaskStore(client, logger),
	}
}

var treeRowDecl = fmt.Sprintf("DECLARE $%s AS %s;", "tree_row_to_add",
	types.Struct(
		types.StructField("tree_id", types.TypeUTF8),
		types.StructField("branch_id", types.TypeUTF8),
		types.StructField("branch", types.TypeBytes),
		types.StructField("branch_encoding", types.TypeUTF8),
	),
)

var nodeRowDecl = fmt.Sprintf("DECLARE $%s AS %s;", "node_row_to_add",
	types.Struct(
		types.StructField("tree_id", types.TypeUTF8),
		types.StructField("branch_id", types.TypeUTF8),
		types.StructField("node_id", types.TypeInt64),
		types.StructField("txn_id", types.TypeInt64),
		types.StructField("prev_txn_id", types.TypeInt64),
		types.StructField("data", types.TypeBytes),
		types.StructField("data_encoding", types.TypeUTF8),
	),
)

var upsertHistoryNodeQ = nodeRowDecl + `
UPSERT INTO history_node (tree_id, branch_id, node_id, txn_id, prev_txn_id, data, data_encoding)
VALUES ($node_row_to_add.tree_id, $node_row_to_add.branch_id, $node_row_to_add.node_id, $node_row_to_add.txn_id, $node_row_to_add.prev_txn_id, $node_row_to_add.data, $node_row_to_add.data_encoding);
`
var upsertHistoryNodeAndTreeQ = treeRowDecl + "\n" + nodeRowDecl + `
UPSERT INTO history_tree (tree_id, branch_id, branch, branch_encoding)
VALUES ($tree_row_to_add.tree_id, $tree_row_to_add.branch_id, $tree_row_to_add.branch, $tree_row_to_add.branch_encoding);

UPSERT INTO history_node (tree_id, branch_id, node_id, txn_id, prev_txn_id, data, data_encoding)
VALUES ($node_row_to_add.tree_id, $node_row_to_add.branch_id, $node_row_to_add.node_id, $node_row_to_add.txn_id, $node_row_to_add.prev_txn_id, $node_row_to_add.data, $node_row_to_add.data_encoding);
`

func upsertHistory(ctx context.Context, client *xydb.Client, nodeRow types.Value, treeRow types.Value) error {
	var err error
	if treeRow != nil {
		err = client.Write(ctx, client.AddQueryPrefix(upsertHistoryNodeAndTreeQ), table.NewQueryParameters(
			table.ValueParam("$tree_row_to_add", treeRow),
			table.ValueParam("$node_row_to_add", nodeRow),
		))
	} else {
		err = client.Write(ctx, client.AddQueryPrefix(upsertHistoryNodeQ), table.NewQueryParameters(
			table.ValueParam("$node_row_to_add", nodeRow),
		))
	}
	return xydb.ConvertToTemporalError("AppendHistoryNodes", err)
}

func (d *ExecutionStore) CreateWorkflowExecution(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (*p.InternalCreateWorkflowExecutionResponse, error) {
	startTime := time.Now().UTC()
	var nodeCount, treeCount int
	for _, req := range request.NewWorkflowNewEvents {
		n, t := d.HistoryStore.prepareAppendHistoryNodesRows(req)
		if err := upsertHistory(ctx, d.Client, n, t); err != nil {
			return nil, err
		}
		nodeCount++
		if t != nil {
			treeCount++
		}
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
	resp, err := d.MutableStateStore.createWorkflowExecution(ctx, request)
	if d.enableDebugMetrics {
		latency := time.Since(startTime)
		handler.Timer("stage2").Record(latency)
	}
	return resp, err
}

func (d *ExecutionStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) error {
	startTime := time.Now().UTC()
	var nodeCount, treeCount int
	for _, req := range request.NewWorkflowNewEvents {
		n, t := d.HistoryStore.prepareAppendHistoryNodesRows(req)
		if err := upsertHistory(ctx, d.Client, n, t); err != nil {
			return err
		}
		nodeCount++
		if t != nil {
			treeCount++
		}
	}
	for _, req := range request.UpdateWorkflowNewEvents {
		n, t := d.HistoryStore.prepareAppendHistoryNodesRows(req)
		if err := upsertHistory(ctx, d.Client, n, t); err != nil {
			return err
		}
		nodeCount++
		if t != nil {
			treeCount++
		}
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
	err := d.MutableStateStore.updateWorkflowExecution(ctx, request)
	if d.enableDebugMetrics {
		latency := time.Since(startTime)
		handler.Timer("stage2").Record(latency)
	}
	return err
}

func (d *ExecutionStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) error {
	startTime := time.Now().UTC()
	var nodeCount, treeCount int
	for _, req := range request.CurrentWorkflowEventsNewEvents {
		n, t := d.HistoryStore.prepareAppendHistoryNodesRows(req)
		if err := upsertHistory(ctx, d.Client, n, t); err != nil {
			return err
		}
		nodeCount++
		if t != nil {
			treeCount++
		}
	}
	for _, req := range request.ResetWorkflowEventsNewEvents {
		n, t := d.HistoryStore.prepareAppendHistoryNodesRows(req)
		if err := upsertHistory(ctx, d.Client, n, t); err != nil {
			return err
		}
		nodeCount++
		if t != nil {
			treeCount++
		}
	}
	for _, req := range request.NewWorkflowEventsNewEvents {
		n, t := d.HistoryStore.prepareAppendHistoryNodesRows(req)
		if err := upsertHistory(ctx, d.Client, n, t); err != nil {
			return err
		}
		nodeCount++
		if t != nil {
			treeCount++
		}
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
	err := d.MutableStateStore.conflictResolveWorkflowExecution(ctx, request)
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
