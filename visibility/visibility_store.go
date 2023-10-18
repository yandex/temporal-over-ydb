package visibility

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	ydbpersistence "github.com/yandex/temporal-over-ydb/persistence"
	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/standard"
	"go.temporal.io/server/common/resolver"
	"golang.org/x/xerrors"
)

const (
	YDBPersistenceName = "ydb"
)

type visibilityStore struct {
	client *xydb.Client
}

func NewVisibilityStore(
	cfg config.CustomDatastoreConfig,
	r resolver.ServiceResolver,
	logger log.Logger,
) (store.VisibilityStore, error) {
	client, err := ydbpersistence.NewYDBClientFromConfig(cfg, r, logger)
	if err != nil {
		return nil, xerrors.Errorf("failed to create ydb client: %w", err)
	}
	return standard.NewVisibilityStore(&visibilityStore{
		client: client,
	}), nil
}

func (v *visibilityStore) GetName() string {
	return YDBPersistenceName
}

func (v *visibilityStore) GetIndexName() string {
	// GetIndexName is used to get cluster metadata, which in versions < v1.20
	// were stored in an empty string key.
	return ""
}

func (v *visibilityStore) Close() {
}

func (v *visibilityStore) ValidateCustomSearchAttributes(searchAttributes map[string]any) (map[string]any, error) {
	// TODO implement me
	return searchAttributes, nil
}

func (v *visibilityStore) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionStartedRequest,
) error {
	template := v.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $workflow_id AS utf8;
DECLARE $run_id AS utf8;
DECLARE $start_time AS timestamp;
DECLARE $execution_time AS Optional<timestamp>;
DECLARE $workflow_type_name AS utf8;
DECLARE $status AS int32;
DECLARE $memo AS string;
DECLARE $encoding AS utf8;
DECLARE $task_queue AS utf8;

UPSERT INTO executions_visibility (namespace_id, workflow_id, run_id, start_time, execution_time, workflow_type_name, status, memo, memo_encoding, task_queue)
VALUES ($namespace_id, $workflow_id, $run_id, $start_time, $execution_time, $workflow_type_name, $status, $memo, $encoding, $task_queue);
`)
	params := table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$workflow_id", types.UTF8Value(request.WorkflowID)),
		table.ValueParam("$run_id", types.UTF8Value(request.RunID)),
		table.ValueParam("$start_time", types.TimestampValueFromTime(ydbpersistence.ToYDBDateTime(request.StartTime))),
		table.ValueParam("$workflow_type_name", types.UTF8Value(request.WorkflowTypeName)),
		table.ValueParam("$status", types.Int32Value(int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING))),
		table.ValueParam("$memo", types.BytesValue(request.Memo.Data)),
		table.ValueParam("$encoding", types.UTF8Value(request.Memo.EncodingType.String())),
		table.ValueParam("$task_queue", types.UTF8Value(request.TaskQueue)),
	)
	if request.ExecutionTime.IsZero() {
		params.Add(
			table.ValueParam("$execution_time", types.NullValue(types.TypeTimestamp)),
		)
	} else {
		params.Add(
			table.ValueParam("$execution_time", types.OptionalValue(types.TimestampValueFromTime(ydbpersistence.ToYDBDateTime(request.ExecutionTime)))),
		)
	}

	err := v.client.Write(ctx, template, params)
	return xydb.ConvertToTemporalError("RecordWorkflowExecutionStarted", err)
}

func (v *visibilityStore) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionClosedRequest,
) error {
	template := v.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $workflow_id AS utf8;
DECLARE $run_id AS utf8;
DECLARE $start_time AS timestamp;
DECLARE $execution_time AS Optional<timestamp>;
DECLARE $workflow_type_name AS utf8;
DECLARE $close_time AS timestamp;
DECLARE $status as int32;
DECLARE $history_length as int64;
DECLARE $memo AS string;
DECLARE $encoding AS utf8;
DECLARE $task_queue AS utf8;

UPSERT INTO executions_visibility (namespace_id, workflow_id, run_id, start_time, execution_time, workflow_type_name, close_time, status, history_length, memo, memo_encoding, task_queue)
VALUES ($namespace_id, $workflow_id, $run_id, $start_time, $execution_time, $workflow_type_name, $close_time, $status, $history_length, $memo, $encoding, $task_queue);
`)
	params := table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$workflow_id", types.UTF8Value(request.WorkflowID)),
		table.ValueParam("$run_id", types.UTF8Value(request.RunID)),
		table.ValueParam("$start_time", types.TimestampValueFromTime(ydbpersistence.ToYDBDateTime(request.StartTime))),
		table.ValueParam("$workflow_type_name", types.UTF8Value(request.WorkflowTypeName)),
		table.ValueParam("$close_time", types.TimestampValueFromTime(ydbpersistence.ToYDBDateTime(request.CloseTime))),
		table.ValueParam("$status", types.Int32Value(int32(request.Status))),
		table.ValueParam("$history_length", types.Int64Value(request.HistoryLength)),
		table.ValueParam("$memo", types.BytesValue(request.Memo.Data)),
		table.ValueParam("$encoding", types.UTF8Value(request.Memo.EncodingType.String())),
		table.ValueParam("$task_queue", types.UTF8Value(request.TaskQueue)),
	)
	if !request.ExecutionTime.IsZero() {
		params.Add(
			table.ValueParam("$execution_time", types.OptionalValue(types.TimestampValueFromTime(ydbpersistence.ToYDBDateTime(request.ExecutionTime)))),
		)
	} else {
		params.Add(
			table.ValueParam("$execution_time", types.NullValue(types.TypeTimestamp)),
		)

	}

	err := v.client.Write(ctx, template, params)
	if err != nil {
		return xydb.ConvertToTemporalError("RecordWorkflowExecutionClosed", err)
	}
	return err
}

func (v *visibilityStore) UpsertWorkflowExecution(
	_ context.Context,
	_ *store.InternalUpsertWorkflowExecutionRequest,
) error {
	return nil
}

type listOpenWorkflowExecutionsQuery struct {
	pageToken        []byte
	namespaceID      string
	pageSize         int
	startTimeGTE     time.Time
	startTimeLTE     time.Time
	workflowTypeName string
	workflowID       string
}

func (v *visibilityStore) listOpenWorkflowExecutions(
	ctx context.Context,
	q *listOpenWorkflowExecutionsQuery,
) ([]*store.InternalWorkflowExecutionInfo, []byte, error) {
	var token *visibilityPageToken
	var err error
	if len(q.pageToken) > 0 {
		token, err = v.deserializePageToken(q.pageToken)
		if err != nil {
			return nil, nil, err
		}
	} else {
		token = &visibilityPageToken{Time: q.startTimeLTE, RunID: ""}
	}

	params := table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(q.namespaceID)),
		table.ValueParam("$page_size", types.Int32Value(int32(q.pageSize))),
	)
	var declares []string
	var conds []string

	if !q.startTimeGTE.IsZero() {
		declares = append(declares, "DECLARE $start_time_gte AS timestamp;")
		params.Add(table.ValueParam("$start_time_gte", types.TimestampValueFromTime(ydbpersistence.ToYDBDateTime(q.startTimeGTE))))
		conds = append(conds, "start_time >= $start_time_gte")
	}
	if !token.Time.IsZero() {
		declares = append(declares, "DECLARE $start_time_lte AS timestamp;")
		params.Add(table.ValueParam("$start_time_lte", types.TimestampValueFromTime(ydbpersistence.ToYDBDateTime(token.Time))))
		if token.RunID != "" {
			declares = append(declares, "DECLARE $run_id_gt AS utf8;")
			params.Add(table.ValueParam("$run_id_gt", types.UTF8Value(token.RunID)))
			conds = append(conds, "((run_id > $run_id_gt and start_time = $start_time_lte) OR (start_time < $start_time_lte))")
		} else {
			conds = append(conds, "start_time <= $start_time_lte")
		}
	}
	if q.workflowTypeName != "" {
		declares = append(declares, "DECLARE $workflow_type_name AS utf8;")
		params.Add(table.ValueParam("$workflow_type_name", types.UTF8Value(q.workflowTypeName)))
		conds = append(conds, "workflow_type_name = $workflow_type_name")
	}
	if q.workflowID != "" {
		declares = append(declares, "DECLARE $workflow_id AS utf8;")
		params.Add(table.ValueParam("$workflow_id", types.UTF8Value(q.workflowID)))
		conds = append(conds, "workflow_id = $workflow_id")
	}
	template := v.client.AddQueryPrefix(strings.Join(declares, "\n") + `
DECLARE $namespace_id AS utf8;
DECLARE $status AS int32;
DECLARE $page_size AS int32;

SELECT workflow_id, run_id, start_time, execution_time, workflow_type_name, status, memo, memo_encoding, task_queue
FROM executions_visibility
WHERE status = 1
AND namespace_id = $namespace_id
AND ` + strings.Join(conds, " AND ") + `
ORDER BY start_time DESC, run_id
LIMIT $page_size;
`)
	res, err := v.client.Do(ctx, template, table.SerializableReadWriteTxControl(table.CommitTx()), params, table.WithIdempotent())
	if err != nil {
		return nil, nil, err
	}
	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, nil, err
	}

	executions := make([]*store.InternalWorkflowExecutionInfo, 0, q.pageSize)
	for res.NextRow() {
		record, err := scanOpenWorkflowExecutionRecord(res)
		if err != nil {
			return nil, nil, err
		}
		executions = append(executions, record)
	}

	var npt []byte
	if len(executions) == q.pageSize {
		lastRow := executions[len(executions)-1]
		npt, err = v.serializePageToken(&visibilityPageToken{
			Time:  lastRow.StartTime,
			RunID: lastRow.RunID,
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return executions, npt, nil
}

type listClosedWorkflowExecutionsQuery struct {
	pageToken        []byte
	namespaceID      string
	pageSize         int
	closeTimeGTE     time.Time
	closeTimeLTE     time.Time
	workflowTypeName string
	workflowID       string
	workflowStatus   enumspb.WorkflowExecutionStatus
}

func (v *visibilityStore) listClosedWorkflowExecutions(
	ctx context.Context,
	q *listClosedWorkflowExecutionsQuery,
) ([]*store.InternalWorkflowExecutionInfo, []byte, error) {
	var token *visibilityPageToken
	var err error
	if len(q.pageToken) > 0 {
		token, err = v.deserializePageToken(q.pageToken)
		if err != nil {
			return nil, nil, err
		}
	} else {
		token = &visibilityPageToken{Time: q.closeTimeLTE, RunID: ""}
	}

	params := table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(q.namespaceID)),
		table.ValueParam("$page_size", types.Int32Value(int32(q.pageSize))),
	)
	var declares []string
	var conds []string

	if !q.closeTimeGTE.IsZero() {
		declares = append(declares, "DECLARE $close_time_gte AS timestamp;")
		params.Add(table.ValueParam("$close_time_gte", types.TimestampValueFromTime(ydbpersistence.ToYDBDateTime(q.closeTimeGTE))))
		conds = append(conds, "close_time >= $close_time_gte")
	}
	if !token.Time.IsZero() {
		declares = append(declares, "DECLARE $close_time_lte AS timestamp;")
		params.Add(table.ValueParam("$close_time_lte", types.TimestampValueFromTime(ydbpersistence.ToYDBDateTime(token.Time))))
		if token.RunID != "" {
			declares = append(declares, "DECLARE $run_id_gt AS utf8;")
			params.Add(table.ValueParam("$run_id_gt", types.UTF8Value(token.RunID)))
			conds = append(conds, "((run_id > $run_id_gt and close_time = $close_time_lte) OR (close_time < $close_time_lte))")
		} else {
			conds = append(conds, "close_time <= $close_time_lte")
		}
	}
	if q.workflowTypeName != "" {
		declares = append(declares, "DECLARE $workflow_type_name AS utf8;")
		params.Add(table.ValueParam("$workflow_type_name", types.UTF8Value(q.workflowTypeName)))
		conds = append(conds, "workflow_type_name = $workflow_type_name")
	}
	if q.workflowID != "" {
		declares = append(declares, "DECLARE $workflow_id AS utf8;")
		params.Add(table.ValueParam("$workflow_id", types.UTF8Value(q.workflowID)))
		conds = append(conds, "workflow_id = $workflow_id")
	}
	if q.workflowStatus != 0 {
		declares = append(declares, "DECLARE $status AS int32;")
		params.Add(table.ValueParam("$status", types.Int32Value(int32(q.workflowStatus))))
		conds = append(conds, "status = $status")
	}
	template := v.client.AddQueryPrefix(strings.Join(declares, "\n") + `
DECLARE $namespace_id AS utf8;
DECLARE $page_size AS int32;

SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, memo_encoding, task_queue
FROM executions_visibility
WHERE status != 1
AND namespace_id = $namespace_id
AND ` + strings.Join(conds, " AND ") + `
ORDER BY close_time DESC, run_id
LIMIT $page_size;
`)
	res, err := v.client.Do(ctx, template, table.SerializableReadWriteTxControl(table.CommitTx()), params, table.WithIdempotent())
	if err != nil {
		return nil, nil, err
	}
	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, nil, err
	}

	executions := make([]*store.InternalWorkflowExecutionInfo, 0, q.pageSize)
	for res.NextRow() {
		record, err := scanClosedWorkflowExecutionRecord(res)
		if err != nil {
			return nil, nil, err
		}
		executions = append(executions, record)
	}

	var npt []byte
	if len(executions) == q.pageSize {
		lastRow := executions[len(executions)-1]
		npt, err = v.serializePageToken(&visibilityPageToken{
			Time:  lastRow.CloseTime,
			RunID: lastRow.RunID,
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return executions, npt, nil
}
func (v *visibilityStore) ListOpenWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	executions, npt, err := v.listOpenWorkflowExecutions(ctx, &listOpenWorkflowExecutionsQuery{
		pageToken:    request.NextPageToken,
		namespaceID:  request.NamespaceID.String(),
		pageSize:     request.PageSize,
		startTimeGTE: request.EarliestStartTime,
		startTimeLTE: request.LatestStartTime,
	})
	if err != nil {
		return nil, xydb.ConvertToTemporalError("ListOpenWorkflowExecutions", err)
	}
	return &store.InternalListWorkflowExecutionsResponse{
		Executions:    executions,
		NextPageToken: npt,
	}, nil
}

func (v *visibilityStore) ListOpenWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	executions, npt, err := v.listOpenWorkflowExecutions(ctx, &listOpenWorkflowExecutionsQuery{
		pageToken:        request.NextPageToken,
		namespaceID:      request.NamespaceID.String(),
		pageSize:         request.PageSize,
		startTimeGTE:     request.EarliestStartTime,
		startTimeLTE:     request.LatestStartTime,
		workflowTypeName: request.WorkflowTypeName,
	})
	if err != nil {
		return nil, xydb.ConvertToTemporalError("ListOpenWorkflowExecutionsByType", err)
	}
	return &store.InternalListWorkflowExecutionsResponse{
		Executions:    executions,
		NextPageToken: npt,
	}, nil
}

func (v *visibilityStore) ListOpenWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	executions, npt, err := v.listOpenWorkflowExecutions(ctx, &listOpenWorkflowExecutionsQuery{
		pageToken:    request.NextPageToken,
		namespaceID:  request.NamespaceID.String(),
		pageSize:     request.PageSize,
		startTimeGTE: request.EarliestStartTime,
		startTimeLTE: request.LatestStartTime,
		workflowID:   request.WorkflowID,
	})
	if err != nil {
		return nil, xydb.ConvertToTemporalError("ListOpenWorkflowExecutionsByWorkflowID", err)
	}
	return &store.InternalListWorkflowExecutionsResponse{
		Executions:    executions,
		NextPageToken: npt,
	}, nil
}

func (v *visibilityStore) ListClosedWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	executions, npt, err := v.listClosedWorkflowExecutions(ctx, &listClosedWorkflowExecutionsQuery{
		pageToken:   request.NextPageToken,
		namespaceID: request.NamespaceID.String(),
		pageSize:    request.PageSize,
		// {Earliest,Latest}StartTime mean {Earliest,Latest}CloseTime in the context of ListClosedWorkflowExecutions
		closeTimeGTE: request.EarliestStartTime,
		closeTimeLTE: request.LatestStartTime,
	})
	if err != nil {
		return nil, xydb.ConvertToTemporalError("ListClosedWorkflowExecutions", err)
	}
	return &store.InternalListWorkflowExecutionsResponse{
		Executions:    executions,
		NextPageToken: npt,
	}, nil
}

func (v *visibilityStore) ListClosedWorkflowExecutionsByType(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByTypeRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	executions, npt, err := v.listClosedWorkflowExecutions(ctx, &listClosedWorkflowExecutionsQuery{
		pageToken:        request.NextPageToken,
		namespaceID:      request.NamespaceID.String(),
		pageSize:         request.PageSize,
		closeTimeGTE:     request.EarliestStartTime,
		closeTimeLTE:     request.LatestStartTime,
		workflowTypeName: request.WorkflowTypeName,
	})
	if err != nil {
		return nil, xydb.ConvertToTemporalError("ListClosedWorkflowExecutionsByType", err)
	}
	return &store.InternalListWorkflowExecutionsResponse{
		Executions:    executions,
		NextPageToken: npt,
	}, nil
}

func (v *visibilityStore) ListClosedWorkflowExecutionsByWorkflowID(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsByWorkflowIDRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	executions, npt, err := v.listClosedWorkflowExecutions(ctx, &listClosedWorkflowExecutionsQuery{
		pageToken:    request.NextPageToken,
		namespaceID:  request.NamespaceID.String(),
		pageSize:     request.PageSize,
		closeTimeGTE: request.EarliestStartTime,
		closeTimeLTE: request.LatestStartTime,
		workflowID:   request.WorkflowID,
	})
	if err != nil {
		return nil, xydb.ConvertToTemporalError("ListClosedWorkflowExecutionsByWorkflowID", err)
	}
	return &store.InternalListWorkflowExecutionsResponse{
		Executions:    executions,
		NextPageToken: npt,
	}, nil
}

func (v *visibilityStore) ListClosedWorkflowExecutionsByStatus(
	ctx context.Context,
	request *manager.ListClosedWorkflowExecutionsByStatusRequest,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	executions, npt, err := v.listClosedWorkflowExecutions(ctx, &listClosedWorkflowExecutionsQuery{
		pageToken:      request.NextPageToken,
		namespaceID:    request.NamespaceID.String(),
		pageSize:       request.PageSize,
		closeTimeGTE:   request.EarliestStartTime,
		closeTimeLTE:   request.LatestStartTime,
		workflowStatus: request.Status,
	})
	if err != nil {
		return nil, xydb.ConvertToTemporalError("ListClosedWorkflowExecutionsByStatus", err)
	}
	return &store.InternalListWorkflowExecutionsResponse{
		Executions:    executions,
		NextPageToken: npt,
	}, nil
}

func (v *visibilityStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	template := v.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $run_id AS utf8;

DELETE FROM executions_visibility
WHERE namespace_id = $namespace_id
AND run_id = $run_id;
`)
	params := table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID.String())),
		table.ValueParam("$run_id", types.UTF8Value(request.RunID)),
	)

	err := v.client.Write(ctx, template, params)
	return xydb.ConvertToTemporalError("DeleteWorkflowExecution", err)
}

func (v *visibilityStore) ListWorkflowExecutions(
	_ context.Context,
	_ *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return nil, store.OperationNotSupportedErr
}

func (v *visibilityStore) ScanWorkflowExecutions(
	_ context.Context,
	_ *manager.ListWorkflowExecutionsRequestV2,
) (*store.InternalListWorkflowExecutionsResponse, error) {
	return nil, store.OperationNotSupportedErr
}

func (v *visibilityStore) CountWorkflowExecutions(
	_ context.Context,
	_ *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	return nil, store.OperationNotSupportedErr
}

func (v *visibilityStore) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*store.InternalGetWorkflowExecutionResponse, error) {
	template := v.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $run_id AS utf8;

SELECT workflow_id, run_id, start_time, execution_time, close_time, workflow_type_name, status, history_length, memo, memo_encoding, task_queue
FROM executions_visibility
WHERE namespace_id = $namespace_id
AND run_id = $run_id;
`)
	params := table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID.String())),
		table.ValueParam("$run_id", types.UTF8Value(request.RunID)),
	)
	res, err := v.client.Do(ctx, template, table.SerializableReadWriteTxControl(table.CommitTx()), params, table.WithIdempotent())
	if err != nil {
		return nil, err
	}
	if err := xydb.EnsureOneRowCursor(ctx, res); err != nil {
		return nil, err
	}
	wfexecution, err := scanClosedWorkflowExecutionRecord(res)
	if err != nil {
		return nil, err
	}
	return &store.InternalGetWorkflowExecutionResponse{
		Execution: wfexecution,
	}, nil
}

func scanOpenWorkflowExecutionRecord(res result.Result) (*store.InternalWorkflowExecutionInfo, error) {
	var workflowID string
	var runID string
	var typeName string
	var startTime time.Time
	var executionTime time.Time
	var memo []byte
	var encoding string
	var taskQueue string

	if err := res.ScanNamed(
		named.OptionalWithDefault("workflow_id", &workflowID),
		named.OptionalWithDefault("run_id", &runID),
		named.OptionalWithDefault("workflow_type_name", &typeName),
		named.OptionalWithDefault("start_time", &startTime),
		named.OptionalWithDefault("execution_time", &executionTime),
		named.OptionalWithDefault("memo", &memo),
		named.OptionalWithDefault("memo_encoding", &encoding),
		named.OptionalWithDefault("task_queue", &taskQueue),
	); err != nil {
		return nil, xerrors.Errorf("failed to scan execution record: %w", err)
	}

	return &store.InternalWorkflowExecutionInfo{
		WorkflowID:    workflowID,
		RunID:         runID,
		TypeName:      typeName,
		StartTime:     ydbpersistence.FromYDBDateTime(startTime),
		ExecutionTime: ydbpersistence.FromYDBDateTime(executionTime),
		Memo:          persistence.NewDataBlob(memo, encoding),
		TaskQueue:     taskQueue,
		Status:        enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}, nil
}

func scanClosedWorkflowExecutionRecord(res result.Result) (*store.InternalWorkflowExecutionInfo, error) {
	var workflowID string
	var runID string
	var typeName string
	var startTime time.Time
	var executionTime time.Time
	var closeTime time.Time
	var status int32
	var historyLength int64
	var memo []byte
	var encoding string
	var taskQueue string

	if err := res.ScanNamed(
		named.OptionalWithDefault("workflow_id", &workflowID),
		named.OptionalWithDefault("run_id", &runID),
		named.OptionalWithDefault("workflow_type_name", &typeName),
		named.OptionalWithDefault("start_time", &startTime),
		named.OptionalWithDefault("execution_time", &executionTime),
		named.OptionalWithDefault("close_time", &closeTime),
		named.OptionalWithDefault("status", &status),
		named.OptionalWithDefault("history_length", &historyLength),
		named.OptionalWithDefault("memo", &memo),
		named.OptionalWithDefault("memo_encoding", &encoding),
		named.OptionalWithDefault("task_queue", &taskQueue),
	); err != nil {
		return nil, xerrors.Errorf("failed to scan execution record: %w", err)
	}

	return &store.InternalWorkflowExecutionInfo{
		WorkflowID:    workflowID,
		RunID:         runID,
		TypeName:      typeName,
		StartTime:     ydbpersistence.FromYDBDateTime(startTime),
		ExecutionTime: ydbpersistence.FromYDBDateTime(executionTime),
		CloseTime:     ydbpersistence.FromYDBDateTime(closeTime),
		Status:        enumspb.WorkflowExecutionStatus(status),
		HistoryLength: historyLength,
		Memo:          persistence.NewDataBlob(memo, encoding),
		TaskQueue:     taskQueue,
	}, nil
}

type visibilityPageToken struct {
	Time  time.Time
	RunID string
}

func (s *visibilityStore) deserializePageToken(
	data []byte,
) (*visibilityPageToken, error) {
	var token visibilityPageToken
	err := json.Unmarshal(data, &token)
	return &token, err
}

func (s *visibilityStore) serializePageToken(
	token *visibilityPageToken,
) ([]byte, error) {
	data, err := json.Marshal(token)
	return data, err
}
