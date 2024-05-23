package persistence

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

type (
	MutableStateTaskStore struct {
		client *xydb.Client
		logger log.Logger
	}
)

func NewMutableStateTaskStore(
	client *xydb.Client,
	logger log.Logger,
) *MutableStateTaskStore {
	return &MutableStateTaskStore{
		client: client,
		logger: logger,
	}
}

func (d *MutableStateTaskStore) RegisterHistoryTaskReader(
	_ context.Context,
	_ *p.RegisterHistoryTaskReaderRequest,
) error {
	// no-op
	return nil
}

func (d *MutableStateTaskStore) UnregisterHistoryTaskReader(
	_ context.Context,
	_ *p.UnregisterHistoryTaskReaderRequest,
) {
	// no-op
}

func (d *MutableStateTaskStore) UpdateHistoryTaskReaderProgress(
	_ context.Context,
	_ *p.UpdateHistoryTaskReaderProgressRequest,
) {
	// no-op
}

func (d *MutableStateTaskStore) AddHistoryTasks(
	ctx context.Context,
	request *p.InternalAddHistoryTasksRequest,
) error {
	q := conditionsToReadQuery(request.ShardID, request.NamespaceID, request.RangeID, nil, nil)
	rows := createTaskRows(request.ShardID, request.Tasks)
	wqs := []*writeQuery{prepareUpsertRowsQuery(rows)}
	err := d.client.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		return executeOneReadAndManyWriteQueries(ctx, d.client, s, q, wqs)
	})
	return xydb.ConvertToTemporalError("AddTasks", err)
}

func (d *MutableStateTaskStore) GetHistoryTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (resp *p.InternalGetHistoryTasksResponse, err error) {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		resp, err = d.getTransferTasks(ctx, request)
	case tasks.CategoryIDTimer:
		resp, err = d.getTimerTasks(ctx, request)
	case tasks.CategoryIDVisibility:
		resp, err = d.getVisibilityTasks(ctx, request)
	case tasks.CategoryIDReplication:
		resp, err = d.getReplicationTasks(ctx, request)
	default:
		switch request.TaskCategory.Type() {
		case tasks.CategoryTypeImmediate:
			return d.getHistoryImmediateTasks(ctx, request)
		case tasks.CategoryTypeScheduled:
			return d.getHistoryScheduledTasks(ctx, request)
		default:
			panic(fmt.Sprintf("Unknown task category type: %v", request.TaskCategory.Type().String()))
		}
	}
	if err != nil {
		err = xydb.ConvertToTemporalError("GetHistoryTasks", err)
	}
	return
}

func (d *MutableStateTaskStore) CompleteHistoryTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) (err error) {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		err = d.completeTransferTask(ctx, request)
	case tasks.CategoryIDTimer:
		err = d.completeTimerTask(ctx, request)
	case tasks.CategoryIDVisibility:
		err = d.completeVisibilityTask(ctx, request)
	case tasks.CategoryIDReplication:
		err = d.completeReplicationTask(ctx, request)
	default:
		switch request.TaskCategory.Type() {
		case tasks.CategoryTypeImmediate:
			return d.completeTaskByID(ctx, request.ShardID, request.TaskCategory.ID(), request.TaskKey.TaskID)
		case tasks.CategoryTypeScheduled:
			return d.completeTaskByIDAndVisibilityTS(ctx, request.ShardID, request.TaskCategory.ID(), request.TaskKey.TaskID, request.TaskKey.FireTime)
		default:
			panic(fmt.Sprintf("Unknown task category type: %v", request.TaskCategory.Type().String()))
		}
	}
	if err != nil {
		err = xydb.ConvertToTemporalError("CompleteHistoryTask", err)
	}
	return
}

func (d *MutableStateTaskStore) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) (err error) {
	switch request.TaskCategory.ID() {
	case tasks.CategoryIDTransfer:
		err = d.rangeCompleteTransferTasks(ctx, request)
	case tasks.CategoryIDTimer:
		err = d.rangeCompleteTimerTasks(ctx, request)
	case tasks.CategoryIDVisibility:
		err = d.rangeCompleteVisibilityTasks(ctx, request)
	case tasks.CategoryIDReplication:
		err = d.rangeCompleteReplicationTasks(ctx, request)
	default:
		err = d.rangeCompleteHistoryTasks(ctx, request)
	}
	if err != nil {
		err = xydb.ConvertToTemporalError("RangeCompleteHistoryTasks", err)
	}
	return
}

func (d *MutableStateTaskStore) getTransferTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	inclusiveMinTaskID, exclusiveMaxTaskID, err := getImmediateTaskReadRange(request)
	if err != nil {
		return nil, err
	}
	historyTasks, err := d.getTasksByIDRange(ctx, request.ShardID, tasks.CategoryIDTransfer, inclusiveMinTaskID, exclusiveMaxTaskID, int32(request.BatchSize))
	if err != nil {
		return nil, err
	}
	response := &p.InternalGetHistoryTasksResponse{
		Tasks: historyTasks,
	}
	if len(historyTasks) == request.BatchSize {
		if response.NextPageToken, err = getImmediateTaskNextPageToken(
			historyTasks[len(historyTasks)-1].Key.TaskID,
			exclusiveMaxTaskID,
		); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (d *MutableStateTaskStore) completeTaskByID(ctx context.Context, shardID int32, categoryID int, taskID int64) error {
	// TODO(romanovich@): can we use blind DELETE here?
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $task_id AS Int64;
DECLARE $task_category_id AS Int32;

DELETE FROM executions
WHERE shard_id = $shard_id
AND namespace_id IS NULL
AND workflow_id IS NULL
AND run_id IS NULL
AND task_category_id = $task_category_id
AND task_visibility_ts IS NULL
AND task_id = $task_id
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`)
	return d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(shardID))),
		table.ValueParam("$task_category_id", types.Int32Value(int32(categoryID))),
		table.ValueParam("$task_id", types.Int64Value(taskID)),
	))
}

func (d *MutableStateTaskStore) completeTaskByIDAndVisibilityTS(ctx context.Context, shardID int32, categoryID int, taskID int64, visibilityTS time.Time) error {
	// TODO(romanovich@): can we use blind DELETE here?
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $task_id AS Int64;
DECLARE $task_category_id AS Int32;
DECLARE $task_visibility_ts AS Timestamp;

DELETE FROM executions
WHERE shard_id = $shard_id
AND namespace_id IS NULL
AND workflow_id IS NULL
AND run_id IS NULL
AND task_category_id = $task_category_id
AND task_visibility_ts = $task_visibility_ts
AND task_id = $task_id
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`)
	return d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(shardID))),
		table.ValueParam("$task_category_id", types.Int32Value(int32(categoryID))),
		table.ValueParam("$task_id", types.Int64Value(taskID)),
		table.ValueParam("$task_visibility_ts", types.TimestampValueFromTime(ToYDBDateTime(visibilityTS))),
	))
}

func (d *MutableStateTaskStore) completeTasksInIDRange(ctx context.Context, shardID int32, categoryID int, inclusiveMinTaskID, exclusiveMaxTaskID int64) error {
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $task_id_gte AS Int64;
DECLARE $task_id_lt AS Int64;
DECLARE $task_category_id AS Int32;

DELETE FROM executions
WHERE shard_id = $shard_id
AND namespace_id IS NULL
AND workflow_id IS NULL
AND run_id IS NULL
AND task_category_id = $task_category_id
AND task_visibility_ts IS NULL
AND task_id >= $task_id_gte
AND task_id < $task_id_lt
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`)
	return d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(shardID))),
		table.ValueParam("$task_category_id", types.Int32Value(int32(categoryID))),
		table.ValueParam("$task_id_gte", types.Int64Value(inclusiveMinTaskID)),
		table.ValueParam("$task_id_lt", types.Int64Value(exclusiveMaxTaskID)),
	))
}

func (d *MutableStateTaskStore) completeTransferTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	return d.completeTaskByID(ctx, request.ShardID, tasks.CategoryIDTransfer, request.TaskKey.TaskID)
}

func (d *MutableStateTaskStore) rangeCompleteTransferTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	return d.completeTasksInIDRange(ctx, request.ShardID, tasks.CategoryIDTransfer, request.InclusiveMinTaskKey.TaskID, request.ExclusiveMaxTaskKey.TaskID)
}

func (d *MutableStateTaskStore) getTimerTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	pageToken := &scheduledTaskPageToken{
		TaskID:    math.MinInt64,
		Timestamp: request.InclusiveMinTaskKey.FireTime,
	}
	if len(request.NextPageToken) > 0 {
		if err := pageToken.deserialize(request.NextPageToken); err != nil {
			return nil, err
		}
	}
	timerTasks, err := d.getTasksByVisibilityTSRange(
		ctx, request.ShardID, tasks.CategoryIDTimer, pageToken.TaskID, pageToken.Timestamp,
		request.ExclusiveMaxTaskKey.FireTime, int32(request.BatchSize))
	if err != nil {
		return nil, err
	}
	response := &p.InternalGetHistoryTasksResponse{
		Tasks: timerTasks,
	}
	if len(timerTasks) == request.BatchSize {
		nextPageToken := &scheduledTaskPageToken{
			TaskID:    timerTasks[request.BatchSize-1].Key.TaskID + 1,
			Timestamp: timerTasks[request.BatchSize-1].Key.FireTime,
		}
		if response.NextPageToken, err = nextPageToken.serialize(); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (d *MutableStateTaskStore) completeTimerTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $task_category_id AS Int32;
DECLARE $task_id AS Int64;
DECLARE $task_visibility_ts AS Timestamp;

DELETE FROM executions
WHERE shard_id = $shard_id
AND namespace_id IS NULL
AND workflow_id IS NULL
AND run_id IS NULL
AND task_category_id = $task_category_id
AND task_visibility_ts = $task_visibility_ts
AND task_id = $task_id
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`)
	return d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$task_category_id", types.Int32Value(tasks.CategoryIDTimer)),
		table.ValueParam("$task_id", types.Int64Value(request.TaskKey.TaskID)),
		table.ValueParam("$task_visibility_ts", types.TimestampValueFromTime(ToYDBDateTime(request.TaskKey.FireTime))),
	))
}

func (d *MutableStateTaskStore) completeTasksInVisibilityTSRange(
	ctx context.Context,
	shardID int32, categoryID int, inclusiveMinVisibilityTS, exclusiveMaxVisibilityTS time.Time,
) error {
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $task_category_id AS Int32;
DECLARE $task_visibility_ts_gte AS Timestamp;
DECLARE $task_visibility_ts_lt AS Timestamp;

DELETE FROM executions
WHERE shard_id = $shard_id
AND namespace_id IS NULL
AND workflow_id IS NULL
AND run_id IS NULL
AND task_category_id = $task_category_id
AND task_visibility_ts >= $task_visibility_ts_gte
AND task_visibility_ts < $task_visibility_ts_lt
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`)
	return d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(shardID))),
		table.ValueParam("$task_category_id", types.Int32Value(int32(categoryID))),
		table.ValueParam("$task_visibility_ts_gte", types.TimestampValueFromTime(ToYDBDateTime(inclusiveMinVisibilityTS))),
		table.ValueParam("$task_visibility_ts_lt", types.TimestampValueFromTime(ToYDBDateTime(exclusiveMaxVisibilityTS))),
	))
}

func (d *MutableStateTaskStore) rangeCompleteTimerTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	return d.completeTasksInVisibilityTSRange(ctx, request.ShardID, tasks.CategoryIDTimer, request.InclusiveMinTaskKey.FireTime, request.ExclusiveMaxTaskKey.FireTime)
}

func (d *MutableStateTaskStore) getTasksByIDRange(ctx context.Context, shardID int32, categoryID int, inclusiveMinTaskID, exclusiveMaxTaskID int64, batchSize int32) (resp []p.InternalHistoryTask, err error) {
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $task_category_id AS Int32;
DECLARE $task_id_gte AS Int64;
DECLARE $task_id_lt AS Int64;
DECLARE $page_size AS Int32;

SELECT shard_id, namespace_id, workflow_id, run_id, task_category_id, task_visibility_ts, task_id, event_type, event_id, event_name, data, data_encoding
FROM executions
WHERE shard_id = $shard_id
AND namespace_id IS NULL
AND workflow_id IS NULL
AND run_id IS NULL
AND task_category_id = $task_category_id
AND task_visibility_ts IS NULL
AND task_id >= $task_id_gte
AND task_id < $task_id_lt
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL
ORDER BY shard_id, namespace_id, workflow_id, run_id, task_category_id, task_visibility_ts, task_id, event_type, event_id, event_name
LIMIT $page_size;
`)
	res, err := d.client.Do(ctx, template, table.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(shardID))),
		table.ValueParam("$task_category_id", types.Int32Value(int32(categoryID))),
		table.ValueParam("$task_id_gte", types.Int64Value(inclusiveMinTaskID)),
		table.ValueParam("$task_id_lt", types.Int64Value(exclusiveMaxTaskID)),
		table.ValueParam("$page_size", types.Int32Value(batchSize)),
	), table.WithIdempotent())
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}

	historyTasks := make([]p.InternalHistoryTask, 0, res.CurrentResultSet().ItemCount())
	for res.NextRow() {
		task, err := scanHistoryTask(res)
		if err != nil {
			return nil, err
		}
		historyTasks = append(historyTasks, task)
	}
	return historyTasks, nil

}

func (d *MutableStateTaskStore) getTasksByVisibilityTSRange(ctx context.Context, shardID int32, categoryID int, inclusiveMinTaskID int64, inclusiveMinVisibilityTS, exclusiveMaxVisibilityTS time.Time, batchSize int32) (rv []p.InternalHistoryTask, err error) {
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $task_category_id AS Int32;
DECLARE $task_visibility_ts_gte AS Timestamp;
DECLARE $task_visibility_ts_lt AS Timestamp;
DECLARE $task_id_gte AS Int64;
DECLARE $page_size AS Int32;

SELECT shard_id, namespace_id, workflow_id, run_id, task_category_id, task_visibility_ts, task_id, event_type, event_id, event_name, data, data_encoding
FROM executions
WHERE shard_id = $shard_id
AND namespace_id IS NULL
AND workflow_id IS NULL
AND run_id IS NULL
AND task_category_id = $task_category_id
-- make sure it's TableRangesScan for YDB:
AND task_id >= $task_id_gte
AND task_visibility_ts >= $task_visibility_ts_gte
AND task_visibility_ts < $task_visibility_ts_lt
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL
-- refine the condition later:
AND (task_visibility_ts > $task_visibility_ts_gte OR (task_visibility_ts = $task_visibility_ts_gte AND task_id >= $task_id_gte))
ORDER BY shard_id, namespace_id, workflow_id, run_id, task_category_id, task_visibility_ts, task_id, event_type, event_id, event_name
LIMIT $page_size;
`)
	res, err := d.client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(shardID))),
		table.ValueParam("$task_category_id", types.Int32Value(int32(categoryID))),
		table.ValueParam("$task_id_gte", types.Int64Value(inclusiveMinTaskID)),
		table.ValueParam("$task_visibility_ts_gte", types.TimestampValueFromTime(ToYDBDateTime(inclusiveMinVisibilityTS))),
		table.ValueParam("$task_visibility_ts_lt", types.TimestampValueFromTime(ToYDBDateTime(exclusiveMaxVisibilityTS))),
		table.ValueParam("$page_size", types.Int32Value(batchSize)),
	), table.WithIdempotent())
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}

	timerTasks := make([]p.InternalHistoryTask, 0, res.CurrentResultSet().ItemCount())
	for res.NextRow() {
		task, err := scanHistoryTimerTask(res)
		if err != nil {
			return nil, err
		}
		timerTasks = append(timerTasks, task)
	}
	return timerTasks, nil
}

func (d *MutableStateTaskStore) getReplicationTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	inclusiveMinTaskID, exclusiveMaxTaskID, err := getImmediateTaskReadRange(request)
	if err != nil {
		return nil, err
	}
	historyTasks, err := d.getTasksByIDRange(
		ctx, request.ShardID, tasks.CategoryIDReplication, inclusiveMinTaskID, exclusiveMaxTaskID, int32(request.BatchSize))
	if err != nil {
		return nil, err
	}
	response := &p.InternalGetHistoryTasksResponse{
		Tasks: historyTasks,
	}
	if len(historyTasks) == request.BatchSize {
		if response.NextPageToken, err = getImmediateTaskNextPageToken(
			historyTasks[len(historyTasks)-1].Key.TaskID,
			exclusiveMaxTaskID,
		); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (d *MutableStateTaskStore) completeReplicationTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	return d.completeTaskByID(ctx, request.ShardID, tasks.CategoryIDReplication, request.TaskKey.TaskID)
}

func (d *MutableStateTaskStore) rangeCompleteReplicationTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	return d.completeTasksInIDRange(ctx, request.ShardID, tasks.CategoryIDReplication, request.InclusiveMinTaskKey.TaskID, request.ExclusiveMaxTaskKey.TaskID)
}

func (d *MutableStateTaskStore) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *p.PutReplicationTaskToDLQRequest,
) (err error) {
	defer func() {
		err = xydb.ConvertToTemporalError("PutReplicationTaskToDLQ", err)
	}()
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $source_cluster_name AS Utf8;
DECLARE $task_id AS Int64;
DECLARE $data AS String;
DECLARE $encoding AS Utf8;

INSERT INTO replication_tasks (shard_id, source_cluster_name, task_id, data, data_encoding)
VALUES ($shard_id, $source_cluster_name, $task_id, $data, $encoding)
`)
	blob, err := serialization.ReplicationTaskInfoToBlob(request.TaskInfo)
	if err != nil {
		return err
	}
	return d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$source_cluster_name", types.UTF8Value(request.SourceClusterName)),
		table.ValueParam("$task_id", types.Int64Value(request.TaskInfo.TaskId)),
		table.ValueParam("$data", types.BytesValue(blob.Data)),
		table.ValueParam("$encoding", types.UTF8Value(blob.EncodingType.String())),
	))
}

func (d *MutableStateTaskStore) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (resp *p.InternalGetHistoryTasksResponse, err error) {
	defer func() {
		err = xydb.ConvertToTemporalError("GetReplicationTasksFromDLQ", err)
	}()

	inclusiveMinTaskID, exclusiveMaxTaskID, err := getImmediateTaskReadRange(&request.GetHistoryTasksRequest)
	if err != nil {
		return nil, err
	}

	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $source_cluster_name AS Utf8;
DECLARE $task_id_gte AS Int64;
DECLARE $task_id_lt AS Int64;
DECLARE $page_size AS Int32;

SELECT task_id, data, data_encoding
FROM replication_tasks
WHERE source_cluster_name = $source_cluster_name
AND shard_id = $shard_id
AND task_id >= $task_id_gte
AND task_id < $task_id_lt
ORDER BY task_id
LIMIT $page_size;
`)
	res, err := d.client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$source_cluster_name", types.UTF8Value(request.SourceClusterName)),
		table.ValueParam("$task_id_gte", types.Int64Value(inclusiveMinTaskID)),
		table.ValueParam("$task_id_lt", types.Int64Value(exclusiveMaxTaskID)),
		table.ValueParam("$page_size", types.Int32Value(int32(request.BatchSize))),
	), table.WithIdempotent())
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}
	historyTasks := make([]p.InternalHistoryTask, 0, res.CurrentResultSet().ItemCount())
	for res.NextRow() {
		task, err := scanHistoryTask(res)
		if err != nil {
			return nil, err
		}
		historyTasks = append(historyTasks, task)
	}
	response := &p.InternalGetHistoryTasksResponse{
		Tasks: historyTasks,
	}
	if len(historyTasks) == request.BatchSize {
		if response.NextPageToken, err = getImmediateTaskNextPageToken(
			historyTasks[len(historyTasks)-1].Key.TaskID,
			exclusiveMaxTaskID,
		); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (d *MutableStateTaskStore) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *p.DeleteReplicationTaskFromDLQRequest,
) error {
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $task_id AS Int64;
DECLARE $source_cluster_name AS Utf8;

DELETE FROM replication_tasks
WHERE shard_id = $shard_id
AND source_cluster_name = $source_cluster_name
AND task_id = $task_id;
`)
	err := d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$source_cluster_name", types.UTF8Value(request.SourceClusterName)),
		table.ValueParam("$task_id", types.Int64Value(request.TaskKey.TaskID)),
	))
	return xydb.ConvertToTemporalError("DeleteReplicationTaskFromDLQ", err)
}

func (d *MutableStateTaskStore) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *p.RangeDeleteReplicationTaskFromDLQRequest,
) error {
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $source_cluster_name AS Utf8;
DECLARE $task_id_gte AS Int64;
DECLARE $task_id_lt AS Int64;

DELETE FROM replication_tasks
WHERE shard_id = $shard_id
AND source_cluster_name = $source_cluster_name
AND task_id >= $task_id_gte
AND task_id < $task_id_lt;
`)
	err := d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$source_cluster_name", types.UTF8Value(request.SourceClusterName)),
		table.ValueParam("$task_id_gte", types.Int64Value(request.InclusiveMinTaskKey.TaskID)),
		table.ValueParam("$task_id_lt", types.Int64Value(request.ExclusiveMaxTaskKey.TaskID)),
	))
	return xydb.ConvertToTemporalError("RangeDeleteReplicationTaskFromDLQ", err)
}

func (d *MutableStateTaskStore) IsReplicationDLQEmpty(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (empty bool, err error) {
	defer func() {
		err = xydb.ConvertToTemporalError("IsReplicationDLQEmpty", err)
	}()

	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $source_cluster_name AS Utf8;
DECLARE $task_id_gte AS Int64;

SELECT task_id
FROM replication_tasks
WHERE source_cluster_name = $source_cluster_name
AND shard_id = $shard_id
AND task_id >= $task_id_gte
LIMIT 1;
`)
	res, err := d.client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$source_cluster_name", types.UTF8Value(request.SourceClusterName)),
		table.ValueParam("$task_id_gte", types.Int64Value(request.InclusiveMinTaskKey.TaskID)),
	), table.WithIdempotent())
	if err != nil {
		return false, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()

	if err = res.NextResultSetErr(ctx); err != nil {
		return false, err
	}
	return !res.NextRow(), nil
}

func (d *MutableStateTaskStore) getVisibilityTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	inclusiveMinTaskID, exclusiveMaxTaskID, err := getImmediateTaskReadRange(request)
	if err != nil {
		return nil, err
	}
	historyTasks, err := d.getTasksByIDRange(ctx, request.ShardID, tasks.CategoryIDVisibility, inclusiveMinTaskID, exclusiveMaxTaskID, int32(request.BatchSize))
	if err != nil {
		return nil, err
	}
	resp := &p.InternalGetHistoryTasksResponse{
		Tasks: historyTasks,
	}
	if len(historyTasks) == request.BatchSize {
		if resp.NextPageToken, err = getImmediateTaskNextPageToken(
			historyTasks[len(historyTasks)-1].Key.TaskID,
			exclusiveMaxTaskID,
		); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (d *MutableStateTaskStore) completeVisibilityTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	return d.completeTaskByID(ctx, request.ShardID, tasks.CategoryIDVisibility, request.TaskKey.TaskID)
}

func (d *MutableStateTaskStore) rangeCompleteVisibilityTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	return d.completeTasksInIDRange(ctx, request.ShardID, tasks.CategoryIDVisibility, request.InclusiveMinTaskKey.TaskID, request.ExclusiveMaxTaskKey.TaskID)
}

func (d *MutableStateTaskStore) getHistoryImmediateTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	inclusiveMinTaskID, exclusiveMaxTaskID, err := getImmediateTaskReadRange(request)
	if err != nil {
		return nil, err
	}

	historyTasks, err := d.getTasksByIDRange(ctx, request.ShardID, request.TaskCategory.ID(), inclusiveMinTaskID, exclusiveMaxTaskID, int32(request.BatchSize))
	if err != nil {
		return nil, err
	}
	response := &p.InternalGetHistoryTasksResponse{
		Tasks: historyTasks,
	}
	if len(historyTasks) == request.BatchSize {
		if response.NextPageToken, err = getImmediateTaskNextPageToken(
			historyTasks[len(historyTasks)-1].Key.TaskID,
			exclusiveMaxTaskID,
		); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (d *MutableStateTaskStore) getHistoryScheduledTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	pageToken := &scheduledTaskPageToken{
		TaskID:    math.MinInt64,
		Timestamp: request.InclusiveMinTaskKey.FireTime,
	}
	if len(request.NextPageToken) > 0 {
		if err := pageToken.deserialize(request.NextPageToken); err != nil {
			return nil, err
		}
	}

	timerTasks, err := d.getTasksByVisibilityTSRange(
		ctx, request.ShardID, request.TaskCategory.ID(),
		pageToken.TaskID, pageToken.Timestamp, request.ExclusiveMaxTaskKey.FireTime, int32(request.BatchSize))
	if err != nil {
		return nil, err
	}

	response := &p.InternalGetHistoryTasksResponse{
		Tasks: timerTasks,
	}
	if len(timerTasks) == request.BatchSize {
		pageToken = &scheduledTaskPageToken{
			TaskID:    timerTasks[request.BatchSize-1].Key.TaskID + 1,
			Timestamp: timerTasks[request.BatchSize-1].Key.FireTime,
		}
		if response.NextPageToken, err = pageToken.serialize(); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func (d *MutableStateTaskStore) completeHistoryTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) error {
	if request.TaskCategory.Type() == tasks.CategoryTypeScheduled {
		return d.completeTaskByIDAndVisibilityTS(ctx, request.ShardID, request.TaskCategory.ID(), request.TaskKey.TaskID, request.TaskKey.FireTime)
	} else {
		return d.completeTaskByID(ctx, request.ShardID, request.TaskCategory.ID(), request.TaskKey.TaskID)
	}
}

func (d *MutableStateTaskStore) rangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	if request.TaskCategory.Type() == tasks.CategoryTypeImmediate {
		return d.completeTasksInIDRange(ctx, request.ShardID, request.TaskCategory.ID(), request.InclusiveMinTaskKey.TaskID, request.ExclusiveMaxTaskKey.TaskID)
	} else {
		return d.completeTasksInVisibilityTSRange(ctx, request.ShardID, request.TaskCategory.ID(), request.InclusiveMinTaskKey.FireTime, request.ExclusiveMaxTaskKey.FireTime)
	}
}

func scanHistoryTask(res result.Result) (p.InternalHistoryTask, error) {
	var taskID int64
	var data []byte
	var encoding string

	if err := res.ScanNamed(
		named.OptionalWithDefault("task_id", &taskID),
		named.OptionalWithDefault("data", &data),
		named.OptionalWithDefault("data_encoding", &encoding),
	); err != nil {
		return p.InternalHistoryTask{}, fmt.Errorf("failed to scan history task: %w", err)
	}

	return p.InternalHistoryTask{
		Key:  tasks.NewImmediateKey(taskID),
		Blob: p.NewDataBlob(data, encoding),
	}, nil
}

func scanHistoryTimerTask(res result.Result) (p.InternalHistoryTask, error) {
	var taskID int64
	var data []byte
	var encoding string
	var timestamp time.Time

	if err := res.ScanNamed(
		named.OptionalWithDefault("task_id", &taskID),
		named.OptionalWithDefault("data", &data),
		named.OptionalWithDefault("data_encoding", &encoding),
		named.OptionalWithDefault("task_visibility_ts", &timestamp),
	); err != nil {
		return p.InternalHistoryTask{}, fmt.Errorf("failed to scan history timer task: %w", err)
	}

	return p.InternalHistoryTask{
		Key:  tasks.NewKey(FromYDBDateTime(timestamp), taskID),
		Blob: p.NewDataBlob(data, encoding),
	}, nil
}

func getImmediateTaskNextPageToken(
	lastTaskID int64,
	exclusiveMaxTaskID int64,
) ([]byte, error) {
	nextTaskID := lastTaskID + 1
	if nextTaskID < exclusiveMaxTaskID {
		token := taskPageToken{TaskID: nextTaskID}
		return token.serialize()
	}
	return nil, nil
}

func getImmediateTaskReadRange(
	request *p.GetHistoryTasksRequest,
) (inclusiveMinTaskID int64, exclusiveMaxTaskID int64, err error) {
	inclusiveMinTaskID = request.InclusiveMinTaskKey.TaskID
	if len(request.NextPageToken) > 0 {
		var token taskPageToken
		if err = token.deserialize(request.NextPageToken); err != nil {
			return 0, 0, err
		}
		inclusiveMinTaskID = token.TaskID
	}
	return inclusiveMinTaskID, request.ExclusiveMaxTaskKey.TaskID, nil
}
