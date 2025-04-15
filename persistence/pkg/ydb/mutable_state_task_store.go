package ydb

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/executor"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/tokens"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/cache"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/rows"
)

type (
	MutableStateTaskStore struct {
		client           *conn.Client
		logger           log.Logger
		tf               executor.TransactionFactory
		cache            executor.EventsCache
		taskCacheFactory cache.TaskCacheFactory
	}
)

func NewMutableStateTaskStore(
	client *conn.Client,
	logger log.Logger,
	cache executor.EventsCache,
	taskCacheFactory cache.TaskCacheFactory,
) *MutableStateTaskStore {
	tf := rows.NewTransactionFactory(client)
	return &MutableStateTaskStore{
		client:           client,
		logger:           logger,
		tf:               tf,
		cache:            cache,
		taskCacheFactory: taskCacheFactory,
	}
}

func (d *MutableStateTaskStore) AddHistoryTasks(
	ctx context.Context,
	request *p.InternalAddHistoryTasksRequest,
) (err error) {
	shardID := request.ShardID
	transaction := d.tf.NewTransaction(shardID)
	transaction.AssertShard(false, request.RangeID)
	transaction.InsertHistoryTasks(request.Tasks)
	err = transaction.Execute(ctx)
	if err != nil {
		err = conn.ConvertToTemporalError("AddHistoryTasks", err)
	}
	return
}

func (d *MutableStateTaskStore) GetHistoryTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (resp *p.InternalGetHistoryTasksResponse, err error) {
	switch request.TaskCategory.Type() {
	case tasks.CategoryTypeImmediate:
		resp, err = d.getHistoryImmediateTasks(ctx, request)
	case tasks.CategoryTypeScheduled:
		resp, err = d.getHistoryScheduledTasks(ctx, request)
	default:
		panic(fmt.Sprintf("Unknown task category type: %v", request.TaskCategory.Type().String()))
	}
	if err != nil {
		err = conn.ConvertToTemporalError("GetHistoryTasks", err)
	}
	return
}

func (d *MutableStateTaskStore) CompleteHistoryTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) (err error) {
	switch request.TaskCategory.Type() {
	case tasks.CategoryTypeImmediate:
		err = d.completeTaskByID(ctx, request.ShardID, int32(request.TaskCategory.ID()), request.TaskKey.TaskID)
	case tasks.CategoryTypeScheduled:
		err = d.completeTaskByIDAndVisibilityTS(ctx, request.ShardID, int32(request.TaskCategory.ID()), request.TaskKey.TaskID, request.TaskKey.FireTime)
	default:
		panic(fmt.Sprintf("Unknown task category type: %v", request.TaskCategory.Type().String()))
	}
	if err != nil {
		err = conn.ConvertToTemporalError("CompleteHistoryTask", err)
	}
	return
}

func (d *MutableStateTaskStore) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) (err error) {
	err = d.rangeCompleteHistoryTasks(ctx, request)
	if err != nil {
		err = conn.ConvertToTemporalError("RangeCompleteHistoryTasks", err)
	}
	return
}

func (d *MutableStateTaskStore) completeTaskByID(ctx context.Context, shardID, categoryID int32, taskID int64) error {
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
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(shardID))),
		table.ValueParam("$task_category_id", types.Int32Value(categoryID)),
		table.ValueParam("$task_id", types.Int64Value(taskID)),
	))
}

func (d *MutableStateTaskStore) completeTaskByIDAndVisibilityTS(ctx context.Context, shardID, categoryID int32, taskID int64, visibilityTS time.Time) error {
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
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(shardID))),
		table.ValueParam("$task_category_id", types.Int32Value(categoryID)),
		table.ValueParam("$task_id", types.Int64Value(taskID)),
		table.ValueParam("$task_visibility_ts", types.TimestampValueFromTime(conn.ToYDBDateTime(visibilityTS))),
	))
}

func (d *MutableStateTaskStore) completeTasksInIDRange(ctx context.Context, shardID, categoryID int32, inclusiveMinTaskID, exclusiveMaxTaskID int64) error {
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
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(shardID))),
		table.ValueParam("$task_category_id", types.Int32Value(categoryID)),
		table.ValueParam("$task_id_gte", types.Int64Value(inclusiveMinTaskID)),
		table.ValueParam("$task_id_lt", types.Int64Value(exclusiveMaxTaskID)),
	))
}

func (d *MutableStateTaskStore) completeTasksInVisibilityTSRange(
	ctx context.Context,
	shardID int32, categoryID int32, inclusiveMinVisibilityTS, exclusiveMaxVisibilityTS time.Time,
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
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(shardID))),
		table.ValueParam("$task_category_id", types.Int32Value(categoryID)),
		table.ValueParam("$task_visibility_ts_gte", types.TimestampValueFromTime(conn.ToYDBDateTime(inclusiveMinVisibilityTS))),
		table.ValueParam("$task_visibility_ts_lt", types.TimestampValueFromTime(conn.ToYDBDateTime(exclusiveMaxVisibilityTS))),
	))
}

func (d *MutableStateTaskStore) getTasksByIDRange(ctx context.Context, shardID, categoryID int32, inclusiveMinTaskID, exclusiveMaxTaskID int64, batchSize int32) (resp []p.InternalHistoryTask, err error) {
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
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(shardID))),
		table.ValueParam("$task_category_id", types.Int32Value(categoryID)),
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
		task, err := d.scanHistoryTask(res)
		if err != nil {
			return nil, err
		}
		historyTasks = append(historyTasks, task)
	}
	return historyTasks, nil

}

func (d *MutableStateTaskStore) getTasksByVisibilityTSRange(ctx context.Context, shardID, categoryID int32, inclusiveMinTaskID int64, inclusiveMinVisibilityTS, exclusiveMaxVisibilityTS time.Time, batchSize int32) (rv []p.InternalHistoryTask, err error) {
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
	res, err := d.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(shardID))),
		table.ValueParam("$task_category_id", types.Int32Value(categoryID)),
		table.ValueParam("$task_id_gte", types.Int64Value(inclusiveMinTaskID)),
		table.ValueParam("$task_visibility_ts_gte", types.TimestampValueFromTime(conn.ToYDBDateTime(inclusiveMinVisibilityTS))),
		table.ValueParam("$task_visibility_ts_lt", types.TimestampValueFromTime(conn.ToYDBDateTime(exclusiveMaxVisibilityTS))),
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
		task, err := d.scanHistoryTimerTask(res)
		if err != nil {
			return nil, err
		}
		timerTasks = append(timerTasks, task)
	}
	return timerTasks, nil
}

func (d *MutableStateTaskStore) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *p.PutReplicationTaskToDLQRequest,
) (err error) {
	defer func() {
		err = conn.ConvertToTemporalError("PutReplicationTaskToDLQ", err)
	}()
	template := d.client.AddQueryPrefix(`
DECLARE $shard_id AS uint32;
DECLARE $source_cluster_name AS Utf8;
DECLARE $task_id AS Int64;
DECLARE $data AS String;
DECLARE $encoding AS ` + d.client.EncodingType().String() + `;

INSERT INTO replication_tasks (shard_id, source_cluster_name, task_id, data, data_encoding)
VALUES ($shard_id, $source_cluster_name, $task_id, $data, $encoding)
`)
	blob, err := serialization.ReplicationTaskInfoToBlob(request.TaskInfo)
	if err != nil {
		return err
	}
	return d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$source_cluster_name", types.UTF8Value(request.SourceClusterName)),
		table.ValueParam("$task_id", types.Int64Value(request.TaskInfo.TaskId)),
		table.ValueParam("$data", types.BytesValue(blob.Data)),
		table.ValueParam("$encoding", d.client.EncodingTypeValue(blob.EncodingType)),
	))
}

func (d *MutableStateTaskStore) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (resp *p.InternalGetHistoryTasksResponse, err error) {
	defer func() {
		err = conn.ConvertToTemporalError("GetReplicationTasksFromDLQ", err)
	}()

	inclusiveMinTaskID, exclusiveMaxTaskID, err := tokens.GetImmediateTaskReadRange(&request.GetHistoryTasksRequest)
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
	res, err := d.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(request.ShardID))),
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
		task, err := d.scanHistoryTask(res)
		if err != nil {
			return nil, err
		}
		historyTasks = append(historyTasks, task)
	}
	response := &p.InternalGetHistoryTasksResponse{
		Tasks: historyTasks,
	}
	if len(historyTasks) == request.BatchSize {
		if response.NextPageToken, err = tokens.GetImmediateTaskNextPageToken(
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
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$source_cluster_name", types.UTF8Value(request.SourceClusterName)),
		table.ValueParam("$task_id", types.Int64Value(request.TaskKey.TaskID)),
	))
	return conn.ConvertToTemporalError("DeleteReplicationTaskFromDLQ", err)
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
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(request.ShardID))),
		table.ValueParam("$source_cluster_name", types.UTF8Value(request.SourceClusterName)),
		table.ValueParam("$task_id_gte", types.Int64Value(request.InclusiveMinTaskKey.TaskID)),
		table.ValueParam("$task_id_lt", types.Int64Value(request.ExclusiveMaxTaskKey.TaskID)),
	))
	return conn.ConvertToTemporalError("RangeDeleteReplicationTaskFromDLQ", err)
}

func (d *MutableStateTaskStore) IsReplicationDLQEmpty(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (empty bool, err error) {
	defer func() {
		err = conn.ConvertToTemporalError("IsReplicationDLQEmpty", err)
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
	res, err := d.client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$shard_id", types.Uint32Value(rows.ToShardIDColumnValue(request.ShardID))),
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

func (d *MutableStateTaskStore) getHistoryImmediateTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (*p.InternalGetHistoryTasksResponse, error) {
	inclusiveMinTaskID, exclusiveMaxTaskID, err := tokens.GetImmediateTaskReadRange(request)
	if err != nil {
		return nil, err
	}

	historyTasks, err := d.getTasksByIDRange(ctx, request.ShardID, int32(request.TaskCategory.ID()), inclusiveMinTaskID, exclusiveMaxTaskID, int32(request.BatchSize))
	if err != nil {
		return nil, err
	}
	response := &p.InternalGetHistoryTasksResponse{
		Tasks: historyTasks,
	}
	if len(historyTasks) == request.BatchSize {
		if response.NextPageToken, err = tokens.GetImmediateTaskNextPageToken(
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
	pageToken := &tokens.ScheduledTaskPageToken{
		TaskID:    math.MinInt64,
		Timestamp: request.InclusiveMinTaskKey.FireTime,
	}
	if len(request.NextPageToken) > 0 {
		if err := pageToken.Deserialize(request.NextPageToken); err != nil {
			return nil, err
		}
	}

	timerTasks, err := d.getTasksByVisibilityTSRange(
		ctx, request.ShardID, int32(request.TaskCategory.ID()),
		pageToken.TaskID, pageToken.Timestamp, request.ExclusiveMaxTaskKey.FireTime, int32(request.BatchSize))
	if err != nil {
		return nil, err
	}

	response := &p.InternalGetHistoryTasksResponse{
		Tasks: timerTasks,
	}
	if len(timerTasks) == request.BatchSize {
		pageToken = &tokens.ScheduledTaskPageToken{
			TaskID:    timerTasks[request.BatchSize-1].Key.TaskID + 1,
			Timestamp: timerTasks[request.BatchSize-1].Key.FireTime,
		}
		if response.NextPageToken, err = pageToken.Serialize(); err != nil {
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
		return d.completeTaskByIDAndVisibilityTS(ctx, request.ShardID, int32(request.TaskCategory.ID()), request.TaskKey.TaskID, request.TaskKey.FireTime)
	} else {
		return d.completeTaskByID(ctx, request.ShardID, int32(request.TaskCategory.ID()), request.TaskKey.TaskID)
	}
}

func (d *MutableStateTaskStore) rangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) error {
	if request.TaskCategory.Type() == tasks.CategoryTypeImmediate {
		return d.completeTasksInIDRange(ctx, request.ShardID, int32(request.TaskCategory.ID()), request.InclusiveMinTaskKey.TaskID, request.ExclusiveMaxTaskKey.TaskID)
	} else {
		return d.completeTasksInVisibilityTSRange(ctx, request.ShardID, int32(request.TaskCategory.ID()), request.InclusiveMinTaskKey.FireTime, request.ExclusiveMaxTaskKey.FireTime)
	}
}

func (d *MutableStateTaskStore) scanHistoryTask(res result.Result) (p.InternalHistoryTask, error) {
	var taskID int64
	var data []byte
	var encoding string
	var encodingType conn.EncodingTypeRaw
	var encodingPtr interface{}
	if d.client.UseIntForEncoding() {
		encodingPtr = &encodingType
	} else {
		encodingPtr = &encoding
	}

	if err := res.ScanNamed(
		named.OptionalWithDefault("task_id", &taskID),
		named.OptionalWithDefault("data", &data),
		named.OptionalWithDefault("data_encoding", encodingPtr),
	); err != nil {
		return p.InternalHistoryTask{}, fmt.Errorf("failed to scan history task: %w", err)
	}

	if d.client.UseIntForEncoding() {
		encoding = enumspb.EncodingType(encodingType).String()
	}

	return p.InternalHistoryTask{
		Key:  tasks.NewImmediateKey(taskID),
		Blob: p.NewDataBlob(data, encoding),
	}, nil
}

func (d *MutableStateTaskStore) scanHistoryTimerTask(res result.Result) (p.InternalHistoryTask, error) {
	var taskID int64
	var data []byte
	var encoding string
	var encodingType conn.EncodingTypeRaw
	var encodingPtr interface{}
	if d.client.UseIntForEncoding() {
		encodingPtr = &encodingType
	} else {
		encodingPtr = &encoding
	}
	var timestamp time.Time

	if err := res.ScanNamed(
		named.OptionalWithDefault("task_id", &taskID),
		named.OptionalWithDefault("data", &data),
		named.OptionalWithDefault("data_encoding", encodingPtr),
		named.OptionalWithDefault("task_visibility_ts", &timestamp),
	); err != nil {
		return p.InternalHistoryTask{}, fmt.Errorf("failed to scan history timer task: %w", err)
	}

	if d.client.UseIntForEncoding() {
		encoding = enumspb.EncodingType(encodingType).String()
	}

	return p.InternalHistoryTask{
		Key:  tasks.NewKey(conn.FromYDBDateTime(timestamp), taskID),
		Blob: p.NewDataBlob(data, encoding),
	}, nil
}
