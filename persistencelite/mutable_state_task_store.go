package persistencelite

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pborman/uuid"
	"github.com/rqlite/gorqlite"
	"github.com/yandex/temporal-over-ydb/persistencelite/cas"
	"github.com/yandex/temporal-over-ydb/persistencelite/conn"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

type (
	MutableStateTaskStore struct {
		baseStore     p.ExecutionStore
		rqliteFactory conn.RqliteFactory
		logger        log.Logger
	}
)

func NewMutableStateTaskStore(
	baseStore p.ExecutionStore,
	rqliteFactory conn.RqliteFactory,
	logger log.Logger,
) *MutableStateTaskStore {
	return &MutableStateTaskStore{
		baseStore:     baseStore,
		rqliteFactory: rqliteFactory,
		logger:        logger,
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
	shardID := request.ShardID
	assertions := []cas.Assertion{
		&cas.ShardRangeCond{
			ShardID:        shardID,
			RangeIDEqualTo: request.RangeID,
		},
	}
	condID := uuid.New()
	rowsToUpsert := createTaskRows(shardID, request.Tasks)
	rql := d.rqliteFactory.GetClient(shardID)
	return cas.ExecuteConditionally(ctx, rql, shardID, request.NamespaceID, condID, assertions, rowsToUpsert, nil, nil)
}

func (d *MutableStateTaskStore) GetHistoryTasks(
	ctx context.Context,
	request *p.GetHistoryTasksRequest,
) (resp *p.InternalGetHistoryTasksResponse, err error) {
	switch request.TaskCategory.Type() {
	case tasks.CategoryTypeImmediate:
		return d.getHistoryImmediateTasks(ctx, request)
	case tasks.CategoryTypeScheduled:
		return d.getHistoryScheduledTasks(ctx, request)
	default:
		panic(fmt.Sprintf("Unknown task category type: %v", request.TaskCategory.Type().String()))
	}
}

func (d *MutableStateTaskStore) CompleteHistoryTask(
	ctx context.Context,
	request *p.CompleteHistoryTaskRequest,
) (err error) {
	switch request.TaskCategory.Type() {
	case tasks.CategoryTypeImmediate:
		return d.completeTaskByID(ctx, request.ShardID, request.TaskCategory.ID(), request.TaskKey.TaskID)
	case tasks.CategoryTypeScheduled:
		return d.completeTaskByIDAndVisibilityTS(ctx, request.ShardID, request.TaskCategory.ID(), request.TaskKey.TaskID, request.TaskKey.FireTime)
	default:
		panic(fmt.Sprintf("Unknown task category type: %v", request.TaskCategory.Type().String()))
	}
}

func (d *MutableStateTaskStore) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *p.RangeCompleteHistoryTasksRequest,
) (err error) {
	if request.TaskCategory.Type() == tasks.CategoryTypeImmediate {
		return d.completeTasksInIDRange(ctx, request.ShardID, request.TaskCategory.ID(), request.InclusiveMinTaskKey.TaskID, request.ExclusiveMaxTaskKey.TaskID)
	} else {
		return d.completeTasksInVisibilityTSRange(ctx, request.ShardID, request.TaskCategory.ID(), request.InclusiveMinTaskKey.FireTime, request.ExclusiveMaxTaskKey.FireTime)
	}
}

func (d *MutableStateTaskStore) completeTaskByID(ctx context.Context, shardID, categoryID int32, taskID int64) error {
	rql := d.rqliteFactory.GetClient(shardID)
	_, err := rql.RequestParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query:     `DELETE FROM tasks WHERE shard_id = ? AND category_id = ? AND visibility_ts = ? AND id = ?`,
			Arguments: []interface{}{shardID, categoryID, 0, taskID},
		},
	})
	return err
}

func (d *MutableStateTaskStore) completeTaskByIDAndVisibilityTS(ctx context.Context, shardID, categoryID int32, taskID int64, visibilityTS time.Time) error {
	rql := d.rqliteFactory.GetClient(shardID)
	_, err := rql.RequestParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query:     `DELETE FROM tasks WHERE shard_id = ? AND category_id = ? AND visibility_ts = ? AND id = ?`,
			Arguments: []interface{}{shardID, categoryID, conn.ToSQLiteDateTime(visibilityTS), taskID},
		},
	})
	return err
}

func (d *MutableStateTaskStore) completeTasksInIDRange(ctx context.Context, shardID, categoryID int32, inclusiveMinTaskID, exclusiveMaxTaskID int64) error {
	rql := d.rqliteFactory.GetClient(shardID)
	_, err := rql.RequestParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query:     `DELETE FROM tasks WHERE shard_id = ? AND category_id = ? AND visibility_ts = ? AND id >= ? AND id < ?`,
			Arguments: []interface{}{shardID, categoryID, 0, inclusiveMinTaskID, exclusiveMaxTaskID},
		},
	})
	return err
}

func (d *MutableStateTaskStore) completeTasksInVisibilityTSRange(
	ctx context.Context,
	shardID int32, categoryID int32, inclusiveMinVisibilityTS, exclusiveMaxVisibilityTS time.Time,
) error {
	rql := d.rqliteFactory.GetClient(shardID)
	_, err := rql.RequestParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query: `DELETE FROM tasks WHERE shard_id = ? AND category_id = ? AND visibility_ts >= ? AND visibility_ts < ?`,
			Arguments: []interface{}{
				shardID,
				categoryID,
				conn.ToSQLiteDateTime(inclusiveMinVisibilityTS),
				conn.ToSQLiteDateTime(exclusiveMaxVisibilityTS),
			},
		},
	})
	return err
}

func (d *MutableStateTaskStore) getTasksByIDRange(ctx context.Context, shardID, categoryID int32, inclusiveMinTaskID, exclusiveMaxTaskID int64, batchSize int32) (resp []p.InternalHistoryTask, err error) {
	rql := d.rqliteFactory.GetClient(shardID)
	rs, err := rql.QueryParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query: `
SELECT id, data, data_encoding 
FROM tasks WHERE shard_id = ? AND category_id = ? AND visibility_ts = ? AND id >= ? AND id < ? ORDER BY id LIMIT ?
`,
			Arguments: []interface{}{shardID, categoryID, 0, inclusiveMinTaskID, exclusiveMaxTaskID, batchSize},
		},
	})
	if err != nil {
		return nil, err
	}
	res := rs[0]

	historyTasks := make([]p.InternalHistoryTask, 0, res.NumRows())
	for res.Next() {
		task, err := scanHistoryTask(res)
		if err != nil {
			return nil, err
		}
		historyTasks = append(historyTasks, task)
	}
	return historyTasks, nil

}

func (d *MutableStateTaskStore) getTasksByVisibilityTSRange(ctx context.Context, shardID, categoryID int32, inclusiveMinTaskID int64, inclusiveMinVisibilityTS, exclusiveMaxVisibilityTS time.Time, batchSize int32) (rv []p.InternalHistoryTask, err error) {
	rql := d.rqliteFactory.GetClient(shardID)
	stmt := gorqlite.ParameterizedStatement{
		Query: `
SELECT id, visibility_ts, data, data_encoding 
FROM tasks
WHERE shard_id = ? AND category_id = ? AND ((visibility_ts >= ? AND id >= ?) OR visibility_ts > ?) AND visibility_ts < ?
ORDER BY visibility_ts, id LIMIT ?
`,
		Arguments: []interface{}{
			shardID,
			categoryID,
			conn.ToSQLiteDateTime(inclusiveMinVisibilityTS),
			inclusiveMinTaskID,
			conn.ToSQLiteDateTime(inclusiveMinVisibilityTS),
			conn.ToSQLiteDateTime(exclusiveMaxVisibilityTS),
			batchSize,
		},
	}
	rs, err := rql.QueryParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		stmt,
	})
	if err != nil {
		return nil, err
	}
	res := rs[0]

	timerTasks := make([]p.InternalHistoryTask, 0, res.NumRows())
	for res.Next() {
		task, err := scanHistoryTimerTask(res)
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
	return d.baseStore.PutReplicationTaskToDLQ(ctx, request)
}

func (d *MutableStateTaskStore) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (resp *p.InternalGetHistoryTasksResponse, err error) {
	return d.baseStore.GetReplicationTasksFromDLQ(ctx, request)
}

func (d *MutableStateTaskStore) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *p.DeleteReplicationTaskFromDLQRequest,
) error {
	return d.baseStore.DeleteReplicationTaskFromDLQ(ctx, request)
}

func (d *MutableStateTaskStore) IsReplicationDLQEmpty(
	ctx context.Context,
	request *p.GetReplicationTasksFromDLQRequest,
) (empty bool, err error) {
	return d.baseStore.IsReplicationDLQEmpty(ctx, request)
}

func (d *ExecutionStore) RangeDeleteReplicationTaskFromDLQ(ctx context.Context, request *p.RangeDeleteReplicationTaskFromDLQRequest) error {
	return d.baseStore.RangeDeleteReplicationTaskFromDLQ(ctx, request)
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

func scanHistoryTask(res gorqlite.QueryResult) (p.InternalHistoryTask, error) {
	var taskID int64
	var dataB64 string
	var encoding string

	if err := res.Scan(&taskID, &dataB64, &encoding); err != nil {
		return p.InternalHistoryTask{}, fmt.Errorf("failed to scan history task: %w", err)
	}

	blob, err := conn.Base64ToBlob(dataB64, encoding)
	if err != nil {
		return p.InternalHistoryTask{}, err
	}
	return p.InternalHistoryTask{
		Key:  tasks.NewImmediateKey(taskID),
		Blob: *blob,
	}, nil
}

func scanHistoryTimerTask(res gorqlite.QueryResult) (p.InternalHistoryTask, error) {
	var taskID int64
	var dataB64 string
	var encoding string
	var timestamp int64

	if err := res.Scan(&taskID, &timestamp, &dataB64, &encoding); err != nil {
		return p.InternalHistoryTask{}, fmt.Errorf("failed to scan history timer task: %w", err)
	}
	blob, err := conn.Base64ToBlob(dataB64, encoding)
	if err != nil {
		return p.InternalHistoryTask{}, err
	}
	return p.InternalHistoryTask{
		Key:  tasks.NewKey(conn.FromSQLiteDateTime(timestamp), taskID),
		Blob: *blob,
	}, nil
}
