package persistence

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
)

type (
	MatchingTaskStore struct {
		client *xydb.Client
		logger log.Logger
	}
)

func NewMatchingTaskStore(
	client *xydb.Client,
	logger log.Logger,
) *MatchingTaskStore {
	return &MatchingTaskStore{
		client: client,
		logger: logger,
	}
}

//nolint:st1003
func (d *MatchingTaskStore) GetTaskQueuesByBuildId(ctx context.Context, request *p.GetTaskQueuesByBuildIdRequest) (rv []string, err error) {
	defer func() {
		if err != nil {
			err = xydb.ConvertToTemporalError("GetTaskQueuesByBuildId", err)
		}
	}()

	template := d.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $build_id AS utf8;

SELECT task_queue_name
FROM build_id_to_task_queue
WHERE namespace_id = $namespace_id
AND build_id = $build_id;
`)
	res, err := d.client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$build_id", types.UTF8Value(request.BuildID)),
	), table.WithIdempotent())
	if err != nil {
		return
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return
	}

	var taskQueues []string
	for res.NextRow() {
		var taskQueueName string
		if err = res.ScanNamed(
			named.OptionalWithDefault("task_queue_name", &taskQueueName),
		); err != nil {
			return nil, fmt.Errorf("failed to scan task_queue_name: %w", err)
		}
		taskQueues = append(taskQueues, taskQueueName)
	}
	return taskQueues, nil
}

//nolint:st1003
func (d *MatchingTaskStore) CountTaskQueuesByBuildId(ctx context.Context, request *p.CountTaskQueuesByBuildIdRequest) (count int, err error) {
	defer func() {
		if err != nil {
			err = xydb.ConvertToTemporalError("CountTaskQueuesByBuildId", err)
		}
	}()

	template := d.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $build_id AS utf8;

SELECT COUNT(*) as count
FROM build_id_to_task_queue
WHERE namespace_id = $namespace_id
AND build_id = $build_id;
`)
	res, err := d.client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$build_id", types.UTF8Value(request.BuildID)),
	), table.WithIdempotent())
	if err != nil {
		return 0, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return 0, err
	}
	if !res.NextRow() {
		return 0, errors.New("failed to scan count")
	}

	var value uint64
	if err = res.ScanNamed(
		named.OptionalWithDefault("count", &value),
	); err != nil {
		return 0, fmt.Errorf("failed to scan count: %w", err)
	}
	return int(value), nil
}

func (d *MatchingTaskStore) GetTaskQueueUserData(
	ctx context.Context,
	request *p.GetTaskQueueUserDataRequest,
) (resp *p.InternalGetTaskQueueUserDataResponse, err error) {
	defer func() {
		if err != nil {
			err = xydb.ConvertToTemporalError("GetTaskQueueUserData", err)
		}
	}()

	template := d.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $task_queue_name AS utf8;

SELECT task_queue_name, data, data_encoding, version
FROM task_queue_user_data
WHERE namespace_id = $namespace_id
AND task_queue_name = $task_queue_name;
`)
	params := table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$task_queue_name", types.UTF8Value(request.TaskQueue)),
	)
	res, err := d.client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), params, table.WithIdempotent())
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = xydb.EnsureOneRowCursor(ctx, res); err != nil {
		return
	}

	userData, err := scanTaskQueueUserData(res)
	if err != nil {
		return nil, err
	}

	resp = &p.InternalGetTaskQueueUserDataResponse{
		Version:  userData.version,
		UserData: p.NewDataBlob(userData.data, userData.encoding),
	}
	return
}

func (d *MatchingTaskStore) ListTaskQueueUserDataEntries(ctx context.Context, request *p.ListTaskQueueUserDataEntriesRequest) (resp *p.InternalListTaskQueueUserDataEntriesResponse, err error) {
	var pageToken taskQueueUserDataPageToken
	if err = pageToken.deserialize(request.NextPageToken); err != nil {
		return nil, err
	}

	template := d.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $task_queue_name_gt AS utf8;
DECLARE $page_size AS int32;

SELECT task_queue_name, data, data_encoding, version
FROM task_queue_user_data
WHERE namespace_id = $namespace_id
AND task_queue_name > $task_queue_name_gt
ORDER BY task_queue_name
LIMIT $page_size;
`)
	params := table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$task_queue_name_gt", types.UTF8Value(pageToken.LastTaskQueueName)),
		table.ValueParam("$page_size", types.Int32Value(int32(request.PageSize))),
	)
	res, err := d.client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), params, table.WithIdempotent())
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

	entries := make([]p.InternalTaskQueueUserDataEntry, 0, request.PageSize)
	var nextPageToken taskQueueUserDataPageToken
	for res.NextRow() {
		ud, err := scanTaskQueueUserData(res)
		if err != nil {
			return nil, err
		}
		entry := p.InternalTaskQueueUserDataEntry{
			TaskQueue: ud.taskQueue,
			Data:      p.NewDataBlob(ud.data, ud.encoding),
			Version:   ud.version,
		}
		entries = append(entries, entry)
		nextPageToken.LastTaskQueueName = ud.taskQueue
	}
	resp = &p.InternalListTaskQueueUserDataEntriesResponse{
		Entries: entries,
	}
	if len(entries) == request.PageSize {
		resp.NextPageToken, err = nextPageToken.serialize()
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (d *MatchingTaskStore) UpdateTaskQueueUserData(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueUserDataRequest,
) (err error) {
	defer func() {
		if err != nil {
			err = xydb.ConvertToTemporalError("UpdateTaskQueueUserData", err)
		}
	}()

	templateInsertTaskQueueUserDataQuery := d.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $task_queue_name AS utf8;
DECLARE $data AS string;
DECLARE $data_encoding AS utf8;

INSERT INTO task_queue_user_data (namespace_id, task_queue_name, data, data_encoding, version)
VALUES ($namespace_id, $task_queue_name, $data, $data_encoding, 1);
`)

	templateUpdateTaskQueueUserDataQuery := d.client.AddQueryPrefix(`
DECLARE $version AS int64;
DECLARE $prev_version AS int64;
DECLARE $namespace_id AS utf8;
DECLARE $task_queue_name AS utf8;
DECLARE $data AS string;
DECLARE $data_encoding AS utf8;

DISCARD SELECT Ensure(version, version == $prev_version, "VERSION_MISMATCH")
FROM task_queue_user_data
WHERE namespace_id = $namespace_id
AND task_queue_name = $task_queue_name;

UPDATE task_queue_user_data SET
data=$data,
data_encoding=$data_encoding,
version=$version
WHERE namespace_id = $namespace_id
AND task_queue_name = $task_queue_name;
`)

	toAdd := make([]types.Value, 0, len(request.BuildIdsAdded))
	for _, buildID := range request.BuildIdsAdded {
		toAdd = append(toAdd, types.StructValue(
			types.StructFieldValue("namespace_id", types.UTF8Value(request.NamespaceID)),
			types.StructFieldValue("build_id", types.UTF8Value(buildID)),
			types.StructFieldValue("task_queue_name", types.UTF8Value(request.TaskQueue)),
		))
	}

	toDelete := make([]types.Value, 0, len(request.BuildIdsRemoved))
	for _, buildID := range request.BuildIdsRemoved {
		toDelete = append(toDelete, types.StructValue(
			types.StructFieldValue("namespace_id", types.UTF8Value(request.NamespaceID)),
			types.StructFieldValue("build_id", types.UTF8Value(buildID)),
			types.StructFieldValue("task_queue_name", types.UTF8Value(request.TaskQueue)),
		))
	}

	return d.client.DB.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		if request.Version == 0 {
			params := table.NewQueryParameters(
				table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
				table.ValueParam("$task_queue_name", types.UTF8Value(request.TaskQueue)),
				table.ValueParam("$data", types.BytesValue(request.UserData.Data)),
				table.ValueParam("$data_encoding", types.UTF8Value(request.UserData.EncodingType.String())),
			)
			if _, err = tx.Execute(ctx, templateInsertTaskQueueUserDataQuery, params); err != nil {
				return err
			}
		} else {
			params := table.NewQueryParameters(
				table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
				table.ValueParam("$task_queue_name", types.UTF8Value(request.TaskQueue)),
				table.ValueParam("$data", types.BytesValue(request.UserData.Data)),
				table.ValueParam("$data_encoding", types.UTF8Value(request.UserData.EncodingType.String())),
				table.ValueParam("$prev_version", types.Int64Value(request.Version)),
				table.ValueParam("$version", types.Int64Value(request.Version+1)),
			)

			if _, err = tx.Execute(ctx, templateUpdateTaskQueueUserDataQuery, params); err != nil {
				return err
			}
		}

		var declares []string
		var stmts []string
		params := table.NewQueryParameters()

		if len(toAdd) > 0 {
			declares = append(declares, `
DECLARE $to_add AS List<Struct<
    namespace_id: Utf8,
	build_id: Utf8,
	task_queue_name: Utf8
>>;
`)
			stmts = append(stmts, `
UPSERT INTO build_id_to_task_queue (namespace_id, build_id, task_queue_name)
SELECT namespace_id, build_id, task_queue_name
FROM AS_TABLE($to_add);
`)
			params.Add(table.ValueParam("$to_add", types.ListValue(toAdd...)))
		}
		if len(toDelete) > 0 {
			declares = append(declares, `
DECLARE $to_delete AS List<Struct<
    namespace_id: Utf8,
	build_id: Utf8,
	task_queue_name: Utf8
>>;
`)
			stmts = append(stmts, "DELETE FROM build_id_to_task_queue ON SELECT * FROM AS_TABLE($to_delete);")
			params.Add(table.ValueParam("$to_delete", types.ListValue(toDelete...)))
		}

		template := d.client.AddQueryPrefix(strings.Join(declares, "\n") + "\n" + strings.Join(stmts, "\n"))
		_, err = tx.Execute(ctx, template, params)
		return err
	})
}

func (d *MatchingTaskStore) CreateTaskQueue(
	ctx context.Context,
	request *p.InternalCreateTaskQueueRequest,
) error {
	template := d.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $task_queue_name AS utf8;
DECLARE $task_queue_type AS int32;
DECLARE $range_id AS int64;
DECLARE $task_queue AS string;
DECLARE $task_queue_encoding AS utf8;

INSERT INTO tasks_and_task_queues (namespace_id, task_queue_name, task_queue_type, task_id, expire_at, range_id, task_queue, task_queue_encoding)
VALUES ($namespace_id, $task_queue_name, $task_queue_type, NULL, NULL, $range_id, $task_queue, $task_queue_encoding);
`)
	if err := d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$task_queue_name", types.UTF8Value(request.TaskQueue)),
		table.ValueParam("$task_queue_type", types.Int32Value(int32(request.TaskType))),
		table.ValueParam("$range_id", types.Int64Value(request.RangeID)),
		table.ValueParam("$task_queue", types.BytesValue(request.TaskQueueInfo.Data)),
		table.ValueParam("$task_queue_encoding", types.UTF8Value(request.TaskQueueInfo.EncodingType.String())),
	)); err != nil {
		details := fmt.Sprintf("name: %v, type: %v", request.TaskQueue, request.TaskType)
		return xydb.ConvertToTemporalError("CreateTaskQueue", err, details)
	}
	return nil
}

func (d *MatchingTaskStore) GetTaskQueue(
	ctx context.Context,
	request *p.InternalGetTaskQueueRequest,
) (resp *p.InternalGetTaskQueueResponse, err error) {
	defer func() {
		if err != nil {
			details := fmt.Sprintf("name: %v, type: %v", request.TaskQueue, request.TaskType)
			err = xydb.ConvertToTemporalError("GetTaskQueue", err, details)
		}
	}()

	template := d.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $task_queue_name AS utf8;
DECLARE $task_queue_type AS int32;
DECLARE $expire_at_gt AS Timestamp;

SELECT range_id, task_queue, task_queue_encoding
FROM tasks_and_task_queues
WHERE namespace_id = $namespace_id
AND task_queue_name = $task_queue_name
AND task_queue_type = $task_queue_type
AND task_id IS NULL
AND (expire_at IS NULL OR expire_at > $expire_at_gt);
`)

	res, err := d.client.Do2(ctx, template, xydb.OnlineReadOnlyTxControl(), func() *table.QueryParameters {
		return table.NewQueryParameters(
			table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
			table.ValueParam("$task_queue_name", types.UTF8Value(request.TaskQueue)),
			table.ValueParam("$task_queue_type", types.Int32Value(int32(request.TaskType))),
			table.ValueParam("$expire_at_gt", types.TimestampValueFromTime(ToYDBDateTime(time.Now()))),
		)
	}, table.WithIdempotent())
	if err != nil {
		return nil, err
	}

	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()

	if err = xydb.EnsureOneRowCursor(ctx, res); err != nil {
		return
	}
	tq, err := scanTaskQueue(res)
	if err != nil {
		return
	}
	resp = &p.InternalGetTaskQueueResponse{
		RangeID:       tq.rangeID,
		TaskQueueInfo: p.NewDataBlob(tq.data, tq.encoding),
	}
	return
}

// UpdateTaskQueue update task queue
func (d *MatchingTaskStore) UpdateTaskQueue(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueRequest,
) (*p.UpdateTaskQueueResponse, error) {
	params := table.NewQueryParameters(
		table.ValueParam("$range_id", types.Int64Value(request.RangeID)),
		table.ValueParam("$task_queue", types.BytesValue(request.TaskQueueInfo.Data)),
		table.ValueParam("$task_queue_encoding", types.UTF8Value(request.TaskQueueInfo.EncodingType.String())),
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$task_queue_name", types.UTF8Value(request.TaskQueue)),
		table.ValueParam("$task_queue_type", types.Int32Value(int32(request.TaskType))),
		table.ValueParam("$prev_range_id", types.Int64Value(request.PrevRangeID)),
	)
	if request.TaskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY {
		if request.ExpiryTime == nil {
			return nil, serviceerror.NewInternal("ExpiryTime cannot be nil for sticky task queue")
		}
		params.Add(table.ValueParam("$expire_at", types.OptionalValue(types.TimestampValueFromTime(ToYDBDateTime(*request.ExpiryTime)))))
	} else {
		params.Add(table.ValueParam("$expire_at", types.NullValue(types.TypeTimestamp)))
	}
	template := d.client.AddQueryPrefix(`
DECLARE $range_id AS int64;
DECLARE $task_queue AS string;
DECLARE $task_queue_encoding AS utf8;
DECLARE $expire_at AS Optional<timestamp>;

DECLARE $namespace_id AS utf8;
DECLARE $task_queue_name AS utf8;
DECLARE $task_queue_type AS int32;
DECLARE $prev_range_id AS int64;

DISCARD SELECT Ensure(range_id, range_id == $prev_range_id, "RANGE_ID_MISMATCH")
FROM tasks_and_task_queues
WHERE namespace_id = $namespace_id
AND task_queue_name = $task_queue_name
AND task_queue_type = $task_queue_type
AND task_id IS NULL;

UPSERT INTO tasks_and_task_queues (namespace_id, task_queue_name, task_queue_type, task_id, expire_at, range_id, task_queue, task_queue_encoding)
VALUES ($namespace_id, $task_queue_name, $task_queue_type, NULL, $expire_at, $range_id, $task_queue, $task_queue_encoding);
`)
	err := d.client.Write(ctx, template, params)
	if err != nil {
		details := fmt.Sprintf("name: %v, type: %v, rangeID: %v", request.TaskQueue, request.TaskType, request.PrevRangeID)
		if xydb.IsPreconditionFailedAndContains(err, "RANGE_ID_MISMATCH") {
			err = xydb.WrapErrorAsRootCause(&p.ConditionFailedError{
				Msg: fmt.Sprintf("DeleteTaskQueue: range id mismatch (%s)", details),
			})
		}
		return nil, xydb.ConvertToTemporalError("UpdateTaskQueue", err, details)
	}
	return &p.UpdateTaskQueueResponse{}, nil
}

func (d *MatchingTaskStore) ListTaskQueue(
	_ context.Context,
	_ *p.ListTaskQueueRequest,
) (*p.InternalListTaskQueueResponse, error) {
	return nil, serviceerror.NewUnavailable("unsupported operation")
}

func (d *MatchingTaskStore) DeleteTaskQueue(
	ctx context.Context,
	request *p.DeleteTaskQueueRequest,
) error {
	namespaceID := request.TaskQueue.NamespaceID
	queueName := request.TaskQueue.TaskQueueName
	queueType := int32(request.TaskQueue.TaskQueueType)
	rangeID := request.RangeID
	template := d.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $task_queue_name AS utf8;
DECLARE $task_queue_type AS int32;
DECLARE $range_id AS int64;

DISCARD SELECT Ensure(range_id, range_id == $range_id, "RANGE_ID_MISMATCH")
FROM tasks_and_task_queues
WHERE namespace_id = $namespace_id
AND task_queue_name = $task_queue_name
AND task_queue_type = $task_queue_type
AND task_id IS NULL;

DELETE FROM tasks_and_task_queues
WHERE namespace_id = $namespace_id
AND task_queue_name = $task_queue_name
AND task_queue_type = $task_queue_type
AND task_id IS NULL;
`)
	err := d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(namespaceID)),
		table.ValueParam("$task_queue_name", types.UTF8Value(queueName)),
		table.ValueParam("$task_queue_type", types.Int32Value(queueType)),
		table.ValueParam("$range_id", types.Int64Value(rangeID)),
	))
	if err != nil {
		details := fmt.Sprintf("name: %v, type: %v, rangeID: %v", queueName, queueType, rangeID)
		if xydb.IsPreconditionFailedAndContains(err, "RANGE_ID_MISMATCH") {
			err = xydb.WrapErrorAsRootCause(&p.ConditionFailedError{
				Msg: fmt.Sprintf("DeleteTaskQueue: range id mismatch (%s)", details),
			})
		}
		return xydb.ConvertToTemporalError("DeleteTaskQueue", err, details)
	}
	return nil
}

// CreateTasks add tasks
func (d *MatchingTaskStore) CreateTasks(
	ctx context.Context,
	request *p.InternalCreateTasksRequest,
) (*p.CreateTasksResponse, error) {
	template := d.client.AddQueryPrefix(`
DECLARE $range_id AS int64;
DECLARE $namespace_id AS utf8;
DECLARE $task_queue_name AS utf8;
DECLARE $task_queue_type AS int32;
DECLARE $tasks AS List<Struct<
	namespace_id: Utf8,
	task_queue_name: Utf8,
	task_queue_type: Int32,
	task_id: Int64,
	expire_at: Optional<timestamp>,
	task: String,
	task_encoding: Utf8
>>;

DISCARD SELECT Ensure(range_id, range_id == $range_id, "RANGE_ID_MISMATCH")
FROM tasks_and_task_queues
WHERE namespace_id = $namespace_id
AND task_queue_name = $task_queue_name
AND task_queue_type = $task_queue_type
AND task_id IS NULL;

UPSERT INTO tasks_and_task_queues (namespace_id, task_queue_name, task_queue_type, task_id, expire_at, task, task_encoding)
SELECT namespace_id, task_queue_name, task_queue_type, task_id, expire_at, task, task_encoding
FROM AS_TABLE($tasks);
`)

	namespaceID := request.NamespaceID
	queueName := request.TaskQueue
	queueType := int32(request.TaskType)
	rangeID := request.RangeID

	taskValues := make([]types.Value, 0, len(request.Tasks))
	for _, t := range request.Tasks {
		taskFields := []types.StructValueOption{
			types.StructFieldValue("namespace_id", types.UTF8Value(namespaceID)),
			types.StructFieldValue("task_queue_name", types.UTF8Value(queueName)),
			types.StructFieldValue("task_queue_type", types.Int32Value(queueType)),
			types.StructFieldValue("task_id", types.Int64Value(t.TaskId)),
			types.StructFieldValue("task", types.BytesValue(t.Task.Data)),
			types.StructFieldValue("task_encoding", types.UTF8Value(t.Task.EncodingType.String())),
		}
		if t.ExpiryTime != nil {
			taskFields = append(taskFields, types.StructFieldValue("expire_at", types.OptionalValue(types.TimestampValueFromTime(ToYDBDateTime(*t.ExpiryTime)))))
		} else {
			taskFields = append(taskFields, types.StructFieldValue("expire_at", types.NullValue(types.TypeTimestamp)))
		}
		taskValues = append(taskValues, types.StructValue(taskFields...))
	}
	err := d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(namespaceID)),
		table.ValueParam("$task_queue_name", types.UTF8Value(queueName)),
		table.ValueParam("$task_queue_type", types.Int32Value(queueType)),
		table.ValueParam("$range_id", types.Int64Value(rangeID)),
		table.ValueParam("$tasks", types.ListValue(taskValues...)),
	))
	if err != nil {
		details := fmt.Sprintf("name: %v, type: %v, rangeID: %v", queueName, queueType, rangeID)
		if xydb.IsPreconditionFailedAndContains(err, "RANGE_ID_MISMATCH") {
			err = xydb.WrapErrorAsRootCause(&p.ConditionFailedError{
				Msg: fmt.Sprintf("CreateTasks: range id mismatch (%s)", details),
			})
		}
		return nil, xydb.ConvertToTemporalError("CreateTasks", err, details)
	}
	return &p.CreateTasksResponse{}, nil
}

// GetTasks get a task
func (d *MatchingTaskStore) GetTasks(
	ctx context.Context,
	request *p.GetTasksRequest,
) (resp *p.InternalGetTasksResponse, err error) {
	queueName := request.TaskQueue
	queueType := int32(request.TaskType)

	defer func() {
		if err != nil {
			details := fmt.Sprintf("name: %v, type: %v", queueName, queueType)
			err = xydb.ConvertToTemporalError("GetTasks", err, details)
		}
	}()

	inclusiveMinTaskID := request.InclusiveMinTaskID
	exclusiveMaxTaskID := request.ExclusiveMaxTaskID

	var pageToken matchingTaskPageToken
	if len(request.NextPageToken) != 0 {
		if err = pageToken.deserialize(request.NextPageToken); err != nil {
			return nil, err
		}
		inclusiveMinTaskID = pageToken.TaskID
	}
	template := d.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $task_queue_name AS utf8;
DECLARE $task_queue_type AS int32;
DECLARE $task_id_gte AS int64;
DECLARE $task_id_lt AS int64;
DECLARE $page_size AS int32;
DECLARE $expire_at_gte AS timestamp;

SELECT namespace_id, task_queue_name, task_queue_type, task_id, task, task_encoding
FROM tasks_and_task_queues
WHERE namespace_id = $namespace_id
AND task_queue_name = $task_queue_name
AND task_queue_type = $task_queue_type
AND task_id >= $task_id_gte
AND task_id < $task_id_lt
AND (expire_at IS NULL OR expire_at >= $expire_at_gte)
ORDER BY namespace_id, task_queue_name, task_queue_type, task_id
LIMIT $page_size;
`)

	res, err := d.client.Do2(ctx, template, xydb.OnlineReadOnlyTxControl(), func() *table.QueryParameters {
		return table.NewQueryParameters(
			table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
			table.ValueParam("$task_queue_name", types.UTF8Value(queueName)),
			table.ValueParam("$task_queue_type", types.Int32Value(queueType)),
			table.ValueParam("$task_id_gte", types.Int64Value(inclusiveMinTaskID)),
			table.ValueParam("$task_id_lt", types.Int64Value(exclusiveMaxTaskID)),
			table.ValueParam("$expire_at_gte", types.TimestampValueFromTime(ToYDBDateTime(time.Now()))),
			table.ValueParam("$page_size", types.Int32Value(int32(request.PageSize))),
		)
	}, table.WithIdempotent())
	if err != nil {
		return
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return
	}

	var lastTaskID int64
	var tasks []*commonpb.DataBlob
	for res.NextRow() {
		t, err := scanTask(res)
		if err != nil {
			return nil, err
		}
		lastTaskID = t.id
		tasks = append(tasks, p.NewDataBlob(t.data, t.encoding))
	}

	resp = &p.InternalGetTasksResponse{
		Tasks: tasks,
	}
	if len(tasks) == request.PageSize {
		nextTaskID := lastTaskID + 1
		if nextTaskID < exclusiveMaxTaskID {
			nextPageToken := &matchingTaskPageToken{
				TaskID: nextTaskID,
			}
			if resp.NextPageToken, err = nextPageToken.serialize(); err != nil {
				return nil, err
			}
		}
	}

	return resp, nil
}

// CompleteTask delete a task
func (d *MatchingTaskStore) CompleteTask(
	ctx context.Context,
	request *p.CompleteTaskRequest,
) error {
	queueName := request.TaskQueue.TaskQueueName
	queueType := int32(request.TaskQueue.TaskQueueType)
	taskID := request.TaskID
	template := d.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $task_queue_name AS utf8;
DECLARE $task_queue_type AS int32;
DECLARE $task_id AS int64;

DELETE FROM tasks_and_task_queues
WHERE namespace_id = $namespace_id
AND task_queue_name = $task_queue_name
AND task_queue_type = $task_queue_type
AND task_id = $task_id;
`)
	if err := d.client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(request.TaskQueue.NamespaceID)),
		table.ValueParam("$task_queue_name", types.UTF8Value(queueName)),
		table.ValueParam("$task_queue_type", types.Int32Value(queueType)),
		table.ValueParam("$task_id", types.Int64Value(taskID)),
	)); err != nil {
		details := fmt.Sprintf("name: %v, type: %v, task_id: %v", queueName, queueType, taskID)
		return xydb.ConvertToTemporalError("CompleteTask", err, details)
	}
	return nil
}

// CompleteTasksLessThan deletes all tasks less than the given task id. This API ignores the
// Limit request parameter i.e. either all tasks leq the task_id will be deleted or an error will
// be returned to the caller
func (d *MatchingTaskStore) CompleteTasksLessThan(
	ctx context.Context,
	request *p.CompleteTasksLessThanRequest,
) (int, error) {
	queueName := request.TaskQueueName
	queueType := int32(request.TaskType)
	taskIDLt := request.ExclusiveMaxTaskID
	params := table.NewQueryParameters(
		table.ValueParam("$namespace_id", types.UTF8Value(request.NamespaceID)),
		table.ValueParam("$task_queue_name", types.UTF8Value(request.TaskQueueName)),
		table.ValueParam("$task_queue_type", types.Int32Value(int32(request.TaskType))),
		table.ValueParam("$task_id_lt", types.Int64Value(taskIDLt)),
	)
	template := d.client.AddQueryPrefix(`
DECLARE $namespace_id AS utf8;
DECLARE $task_queue_name AS utf8;
DECLARE $task_queue_type AS int32;
DECLARE $task_id_lt AS int64;

DELETE FROM tasks_and_task_queues
WHERE namespace_id = $namespace_id
AND task_queue_name = $task_queue_name
AND task_queue_type = $task_queue_type
AND task_id < $task_id_lt;
`)

	if err := d.client.Write(ctx, template, params); err != nil {
		details := fmt.Sprintf("name: %v, type: %v, task_id_lt: %v", queueName, queueType, taskIDLt)
		return 0, xydb.ConvertToTemporalError("CompleteTasksLessThan", err, details)
	}
	return p.UnknownNumRowsAffected, nil
}

func (d *MatchingTaskStore) GetName() string {
	return ydbPersistenceName
}

func (d *MatchingTaskStore) Close() {
}

type task struct {
	id       int64
	data     []byte
	encoding string
}

func scanTask(res result.Result) (*task, error) {
	t := &task{}
	if err := res.ScanNamed(
		named.OptionalWithDefault("task_id", &t.id),
		named.OptionalWithDefault("task", &t.data),
		named.OptionalWithDefault("task_encoding", &t.encoding),
	); err != nil {
		return nil, fmt.Errorf("failed to scan task: %w", err)
	}
	return t, nil
}

type taskQueue struct {
	rangeID  int64
	data     []byte
	encoding string
}

func scanTaskQueue(res result.Result) (*taskQueue, error) {
	tq := &taskQueue{}
	if err := res.ScanNamed(
		named.OptionalWithDefault("range_id", &tq.rangeID),
		named.OptionalWithDefault("task_queue", &tq.data),
		named.OptionalWithDefault("task_queue_encoding", &tq.encoding),
	); err != nil {
		return nil, fmt.Errorf("failed to scan task queue: %w", err)
	}
	return tq, nil
}

type taskQueueUserData struct {
	taskQueue string
	version   int64
	data      []byte
	encoding  string
}

func scanTaskQueueUserData(res result.Result) (*taskQueueUserData, error) {
	tq := &taskQueueUserData{}
	if err := res.ScanNamed(
		named.OptionalWithDefault("task_queue_name", &tq.taskQueue),
		named.OptionalWithDefault("version", &tq.version),
		named.OptionalWithDefault("data", &tq.data),
		named.OptionalWithDefault("data_encoding", &tq.encoding),
	); err != nil {
		return nil, fmt.Errorf("failed to scan task queue user data: %w", err)
	}
	return tq, nil
}
