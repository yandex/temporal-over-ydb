package rows

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/tasks"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/executor"
	baserows "github.com/yandex/temporal-over-ydb/persistence/pkg/base/rows"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

type transactionImpl struct {
	client     *conn.Client
	assertions []assertion
	shardID    int32

	ExecutionsTableRowsToInsert           []types.Value
	ExecutionsStateItemsTableRowsToDelete []types.Value
	HistoryTableRowsToInsert              []types.Value
	DeleteQueries                         []*conn.Query
	UpsertQueries                         []*conn.Query
}

func (f *transactionImpl) createHistoryTaskRows(category tasks.Category, historyTasks []p.InternalHistoryTask) []types.Value {
	isScheduledTask := category.Type() == tasks.CategoryTypeScheduled
	var rows []types.Value
	for _, t := range historyTasks {
		if isScheduledTask {
			rows = append(rows, createStructValue(map[string]types.Value{
				"shard_id":           types.Uint32Value(ToShardIDColumnValue(f.shardID)),
				"task_category_id":   types.OptionalValue(types.Int32Value(int32(category.ID()))),
				"task_visibility_ts": types.OptionalValue(types.TimestampValueFromTime(conn.ToYDBDateTime(t.Key.FireTime))),
				"task_id":            types.OptionalValue(types.Int64Value(t.Key.TaskID)),
				"data":               types.OptionalValue(types.BytesValue(t.Blob.Data)),
				"data_encoding":      types.OptionalValue(f.client.EncodingTypeValue(t.Blob.EncodingType)),
			}))
		} else {
			rows = append(rows, createStructValue(map[string]types.Value{
				"shard_id":           types.Uint32Value(ToShardIDColumnValue(f.shardID)),
				"task_category_id":   types.OptionalValue(types.Int32Value(int32(category.ID()))),
				"task_visibility_ts": types.NullValue(types.TypeTimestamp),
				"task_id":            types.OptionalValue(types.Int64Value(t.Key.TaskID)),
				"data":               types.OptionalValue(types.BytesValue(t.Blob.Data)),
				"data_encoding":      types.OptionalValue(f.client.EncodingTypeValue(t.Blob.EncodingType)),
			}))
		}
	}
	return rows
}

func (f *transactionImpl) getIdentifiedStateItem(namespaceID primitives.UUID, workflowID string, runID primitives.UUID, itemType int32, itemID int64, blob *commonpb.DataBlob) types.Value {
	return f.createExecutionsTableRow(f.shardID, map[string]types.Value{
		"namespace_id":  f.client.NamespaceIDValueFromUUID(namespaceID),
		"workflow_id":   types.UTF8Value(workflowID),
		"run_id":        f.client.RunIDValueFromUUID(runID),
		"event_type":    types.Int32Value(itemType),
		"event_id":      types.Int64Value(itemID),
		"data":          types.BytesValue(blob.Data),
		"data_encoding": f.client.EncodingTypeValue(blob.EncodingType),
	})
}

func (f *transactionImpl) getNamedStateItem(namespaceID primitives.UUID, workflowID string, runID primitives.UUID, itemType int32, itemName string, blob *commonpb.DataBlob) types.Value {
	return f.createExecutionsTableRow(f.shardID, map[string]types.Value{
		"namespace_id":  f.client.NamespaceIDValueFromUUID(namespaceID),
		"workflow_id":   types.UTF8Value(workflowID),
		"run_id":        f.client.RunIDValueFromUUID(runID),
		"event_type":    types.Int32Value(itemType),
		"event_name":    types.UTF8Value(itemName),
		"data":          types.BytesValue(blob.Data),
		"data_encoding": f.client.EncodingTypeValue(blob.EncodingType),
	})
}

func (f *transactionImpl) getSignalRequestedItem(namespaceID primitives.UUID, workflowID string, runID primitives.UUID, signalRequestedID string) types.Value {
	return f.createExecutionsTableRow(f.shardID, map[string]types.Value{
		"namespace_id": f.client.NamespaceIDValueFromUUID(namespaceID),
		"workflow_id":  types.UTF8Value(workflowID),
		"run_id":       f.client.RunIDValueFromUUID(runID),
		"event_type":   types.Int32Value(baserows.ItemTypeSignalRequested),
		"event_name":   types.UTF8Value(signalRequestedID),
	})
}

func (f *transactionImpl) getBufferedEventItemRow(namespaceID primitives.UUID, workflowID string, runID primitives.UUID, bufferedEventID string, bufferedEvent *commonpb.DataBlob) types.Value {
	return f.createExecutionsTableRow(f.shardID, map[string]types.Value{
		"namespace_id":  f.client.NamespaceIDValueFromUUID(namespaceID),
		"workflow_id":   types.UTF8Value(workflowID),
		"run_id":        f.client.RunIDValueFromUUID(runID),
		"event_type":    types.Int32Value(baserows.ItemTypeBufferedEvent),
		"event_name":    types.OptionalValue(types.UTF8Value(bufferedEventID)),
		"data":          types.OptionalValue(types.BytesValue(bufferedEvent.Data)),
		"data_encoding": types.OptionalValue(f.client.EncodingTypeValue(bufferedEvent.EncodingType)),
	})
}

func (f *transactionImpl) getIdentifiedStateItemKey(namespaceID primitives.UUID, workflowID string, runID primitives.UUID, itemType int32, itemID int64) types.Value {
	return types.StructValue(
		types.StructFieldValue("shard_id", types.Uint32Value(ToShardIDColumnValue(f.shardID))),
		types.StructFieldValue("namespace_id", f.client.NamespaceIDValueFromUUID(namespaceID)),
		types.StructFieldValue("workflow_id", types.UTF8Value(workflowID)),
		types.StructFieldValue("run_id", f.client.RunIDValueFromUUID(runID)),
		types.StructFieldValue("event_type", types.Int32Value(itemType)),
		types.StructFieldValue("event_id", types.OptionalValue(types.Int64Value(itemID))),
		types.StructFieldValue("event_name", types.NullValue(types.TypeUTF8)),
	)
}

func (f *transactionImpl) getNamedStateItemKey(namespaceID primitives.UUID, workflowID string, runID primitives.UUID, itemType int32, itemName string) types.Value {
	return types.StructValue(
		types.StructFieldValue("shard_id", types.Uint32Value(ToShardIDColumnValue(f.shardID))),
		types.StructFieldValue("namespace_id", f.client.NamespaceIDValueFromUUID(namespaceID)),
		types.StructFieldValue("workflow_id", types.UTF8Value(workflowID)),
		types.StructFieldValue("run_id", f.client.RunIDValueFromUUID(runID)),
		types.StructFieldValue("event_type", types.Int32Value(itemType)),
		types.StructFieldValue("event_id", types.NullValue(types.TypeInt64)),
		types.StructFieldValue("event_name", types.OptionalValue(types.UTF8Value(itemName))),
	)
}

func (f *transactionImpl) getSignalRequestedItemKey(namespaceID primitives.UUID, workflowID string, runID primitives.UUID, signalRequestedID string) types.Value {
	return types.StructValue(
		types.StructFieldValue("shard_id", types.Uint32Value(ToShardIDColumnValue(f.shardID))),
		types.StructFieldValue("namespace_id", f.client.NamespaceIDValueFromUUID(namespaceID)),
		types.StructFieldValue("workflow_id", types.UTF8Value(workflowID)),
		types.StructFieldValue("run_id", f.client.RunIDValueFromUUID(runID)),
		types.StructFieldValue("event_type", types.Int32Value(baserows.ItemTypeSignalRequested)),
		types.StructFieldValue("event_id", types.NullValue(types.TypeInt64)),
		types.StructFieldValue("event_name", types.OptionalValue(types.UTF8Value(signalRequestedID))),
	)
}

func (f *transactionImpl) getStateMapRowsFromMutation(bufferedEventID string, workflowMutation *p.InternalWorkflowMutation) []types.Value {
	namespaceID := primitives.MustParseUUID(workflowMutation.NamespaceID)
	workflowID := workflowMutation.WorkflowID
	runID := primitives.MustParseUUID(workflowMutation.RunID)

	ups := make([]types.Value, 0)
	for itemID, item := range workflowMutation.UpsertActivityInfos {
		ups = append(ups, f.getIdentifiedStateItem(namespaceID, workflowID, runID, baserows.ItemTypeActivity, itemID, item))
	}
	for itemName, item := range workflowMutation.UpsertTimerInfos {
		ups = append(ups, f.getNamedStateItem(namespaceID, workflowID, runID, baserows.ItemTypeTimer, itemName, item))
	}
	for itemID, item := range workflowMutation.UpsertChildExecutionInfos {
		ups = append(ups, f.getIdentifiedStateItem(namespaceID, workflowID, runID, baserows.ItemTypeChildExecution, itemID, item))
	}
	for itemID, item := range workflowMutation.UpsertRequestCancelInfos {
		ups = append(ups, f.getIdentifiedStateItem(namespaceID, workflowID, runID, baserows.ItemTypeRequestCancel, itemID, item))
	}
	for itemID, item := range workflowMutation.UpsertSignalInfos {
		ups = append(ups, f.getIdentifiedStateItem(namespaceID, workflowID, runID, baserows.ItemTypeSignal, itemID, item))
	}
	for signalID := range workflowMutation.UpsertSignalRequestedIDs {
		ups = append(ups, f.getSignalRequestedItem(namespaceID, workflowID, runID, signalID))
	}
	if workflowMutation.NewBufferedEvents != nil {
		ups = append(ups, f.getBufferedEventItemRow(namespaceID, workflowID, runID, bufferedEventID, workflowMutation.NewBufferedEvents))
	}
	return ups
}

func (f *transactionImpl) getStateItemRowsFromSnapshot(s *p.InternalWorkflowSnapshot) []types.Value {
	namespaceID := primitives.MustParseUUID(s.NamespaceID)
	workflowID := s.WorkflowID
	runID := primitives.MustParseUUID(s.RunID)

	items := make([]types.Value, 0, len(s.ActivityInfos)+len(s.TimerInfos)+len(s.ChildExecutionInfos)+len(s.RequestCancelInfos)+len(s.SignalInfos)+len(s.SignalRequestedIDs))
	for id, blob := range s.ActivityInfos {
		items = append(items, f.getIdentifiedStateItem(namespaceID, workflowID, runID, baserows.ItemTypeActivity, id, blob))
	}
	for itemName, item := range s.TimerInfos {
		items = append(items, f.getNamedStateItem(namespaceID, workflowID, runID, baserows.ItemTypeTimer, itemName, item))
	}
	for itemID, item := range s.ChildExecutionInfos {
		items = append(items, f.getIdentifiedStateItem(namespaceID, workflowID, runID, baserows.ItemTypeChildExecution, itemID, item))
	}
	for itemID, item := range s.RequestCancelInfos {
		items = append(items, f.getIdentifiedStateItem(namespaceID, workflowID, runID, baserows.ItemTypeRequestCancel, itemID, item))
	}
	for itemID, item := range s.SignalInfos {
		items = append(items, f.getIdentifiedStateItem(namespaceID, workflowID, runID, baserows.ItemTypeSignal, itemID, item))
	}
	for signalID := range s.SignalRequestedIDs {
		items = append(items, f.getSignalRequestedItem(namespaceID, workflowID, runID, signalID))
	}
	return items
}

func (f *transactionImpl) getStateKeysFromMutation(workflowMutation *p.InternalWorkflowMutation) []types.Value {
	namespaceID := primitives.MustParseUUID(workflowMutation.NamespaceID)
	workflowID := workflowMutation.WorkflowID
	runID := primitives.MustParseUUID(workflowMutation.RunID)
	keys := make([]types.Value, 0, len(workflowMutation.DeleteActivityInfos)+len(workflowMutation.DeleteTimerInfos)+len(workflowMutation.DeleteChildExecutionInfos)+len(workflowMutation.DeleteRequestCancelInfos)+len(workflowMutation.DeleteSignalInfos)+len(workflowMutation.DeleteSignalRequestedIDs))
	for itemID := range workflowMutation.DeleteActivityInfos {
		keys = append(keys, f.getIdentifiedStateItemKey(namespaceID, workflowID, runID, baserows.ItemTypeActivity, itemID))
	}
	for itemName := range workflowMutation.DeleteTimerInfos {
		keys = append(keys, f.getNamedStateItemKey(namespaceID, workflowID, runID, baserows.ItemTypeTimer, itemName))
	}
	for itemID := range workflowMutation.DeleteChildExecutionInfos {
		keys = append(keys, f.getIdentifiedStateItemKey(namespaceID, workflowID, runID, baserows.ItemTypeChildExecution, itemID))
	}
	for itemID := range workflowMutation.DeleteRequestCancelInfos {
		keys = append(keys, f.getIdentifiedStateItemKey(namespaceID, workflowID, runID, baserows.ItemTypeRequestCancel, itemID))
	}
	for itemID := range workflowMutation.DeleteSignalInfos {
		keys = append(keys, f.getIdentifiedStateItemKey(namespaceID, workflowID, runID, baserows.ItemTypeSignal, itemID))
	}
	for signalID := range workflowMutation.DeleteSignalRequestedIDs {
		keys = append(keys, f.getSignalRequestedItemKey(namespaceID, workflowID, runID, signalID))
	}
	return keys
}

func (f *transactionImpl) InsertHistoryTasks(insertTasks map[tasks.Category][]p.InternalHistoryTask) {
	for category, tasksByCategory := range insertTasks {
		f.HistoryTableRowsToInsert = append(f.HistoryTableRowsToInsert, f.createHistoryTaskRows(category, tasksByCategory)...)
	}
}

func (f *transactionImpl) UpsertHistoryTasks(category tasks.Category, tasksToUpsert []p.InternalHistoryTask) error {
	return errors.New("not implemented")
}

func (f *transactionImpl) UpsertShard(rangeID int64, shardInfo *commonpb.DataBlob) {
	f.ExecutionsTableRowsToInsert = append(f.ExecutionsTableRowsToInsert, f.createExecutionsTableRow(f.shardID, map[string]types.Value{
		"range_id":       types.Int64Value(rangeID),
		"namespace_id":   f.client.EmptyNamespaceIDValue(),
		"workflow_id":    types.UTF8Value(""),
		"run_id":         f.client.EmptyRunIDValue(),
		"shard":          types.BytesValue(shardInfo.Data),
		"shard_encoding": f.client.EncodingTypeValue(shardInfo.EncodingType),
	}))
}

func (f *transactionImpl) UpsertCurrentWorkflow(namespaceID primitives.UUID, workflowID string, currentRunID primitives.UUID, executionStateBlob *commonpb.DataBlob, lastWriteVersion int64, state enums.WorkflowExecutionState) {
	f.ExecutionsTableRowsToInsert = append(f.ExecutionsTableRowsToInsert, f.createExecutionsTableRow(f.shardID, map[string]types.Value{
		"namespace_id":             f.client.NamespaceIDValueFromUUID(namespaceID),
		"workflow_id":              types.UTF8Value(workflowID),
		"run_id":                   f.client.EmptyRunIDValue(),
		"current_run_id":           f.client.RunIDValueFromUUID(currentRunID),
		"execution_state":          types.BytesValue(executionStateBlob.Data),
		"execution_state_encoding": f.client.EncodingTypeValue(executionStateBlob.EncodingType),
		"last_write_version":       types.Int64Value(lastWriteVersion),
		"state":                    types.Int32Value(int32(state)),
	}))
}

func (f *transactionImpl) HandleWorkflowMutation(mutation *p.InternalWorkflowMutation) {
	bufferedEventID := primitives.NewUUID().String()
	f.ExecutionsTableRowsToInsert = append(f.ExecutionsTableRowsToInsert, f.createWorkflowExecutionRow(
		primitives.MustParseUUID(mutation.NamespaceID),
		mutation.WorkflowID,
		primitives.MustParseUUID(mutation.RunID),
		mutation.ExecutionInfoBlob,
		mutation.ExecutionStateBlob,
		mutation.NextEventID,
		mutation.DBRecordVersion,
		mutation.Checksum,
	))
	f.ExecutionsTableRowsToInsert = append(f.ExecutionsTableRowsToInsert, f.getStateMapRowsFromMutation(bufferedEventID, mutation)...)
	f.ExecutionsStateItemsTableRowsToDelete = append(f.ExecutionsStateItemsTableRowsToDelete, f.getStateKeysFromMutation(mutation)...)
	f.InsertHistoryTasks(mutation.Tasks)
}

func (f *transactionImpl) HandleWorkflowSnapshot(snapshot *p.InternalWorkflowSnapshot) {
	f.InsertHistoryTasks(snapshot.Tasks)
	f.ExecutionsTableRowsToInsert = append(f.ExecutionsTableRowsToInsert, f.createWorkflowExecutionRow(
		primitives.MustParseUUID(snapshot.NamespaceID),
		snapshot.WorkflowID,
		primitives.MustParseUUID(snapshot.RunID),
		snapshot.ExecutionInfoBlob,
		snapshot.ExecutionStateBlob,
		snapshot.NextEventID,
		snapshot.DBRecordVersion,
		snapshot.Checksum,
	))
	f.ExecutionsTableRowsToInsert = append(f.ExecutionsTableRowsToInsert, f.getStateItemRowsFromSnapshot(snapshot)...)
}

func (f *transactionImpl) DeleteBufferedEvents(namespaceID primitives.UUID, workflowID string, runID primitives.UUID) {
	f.DeleteQueries = append(f.DeleteQueries, conn.NewQueryPart(
		map[string]types.Value{
			"shard_id":            types.Uint32Value(ToShardIDColumnValue(f.shardID)),
			"namespace_id":        f.client.NamespaceIDValueFromUUID(namespaceID),
			"workflow_id":         types.UTF8Value(workflowID),
			"run_id":              f.client.RunIDValueFromUUID(runID),
			"buffered_event_type": types.Int32Value(baserows.ItemTypeBufferedEvent),
		},
		`
DELETE FROM executions
WHERE $incorrect = 0
AND shard_id = $%[1]sshard_id
AND namespace_id = $%[1]snamespace_id
AND workflow_id = $%[1]sworkflow_id
AND run_id = $%[1]srun_id
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type = $%[1]sbuffered_event_type;
`))
}

func (f *transactionImpl) DeleteStateItems(namespaceID primitives.UUID, workflowID string, runID primitives.UUID) {
	f.DeleteQueries = append(f.DeleteQueries, conn.NewQueryPart(
		map[string]types.Value{
			"shard_id":     types.Uint32Value(ToShardIDColumnValue(f.shardID)),
			"namespace_id": f.client.NamespaceIDValueFromUUID(namespaceID),
			"workflow_id":  types.UTF8Value(workflowID),
			"run_id":       f.client.RunIDValueFromUUID(runID),
		},
		`
DELETE FROM executions
WHERE $incorrect = 0
AND shard_id = $%[1]sshard_id
AND namespace_id = $%[1]snamespace_id
AND workflow_id = $%[1]sworkflow_id
AND run_id = $%[1]srun_id
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NOT NULL;
`,
	))
}

func (f *transactionImpl) gatherQueries() (qs []*conn.Query) {
	// Delete queries go first
	qs = append(qs, f.DeleteQueries...)

	if len(f.ExecutionsStateItemsTableRowsToDelete) > 0 {
		qs = append(qs, conn.NewQueryPart(
			map[string]types.Value{
				"events_to_delete": types.ListValue(f.ExecutionsStateItemsTableRowsToDelete...),
			},
			`
DELETE FROM executions
ON SELECT
shard_id,
namespace_id,
workflow_id,
run_id,
NULL as task_id,
NULL as task_category_id,
NULL as task_visibility_ts,
event_type,
event_id,
event_name
FROM AS_TABLE($%[1]sevents_to_delete)
WHERE $incorrect = 0;
`,
		))
	}

	if len(f.ExecutionsTableRowsToInsert) > 0 {
		qs = append(qs, conn.NewSingleArgQueryPart(
			"rows_to_add",
			types.ListValue(f.ExecutionsTableRowsToInsert...),
			`
UPSERT INTO executions (shard_id, namespace_id, workflow_id, run_id, task_category_id, task_id, event_type, event_id, event_name, task_visibility_ts, data, data_encoding, execution, execution_encoding, execution_state, execution_state_encoding, checksum, checksum_encoding, next_event_id, db_record_version, current_run_id, last_write_version, state, range_id, shard, shard_encoding)
SELECT shard_id, namespace_id, workflow_id, run_id, task_category_id, task_id, event_type, event_id, event_name, task_visibility_ts, data, data_encoding, execution, execution_encoding, execution_state, execution_state_encoding, checksum, checksum_encoding, next_event_id, db_record_version, current_run_id, last_write_version, state, range_id, shard, shard_encoding
FROM AS_TABLE($%[1]srows_to_add)
WHERE $incorrect = 0;
`))
	}

	if len(f.HistoryTableRowsToInsert) > 0 {
		qs = append(qs, conn.NewSingleArgQueryPart(
			"history_tasks",
			types.ListValue(f.HistoryTableRowsToInsert...),
			`
UPSERT INTO executions (shard_id, namespace_id, workflow_id, run_id, task_category_id, task_visibility_ts, task_id, event_type, event_id, event_name, data, data_encoding)
SELECT shard_id, NULL, NULL, NULL, task_category_id, task_visibility_ts, task_id, NULL, NULL, NULL, data, data_encoding
FROM AS_TABLE($%[1]shistory_tasks)
WHERE $incorrect = 0;
`))
	}

	qs = append(qs, f.UpsertQueries...)
	return qs
}

func (f *transactionImpl) AssertShard(lockForUpdate bool, rangeIDEqualTo int64) {
	a := &shardAssertion{
		Prefix:         fmt.Sprintf("s_%d_", len(f.assertions)),
		ShardID:        f.shardID,
		RangeIDEqualTo: rangeIDEqualTo,
	}
	f.assertions = append(f.assertions, a)
}

func (f *transactionImpl) AssertCurrentWorkflow(
	lockForUpdate bool, namespaceID primitives.UUID, workflowID string,
	currentRunIDNotEqualTo primitives.UUID, currentRunIDEqualTo primitives.UUID,
	mustNotExist bool, currentRunIDAndLastWriteVersionEqualTo *executor.CurrentRunIDAndLastWriteVersion,
) {
	a := &currentWorkflowAssertion{
		client:                                 f.client,
		Prefix:                                 fmt.Sprintf("cw_%d_", len(f.assertions)),
		ShardID:                                f.shardID,
		NamespaceID:                            namespaceID,
		WorkflowID:                             workflowID,
		CurrentRunIDNotEqualTo:                 currentRunIDNotEqualTo,
		CurrentRunIDEqualTo:                    currentRunIDEqualTo,
		MustNotExist:                           mustNotExist,
		CurrentRunIDAndLastWriteVersionEqualTo: currentRunIDAndLastWriteVersionEqualTo,
	}
	f.assertions = append(f.assertions, a)
}

func (f *transactionImpl) AssertWorkflowExecution(
	lockForUpdate bool, namespaceID primitives.UUID, workflowID string, runID primitives.UUID,
	recordVersionEqualTo *int64, mustNotExist bool,
) {
	a := &workflowExecutionAssertion{
		client:               f.client,
		Prefix:               fmt.Sprintf("we_%d_", len(f.assertions)),
		ShardID:              f.shardID,
		NamespaceID:          namespaceID,
		WorkflowID:           workflowID,
		RunID:                runID,
		RecordVersionEqualTo: recordVersionEqualTo,
		MustNotExist:         mustNotExist,
	}
	f.assertions = append(f.assertions, a)
}

func (f *transactionImpl) Execute(ctx context.Context) error {
	params := make([]table.ParameterOption, 0, 32)
	yqlParts := make([]string, 0, 10)
	incorrectVarAssignmentParts := []string{}
	incorrectVarAssignmentHead := "$incorrect = SELECT count(*) AS incorrect FROM AS_TABLE(AsList(\n"

	for _, a := range f.assertions {
		yqlParts = append(yqlParts, a.toMissingRowVarAssignment())
		yqlParts = append(yqlParts, a.toRowVarAssignment())
		params = append(params, a.toParams()...)
		incorrectVarAssignmentParts = append(incorrectVarAssignmentParts, a.toInvalidConditionQuery())
	}
	incorrectVarAssignmentTail := ")) WHERE correct = false;"
	yqlParts = append(yqlParts, incorrectVarAssignmentHead+strings.Join(incorrectVarAssignmentParts, ",\n")+incorrectVarAssignmentTail)
	yqlParts = append(yqlParts, "SELECT $incorrect AS incorrect;")
	for _, a := range f.assertions {
		yqlParts = append(yqlParts, a.toRowQuery())
	}

	for i, r := range f.gatherQueries() {
		alias := fmt.Sprintf("q_%d_", i)
		s := r.ToStmt(alias, nil)

		yqlParts = append(yqlParts, s.Queries...)
		params = append(params, s.Params...)
	}

	declareSection, err := sugar.GenerateDeclareSection(params)
	if err != nil {
		return err
	}
	yql := f.client.AddQueryPrefix(declareSection + "\n" + strings.Join(yqlParts, "\n"))
	// fmt.Println("---")
	// for _, p := range params {
	//	fmt.Println()
	//	fmt.Println(p.Name(), ":", p.Value().Yql())
	//	fmt.Println()
	// }
	// fmt.Println(yql)
	// fmt.Println("---")
	res, err := f.client.Do(ctx, yql, table.SerializableReadWriteTxControl(table.CommitTx()), table.NewQueryParameters(params...))
	if err != nil {
		return err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()

	if err = res.NextResultSetErr(ctx); err != nil {
		return err
	}
	if !res.NextRow() {
		return fmt.Errorf("failed to count incorrect rows: no rows")
	}
	var count uint64
	if err = res.ScanNamed(
		named.OptionalWithDefault("incorrect", &count),
	); err != nil {
		return fmt.Errorf("failed to count incorrect rows: %w", err)
	}
	if count == 0 {
		return nil
	}

	for _, a := range f.assertions {
		if err = res.NextResultSetErr(ctx); err != nil {
			return err
		}
		if err = a.extractError(res); err != nil {
			return conn.WrapErrorAsRootCause(err)
		}
	}
	panic("failed to extract error")
}

var _ executor.Transaction = (*transactionImpl)(nil)
