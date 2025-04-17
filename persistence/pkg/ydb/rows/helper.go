package rows

import (
	"math"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/primitives"
)

const SlowDeleteBatchSize = 10000

var NumHistoryShards = 1024

func ToShardIDColumnValue(shardID int32) uint32 {
	// Uniformly spread shard across (0, math.MaxUint32) interval
	step := math.MaxUint32/NumHistoryShards - 1
	return uint32(shardID * int32(step))
}

func structNullValue(name string, t types.Type) types.StructValueOption {
	return types.StructFieldValue(name, types.NullValue(t))
}

func (f *transactionImpl) getNullStructFieldValues() map[string]types.StructValueOption {
	return map[string]types.StructValueOption{
		"namespace_id":             structNullValue("namespace_id", f.client.NamespaceIDType()),
		"workflow_id":              structNullValue("workflow_id", types.TypeUTF8),
		"run_id":                   structNullValue("run_id", f.client.RunIDType()),
		"task_category_id":         structNullValue("task_category_id", types.TypeInt32),
		"task_id":                  structNullValue("task_id", types.TypeInt64),
		"event_type":               structNullValue("event_type", types.TypeInt32),
		"event_id":                 structNullValue("event_id", types.TypeInt64),
		"event_name":               structNullValue("event_name", types.TypeUTF8),
		"task_visibility_ts":       structNullValue("task_visibility_ts", types.TypeTimestamp),
		"data":                     structNullValue("data", types.TypeBytes),
		"data_encoding":            structNullValue("data_encoding", f.client.EncodingType()),
		"execution":                structNullValue("execution", types.TypeBytes),
		"execution_encoding":       structNullValue("execution_encoding", f.client.EncodingType()),
		"execution_state":          structNullValue("execution_state", types.TypeBytes),
		"execution_state_encoding": structNullValue("execution_state_encoding", f.client.EncodingType()),
		"checksum":                 structNullValue("checksum", types.TypeBytes),
		"checksum_encoding":        structNullValue("checksum_encoding", f.client.EncodingType()),
		"next_event_id":            structNullValue("next_event_id", types.TypeInt64),
		"db_record_version":        structNullValue("db_record_version", types.TypeInt64),
		"current_run_id":           structNullValue("current_run_id", f.client.RunIDType()),
		"last_write_version":       structNullValue("last_write_version", types.TypeInt64),
		"state":                    structNullValue("state", types.TypeInt32),

		"range_id":       structNullValue("range_id", types.TypeInt64),
		"shard":          structNullValue("shard", types.TypeBytes),
		"shard_encoding": structNullValue("shard_encoding", f.client.EncodingType()),
	}
}

func (f *transactionImpl) createExecutionsTableRow(shardID int32, fields map[string]types.Value) types.Value {
	rv := make([]types.StructValueOption, 0, len(fields)+1)
	rv = append(rv, types.StructFieldValue("shard_id", types.Uint32Value(ToShardIDColumnValue(shardID))))
	for k, nullValue := range f.getNullStructFieldValues() {
		if value, ok := fields[k]; ok {
			rv = append(rv, types.StructFieldValue(k, types.OptionalValue(value)))
		} else {
			rv = append(rv, nullValue)
		}
	}
	return types.StructValue(rv...)
}

func createStructValue(fields map[string]types.Value) types.Value {
	rv := make([]types.StructValueOption, 0, len(fields))
	for k, v := range fields {
		rv = append(rv, types.StructFieldValue(k, v))
	}
	return types.StructValue(rv...)
}

func (f *transactionImpl) createWorkflowExecutionRow(
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	executionInfoBlob *commonpb.DataBlob,
	executionStateBlob *commonpb.DataBlob,
	nextEventID int64,
	dbRecordVersion int64,
	checksumBlob *commonpb.DataBlob,
) types.Value {
	return f.createExecutionsTableRow(f.shardID, map[string]types.Value{
		"namespace_id":             f.client.NamespaceIDValueFromUUID(namespaceID),
		"workflow_id":              types.UTF8Value(workflowID),
		"run_id":                   f.client.RunIDValueFromUUID(runID),
		"execution":                types.BytesValue(executionInfoBlob.Data),
		"execution_encoding":       f.client.EncodingTypeValue(executionInfoBlob.EncodingType),
		"execution_state":          types.BytesValue(executionStateBlob.Data),
		"execution_state_encoding": f.client.EncodingTypeValue(executionStateBlob.EncodingType),
		"checksum":                 types.BytesValue(checksumBlob.Data),
		"checksum_encoding":        f.client.EncodingTypeValue(checksumBlob.EncodingType),
		"next_event_id":            types.Int64Value(nextEventID),
		"db_record_version":        types.Int64Value(dbRecordVersion),
	})
}
