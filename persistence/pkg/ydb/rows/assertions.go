package rows

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"

	baseerrors "github.com/yandex/temporal-over-ydb/persistence/pkg/base/errors"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/executor"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

func getCurrentWorkflowConflictError(
	msg string,
	executionStateBlob *commonpb.DataBlob,
	lastWriteVersion int64,
) error {
	executionState, err := serialization.WorkflowExecutionStateFromBlob(executionStateBlob.Data, executionStateBlob.EncodingType.String())
	if err != nil {
		return baseerrors.NewInternalF("failed to get execution state from blob: %v", err)
	}
	return &persistence.CurrentWorkflowConditionFailedError{
		Msg:              msg,
		RequestID:        executionState.CreateRequestId,
		RunID:            executionState.RunId,
		State:            executionState.State,
		Status:           executionState.Status,
		LastWriteVersion: lastWriteVersion,
	}
}

type shardAssertion struct {
	Prefix         string
	ShardID        int32
	RangeIDEqualTo int64
}

func (cond *shardAssertion) toMissingRowVarAssignment() string {
	return fmt.Sprintf(`$%[1]smissing_shard_row = AsStruct(
	NULL AS range_id,
	false AS correct
);`, cond.Prefix)
}

func (cond *shardAssertion) toRowVarAssignment() string {
	return fmt.Sprintf(`
$%[1]sshard_row = SELECT (
	range_id AS range_id,
	(range_id = $%[1]sexpected_range_id) AS correct
) FROM executions
WHERE shard_id = $%[1]sshard_id
AND namespace_id = ""
AND workflow_id = ""
AND run_id = ""
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`, cond.Prefix)
}

func (cond *shardAssertion) toInvalidConditionQuery() string {
	return fmt.Sprintf("Coalesce($%[1]sshard_row, $%[1]smissing_shard_row)", cond.Prefix)
}

func (cond *shardAssertion) toRowQuery() string {
	return fmt.Sprintf("SELECT * FROM AS_TABLE(ListNotNull(AsList($%[1]sshard_row))) WHERE $incorrect > 0;", cond.Prefix)
}

func (cond *shardAssertion) toParams() (rv []table.ParameterOption) {
	rv = append(rv, table.ValueParam("$"+cond.Prefix+"shard_id", types.Uint32Value(ToShardIDColumnValue(cond.ShardID))))
	rv = append(rv, table.ValueParam("$"+cond.Prefix+"expected_range_id", types.Int64Value(cond.RangeIDEqualTo)))
	return rv
}

func (cond *shardAssertion) extractError(res result.Result) error {
	var currRangeID int64
	var correct bool

	if !res.NextRow() {
		return &persistence.ShardOwnershipLostError{
			ShardID: cond.ShardID,
			Msg:     fmt.Sprintf("Shard %d does not exist", cond.ShardID),
		}
	}
	if err := res.ScanNamed(
		named.OptionalWithDefault("range_id", &currRangeID),
		named.OptionalWithDefault("correct", &correct),
	); err != nil {
		return fmt.Errorf("failed to scan shard row: %w", err)
	}

	if currRangeID == cond.RangeIDEqualTo {
		if !correct {
			panic("XXX")
		}
		return nil
	}
	return &persistence.ShardOwnershipLostError{
		ShardID: cond.ShardID,
		Msg: fmt.Sprintf("Encounter shard ownership lost, request range ID: %d, actual range ID: %d",
			cond.RangeIDEqualTo, currRangeID),
	}
}

type currentWorkflowAssertion struct {
	client      *conn.Client
	Prefix      string
	WorkflowID  string
	ShardID     int32
	NamespaceID primitives.UUID
	// condition, one of
	CurrentRunIDNotEqualTo                 primitives.UUID
	CurrentRunIDEqualTo                    primitives.UUID
	MustNotExist                           bool
	CurrentRunIDAndLastWriteVersionEqualTo *executor.CurrentRunIDAndLastWriteVersion
}

func (cond *currentWorkflowAssertion) toMissingRowVarAssignment() string {
	if cond.MustNotExist {
		return fmt.Sprintf(`$%[1]smissing_curr_exec_row = AsStruct(
	NULL AS workflow_id,
	NULL AS run_id,
	true AS correct,
	NULL AS execution_state,
	NULL AS execution_state_encoding,
	NULL AS current_run_id,
	NULL AS last_write_version,
	NULL AS state
);`, cond.Prefix)
	} else {
		return fmt.Sprintf(`$%[1]smissing_curr_exec_row = AsStruct(
	NULL AS workflow_id,
	NULL AS run_id,
	false AS correct,
	NULL AS execution_state,
	NULL AS execution_state_encoding,
	NULL AS current_run_id,
	NULL AS last_write_version,
	NULL AS state
);`, cond.Prefix)
	}
}

func (cond *currentWorkflowAssertion) toParams() (rv []table.ParameterOption) {
	rv = append(rv, table.ValueParam("$"+cond.Prefix+"shard_id", types.Uint32Value(ToShardIDColumnValue(cond.ShardID))))
	rv = append(rv, table.ValueParam("$"+cond.Prefix+"namespace_id", cond.client.NamespaceIDValueFromUUID(cond.NamespaceID)))
	rv = append(rv, table.ValueParam("$"+cond.Prefix+"workflow_id", types.UTF8Value(cond.WorkflowID)))
	if cond.MustNotExist {
	} else if len(cond.CurrentRunIDNotEqualTo) > 0 {
		currentRunIDNE := cond.Prefix + "current_run_id_ne"
		rv = append(rv, table.ValueParam(currentRunIDNE, cond.client.RunIDValueFromUUID(cond.CurrentRunIDNotEqualTo)))
	} else if len(cond.CurrentRunIDEqualTo) > 0 {
		currentRunIDEq := cond.Prefix + "current_run_id_eq"
		rv = append(rv, table.ValueParam(currentRunIDEq, cond.client.RunIDValueFromUUID(cond.CurrentRunIDEqualTo)))
	} else if cond.CurrentRunIDAndLastWriteVersionEqualTo != nil {
		lastWriteVersionEq := cond.Prefix + "last_write_version_eq"
		currentRunIDEq := cond.Prefix + "current_run_id_eq"
		rv = append(rv,
			table.ValueParam(lastWriteVersionEq, types.Int64Value(cond.CurrentRunIDAndLastWriteVersionEqualTo.LastWriteVersion)),
			table.ValueParam(currentRunIDEq, cond.client.RunIDValueFromUUID(cond.CurrentRunIDAndLastWriteVersionEqualTo.CurrentRunID)))
	} else {
		panic("empty current workflow row condition")
	}
	return rv
}

func (cond *currentWorkflowAssertion) toRowVarAssignment() string {
	var correct string
	if cond.MustNotExist {
		correct = "false"
	} else if len(cond.CurrentRunIDNotEqualTo) > 0 {
		correct = "current_run_id != $" + cond.Prefix + "current_run_id_ne"
	} else if len(cond.CurrentRunIDEqualTo) > 0 {
		correct = "current_run_id = $" + cond.Prefix + "current_run_id_eq"
	} else if cond.CurrentRunIDAndLastWriteVersionEqualTo != nil {
		lastWriteVersionEq := cond.Prefix + "last_write_version_eq"
		currentRunIDEq := cond.Prefix + "current_run_id_eq"
		correct = fmt.Sprintf("state = %d "+
			"AND last_write_version = $%s "+
			"AND current_run_id = $%s", int32(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED), lastWriteVersionEq, currentRunIDEq)
	} else {
		panic("empty current workflow row condition")
	}
	return fmt.Sprintf(`
$%[1]scurr_exec_row = SELECT (
	workflow_id	AS workflow_id,
	run_id AS run_id,
	(%[2]s) AS correct,
	execution_state AS execution_state,
	execution_state_encoding AS execution_state_encoding,
	current_run_id AS current_run_id,
	last_write_version AS last_write_version,
	state AS state
) FROM executions
WHERE shard_id = $%[1]sshard_id
AND namespace_id = $%[1]snamespace_id
AND workflow_id = $%[1]sworkflow_id
AND run_id = ""
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`, cond.Prefix, correct)
}

func (cond *currentWorkflowAssertion) toInvalidConditionQuery() string {
	return fmt.Sprintf("Coalesce($%[1]scurr_exec_row, $%[1]smissing_curr_exec_row)", cond.Prefix)
}

func (cond *currentWorkflowAssertion) toRowQuery() string {
	return fmt.Sprintf("SELECT * FROM AS_TABLE(ListNotNull(AsList($%[1]scurr_exec_row))) WHERE $incorrect > 0;", cond.Prefix)
}

func (cond *currentWorkflowAssertion) extractError(res result.Result) error {
	var data []byte
	var encoding string
	var encodingType conn.EncodingTypeRaw
	var encodingPtr interface{}
	if cond.client.UseIntForEncoding() {
		encodingPtr = &encodingType
	} else {
		encodingPtr = &encoding
	}

	var currentRunID string
	var currentRunIDBytes primitives.UUID
	var lastWriteVersion int64
	var state int32
	var correct bool

	empty := !res.NextRow()
	if !empty {
		var currentRunIDNamedValue named.Value
		if cond.client.UseBytesForRunIDs() {
			currentRunIDNamedValue = named.OptionalWithDefault("current_run_id", &currentRunIDBytes)
		} else {
			currentRunIDNamedValue = named.OptionalWithDefault("current_run_id", &currentRunID)
		}
		if err := res.ScanNamed(
			named.OptionalWithDefault("execution_state", &data),
			named.OptionalWithDefault("execution_state_encoding", encodingPtr),
			currentRunIDNamedValue,
			named.OptionalWithDefault("last_write_version", &lastWriteVersion),
			named.OptionalWithDefault("state", &state),
			named.OptionalWithDefault("correct", &correct),
		); err != nil {
			return fmt.Errorf("failed to scan current workflow row: %w", err)
		}
		if cond.client.UseBytesForRunIDs() {
			currentRunID = currentRunIDBytes.String()
		}
		if cond.client.UseIntForEncoding() {
			encoding = enumspb.EncodingType(encodingType).String()
		}
	}

	if cond.MustNotExist {
		if !empty {
			blob := persistence.NewDataBlob(data, encoding)
			msg := "must not exist"
			return getCurrentWorkflowConflictError(msg, blob, lastWriteVersion)
		}
	} else {
		if empty {
			// What about cond.currentRunIDNotEqualTo != ""?
			return &persistence.CurrentWorkflowConditionFailedError{
				Msg: "must exist",
			}
		}
		if len(cond.CurrentRunIDNotEqualTo) > 0 {
			if currentRunID == cond.CurrentRunIDNotEqualTo.String() {
				blob := persistence.NewDataBlob(data, encoding)
				msg := fmt.Sprintf("current run id %s must not be equal to %s", currentRunID, cond.CurrentRunIDNotEqualTo.String())
				return getCurrentWorkflowConflictError(msg, blob, lastWriteVersion)
			} else {
				if !correct {
					panic("XXX")
				}
				return nil
			}
		} else if len(cond.CurrentRunIDEqualTo) > 0 {
			if currentRunID != cond.CurrentRunIDEqualTo.String() {
				blob := persistence.NewDataBlob(data, encoding)
				msg := fmt.Sprintf("current run id %s must be equal to %s", currentRunID, cond.CurrentRunIDEqualTo.String())
				return getCurrentWorkflowConflictError(msg, blob, lastWriteVersion)
			} else {
				if !correct {
					panic("XXX")
				}
				return nil
				// return baseerrors.NewInternalF("failed to extract range curr wf error: the row is not valid, but current_run_id == cond.CurrentRunIDEqualTo (%s == %s)", currentRunID, cond.CurrentRunIDEqualTo.String())
			}
		} else if cond.CurrentRunIDAndLastWriteVersionEqualTo != nil {
			expectedCurrentRunID := cond.CurrentRunIDAndLastWriteVersionEqualTo.CurrentRunID.String()
			if state == int32(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED) &&
				lastWriteVersion == cond.CurrentRunIDAndLastWriteVersionEqualTo.LastWriteVersion &&
				currentRunID == expectedCurrentRunID {
				if !correct {
					panic("XXX")
				}
				return nil
				// return fmt.Errorf("failed to extract range curr wf error: the row is not valid, but CurrentRunIDAndLastWriteVersionEqualTo holds")
			}

			blob := persistence.NewDataBlob(data, encoding)
			msg := fmt.Sprintf(
				"state %d must be equal to %d, current run id %s must be equal to %s, last write version %d must be equal to %d",
				state,
				enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				currentRunID,
				cond.CurrentRunIDAndLastWriteVersionEqualTo.CurrentRunID,
				lastWriteVersion,
				cond.CurrentRunIDAndLastWriteVersionEqualTo.LastWriteVersion)
			return getCurrentWorkflowConflictError(msg, blob, lastWriteVersion)
		} else {
			panic("empty current workflow row condition")
		}
	}
	return nil
}

type workflowExecutionAssertion struct {
	client      *conn.Client
	Prefix      string
	ShardID     int32
	NamespaceID primitives.UUID
	WorkflowID  string
	RunID       primitives.UUID
	// conditions, one of
	RecordVersionEqualTo *int64
	MustNotExist         bool
}

func (cond *workflowExecutionAssertion) toMissingRowVarAssignment() string {
	if cond.MustNotExist {
		return fmt.Sprintf(`$%[1]smissing_run_row = AsStruct(
	NULL AS workflow_id,
	NULL AS run_id,
	true AS correct,
	NULL AS db_record_version
);`, cond.Prefix)
	} else {
		return fmt.Sprintf(`$%[1]smissing_run_row = AsStruct(
	NULL AS workflow_id,
	NULL AS run_id,
	false AS correct,
	NULL AS db_record_version
);`, cond.Prefix)
	}
}

func (cond *workflowExecutionAssertion) toRowVarAssignment() string {
	var correctExpr string
	if cond.MustNotExist {
		correctExpr = "false"
	} else if cond.RecordVersionEqualTo != nil {
		correctExpr = "db_record_version = $" + cond.Prefix + "db_record_version_eq"
	} else {
		panic("empty workflow execution row condition")
	}

	return fmt.Sprintf(`
$%[1]srun_row = SELECT (
	workflow_id	AS workflow_id,
	run_id AS run_id,
	(%[2]s) AS correct,
	db_record_version AS db_record_version
) FROM executions
WHERE shard_id = $%[1]sshard_id
AND namespace_id = $%[1]snamespace_id
AND workflow_id = $%[1]sworkflow_id
AND run_id = $%[1]srun_id
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`, cond.Prefix, correctExpr)
}

func (cond *workflowExecutionAssertion) toInvalidConditionQuery() string {
	return fmt.Sprintf("Coalesce($%[1]srun_row, $%[1]smissing_run_row)", cond.Prefix)
}

func (cond *workflowExecutionAssertion) toParams() (rv []table.ParameterOption) {
	rv = append(rv, table.ValueParam("$"+cond.Prefix+"shard_id", types.Uint32Value(ToShardIDColumnValue(cond.ShardID))))
	rv = append(rv, table.ValueParam("$"+cond.Prefix+"namespace_id", cond.client.NamespaceIDValueFromUUID(cond.NamespaceID)))
	rv = append(rv, table.ValueParam("$"+cond.Prefix+"workflow_id", types.UTF8Value(cond.WorkflowID)))
	rv = append(rv, table.ValueParam("$"+cond.Prefix+"run_id", cond.client.RunIDValueFromUUID(cond.RunID)))
	if cond.RecordVersionEqualTo != nil {
		rv = append(rv, table.ValueParam("$"+cond.Prefix+"db_record_version_eq", types.Int64Value(*cond.RecordVersionEqualTo)))
	}
	return rv
}

func (cond *workflowExecutionAssertion) toRowQuery() string {
	return fmt.Sprintf("SELECT * FROM AS_TABLE(ListNotNull(AsList($%[1]srun_row))) WHERE $incorrect > 0;", cond.Prefix)
}

func (cond *workflowExecutionAssertion) extractError(res result.Result) error {
	var dbRecordVersion int64
	var correct bool

	empty := !res.NextRow()
	if !empty {
		if err := res.ScanNamed(
			named.OptionalWithDefault("db_record_version", &dbRecordVersion),
			named.OptionalWithDefault("correct", &correct),
		); err != nil {
			return fmt.Errorf("failed to scan execution row: %w", err)
		}
	}

	if cond.MustNotExist {
		if !empty {
			return &persistence.WorkflowConditionFailedError{
				Msg: fmt.Sprintf("Workflow %s must not exist", cond.WorkflowID),
			}
		} else {
			panic("XXX")
		}
	} else {
		if empty {
			return &persistence.ConditionFailedError{
				Msg: fmt.Sprintf("Workflow execution %s must exist", cond.WorkflowID),
			}
		}
		if cond.RecordVersionEqualTo != nil {
			if *cond.RecordVersionEqualTo != dbRecordVersion {
				return &persistence.WorkflowConditionFailedError{
					Msg: fmt.Sprintf("Encounter workflow db version mismatch, request db version: %v, actual db version: %v",
						*cond.RecordVersionEqualTo, dbRecordVersion),
					DBRecordVersion: dbRecordVersion,
				}
			} else {
				if !correct {
					panic("XXX")
				}
			}
		} else {
			panic("empty workflow execution row condition")
		}
	}
	return nil
}

type assertion interface {
	toMissingRowVarAssignment() string
	toRowVarAssignment() string
	toInvalidConditionQuery() string
	extractError(res result.Result) error
	toParams() (rv []table.ParameterOption)
	toRowQuery() string
}

var _ assertion = (*shardAssertion)(nil)
var _ assertion = (*currentWorkflowAssertion)(nil)
var _ assertion = (*workflowExecutionAssertion)(nil)
