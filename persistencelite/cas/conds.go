package cas

import (
	"fmt"

	"github.com/rqlite/gorqlite"
	"github.com/yandex/temporal-over-ydb/persistencelite/conn"
	errorslite "github.com/yandex/temporal-over-ydb/persistencelite/errors"
	commonpb "go.temporal.io/api/common/v1"
	enums2 "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

func getExecutionStateFromBlob(blob *commonpb.DataBlob) (*persistencespb.WorkflowExecutionState, error) {
	return serialization.WorkflowExecutionStateFromBlob(blob.Data, blob.EncodingType.String())
}

func getCurrentWorkflowConflictError(
	executionStateBlob *commonpb.DataBlob,
	actualCurrentRunID string,
	requestCurrentRunID string,
	workflowLastWriteVersion int64,
) error {
	executionState, err := getExecutionStateFromBlob(executionStateBlob)
	if err != nil {
		return errorslite.NewInternalF("failed to get execution state from blob: %v", err)
	}
	return &persistence.CurrentWorkflowConditionFailedError{
		Msg: fmt.Sprintf("Encounter current workflow error, request run ID: %v, actual run ID: %v",
			requestCurrentRunID, actualCurrentRunID),
		RequestID:        executionState.CreateRequestId,
		RunID:            executionState.RunId,
		State:            executionState.State,
		Status:           executionState.Status,
		LastWriteVersion: workflowLastWriteVersion,
	}
}

type ShardRangeCond struct {
	ShardID        int32
	RangeIDEqualTo int64
}

func (cond *ShardRangeCond) toAssertionQuery(shardID int32, namespaceID string) assertionQuery {
	return assertionQuery{
		table:              "shards",
		validConditionExpr: "coalesce(min(range_id), -1) = ?",
		validConditionArgs: []interface{}{cond.RangeIDEqualTo},
		additionalFields:   []string{"coalesce(min(range_id), -1) range_id"},
		selectorExpr:       "shard_id = ?",
		selectorArgs:       []interface{}{cond.ShardID},
	}
}

func (cond *ShardRangeCond) extractError(res gorqlite.QueryResult) error {
	if err := mustNext(&res); err != nil {
		return errorslite.NewInternalF("failed to extract shard range condition error: %v", err)
	}
	var valid bool
	var currRangeID int64
	if err := res.Scan(&valid, &currRangeID); err != nil {
		return errorslite.NewInternalF("failed to extract range condition error: failed to scan: %v", err)
	}
	if valid {
		return nil
	}
	if currRangeID == cond.RangeIDEqualTo {
		return errorslite.NewInternalF(
			"failed to extract range condition error: the row is not valid, but curr range id == expected range id")
	}
	return &persistence.ShardOwnershipLostError{
		ShardID: cond.ShardID,
		Msg: fmt.Sprintf("Encounter shard ownership lost, request range ID: %d, actual range ID: %d",
			cond.RangeIDEqualTo, currRangeID),
	}
}

type CurrentWorkflowsRowCond struct {
	WorkflowID string
	// condition, one of
	CurrentRunIDNotEqualTo                 string
	CurrentRunIDEqualTo                    string
	MustNotExist                           bool
	CurrentRunIDAndLastWriteVersionEqualTo *CurrentRunIDAndLastWriteVersionEqualToCond
}

func (cond *CurrentWorkflowsRowCond) toAssertionQuery(shardID int32, namespaceID string) assertionQuery {
	c := assertionQuery{
		table:            "current_executions",
		selectorExpr:     "shard_id = ? AND namespace_id = ? AND workflow_id = ?",
		selectorArgs:     []interface{}{shardID, namespaceID, cond.WorkflowID},
		additionalFields: []string{"workflow_id", "current_run_id", "execution_state", "execution_state_encoding", "last_write_version", "state"},
	}
	if cond.MustNotExist {
		c.validConditionExpr = `coalesce(min(current_run_id), "") = ""`
	} else {
		if cond.CurrentRunIDNotEqualTo != "" {
			c.validConditionExpr = `coalesce(min(current_run_id), "") != ?`
			c.validConditionArgs = []interface{}{cond.CurrentRunIDNotEqualTo}
		} else if cond.CurrentRunIDEqualTo != "" {
			c.validConditionExpr = `coalesce(min(current_run_id), "") = ?`
			c.validConditionArgs = []interface{}{cond.CurrentRunIDEqualTo}
		} else if cond.CurrentRunIDAndLastWriteVersionEqualTo != nil {
			c.validConditionExpr = `coalesce(min(current_run_id), "") = ? AND coalesce(min(last_write_version), -1) = ? AND state = ?`
			c.validConditionArgs = []interface{}{
				cond.CurrentRunIDAndLastWriteVersionEqualTo.CurrentRunID,
				cond.CurrentRunIDAndLastWriteVersionEqualTo.LastWriteVersion,
				int64(enums.WORKFLOW_EXECUTION_STATE_COMPLETED),
			}
		} else {
			panic("empty current workflow row condition")
		}
	}
	return c
}

func (cond *CurrentWorkflowsRowCond) extractError(res gorqlite.QueryResult) error {
	if err := mustNext(&res); err != nil {
		return errorslite.NewInternalF("failed to extract curr wf condition error: %v", err)
	}

	var valid bool
	var workflowID string
	var currentRunID string
	var executionStateBase64 string
	var executionStateEncoding string
	var lastWriteVersion int64
	var state int64

	if err := res.Scan(&valid, &workflowID, &currentRunID, &executionStateBase64, &executionStateEncoding, &lastWriteVersion, &state); err != nil {
		return errorslite.NewInternalF("failed to extract curr wf condition error: failed to scan: %v", err)
	}
	data, err := conn.Base64ToBlob(executionStateBase64, executionStateEncoding)
	if err != nil {
		return errorslite.NewInternalF("failed to decode execution state: %v", err)
	}

	if valid {
		return nil
	}

	if cond.MustNotExist {
		return getCurrentWorkflowConflictError(data, currentRunID, "", lastWriteVersion)
	} else if cond.CurrentRunIDNotEqualTo != "" {
		if currentRunID != cond.CurrentRunIDNotEqualTo {
			return errorslite.NewInternalF("failed to extract range curr wf error: the row is not valid, but current_run_id != cond.CurrentRunIDNotEqualTo")
		}
		return &persistence.CurrentWorkflowConditionFailedError{
			Msg:              fmt.Sprintf("Assertion on current record failed. Current run ID is not expected: %v", currentRunID),
			RequestID:        "",
			RunID:            "",
			State:            enums.WORKFLOW_EXECUTION_STATE_UNSPECIFIED,
			Status:           enums2.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED,
			LastWriteVersion: 0,
		}
	} else if cond.CurrentRunIDEqualTo != "" {
		if currentRunID == cond.CurrentRunIDEqualTo {
			return errorslite.NewInternalF("failed to extract range curr wf error: the row is not valid, but current_run_id == cond.CurrentRunIDEqualTo")
		}
		return getCurrentWorkflowConflictError(data, currentRunID, cond.CurrentRunIDEqualTo, lastWriteVersion)
	} else if cond.CurrentRunIDAndLastWriteVersionEqualTo != nil {
		expectedCurrentRunID := cond.CurrentRunIDAndLastWriteVersionEqualTo.CurrentRunID
		if int32(state) != int32(enums.WORKFLOW_EXECUTION_STATE_COMPLETED) ||
			lastWriteVersion != cond.CurrentRunIDAndLastWriteVersionEqualTo.LastWriteVersion ||
			currentRunID != expectedCurrentRunID {
			return getCurrentWorkflowConflictError(data, currentRunID, expectedCurrentRunID, lastWriteVersion)
		}
		return errorslite.NewInternalF("failed to extract range curr wf error: the row is not valid, but state = WORKFLOW_EXECUTION_STATE_COMPLETED, last_write_version = expected one, current_run_id = expected one")
	}
	return nil
}

type WorkflowExecutionsRowCond struct {
	WorkflowID string
	RunID      string
	// conditions, one of
	RecordVersionEqualTo *int64
	MustNotExist         bool
}

func (cond *WorkflowExecutionsRowCond) toAssertionQuery(shardID int32, namespaceID string) assertionQuery {
	c := assertionQuery{
		table:            "executions",
		selectorExpr:     "shard_id = ? AND namespace_id = ? AND workflow_id = ? AND run_id = ?",
		selectorArgs:     []interface{}{shardID, namespaceID, cond.WorkflowID, cond.RunID},
		additionalFields: []string{"db_record_version"},
	}
	if cond.MustNotExist {
		c.validConditionExpr = "coalesce(min(db_record_version), -1) = -1"
	} else if cond.RecordVersionEqualTo != nil {
		c.validConditionExpr = "coalesce(min(db_record_version), -1) = ?"
		c.validConditionArgs = []interface{}{*cond.RecordVersionEqualTo}
	} else {
		panic("empty workflow execution row condition")
	}
	return c
}

func mustNext(res *gorqlite.QueryResult) error {
	if res.Err != nil {
		return errorslite.NewInternalF("result has an error: %v", res.Err)
	}
	if !res.Next() {
		return errorslite.NewInternal("no rows")
	}
	return nil
}

func (cond *WorkflowExecutionsRowCond) extractError(res gorqlite.QueryResult) error {
	if err := mustNext(&res); err != nil {
		return errorslite.NewInternalF("failed to extract wf condition error: %v", err)
	}

	var valid bool
	dbRecordVersionN := gorqlite.NullInt64{}

	if err := res.Scan(&valid, &dbRecordVersionN); err != nil {
		return errorslite.NewInternalF("failed to extract wf condition error: failed to scan: %v", err)
	}

	if valid {
		return nil
	}

	if cond.MustNotExist {
		if dbRecordVersionN.Valid {
			return &persistence.WorkflowConditionFailedError{
				Msg: fmt.Sprintf("Workflow %s must not exist", cond.WorkflowID),
			}
		}
	} else {
		if !dbRecordVersionN.Valid {
			return &persistence.ConditionFailedError{
				Msg: fmt.Sprintf("Encounter workflow db version mismatch, request db version: %v, actual db version: not exist", *cond.RecordVersionEqualTo),
			}
		}
		dbRecordVersion := dbRecordVersionN.Int64
		if cond.RecordVersionEqualTo != nil {
			if *cond.RecordVersionEqualTo != dbRecordVersionN.Int64 {
				return &persistence.WorkflowConditionFailedError{
					Msg: fmt.Sprintf("Encounter workflow db version mismatch, request db version: %v, actual db version: %v",
						*cond.RecordVersionEqualTo, dbRecordVersion),
					DBRecordVersion: dbRecordVersion,
				}
			}
		} else {
			panic("empty workflow execution row condition")
		}
	}
	return nil
}

type Assertion interface {
	extractError(res gorqlite.QueryResult) error
	toAssertionQuery(shardID int32, namespaceID string) assertionQuery
}
