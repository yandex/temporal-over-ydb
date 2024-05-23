package persistence

import (
	"context"
	"fmt"
	"strings"

	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

type readQuery struct {
	decl   string
	query  string
	params []table.ParameterOption
	cb     func(ctx context.Context, res result.Result) error
}

type writeQuery struct {
	decl   string
	query  string
	params []table.ParameterOption
}

type currentRunIDAndLastWriteVersionEqualToCond struct {
	lastWriteVersion int64
	currentRunID     string
}

type currentWorkflowsRowCond struct {
	workflowID string
	// condition, one of
	currentRunIDNotEqualTo                 string
	currentRunIDEqualTo                    string
	mustNotExist                           bool
	currentRunIDAndLastWriteVersionEqualTo *currentRunIDAndLastWriteVersionEqualToCond
}

type workflowExecutionsRowCond struct {
	workflowID string
	runID      string
	// conditions, one of
	dbRecordVersionEqualTo *int64
	mustNotExist           bool
}

func executeOneReadAndManyWriteQueries(ctx context.Context,
	client *xydb.Client,
	s table.Session,
	rq *readQuery,
	wqs []*writeQuery,
) (err error) {
	var buf strings.Builder
	params := table.NewQueryParameters(rq.params...)
	_, _ = buf.WriteString(rq.decl)
	_, _ = buf.WriteString("\n")
	for _, q := range wqs {
		_, _ = buf.WriteString(q.decl)
		params.Add(q.params...)
	}
	_, _ = buf.WriteString(rq.query)
	_, _ = buf.WriteString("\n")
	for _, q := range wqs {
		_, _ = buf.WriteString(q.query)
		_, _ = buf.WriteString("\n")
	}

	query := client.AddQueryPrefix(buf.String())
	_, res, err := s.Execute(ctx, table.SerializableReadWriteTxControl(table.CommitTx()), query, params)
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
	if err = rq.cb(ctx, res); err != nil {
		return err
	}
	return nil
}

func getCurrentWorkflowConflictError(
	executionStateBlob *commonpb.DataBlob,
	actualCurrentRunID string,
	requestCurrentRunID string,
	workflowLastWriteVersion int64,
) error {
	executionState := &persistencespb.WorkflowExecutionState{}
	if state, err := serialization.WorkflowExecutionStateFromBlob(
		executionStateBlob.Data,
		executionStateBlob.EncodingType.String(),
	); err == nil {
		executionState = state
	}

	return xydb.WrapErrorAsRootCause(&p.CurrentWorkflowConditionFailedError{
		Msg: fmt.Sprintf("Encounter current workflow error, request run ID: %v, actual run ID: %v",
			requestCurrentRunID, actualCurrentRunID),
		RequestID:        executionState.CreateRequestId,
		RunID:            executionState.RunId,
		State:            executionState.State,
		Status:           executionState.Status,
		LastWriteVersion: workflowLastWriteVersion,
	})
}

func extractWorkflowExecutionRowCondError(res result.Result, cond workflowExecutionsRowCond) error {
	if res.NextRow() {
		// Workflow execution row exists,
		if cond.mustNotExist {
			// ...but must not

			// Perform a sanity check and make sure that the database condition also marked this existing row as incorrect
			var correct bool
			if err := res.ScanNamed(named.OptionalWithDefault("correct", &correct)); err != nil {
				return fmt.Errorf("failed to scan execution row: %w", err)
			}
			if correct {
				return fmt.Errorf("database condition for execution row %s/%s must be false", cond.workflowID, cond.runID)
			}

			return xydb.WrapErrorAsRootCause(&p.WorkflowConditionFailedError{
				Msg: fmt.Sprintf("Workflow %s must not exist", cond.workflowID),
			})
		}
		// ...and must exist. This is fine, we check cond.dbRecordVersionEqualTo below
	} else {
		// Workflow execution row does not exist,
		if cond.mustNotExist {
			// ...and must not. This is fine
			return nil
		} else {
			// ...but must exist.
			return xydb.WrapErrorAsRootCause(&p.ConditionFailedError{
				Msg: fmt.Sprintf("Workflow execution %s must exist", cond.workflowID),
			})
		}
	}

	var dbRecordVersion int64
	var correct bool
	if err := res.ScanNamed(
		named.OptionalWithDefault("db_record_version", &dbRecordVersion),
		named.OptionalWithDefault("correct", &correct),
	); err != nil {
		return fmt.Errorf("failed to scan execution row: %w", err)
	}

	if cond.dbRecordVersionEqualTo != nil && *cond.dbRecordVersionEqualTo != dbRecordVersion {
		if correct {
			return fmt.Errorf("database condition for execution row %s/%s must be false", cond.workflowID, cond.runID)
		}
		return xydb.WrapErrorAsRootCause(&p.WorkflowConditionFailedError{
			Msg: fmt.Sprintf("Encounter workflow db version mismatch, request db version: %v, actual db version: %v",
				*cond.dbRecordVersionEqualTo, dbRecordVersion),
			DBRecordVersion: dbRecordVersion,
		})
	}

	if !correct {
		return fmt.Errorf("database condition for execution row %s/%s must be true", cond.workflowID, cond.runID)
	}

	return nil
}

func extractRangeRowCondError(res result.Result, shardID int32, rangeID int64) error {
	if !res.NextRow() {
		return fmt.Errorf("shard %d row does not exist", shardID)
	}

	var currRangeID int64
	var correct bool
	if err := res.ScanNamed(
		named.OptionalWithDefault("range_id", &currRangeID),
		named.OptionalWithDefault("correct", &correct),
	); err != nil {
		return fmt.Errorf("failed to scan shard row: %w", err)
	}
	if currRangeID != rangeID {
		if correct {
			return fmt.Errorf("database condition for shard row %d must be false", shardID)
		}
		return xydb.WrapErrorAsRootCause(&p.ShardOwnershipLostError{
			ShardID: shardID,
			Msg: fmt.Sprintf("Encounter shard ownership lost, request range ID: %d, actual range ID: %d",
				rangeID, currRangeID),
		})
	}
	if !correct {
		return fmt.Errorf("database condition for shard row %d must be true", shardID)
	}
	return nil
}

func extractCurrentWorkflowRowCondError(res result.Result, cond *currentWorkflowsRowCond) error {
	if !res.NextRow() {
		if cond.currentRunIDNotEqualTo != "" || cond.mustNotExist {
			return nil
		}
	}

	var data []byte
	var encoding string
	var currentRunID string
	var lastWriteVersion int64
	var state int32
	var correct bool
	if err := res.ScanNamed(
		named.OptionalWithDefault("execution_state", &data),
		named.OptionalWithDefault("execution_state_encoding", &encoding),
		named.OptionalWithDefault("current_run_id", &currentRunID),
		named.OptionalWithDefault("last_write_version", &lastWriteVersion),
		named.OptionalWithDefault("state", &state),
		named.OptionalWithDefault("correct", &correct),
	); err != nil {
		return fmt.Errorf("failed to scan current workflow row: %w", err)
	}

	if cond.mustNotExist {
		if correct {
			return fmt.Errorf("database condition for current workflow row %s must be false", cond.workflowID)
		}
		return getCurrentWorkflowConflictError(p.NewDataBlob(data, encoding), currentRunID, "", lastWriteVersion)
	} else if cond.currentRunIDNotEqualTo != "" {
		if currentRunID == cond.currentRunIDNotEqualTo {
			if correct {
				return fmt.Errorf("database condition for current workflow row %s must be false", cond.workflowID)
			}
			return xydb.WrapErrorAsRootCause(&p.CurrentWorkflowConditionFailedError{
				Msg:              fmt.Sprintf("Assertion on current record failed. Current run ID is not expected: %v", currentRunID),
				RequestID:        "",
				RunID:            "",
				State:            enumsspb.WORKFLOW_EXECUTION_STATE_UNSPECIFIED,
				Status:           enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED,
				LastWriteVersion: 0,
			})
		}
	} else if cond.currentRunIDEqualTo != "" {
		if currentRunID != cond.currentRunIDEqualTo {
			if correct {
				return fmt.Errorf("database condition for current workflow row %s must be false", cond.workflowID)
			}
			return getCurrentWorkflowConflictError(p.NewDataBlob(data, encoding), currentRunID, cond.currentRunIDEqualTo, lastWriteVersion)
		}
	} else if cond.currentRunIDAndLastWriteVersionEqualTo != nil {
		expectedCurrentRunID := cond.currentRunIDAndLastWriteVersionEqualTo.currentRunID
		if state != int32(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED) {
			if correct {
				return fmt.Errorf("database condition for current workflow row %s must be false", cond.workflowID)
			}
			return getCurrentWorkflowConflictError(p.NewDataBlob(data, encoding), currentRunID, expectedCurrentRunID, lastWriteVersion)
		}
		if lastWriteVersion != cond.currentRunIDAndLastWriteVersionEqualTo.lastWriteVersion {
			if correct {
				return fmt.Errorf("database condition for current workflow row %s must be false", cond.workflowID)
			}
			return getCurrentWorkflowConflictError(p.NewDataBlob(data, encoding), currentRunID, expectedCurrentRunID, lastWriteVersion)
		}
		if currentRunID != expectedCurrentRunID {
			if correct {
				return fmt.Errorf("database condition for current workflow row %s must be false", cond.workflowID)
			}
			return getCurrentWorkflowConflictError(p.NewDataBlob(data, encoding), currentRunID, expectedCurrentRunID, lastWriteVersion)
		}
	}
	if !correct {
		return fmt.Errorf("database condition for current workflow row %s must be true", cond.workflowID)
	}
	return nil
}

func conditionsToReadQuery(
	shardID int32,
	namespaceID string,
	expectedRangeID int64,
	currentWorkflowsRowC *currentWorkflowsRowCond,
	workflowExecutionsRowCs []workflowExecutionsRowCond,
) *readQuery {
	q := xydb.NewQuery()
	q.Declare("shard_id", types.TypeUint32)
	q.Declare("namespace_id", types.TypeUTF8)
	q.Declare("expected_range_id", types.TypeInt64)

	params := []table.ParameterOption{
		table.ValueParam("$shard_id", types.Uint32Value(ToShardIDColumnValue(shardID))),
		table.ValueParam("$expected_range_id", types.Int64Value(expectedRangeID)),
		table.ValueParam("$namespace_id", types.UTF8Value(namespaceID)),
	}

	headStmts := []string{`
$missing_shard_row = AsStruct(
	NULL AS workflow_id,
	NULL AS run_id,
	NULL AS range_id,
	false AS correct
);
`}
	bodyStmts := []string{`
$shard_row = SELECT (
	NULL AS workflow_id,
	NULL AS run_id,
	range_id AS range_id,
	(range_id = $expected_range_id) AS correct
) FROM executions
WHERE shard_id = $shard_id
AND namespace_id = ""
AND workflow_id = ""
AND run_id = ""
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`}
	incorrectRowSelects := []string{
		"SELECT * FROM AS_TABLE(ListNotNull(AsList($shard_row))) WHERE $incorrect > 0;",
	}
	conditionRows := []string{
		"Coalesce($shard_row, $missing_shard_row)",
	}

	if currentWorkflowsRowC != nil {
		suffix := "cw"
		q.Declare("workflow_id_"+suffix, types.TypeUTF8)
		params = append(params, table.ValueParam("$workflow_id_"+suffix, types.UTF8Value(currentWorkflowsRowC.workflowID)))

		var correct string
		if currentWorkflowsRowC.mustNotExist {
			correct = "false"
		} else {
			if currentWorkflowsRowC.currentRunIDNotEqualTo != "" {
				currentRunIDNE := "current_run_id_" + suffix + "_ne"
				q.Declare(currentRunIDNE, types.TypeUTF8)
				params = append(params, table.ValueParam(currentRunIDNE, types.UTF8Value(currentWorkflowsRowC.currentRunIDNotEqualTo)))
				correct = "current_run_id != $" + currentRunIDNE
			} else if currentWorkflowsRowC.currentRunIDEqualTo != "" {
				currentRunIDEq := "current_run_id_" + suffix + "_eq"
				q.Declare(currentRunIDEq, types.TypeUTF8)
				params = append(params, table.ValueParam(currentRunIDEq, types.UTF8Value(currentWorkflowsRowC.currentRunIDEqualTo)))
				correct = "current_run_id = $" + currentRunIDEq
			} else if currentWorkflowsRowC.currentRunIDAndLastWriteVersionEqualTo != nil {
				lastWriteVersionEq := "last_write_version_" + suffix + "_eq"
				currentRunIDEq := "current_run_id_" + suffix + "_eq"
				q.Declare(lastWriteVersionEq, types.TypeInt64)
				q.Declare(currentRunIDEq, types.TypeUTF8)
				params = append(params,
					table.ValueParam(lastWriteVersionEq, types.Int64Value(currentWorkflowsRowC.currentRunIDAndLastWriteVersionEqualTo.lastWriteVersion)),
					table.ValueParam(currentRunIDEq, types.UTF8Value(currentWorkflowsRowC.currentRunIDAndLastWriteVersionEqualTo.currentRunID)))
				correct = fmt.Sprintf("state = %d "+
					"AND last_write_version = $%s "+
					"AND current_run_id = $%s", int32(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED), lastWriteVersionEq, currentRunIDEq)
			} else {
				panic("empty current workflow row condition")
			}
		}
		headStmts = append(headStmts, `
$missing_curr_exec_row = AsStruct(
	NULL AS workflow_id,
	NULL AS run_id,
	false AS correct,
	NULL AS execution_state,
	NULL AS execution_state_encoding,
	NULL AS current_run_id,
	NULL AS last_write_version,
	NULL AS state
);
`)
		bodyStmts = append(bodyStmts, fmt.Sprintf(`
$curr_exec_row = SELECT (
	workflow_id	AS workflow_id,
	run_id AS run_id,
	(%s) AS correct,
	execution_state AS execution_state,
	execution_state_encoding AS execution_state_encoding,
	current_run_id AS current_run_id,
	last_write_version AS last_write_version,
	state AS state
) FROM executions
WHERE shard_id = $shard_id
AND namespace_id = $namespace_id
AND workflow_id = $workflow_id_%s
AND run_id = ""
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`, correct, suffix))
		incorrectRowSelects = append(incorrectRowSelects, "SELECT * FROM AS_TABLE(ListNotNull(AsList($curr_exec_row))) WHERE $incorrect > 0;")
		if currentWorkflowsRowC.mustNotExist {
			conditionRows = append(conditionRows, "$curr_exec_row")
		} else {
			conditionRows = append(conditionRows, "Coalesce($curr_exec_row, $missing_curr_exec_row)")
		}
	}
	for i, cond := range workflowExecutionsRowCs {
		suffix := fmt.Sprintf("%d", i)
		var correct string
		if cond.mustNotExist {
			correct = "false"
		} else if cond.dbRecordVersionEqualTo != nil {
			dbVersionRecordEq := "db_record_version_eq_" + suffix
			q.Declare(dbVersionRecordEq, types.TypeInt64)
			params = append(params, table.ValueParam(dbVersionRecordEq, types.Int64Value(*cond.dbRecordVersionEqualTo)))
			correct = "db_record_version = $" + dbVersionRecordEq
		} else {
			panic("empty workflow execution row condition")
		}
		workflowID := "workflow_id_d_" + suffix
		runID := "run_id_d_" + suffix
		q.Declare(workflowID, types.TypeUTF8)
		q.Declare(runID, types.TypeUTF8)
		params = append(params, table.ValueParam(workflowID, types.UTF8Value(cond.workflowID)))
		params = append(params, table.ValueParam(runID, types.UTF8Value(cond.runID)))
		headStmts = append(headStmts, fmt.Sprintf(`$missing_run_row_%s = AsStruct(
	NULL AS workflow_id,
	NULL AS run_id,
	false AS correct,
	NULL AS db_record_version
);
`, suffix))
		bodyStmts = append(bodyStmts, fmt.Sprintf(`
$run_row_%s = SELECT (
	workflow_id	AS workflow_id,
	run_id AS run_id,
	(%s) AS correct,
	db_record_version AS db_record_version
) FROM executions
WHERE shard_id = $shard_id
AND namespace_id = $namespace_id
AND workflow_id = $%s
AND run_id = $%s
AND task_id IS NULL
AND task_category_id IS NULL
AND task_visibility_ts IS NULL
AND event_type IS NULL
AND event_id IS NULL
AND event_name IS NULL;
`, suffix, correct, workflowID, runID))
		incorrectRowSelects = append(incorrectRowSelects, fmt.Sprintf("SELECT * FROM AS_TABLE(ListNotNull(AsList($run_row_%s))) WHERE $incorrect > 0;", suffix))

		if cond.mustNotExist {
			conditionRows = append(conditionRows, fmt.Sprintf("$run_row_%s", suffix))
		} else {
			conditionRows = append(conditionRows, fmt.Sprintf("Coalesce($run_row_%s, $missing_run_row_%s)", suffix, suffix))
		}
	}
	conditionsTableExpr := fmt.Sprintf("AS_TABLE(AsList(\n%s\n))", strings.Join(conditionRows, ",\n"))
	q.AddQuery(strings.Join(headStmts, "\n"))
	q.AddQuery(strings.Join(bodyStmts, "\n"))
	q.AddQuery("$incorrect = SELECT count(*) AS incorrect FROM " + conditionsTableExpr + " WHERE correct = false;")

	// Add statements that allow client to count and read rows with incorrect conditions
	q.AddQuery("SELECT $incorrect AS incorrect;")
	q.AddQuery(strings.Join(incorrectRowSelects, "\n") + ";")

	return &readQuery{
		params: params,
		decl:   strings.Join(q.Declaration, "\n"),
		query:  strings.Join(q.Queries, "\n"),
		cb: func(ctx context.Context, res result.Result) error {
			var count uint64
			if !res.NextRow() {
				return fmt.Errorf("failed to count incorrect rows: no rows")
			}
			if err := res.ScanNamed(
				named.OptionalWithDefault("incorrect", &count),
			); err != nil {
				return fmt.Errorf("failed to count incorrect rows: %w", err)
			}
			if count == 0 {
				return nil
			}

			if err := res.NextResultSetErr(ctx); err != nil {
				return err
			}
			if err := extractRangeRowCondError(res, shardID, expectedRangeID); err != nil {
				return err
			}

			if currentWorkflowsRowC != nil {
				if err := res.NextResultSetErr(ctx); err != nil {
					return err
				}
				if err := extractCurrentWorkflowRowCondError(res, currentWorkflowsRowC); err != nil {
					return err
				}
			}

			for _, cond := range workflowExecutionsRowCs {
				if err := res.NextResultSetErr(ctx); err != nil {
					return err
				}
				if err := extractWorkflowExecutionRowCondError(res, cond); err != nil {
					return err
				}
			}

			return nil
		},
	}
}
