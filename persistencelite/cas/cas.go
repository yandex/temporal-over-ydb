package cas

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rqlite/gorqlite"
	"github.com/yandex/temporal-over-ydb/persistencelite/conn"
	errorslite "github.com/yandex/temporal-over-ydb/persistencelite/errors"
	"github.com/yandex/temporal-over-ydb/persistencelite/rows"
)

type ReadAndCheckQuery struct {
	stmt  gorqlite.ParameterizedStatement
	check func(gorqlite.QueryResult) error
}

type assertionQuery struct {
	// Table to select from
	table string
	// Bool expression whether the condition is valid
	validConditionExpr string
	// Fields to select along with the validConditionExpr result for inspection in case assertion fails
	additionalFields []string
	// Bool expression to filter table
	selectorExpr string
	// Arguments for validConditionExpr
	validConditionArgs []interface{}
	// Arguments for selectorExpr
	selectorArgs []interface{}
}

type CheckQuery []assertionQuery

func (cqs CheckQuery) toWriteQuery(uuid string) gorqlite.ParameterizedStatement {
	parts := make([]string, 0, len(cqs))
	args := make([]interface{}, 0, len(cqs)+1)
	args = append(args, uuid)
	for _, cq := range cqs {
		parts = append(parts, cq.toQuery())
		args = append(args, cq.toArgs()...)
	}
	unionExpr := strings.Join(parts, "\nUNION\n")
	countInvalidConditionsExpr := fmt.Sprintf("SELECT COUNT(*) FROM (%s) WHERE valid = false", unionExpr)
	queryStmt := fmt.Sprintf("INSERT INTO conditions (id, uuid, valid) VALUES (NULL, ?, (%s) = 0)", countInvalidConditionsExpr)
	return gorqlite.ParameterizedStatement{
		Query:     queryStmt,
		Arguments: args,
	}
}

func newConditionReadQuery(uuid string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query:     "SELECT valid FROM conditions WHERE uuid = ?",
		Arguments: []interface{}{uuid},
	}
}

func (c *assertionQuery) toQuery() string {
	return fmt.Sprintf("SELECT (%s) valid FROM %s WHERE %s", c.validConditionExpr, c.table, c.selectorExpr)
}

func (c *assertionQuery) toArgs() []interface{} {
	return append(c.validConditionArgs, c.selectorArgs...)
}

func (c *assertionQuery) toQueryWithAdditionalFields() string {
	if len(c.additionalFields) > 0 {
		return fmt.Sprintf("SELECT (%s) valid, %s FROM %s WHERE %s", c.validConditionExpr, strings.Join(c.additionalFields, ", "), c.table, c.selectorExpr)
	}
	return c.toQuery()
}

type CurrentRunIDAndLastWriteVersionEqualToCond struct {
	LastWriteVersion int64
	CurrentRunID     string
}

func getAssertionQueries(shardID int32, namespaceID string, as []Assertion) (CheckQuery, []ReadAndCheckQuery) {
	var cq CheckQuery
	var rcqs []ReadAndCheckQuery
	for _, a := range as {
		c := a.toAssertionQuery(shardID, namespaceID)
		q := ReadAndCheckQuery{
			stmt: gorqlite.ParameterizedStatement{
				Query:     c.toQueryWithAdditionalFields(),
				Arguments: c.toArgs(),
			},
			check: a.extractError,
		}
		cq = append(cq, c)
		rcqs = append(rcqs, q)
	}
	return cq, rcqs
}

func assertSingleRowAffected(res gorqlite.RequestResult) error {
	if res.Err != nil {
		return res.Err
	}
	if res.Write.RowsAffected != 1 {
		return fmt.Errorf("expected 1 row affected, got %d", res.Write.RowsAffected)
	}
	return nil
}

func readSingleBoolValue(res gorqlite.RequestResult) (bool, error) {
	if res.Err != nil {
		return false, res.Err
	}
	qRes := res.Query
	if !qRes.Next() {
		return false, errors.New("result set is empty")
	}
	var valid bool
	if err := qRes.Scan(&valid); err != nil {
		return false, fmt.Errorf("failed to scan bool: %w", err)
	}
	return valid, nil
}

func ExecuteConditionally(ctx context.Context, conn conn.ThreadSafeRqliteConnection, shardID int32, namespaceID string, condID string, assertions []Assertion, rowsToUpsert []rows.Upsertable, rowsToDelete []rows.Deletable, otherStmts []gorqlite.ParameterizedStatement) error {
	checkQuery, readAndCheckQueries := getAssertionQueries(shardID, namespaceID, assertions)
	stmts := make([]gorqlite.ParameterizedStatement, 0, 2+len(readAndCheckQueries)+len(rowsToUpsert)+len(rowsToDelete)+len(otherStmts))
	// Read statements to be able to see what went wrong in case of condition failure
	for _, q := range readAndCheckQueries {
		stmts = append(stmts, q.stmt)
	}
	// Write condition in conditions table
	stmts = append(stmts, checkQuery.toWriteQuery(condID))
	writeConditionQueryIdx := len(stmts) - 1
	// Read the bool value of the condition
	stmts = append(stmts, newConditionReadQuery(condID))
	readConditionQueryIdx := len(stmts) - 1
	// Do conditional deletes
	for _, r := range rowsToDelete {
		stmts = append(stmts, r.ToDeleteQuery(condID))
	}
	stmts = append(stmts, otherStmts...)
	// Do conditional upserts
	for _, r := range rowsToUpsert {
		stmts = append(stmts, r.ToWriteQuery(condID))
	}

	res, err := conn.RequestParameterizedContext(ctx, stmts)
	if err != nil {
		return err
	}
	if err = assertSingleRowAffected(res[writeConditionQueryIdx]); err != nil {
		return errorslite.NewInternalF("seemingly did not write condition value: %v", err)
	}
	valid, err := readSingleBoolValue(res[readConditionQueryIdx])
	if err != nil {
		return errorslite.NewInternalF("failed to read valid result: %v", err)
	}
	if valid {
		return nil
	}
	for i, q := range readAndCheckQueries {
		if res[i].Err != nil {
			return errorslite.NewInternalF("failed to execute query %d: %s", i, res[i].Err)
		}
		if err = q.check(*res[i].Query); err != nil {
			return err
		}
	}
	return errorslite.NewInternal("failed to capture assertion error")
}
