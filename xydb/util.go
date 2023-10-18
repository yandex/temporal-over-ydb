package xydb

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"go.temporal.io/api/serviceerror"
)

func EnsureOneRowCursor(ctx context.Context, res result.Result) error {
	if err := res.NextResultSetErr(ctx); err != nil {
		return NewRootCauseError(
			serviceerror.NewInternal, fmt.Sprintf("failed to get first result set: %s", err.Error()))
	}
	if !res.NextRow() {
		return NewRootCauseError(
			serviceerror.NewNotFound, "failed to get first row - empty result")
	}
	if res.HasNextRow() {
		return NewRootCauseError(
			serviceerror.NewInternal,
			fmt.Sprintf("result contains more than one row (%d)", res.CurrentResultSet().RowCount()))
	}
	return nil
}
