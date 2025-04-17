package conn

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
)

func convertError(operation string, err error, details ...string) error {
	msg := fmt.Sprintf("operation %v encountered %v", operation, err.Error())
	if len(details) > 0 {
		msg += " (" + strings.Join(details, ", ") + ")"
	}
	if err == context.DeadlineExceeded || ydb.IsTimeoutError(err) {
		return &persistence.TimeoutError{Msg: msg}
	}
	if ydb.IsOperationErrorNotFoundError(err) {
		return serviceerror.NewNotFound(msg)
	}
	if ydb.IsOperationError(err, Ydb.StatusIds_PRECONDITION_FAILED) {
		return &persistence.ConditionFailedError{Msg: msg}
	}
	if ydb.IsOperationErrorOverloaded(err) {
		return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, msg)
	}
	return serviceerror.NewUnavailable(msg)
}

func IsPreconditionFailedAndContains(err error, substr string) bool {
	rv := false
	if ydb.IsOperationError(err, Ydb.StatusIds_PRECONDITION_FAILED) {
		ydb.IterateByIssues(err, func(message string, code Ydb.StatusIds_StatusCode, severity uint32) {
			if strings.Contains(message, substr) {
				rv = true
			}
		})
	}
	if !rv && ydb.IsOperationError(err, Ydb.StatusIds_GENERIC_ERROR) {
		ydb.IterateByIssues(err, func(message string, code Ydb.StatusIds_StatusCode, severity uint32) {
			if strings.Contains(message, substr) {
				rv = true
			}
		})
	}
	return rv
}

func IsIntermediateDataMaterializationExceededSizeLimitError(err error) bool {
	return IsPreconditionFailedAndContains(err, "Intermediate data materialization exceeded size limit")
}

type RootCauseError struct {
	new     func(message string) error
	message string
	// or
	err error
}

func (e *RootCauseError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return e.new(e.message).Error()
}

func NewRootCauseError(new func(message string) error, message string) error {
	return &RootCauseError{new: new, message: message}
}

func WrapErrorAsRootCause(err error) error {
	return &RootCauseError{err: err}
}

func ConvertToTemporalError(operation string, err error, details ...string) error {
	if err == nil {
		return nil
	}
	var rv *RootCauseError
	if errors.As(err, &rv) {
		if rv.err != nil {
			return rv.err
		} else {
			msg := fmt.Sprintf("operation %s failed: %s", operation, rv.message)
			if len(details) > 0 {
				msg += " (" + strings.Join(details, ", ") + ")"
			}
			return rv.new(msg)
		}
	}
	return convertError(operation, err, details...)
}
