// The MIT License
//
// Copyright (g) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (g) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package xydb

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
