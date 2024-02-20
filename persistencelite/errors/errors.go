package errors

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
)

func NewInternal(msg string) error {
	return serviceerror.NewInternal(msg)
}

func NewInternalF(format string, args ...interface{}) error {
	return serviceerror.NewInternal(fmt.Sprintf(format, args...))
}

func NewInvalidArgumentF(format string, args ...interface{}) error {
	return serviceerror.NewInvalidArgument(fmt.Sprintf(format, args...))
}

func NewNotFoundF(format string, args ...interface{}) error {
	return serviceerror.NewNotFound(fmt.Sprintf(format, args...))
}

func NewUnavailableF(format string, args ...interface{}) error {
	return serviceerror.NewUnavailable(fmt.Sprintf(format, args...))
}
