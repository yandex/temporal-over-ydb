package cache

import (
	"fmt"
)

type TooManyFutureTasks struct {
	stats Statistics
}

func newTooManyFutureTasks(stats Statistics) *TooManyFutureTasks {
	return &TooManyFutureTasks{
		stats: stats,
	}
}

func (e *TooManyFutureTasks) Error() string {
	return fmt.Sprintf("too many future tasks (%s)", e.stats.String())
}

type InvalidCacheError struct {
}

func newInvalidCacheError() *InvalidCacheError {
	return &InvalidCacheError{}
}

func (e *InvalidCacheError) Error() string {
	return "cache is not valid"
}

type InvalidRequestError struct {
	message string
}

func newInvalidRequestError(message string) *InvalidRequestError {
	return &InvalidRequestError{
		message: message,
	}
}

func (e *InvalidRequestError) Error() string {
	return e.message
}
