package conn

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

/*
Executor is a very primitive wrapper that provides the same interface for executing
queries in either session or transaction.

Allows having helper functions that could be used in both cases.
*/
type Executor interface {
	Execute(
		ctx context.Context,
		query string,
		params *table.QueryParameters,
	) (result.Result, error)

	// Write is a special case of Execute that immediately closes its result.
	Write(
		ctx context.Context,
		query string,
		params *table.QueryParameters,
	) error
}

func NewExecutorFromSession(s table.Session, tx *table.TransactionControl) Executor {
	return &sessionExecutor{session: s, tx: tx}
}

type sessionExecutor struct {
	session table.Session
	tx      *table.TransactionControl
}

func (s *sessionExecutor) Execute(ctx context.Context, query string, params *table.QueryParameters) (result.Result, error) {
	_, res, err := s.session.Execute(ctx, s.tx, query, params)
	return res, err
}

func (s *sessionExecutor) Write(ctx context.Context, query string, params *table.QueryParameters) error {
	res, err := s.Execute(ctx, query, params)
	if err != nil {
		return err
	}
	return res.Close()
}

type actorExecutor struct {
	a table.TransactionActor
}

func (a actorExecutor) Execute(ctx context.Context, query string, params *table.QueryParameters) (result.Result, error) {
	return a.a.Execute(ctx, query, params)
}

func (a actorExecutor) Write(ctx context.Context, query string, params *table.QueryParameters) error {
	res, err := a.Execute(ctx, query, params)
	if err != nil {
		return err
	}
	return res.Close()
}

func NewExecutorFromTransactionActor(a table.TransactionActor) Executor {
	return &actorExecutor{a: a}
}
