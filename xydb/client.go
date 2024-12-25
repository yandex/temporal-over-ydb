package xydb

import (
	"context"
	"fmt"
	"time"

	xydblog "github.com/yandex/temporal-over-ydb/xydb/log"
	xydbmetrics "github.com/yandex/temporal-over-ydb/xydb/metrics"
	ydbenv "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	ydbmetrics "github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	tlog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/atomic"
	"golang.org/x/xerrors"
)

type Client struct {
	DB         *ydb.Driver
	Database   string
	Folder     string
	logger     tlog.Logger
	logQueries atomic.Bool
}

func SnapshotReadOnlyTxControl(opts ...table.TxOnlineReadOnlyOption) *table.TransactionControl {
	return table.TxControl(
		table.BeginTx(table.WithOnlineReadOnly(opts...)),
		table.CommitTx(), // open transactions not supported for OnlineReadOnly
	)
}

func OnlineReadOnlyTxControl(opts ...table.TxOnlineReadOnlyOption) *table.TransactionControl {
	return table.OnlineReadOnlyTxControl(opts...)
}

const detailsFull = trace.DriverRepeaterEvents |
	trace.DriverConnEvents |
	trace.DriverBalancerEvents |
	trace.TablePoolEvents |
	trace.RetryEvents |
	trace.DiscoveryEvents |
	trace.SchemeEvents

const detailsTiny = trace.DriverConnEvents |
	trace.TablePoolEvents

const detailsNone = trace.Details(0)

const details = detailsNone
const _ = detailsNone
const _ = detailsTiny
const _ = detailsFull

func setupLogger(l tlog.Logger) []ydb.Option {
	opts := make([]ydb.Option, 0)
	opts = append(opts, xydblog.WithTraces(l, detailsNone))
	return opts
}

func setupMetrics(mh metrics.Handler) []ydb.Option {
	opts := make([]ydb.Option, 0)
	if mh == nil {
		return opts
	}
	mc := xydbmetrics.MakeConfig(
		mh,
		xydbmetrics.WithNamespace("xydb"),
		xydbmetrics.WithDetails(detailsFull),
		xydbmetrics.WithSeparator("_"),
	)
	opts = append(opts,
		ydbmetrics.WithTraces(mc),
	)
	return opts
}

func NewClient(ctx context.Context, config Config, logger tlog.Logger, mh metrics.Handler) (
	*Client,
	error,
) {
	opts := make([]ydb.Option, 0)
	if config.Token != "" {
		opts = append(opts, ydb.WithAccessTokenCredentials(config.Token))
	} else {
		opts = append(opts, ydbenv.WithEnvironCredentials(ctx))
	}
	opts = append(opts, setupLogger(logger)...)
	opts = append(opts, setupMetrics(mh)...)
	opts = append(opts, ydb.WithSessionPoolIdleThreshold(time.Second*10))

	balancerConfig := balancers.RandomChoice()
	if config.PreferLocalDC {
		balancerConfig = balancers.PreferLocalDC(balancerConfig)
	}
	opts = append(opts, ydb.WithBalancer(balancerConfig))

	opts = append(opts, ydb.WithDialTimeout(10*time.Second))
	sessionPoolSizeLimit := config.SessionPoolSizeLimit
	if sessionPoolSizeLimit > 0 {
		opts = append(opts, ydb.WithSessionPoolSizeLimit(sessionPoolSizeLimit))
	}

	db, err := ydb.Open(
		ctx,
		sugar.DSN(config.Endpoint, config.Database, sugar.WithSecure(config.UseSSL)),
		opts...,
	)
	if err != nil {
		return nil, xerrors.Errorf("connect error: %w", err)
	}
	return &Client{
		DB:       db,
		Database: config.Database,
		Folder:   config.Folder,
		logger:   logger,
	}, nil
}

func (client *Client) Close(ctx context.Context) error {
	closeCtx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	ts := time.Now()
	defer func() {
		client.logger.Info(fmt.Sprintf("ydb close duration: %s", time.Since(ts)))
	}()
	return client.DB.Close(closeCtx)
}

func (client *Client) GetPrefix() string {
	return fmt.Sprintf("%s/%s", client.Database, client.Folder)
}

func (client *Client) queryPrefix() string {
	return fmt.Sprintf("--!syntax_v1\nPRAGMA TablePathPrefix(\"%s\");\n", client.GetPrefix())
}

func (client *Client) AddQueryPrefix(query string) string {
	return client.queryPrefix() + query
}

func (client *Client) Write(
	ctx context.Context,
	query string,
	params *table.QueryParameters,
	opts ...table.Option,
) error {
	res, errDo := client.Do(ctx, query, table.SerializableReadWriteTxControl(table.CommitTx()), params, opts...)
	if errDo != nil {
		return errDo
	}
	return res.Close()
}

func (client *Client) Write2(
	ctx context.Context,
	query string,
	getQueryParameters func() *table.QueryParameters,
	opts ...table.Option,
) error {
	res, errDo := client.Do(ctx, query, table.SerializableReadWriteTxControl(table.CommitTx()), getQueryParameters(), opts...)
	if errDo != nil {
		return errDo
	}
	return res.Close()
}

func (client *Client) Do(
	ctx context.Context,
	query string,
	tx *table.TransactionControl,
	params *table.QueryParameters,
	opts ...table.Option,
) (res result.Result, err error) {
	err = client.DB.Table().Do(
		ctx,
		func(c context.Context, s table.Session) (err error) {
			_, res, err = s.Execute(c, tx, query, params)
			return
		},
		opts...,
	)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (client *Client) Do2(
	ctx context.Context,
	query string,
	tx *table.TransactionControl,
	getQueryParameters func() *table.QueryParameters,
	opts ...table.Option,
) (res result.Result, err error) {
	err = client.DB.Table().Do(
		ctx,
		func(c context.Context, s table.Session) (err error) {
			_, res, err = s.Execute(c, tx, query, getQueryParameters())
			return
		},
		opts...,
	)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (client *Client) DoSchema(
	ctx context.Context,
	query string,
	params ...options.ExecuteSchemeQueryOption,
) (err error) {
	err = client.DB.Table().Do(
		ctx, func(ctx context.Context, s table.Session) (err error) {
			err = s.ExecuteSchemeQuery(
				ctx, query, params...,
			)
			return
		}, table.WithIdempotent(),
	)

	if err != nil {
		return xerrors.Errorf("failed ydb request: %w", err)
	}
	return nil
}
