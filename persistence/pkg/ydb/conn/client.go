package conn

import (
	"context"
	"fmt"
	"os"
	"slices"
	"time"

	ydbenv "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	ydbconfig "github.com/ydb-platform/ydb-go-sdk/v3/config"
	ydbmetrics "github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"go.temporal.io/api/serviceerror"
	tlog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/atomic"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/config"
	connlog "github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn/log"
	connmetrics "github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn/metrics"
)

var (
	ydbCredentialEnvs = [...]string{
		"YDB_SERVICE_ACCOUNT_KEY_CREDENTIALS",
		"YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS",
		"YDB_ANONYMOUS_CREDENTIALS",
		"YDB_METADATA_CREDENTIALS",
		"YDB_ACCESS_TOKEN_CREDENTIALS",
	}
)

type Client struct {
	Database    string
	Folder      string
	logger      tlog.Logger
	logQueries  atomic.Bool
	useOldTypes bool

	// driverLock is a hand-rolled mutex: acquire = send, release = receive.
	// Unlike sync.Mutex, a goroutine waiting to acquire it can give up early.
	driverLock chan struct{}
	db         *ydb.Driver
	ctx        context.Context
	cancel     context.CancelFunc
	openDriver func(context.Context) (*ydb.Driver, error)
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
	opts = append(opts, connlog.WithTraces(l, detailsNone))
	return opts
}

func setupMetrics(mh metrics.Handler) []ydb.Option {
	opts := make([]ydb.Option, 0)
	if mh == nil {
		return opts
	}
	mc := connmetrics.MakeConfig(
		mh,
		connmetrics.WithNamespace("xydb"),
		connmetrics.WithDetails(detailsFull),
		connmetrics.WithSeparator("_"),
	)
	opts = append(opts,
		ydbmetrics.WithTraces(mc),
	)
	return opts
}

func haveCredentialsInEnv() bool {
	for _, env := range ydbCredentialEnvs {
		if os.Getenv(env) != "" {
			return true
		}
	}
	return false
}

func NewClient(ctx context.Context, cfg config.Config, logger tlog.Logger, mh metrics.Handler, opts ...ydb.Option) (
	*Client,
	error,
) {
	client := NewLazyClient(cfg, logger, mh, opts...)
	db, err := client.openDriver(ctx)
	if err != nil {
		client.cancel()
		return nil, err
	}
	client.db = db
	return client, nil
}

// NewLazyClient defers opening the SDK driver until the first operation.
// Failed opens are retried by the next operation.
func NewLazyClient(cfg config.Config, logger tlog.Logger, mh metrics.Handler, opts ...ydb.Option) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	opts = slices.Clone(opts)
	return &Client{
		Database:    cfg.Database,
		Folder:      cfg.Folder,
		logger:      logger,
		useOldTypes: cfg.UseOldTypes,
		ctx:         ctx,
		cancel:      cancel,
		driverLock:  make(chan struct{}, 1),
		openDriver: func(ctx context.Context) (*ydb.Driver, error) {
			return openDriver(ctx, cfg, logger, mh, opts...)
		},
	}
}

// lockDriver acquires the driver lock. It gives up if ctx is canceled while
// another goroutine is opening the driver, or if the client gets closed.
func (client *Client) lockDriver(ctx context.Context) error {
	select {
	case client.driverLock <- struct{}{}: // acquired the lock
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-client.ctx.Done():
		return serviceerror.NewUnavailable("ydb client is closed")
	}
}

// unlockDriver releases the driver lock.
func (client *Client) unlockDriver() {
	<-client.driverLock
}

// Driver opens the SDK driver on demand. Prefer the operation methods unless
// direct access to SDK services is needed.
func (client *Client) Driver(ctx context.Context) (*ydb.Driver, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := client.lockDriver(ctx); err != nil {
		return nil, err
	}
	defer client.unlockDriver()
	if client.ctx.Err() != nil {
		return nil, serviceerror.NewUnavailable("ydb client is closed")
	}
	if client.db != nil {
		return client.db, nil
	}
	openCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	stop := context.AfterFunc(client.ctx, cancel)
	defer stop()
	db, err := client.openDriver(openCtx)
	if err != nil {
		return nil, err
	}
	client.db = db
	return db, nil
}

func openDriver(ctx context.Context, cfg config.Config, logger tlog.Logger, mh metrics.Handler, opts ...ydb.Option) (*ydb.Driver, error) {
	if cfg.Token != "" {
		opts = append(opts, ydb.WithAccessTokenCredentials(cfg.Token))
	} else if haveCredentialsInEnv() {
		opts = append(opts, ydbenv.WithEnvironCredentials())
	} else {
		logger.Info("no credentials provided for ydb client, relying on opts...")
	}

	opts = append(opts, setupLogger(logger)...)
	// opts = append(opts, setupMetrics(mh)...)
	opts = append(opts, ydb.WithSessionPoolIdleThreshold(time.Second*10))

	balancerConfig := balancers.RandomChoice()
	if cfg.PreferLocalDC {
		balancerConfig = balancers.PreferNearestDCWithFallBack(balancerConfig)
	}
	if cfg.DangerouslySkipDiscovery {
		balancerConfig = balancers.SingleConn()
	}
	opts = append(opts, ydb.WithBalancer(balancerConfig))

	if cfg.DBEndpointConnectParams != nil {
		connectParams := grpc.ConnectParams{
			MinConnectTimeout: cfg.DBEndpointConnectParams.MinConnectTimeout,
		}
		if cfg.DBEndpointConnectParams.Backoff != nil {
			connectParams.Backoff = backoff.Config{
				BaseDelay:  cfg.DBEndpointConnectParams.Backoff.BaseDelay,
				Multiplier: cfg.DBEndpointConnectParams.Backoff.Multiplier,
				Jitter:     cfg.DBEndpointConnectParams.Backoff.Jitter,
				MaxDelay:   cfg.DBEndpointConnectParams.Backoff.MaxDelay,
			}
		}
		opts = append(opts, ydb.With(ydbconfig.WithGrpcOptions(
			grpc.WithConnectParams(connectParams),
		)))
	}

	discoveryDialTimeout := cfg.DiscoveryDialTimeout
	if discoveryDialTimeout == 0 {
		discoveryDialTimeout = 10 * time.Second
	}
	opts = append(opts, ydb.WithDialTimeout(discoveryDialTimeout))

	sessionPoolSizeLimit := cfg.SessionPoolSizeLimit
	if sessionPoolSizeLimit > 0 {
		opts = append(opts, ydb.WithSessionPoolSizeLimit(sessionPoolSizeLimit))
	}

	db, err := ydb.Open(
		ctx,
		sugar.DSN(cfg.Endpoint, cfg.Database, sugar.WithSecure(cfg.UseSSL)),
		opts...,
	)
	if err != nil {
		return nil, xerrors.Errorf("connect error: %w", err)
	}
	return db, nil
}

func (client *Client) Close(ctx context.Context) error {
	client.cancel()
	// Plain send instead of lockDriver: cancel() above has already fired
	// client.ctx.Done(), so lockDriver would refuse to acquire the lock.
	client.driverLock <- struct{}{}
	defer client.unlockDriver()
	db := client.db
	client.db = nil
	if db == nil {
		return nil
	}
	closeCtx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	ts := time.Now()
	defer func() {
		client.logger.Info(fmt.Sprintf("ydb close duration: %s", time.Since(ts)))
	}()
	return db.Close(closeCtx)
}

func (client *Client) GetDefaultPrefix() string {
	return client.GetPrefix(client.Folder)
}

func (client *Client) GetPrefix(folder string) string {
	return fmt.Sprintf("%s/%s", client.Database, folder)
}

func (client *Client) queryPrefix(tablePath string) string {
	return fmt.Sprintf("--!syntax_v1\nPRAGMA TablePathPrefix(\"%s\");\n", tablePath)
}

func (client *Client) AddQueryPrefix(query string) string {
	return client.queryPrefix(client.GetDefaultPrefix()) + query
}

func (client *Client) AddCustomQueryPrefix(folder string, query string) string {
	return client.queryPrefix(client.GetPrefix(folder)) + query
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

func (client *Client) Query(
	ctx context.Context,
	sql string,
	params *table.QueryParameters,
) (rs query.ClosableResultSet, err error) {
	db, err := client.Driver(ctx)
	if err != nil {
		return nil, err
	}
	return db.Query().QueryResultSet(ctx,
		sql,
		query.WithParameters(params),
		query.WithTxControl(query.SnapshotReadOnlyTxControl()),
		query.WithIdempotent(),
	)
}

func (client *Client) Do(
	ctx context.Context,
	query string,
	tx *table.TransactionControl,
	params *table.QueryParameters,
	opts ...table.Option,
) (res result.Result, err error) {
	err = client.DoSession(
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
	err = client.DoSession(
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
	err = client.DoSession(
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

// DoSession runs operation on a table session, opening the driver on first use.
func (client *Client) DoSession(ctx context.Context, operation table.Operation, opts ...table.Option) error {
	db, err := client.Driver(ctx)
	if err != nil {
		return err
	}
	return db.Table().Do(ctx, operation, opts...)
}

// DoTx runs operation in a transaction, opening the driver on first use.
func (client *Client) DoTx(ctx context.Context, operation table.TxOperation, opts ...table.Option) error {
	db, err := client.Driver(ctx)
	if err != nil {
		return err
	}
	return db.Table().DoTx(ctx, operation, opts...)
}
