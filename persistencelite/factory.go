package persistencelite

import (
	"context"
	"sync"

	ydbp "github.com/yandex/temporal-over-ydb/persistence"
	"github.com/yandex/temporal-over-ydb/persistencelite/conn"
	"github.com/yandex/temporal-over-ydb/xydb"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
)

const persistenceName = "ydb"

type (
	// Factory vends datastore implementations backed by YDB and SQLite
	Factory struct {
		sync.RWMutex
		clusterName     string
		logger          log.Logger
		ydbStoreFactory *ydbp.Factory
		ydbClient       *xydb.Client
		rqliteFactory   conn.RqliteFactory
		shardCount      int32
		metricsHandler  metrics.Handler
	}
)

type ydbLiteAbstractDataStoreFactory struct {
}

func NewYDBAbstractDataStoreFactory() client.AbstractDataStoreFactory {
	return &ydbLiteAbstractDataStoreFactory{}
}

func (*ydbLiteAbstractDataStoreFactory) NewFactory(
	cfg config.CustomDatastoreConfig,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
) client.DataStoreFactory {
	return NewFactory(
		cfg,
		resolver.NewNoopResolver(),
		clusterName,
		logger,
		metricsHandler,
	)
}

// NewFactory returns an instance of a factory object which can be used to create
// data stores that are backed by YDB
func NewFactory(
	cfg config.CustomDatastoreConfig,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *Factory {
	ydbCfg, err := OptionsToYDBLiteConfig(cfg.Options)
	if err != nil {
		logger.Fatal("Failed to read config", tag.Error(err))
	}
	return NewFactoryFromYDBConfig(clusterName, ydbCfg, r, logger, metricsHandler)
}

func NewFactoryFromYDBConfig(
	clusterName string,
	cfg YDBLiteConfig,
	r resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *Factory {
	ydbClient, err := xydb.NewClient(context.Background(), cfg.Config, logger, metricsHandler)
	if err != nil {
		logger.Fatal("Failed to create YDB client", tag.Error(err))
	}
	if cfg.RqliteConnN == 0 {
		cfg.RqliteConnN = 1
	}
	if cfg.ShardCount == 0 {
		logger.Fatal("Shard count is unknown")
	}
	rqliteFactory, err := conn.NewRqliteFactoryFromConnURL(metricsHandler, cfg.RqliteConnURL, cfg.RqliteConnN)
	if err != nil {
		logger.Fatal("Failed to create RQLite factory", tag.Error(err))
	}
	return &Factory{
		clusterName:     clusterName,
		logger:          logger,
		ydbClient:       ydbClient,
		rqliteFactory:   rqliteFactory,
		ydbStoreFactory: ydbp.NewFactoryFromYDBConfig(clusterName, cfg.Config, r, logger, metricsHandler),
		shardCount:      cfg.ShardCount,
		metricsHandler:  metricsHandler,
	}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	return f.ydbStoreFactory.NewTaskStore()
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	return NewShardStore(f.clusterName, f.rqliteFactory, f.logger), nil
}

// NewMetadataStore returns a metadata store
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	return f.ydbStoreFactory.NewMetadataStore()
}

// NewClusterMetadataStore returns a metadata store
func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	return f.ydbStoreFactory.NewClusterMetadataStore()
}

// NewExecutionStore returns a new ExecutionStore.
func (f *Factory) NewExecutionStore() (p.ExecutionStore, error) {
	ydbStore, err := f.ydbStoreFactory.NewExecutionStore()
	if err != nil {
		return nil, err
	}
	return NewExecutionStore(ydbStore, f.shardCount, f.rqliteFactory, f.logger, f.metricsHandler), nil
}

// NewQueue returns a new queue backed by YDB
func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	return f.ydbStoreFactory.NewQueue(queueType)
}

// Close closes the factory
func (f *Factory) Close() {
	f.Lock()
	defer f.Unlock()
	f.ydbStoreFactory.Close()
}
