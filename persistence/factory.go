package persistence

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/yandex/temporal-over-ydb/xydb"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
)

type (
	// Factory vends datastore implementations backed by YDB
	Factory struct {
		sync.RWMutex
		clusterName    string
		cfg            xydb.Config
		logger         log.Logger
		client         *xydb.Client
		metricsHandler metrics.Handler
	}
)

func OptionsToYDBConfig(options map[string]any) (xydb.Config, error) {
	cfg := xydb.Config{}

	if endpoint, ok := options["endpoint"].(string); ok {
		cfg.Endpoint = endpoint
	} else {
		return xydb.Config{}, errors.New("missing required option: endpoint")
	}

	if database, ok := options["database"].(string); ok {
		cfg.Database = database
	} else {
		return xydb.Config{}, errors.New("missing required option: database")
	}

	if folder, ok := options["folder"].(string); ok {
		cfg.Folder = folder
	} else {
		return xydb.Config{}, errors.New("missing required option: folder")
	}

	if token, ok := options["token"].(string); ok {
		cfg.Token = token
	} else {
		return xydb.Config{}, errors.New("missing required option: token")
	}

	if sessionPoolSizeLimit, ok := options["session_pool_size_limit"].(int); ok {
		cfg.SessionPoolSizeLimit = sessionPoolSizeLimit
	}

	if useSSLStr, ok := options["use_ssl"].(string); ok {
		parsedUseSSL, err := strconv.ParseBool(useSSLStr)
		if err != nil {
			return xydb.Config{}, errors.New("invalid value for use_ssl option")
		}
		cfg.UseSSL = parsedUseSSL
	} else if useSSLBool, ok := options["use_ssl"].(bool); ok {
		cfg.UseSSL = useSSLBool
	} else {
		cfg.UseSSL = false
	}

	return cfg, nil
}

type ydbAbstractDataStoreFactory struct {
}

func NewYDBAbstractDataStoreFactory() client.AbstractDataStoreFactory {
	return &ydbAbstractDataStoreFactory{}
}

func (*ydbAbstractDataStoreFactory) NewFactory(
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
	ydbCfg, err := OptionsToYDBConfig(cfg.Options)
	if err != nil {
		logger.Fatal("unable to initialize custom datastore config for YDB", tag.Error(err))
	}
	return NewFactoryFromYDBConfig(clusterName, ydbCfg, r, logger, metricsHandler)
}

func NewYDBClientFromConfig(cfg config.CustomDatastoreConfig, r resolver.ServiceResolver, logger log.Logger) (*xydb.Client, error) {
	ydbCfg, err := OptionsToYDBConfig(cfg.Options)
	if err != nil {
		logger.Fatal("unable to initialize custom datastore config for YDB", tag.Error(err))
	}
	var noopMetricsHandler metrics.Handler = nil // XXX(romanovich@)
	ydbClient, err := xydb.NewClient(context.Background(), ydbCfg, logger, noopMetricsHandler)
	return ydbClient, err
}

func NewFactoryFromYDBConfig(
	clusterName string,
	ydbCfg xydb.Config,
	r resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *Factory {
	ydbCfg.Endpoint = r.Resolve(ydbCfg.Endpoint)[0]
	ydbClient, err := xydb.NewClient(context.Background(), ydbCfg, logger, metricsHandler)
	if err != nil {
		logger.Fatal("unable to initialize YDB session", tag.Error(err))
	}
	return &Factory{
		clusterName:    clusterName,
		cfg:            ydbCfg,
		logger:         logger,
		client:         ydbClient,
		metricsHandler: metricsHandler,
	}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	return NewMatchingTaskStore(f.client, f.logger), nil
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	return NewShardStore(f.clusterName, f.client, f.logger), nil
}

// NewMetadataStore returns a metadata store
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	return NewMetadataStore(f.clusterName, f.client, f.logger)
}

// NewClusterMetadataStore returns a metadata store
func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	return NewClusterMetadataStore(f.client, f.logger)
}

// NewExecutionStore returns a new ExecutionStore.
func (f *Factory) NewExecutionStore() (p.ExecutionStore, error) {
	return NewExecutionStore(f.client, f.logger, f.metricsHandler), nil
}

// NewQueue returns a new queue backed by YDB
func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	return NewQueueStore(queueType, f.client, f.logger)
}

func (f *Factory) NewQueueV2() (p.QueueV2, error) {
	// return NewQueueV2Store(), nil
	return nil, nil
}

func (f *Factory) NewNexusIncomingServiceStore() (p.NexusIncomingServiceStore, error) {
	return NewNexusStore(f.client, f.logger)
}

// Close closes the factory
func (f *Factory) Close() {
	f.Lock()
	defer f.Unlock()
	_ = f.client.Close(context.TODO())
}
