package ydb

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/cache"
	ydbconfig "github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/config"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

type (
	// Factory vends datastore implementations backed by YDB
	Factory struct {
		sync.RWMutex
		clusterName      string
		cfg              ydbconfig.Config
		logger           log.Logger
		Client           *conn.Client
		metricsHandler   metrics.Handler
		taskCacheFactory cache.TaskCacheFactory

		clientOptions []ydb.Option
	}
)

func readBoolOption(options map[string]any, key string, defaultValue bool) (bool, error) {
	if valueStr, ok := options["use_ssl"].(string); ok {
		value, err := strconv.ParseBool(valueStr)
		if err != nil {
			return false, errors.New("invalid value for use_ssl option")
		}
		return value, nil
	} else if value, ok := options["use_ssl"].(bool); ok {
		return value, nil
	} else {
		return defaultValue, nil
	}
}

func OptionsToYDBConfig(options map[string]any) (ydbconfig.Config, error) {
	cfg := ydbconfig.Config{}

	if endpoint, ok := options["endpoint"].(string); ok {
		cfg.Endpoint = endpoint
	} else {
		return ydbconfig.Config{}, errors.New("missing required option: endpoint")
	}

	if database, ok := options["database"].(string); ok {
		cfg.Database = database
	} else {
		return ydbconfig.Config{}, errors.New("missing required option: database")
	}

	if folder, ok := options["folder"].(string); ok {
		cfg.Folder = folder
	} else {
		return ydbconfig.Config{}, errors.New("missing required option: folder")
	}

	if token, ok := options["token"].(string); ok {
		cfg.Token = token
	}

	if sessionPoolSizeLimit, ok := options["session_pool_size_limit"].(int); ok {
		cfg.SessionPoolSizeLimit = sessionPoolSizeLimit
	}

	useSSL, err := readBoolOption(options, "use_ssl", false)
	if err != nil {
		return ydbconfig.Config{}, errors.New("invalid value for use_ssl option")
	}
	cfg.UseSSL = useSSL

	// YDB-only persistence always uses old types
	cfg.UseOldTypes = true

	return cfg, nil
}

type ydbAbstractDataStoreFactory struct {
	ydbClientOptions []ydb.Option
}

func NewYDBAbstractDataStoreFactory(ydbClientOptions ...ydb.Option) client.AbstractDataStoreFactory {
	return &ydbAbstractDataStoreFactory{
		ydbClientOptions: ydbClientOptions,
	}
}

func (f *ydbAbstractDataStoreFactory) NewFactory(
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
		f.ydbClientOptions,
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
	ydbClientOptions []ydb.Option,
) *Factory {
	ydbCfg, err := OptionsToYDBConfig(cfg.Options)
	if err != nil {
		logger.Fatal("unable to initialize custom datastore config for YDB", tag.Error(err))
	}
	return NewFactoryFromYDBConfig(clusterName, ydbCfg, r, logger, metricsHandler, ydbClientOptions)
}

func NewFactoryFromYDBConfig(
	clusterName string,
	ydbCfg ydbconfig.Config,
	r resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
	ydbClientOptions []ydb.Option,
) *Factory {
	ydbCfg.Endpoint = r.Resolve(ydbCfg.Endpoint)[0]
	ydbClient, err := conn.NewClient(context.Background(), ydbCfg, logger, metricsHandler, ydbClientOptions...)
	if err != nil {
		logger.Fatal("unable to initialize YDB session", tag.Error(err))
	}
	taskCacheFactory := cache.NewNoopTaskCacheFactory()
	// if v := os.Getenv("TEMPORAL_YDBPGX_CACHE_CAPACITY"); v != "" {
	//	if cacheCapacity, err := strconv.Atoi(v); err == nil {
	//		taskCacheFactory = cache.NewTaskCacheFactory(logger, metricsHandler, cacheCapacity)
	//	} else {
	//		logger.Warn("unable to parse TEMPORAL_YDBPGX_CACHE_CAPACITY", tag.Error(err))
	//	}
	// }
	// if cfg.ShardQueueCache.Capacity > 0 {
	//	taskCacheFactory = cache.NewTaskCacheFactory(logger, metricsHandler, cfg.ShardQueueCache.Capacity)
	// }
	return &Factory{
		clusterName:      clusterName,
		cfg:              ydbCfg,
		logger:           logger,
		Client:           ydbClient,
		metricsHandler:   metricsHandler,
		taskCacheFactory: taskCacheFactory,
	}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	return NewMatchingTaskStore(f.Client, f.logger), nil
}

// NewMirroringTaskStore returns a new task store
func (f *Factory) NewMirroringTaskStore() (p.TaskStore, error) {
	return NewMirroringMatchingTaskStore(f.Client, f.logger), nil
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	return NewShardStore(f.clusterName, f.Client, f.logger), nil
}

// NewMetadataStore returns a metadata store
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	return NewMetadataStore(f.clusterName, f.Client, f.logger)
}

// NewMirroringMetadataStore returns a metadata store
func (f *Factory) NewMirroringMetadataStore() (*MirroringMetadataStore, error) {
	return NewMirroringMetadataStore(f.Client)
}

// NewClusterMetadataStore returns a metadata store
func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	return NewClusterMetadataStore(f.Client, f.logger)
}

// NewMirroringClusterMetadataStore returns a new metadata store
func (f *Factory) NewMirroringClusterMetadataStore() (*MirroringClusterMetadataStore, error) {
	return NewMirroringClusterMetadataStore(f.Client)
}

// NewExecutionStore returns a new ExecutionStore.
func (f *Factory) NewExecutionStore() (p.ExecutionStore, error) {
	return NewExecutionStore(f.Client, f.logger, f.metricsHandler, f.taskCacheFactory), nil
}

// NewQueue returns a new queue backed by YDB
func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	return NewQueueStore(queueType, f.Client, f.logger)
}

func (f *Factory) NewQueueV2() (p.QueueV2, error) {
	return NewQueueStoreV2(f.Client, f.logger)
}

func (f *Factory) NewNexusIncomingServiceStore() (p.NexusIncomingServiceStore, error) {
	return NewNexusEndpointStore(f.Client, f.logger)
}

// Close closes the factory
func (f *Factory) Close() {
	f.Lock()
	defer f.Unlock()
	_ = f.Client.Close(context.TODO())
}
