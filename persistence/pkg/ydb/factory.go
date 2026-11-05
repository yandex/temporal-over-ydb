package ydb

import (
	"context"
	"sync"

	"github.com/mitchellh/mapstructure"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"

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
		dc               *dynamicconfig.Collection
		numHistoryShards int32
	}
)

func OptionsToYDBConfig(options map[string]any) (ydbconfig.Config, error) {
	cfg := ydbconfig.Config{}
	if err := mapstructure.WeakDecode(options, &cfg); err != nil {
		return ydbconfig.Config{}, err
	}

	// YDB-only persistence always uses old types
	cfg.UseOldTypes = true

	if err := cfg.Validate(); err != nil {
		return ydbconfig.Config{}, err
	}

	return cfg, nil
}

type factoryOptions struct {
	useRawShardIDs   bool
	ydbClientOptions []ydb.Option
	numHistoryShards int32
	lazyClient       bool
}

// Option configures YDB data store factories.
type Option func(*factoryOptions)

// WithLazyClient opens the SDK driver on the first operation rather than during factory construction.
func WithLazyClient() Option {
	return func(o *factoryOptions) { o.lazyClient = true }
}

// WithYDBOptions appends extra ydb.Option values to be passed to the underlying
// YDB client when sessions are constructed.
func WithYDBOptions(opts ...ydb.Option) Option {
	return func(o *factoryOptions) {
		o.ydbClientOptions = append(o.ydbClientOptions, opts...)
	}
}

// WithNumHistoryShards sets the history shard count used to encode YDB shard keys.
// It must match the server's history shard count and remain unchanged for an existing database.
// Omitting this option preserves the legacy default of 1024 shards.
// WithNumHistoryShards panics if numHistoryShards is not positive.
func WithNumHistoryShards(numHistoryShards int32) Option {
	if numHistoryShards <= 0 {
		panic("number of history shards must be positive")
	}
	return func(o *factoryOptions) {
		o.numHistoryShards = numHistoryShards
	}
}

// WithRawShardIDs enables storing original shard IDs without spreading them across
// the Uint32 range when enabled is true. Raw shard IDs are used if enabled here
// or through the use_raw_shard_ids datastore option.
// The encoding mode must remain unchanged for an existing database.
func WithRawShardIDs(enabled bool) Option {
	return func(o *factoryOptions) {
		o.useRawShardIDs = enabled
	}
}

type ydbAbstractDataStoreFactory struct {
	opts []Option
	dc   *dynamicconfig.Collection
}

func NewYDBAbstractDataStoreFactory(dc *dynamicconfig.Collection, opts ...Option) client.AbstractDataStoreFactory {
	return &ydbAbstractDataStoreFactory{opts: opts, dc: dc}
}

func (f *ydbAbstractDataStoreFactory) NewFactory(
	cfg config.CustomDatastoreConfig,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
) p.DataStoreFactory {
	// ydbClientOptions is nil here: any WithYDBOptions inside f.opts is merged
	// into the client options by OpenFactoryFromYDBConfig.
	return NewFactory(
		cfg,
		resolver.NewNoopResolver(),
		clusterName,
		logger,
		metricsHandler,
		f.dc,
		nil,
		f.opts...,
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
	dc *dynamicconfig.Collection,
	ydbClientOptions []ydb.Option,
	opts ...Option,
) *Factory {
	ydbCfg, err := OptionsToYDBConfig(cfg.Options)
	if err != nil {
		logger.Fatal("unable to initialize custom datastore config for YDB", tag.Error(err))
	}
	return NewFactoryFromYDBConfig(clusterName, ydbCfg, r, logger, metricsHandler, dc, ydbClientOptions, opts...)
}

func NewFactoryFromYDBConfig(
	clusterName string,
	ydbCfg ydbconfig.Config,
	r resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
	dc *dynamicconfig.Collection,
	ydbClientOptions []ydb.Option,
	opts ...Option,
) *Factory {
	f, err := OpenFactoryFromYDBConfig(
		context.Background(), clusterName, ydbCfg, r, logger, metricsHandler, dc, ydbClientOptions, opts...)
	if err != nil {
		logger.Fatal("unable to initialize YDB session", tag.Error(err))
	}
	return f
}

func OpenFactoryFromYDBConfig(
	ctx context.Context,
	clusterName string,
	ydbCfg ydbconfig.Config,
	r resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
	dc *dynamicconfig.Collection,
	ydbClientOptions []ydb.Option,
	opts ...Option,
) (*Factory, error) {
	options := &factoryOptions{numHistoryShards: 1024}
	for _, opt := range opts {
		opt(options)
	}
	ydbCfg.UseRawShardIDs = ydbCfg.UseRawShardIDs || options.useRawShardIDs
	ydbClientOptions = append(ydbClientOptions, options.ydbClientOptions...)
	ydbCfg.Endpoint = r.Resolve(ydbCfg.Endpoint)[0]
	var ydbClient *conn.Client
	if options.lazyClient {
		ydbClient = conn.NewLazyClient(ydbCfg, logger, metricsHandler, ydbClientOptions...)
	} else {
		var err error
		ydbClient, err = conn.NewClient(ctx, ydbCfg, logger, metricsHandler, ydbClientOptions...)
		if err != nil {
			return nil, err
		}
	}
	return &Factory{
		clusterName:      clusterName,
		cfg:              ydbCfg,
		logger:           logger,
		Client:           ydbClient,
		metricsHandler:   metricsHandler,
		dc:               dc,
		numHistoryShards: options.numHistoryShards,
	}, nil
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	return NewMatchingTaskStore(f.Client, f.logger), nil
}

// NewFairTaskStore returns a new task store with fairness enabled.
// Currently delegates to the regular task store.
func (f *Factory) NewFairTaskStore() (p.TaskStore, error) {
	return f.NewTaskStore()
}

// NewMirroringTaskStore returns a new task store
func (f *Factory) NewMirroringTaskStore() (p.TaskStore, error) {
	return NewMirroringMatchingTaskStore(f.Client, f.logger), nil
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	return NewShardStore(f.clusterName, f.Client, f.logger, f.numHistoryShards, f.cfg.UseRawShardIDs), nil
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
	return NewExecutionStore(f.Client, f.logger, f.metricsHandler, f.numHistoryShards, f.cfg.UseRawShardIDs), nil
}

// NewQueue returns a new queue backed by YDB
func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	return NewQueueStore(queueType, f.Client, f.logger)
}

func (f *Factory) NewQueueV2() (p.QueueV2, error) {
	return NewQueueStoreV2(f.Client, f.logger)
}

func (f *Factory) NewNexusEndpointStore() (p.NexusEndpointStore, error) {
	return NewNexusEndpointStore(f.Client, f.logger)
}

// Close closes the factory
func (f *Factory) Close() {
	f.Lock()
	defer f.Unlock()
	_ = f.Client.Close(context.TODO())
}
