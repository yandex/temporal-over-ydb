package visibility

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/resolver"
)

type ydbVisibilityStoreFactory struct {
}

func (y ydbVisibilityStoreFactory) NewVisibilityStore(cfg config.CustomDatastoreConfig, r resolver.ServiceResolver, logger log.Logger, metricsHandler metrics.Handler) (store.VisibilityStore, error) {
	return NewVisibilityStore(cfg, r, logger)
}

func NewYDBVisibilityStoreFactory() visibility.VisibilityStoreFactory {
	return &ydbVisibilityStoreFactory{}
}
