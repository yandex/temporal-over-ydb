package visibility

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

type ydbVisibilityStoreFactory struct {
	searchAttributesProvider       searchattribute.Provider
	searchAttributesMapperProvider searchattribute.MapperProvider
}

func (y ydbVisibilityStoreFactory) NewVisibilityStore(cfg config.CustomDatastoreConfig, r resolver.ServiceResolver, logger log.Logger, metricsHandler metrics.Handler) (store.VisibilityStore, error) {
	return NewVisibilityStore(cfg, r, y.searchAttributesProvider, y.searchAttributesMapperProvider, logger)
}

func NewYDBVisibilityStoreFactory(
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
) visibility.VisibilityStoreFactory {
	return &ydbVisibilityStoreFactory{
		searchAttributesProvider:       searchAttributesProvider,
		searchAttributesMapperProvider: searchAttributesMapperProvider,
	}
}
