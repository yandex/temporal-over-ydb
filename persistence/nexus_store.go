package persistence

import (
	"context"

	"github.com/yandex/temporal-over-ydb/xydb"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

type (
	NoopNexusStore struct {
		client *xydb.Client
		logger log.Logger
	}
)

func NewNexusStore(
	client *xydb.Client,
	logger log.Logger,
) (persistence.NexusIncomingServiceStore, error) {
	return &NoopNexusStore{
		client: client,
		logger: logger,
	}, nil
}

func (n NoopNexusStore) Close() {
}

func (n NoopNexusStore) GetName() string {
	return ""
}

func (n NoopNexusStore) CreateOrUpdateNexusIncomingService(ctx context.Context, request *persistence.InternalCreateOrUpdateNexusIncomingServiceRequest) error {
	return nil
}

func (n NoopNexusStore) DeleteNexusIncomingService(ctx context.Context, request *persistence.DeleteNexusIncomingServiceRequest) error {
	return nil
}

func (n NoopNexusStore) GetNexusIncomingService(ctx context.Context, request *persistence.GetNexusIncomingServiceRequest) (*persistence.InternalNexusIncomingService, error) {
	return &persistence.InternalNexusIncomingService{}, nil
}

func (n NoopNexusStore) ListNexusIncomingServices(ctx context.Context, request *persistence.ListNexusIncomingServicesRequest) (*persistence.InternalListNexusIncomingServicesResponse, error) {
	return &persistence.InternalListNexusIncomingServicesResponse{}, nil
}

var _ persistence.NexusIncomingServiceStore = (*NoopNexusStore)(nil)
