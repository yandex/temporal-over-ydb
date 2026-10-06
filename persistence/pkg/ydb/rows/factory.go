package rows

import (
	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/executor"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

type transactionFactoryImpl struct {
	client           *conn.Client
	numHistoryShards int32
}

func NewTransactionFactory(client *conn.Client, numHistoryShards int32) executor.TransactionFactory {
	return &transactionFactoryImpl{
		client:           client,
		numHistoryShards: numHistoryShards,
	}
}

func (e *transactionFactoryImpl) NewTransaction(shardID int32) executor.Transaction {
	return &transactionImpl{
		client:           e.client,
		numHistoryShards: e.numHistoryShards,
		shardID:          shardID,
	}
}
