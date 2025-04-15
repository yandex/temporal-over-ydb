package rows

import (
	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/executor"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

type transactionFactoryImpl struct {
	client *conn.Client
}

func NewTransactionFactory(client *conn.Client) executor.TransactionFactory {
	return &transactionFactoryImpl{
		client: client,
	}
}

func (e *transactionFactoryImpl) NewTransaction(shardID int32) executor.Transaction {
	return &transactionImpl{
		client:  e.client,
		shardID: shardID,
	}
}
