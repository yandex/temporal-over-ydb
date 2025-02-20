package persistencetests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/tests"
)

var logger = log.NewNoopLogger()

func TestYDBShardStoreSuite(t *testing.T) {
	factory, tearDown := setUpYDBTest(t)
	defer tearDown()

	shardStore, err := factory.NewShardStore()
	require.NoError(t, err)

	s := tests.NewShardSuite(
		t,
		shardStore,
		serialization.NewSerializer(),
		logger,
	)
	suite.Run(t, s)
}

func TestYDBExecutionMutableStateStoreSuite(t *testing.T) {
	factory, tearDown := setUpYDBTest(t)
	defer tearDown()

	shardStore, err := factory.NewShardStore()
	require.NoError(t, err)

	executionStore, err := factory.NewExecutionStore()
	require.NoError(t, err)

	s := tests.NewExecutionMutableStateSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		&persistence.HistoryBranchUtilImpl{},
		logger,
	)
	suite.Run(t, s)
}

func TestYDBExecutionMutableStateTaskStoreSuite(t *testing.T) {
	factory, tearDown := setUpYDBTest(t)
	defer tearDown()

	shardStore, err := factory.NewShardStore()
	require.NoError(t, err)

	executionStore, err := factory.NewExecutionStore()
	require.NoError(t, err)

	s := tests.NewExecutionMutableStateTaskSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		logger,
	)
	suite.Run(t, s)
}

func TestYDBHistoryStoreSuite(t *testing.T) {
	factory, tearDown := setUpYDBTest(t)
	defer tearDown()

	executionStore, err := factory.NewExecutionStore()
	require.NoError(t, err)

	s := tests.NewHistoryEventsSuite(t, executionStore, logger)
	suite.Run(t, s)
}

func TestYDBTaskQueueSuite(t *testing.T) {
	factory, tearDown := setUpYDBTest(t)
	defer tearDown()

	taskQueueStore, err := factory.NewTaskStore()
	require.NoError(t, err)
	s := tests.NewTaskQueueSuite(t, taskQueueStore, logger)
	suite.Run(t, s)
}

func TestYDBTaskQueueTaskSuite(t *testing.T) {
	factory, tearDown := setUpYDBTest(t)
	defer tearDown()

	taskQueueStore, err := factory.NewTaskStore()
	require.NoError(t, err)

	s := tests.NewTaskQueueTaskSuite(t, taskQueueStore, logger)
	suite.Run(t, s)
}

func TestYDBHistoryV2Persistence(t *testing.T) {
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = NewTestBaseWithYDB(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestYDBMetadataPersistenceV2(t *testing.T) {
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = NewTestBaseWithYDB(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestYDBQueuePersistence(t *testing.T) {
	s := new(persistencetests.QueuePersistenceSuite)
	s.TestBase = NewTestBaseWithYDB(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestYDBClusterMetadataPersistence(t *testing.T) {
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = NewTestBaseWithYDB(&persistencetests.TestBaseOptions{})
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}
