package persistencetests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/tests"
)

func TestYDBShardStoreSuite(t *testing.T) {
	testData, tearDown := setUpYDBTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	require.NoError(t, err)

	s := tests.NewShardSuite(
		t,
		shardStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestYDBExecutionMutableStateStoreSuite(t *testing.T) {
	testData, tearDown := setUpYDBTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	require.NoError(t, err)

	executionStore, err := testData.Factory.NewExecutionStore()
	require.NoError(t, err)

	s := tests.NewExecutionMutableStateSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestYDBExecutionMutableStateTaskStoreSuite(t *testing.T) {
	testData, tearDown := setUpYDBTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	require.NoError(t, err)

	executionStore, err := testData.Factory.NewExecutionStore()
	require.NoError(t, err)

	s := tests.NewExecutionMutableStateTaskSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestYDBHistoryStoreSuite(t *testing.T) {
	testData, tearDown := setUpYDBTest(t)
	defer tearDown()

	executionStore, err := testData.Factory.NewExecutionStore()
	require.NoError(t, err)

	s := tests.NewHistoryEventsSuite(t, executionStore, testData.Logger)
	suite.Run(t, s)
}

func TestYDBTaskQueueSuite(t *testing.T) {
	testData, tearDown := setUpYDBTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	require.NoError(t, err)
	s := tests.NewTaskQueueSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestYDBTaskQueueTaskSuite(t *testing.T) {
	testData, tearDown := setUpYDBTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	require.NoError(t, err)

	s := tests.NewTaskQueueTaskSuite(t, taskQueueStore, testData.Logger)
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
