// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package persistencetests

import (
	"testing"

	"github.com/yandex/temporal-over-ydb/persistence"
	"github.com/yandex/temporal-over-ydb/xydb"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/resolver"
)

const (
	testYDBExecutionSchemaDir = "../../schema/temporal/"
)

type (
	YDBTestData struct {
		Cfg     xydb.Config
		Factory *persistence.Factory
		Logger  log.Logger
	}
)

func setUpYDBTest(t *testing.T) (*persistence.Factory, func()) {
	testCluster := NewTestClusterWithYDB(&persistencetests.TestBaseOptions{})
	testCluster.SetupTestDatabase()

	factory := persistence.NewFactoryFromYDBConfig(
		"test",
		testCluster.Cfg,
		resolver.NewNoopResolver(),
		testCluster.Logger,
		metrics.NoopMetricsHandler,
	)
	tearDown := func() {
		factory.Close()
		testCluster.TearDownTestDatabase()
	}
	return factory, tearDown
}

// NewTestClusterWithYDB returns a test cluster backed by YDB
func NewTestClusterWithYDB(options *persistencetests.TestBaseOptions) *persistence.TestCluster {
	if options.DBName == "" {
		options.DBName = "test_" + persistencetests.GenerateRandomDBName(3)
	}
	if options.SchemaDir == "" {
		options.SchemaDir = testYDBExecutionSchemaDir
	}
	logger := log.NewTestLogger()
	return persistence.NewTestCluster(options.DBName, options.DBUsername, options.DBPassword, options.DBHost, options.DBPort, options.SchemaDir, options.FaultInjection, logger)
}

// NewTestBaseWithYDB returns a persistence test base backed by YDB
func NewTestBaseWithYDB(options *persistencetests.TestBaseOptions) *persistencetests.TestBase {
	testCluster := NewTestClusterWithYDB(options)
	tb := persistencetests.NewTestBaseForCluster(testCluster, testCluster.Logger)
	tb.AbstractDataStoreFactory = persistence.NewYDBAbstractDataStoreFactory()
	return tb
}
