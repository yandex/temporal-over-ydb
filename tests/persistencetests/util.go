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
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yandex/temporal-over-ydb/persistence"
	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/resolver"
	"go.uber.org/zap/zaptest"
)

const (
	testYDBExecutionSchema  = "../../schema/temporal/schema.yql"
	testYDBVisibilitySchema = "../../schema/visibility/schema.yql"

	testYDBDatabaseName    = "local"
	testYDBTablesDirectory = "tests2"
)

type (
	YDBTestData struct {
		Cfg     xydb.Config
		Factory *persistence.Factory
		Logger  log.Logger
	}
)

func setUpYDBTest(t *testing.T) (YDBTestData, func()) {
	testYDBPrefix := path.Join(testYDBDatabaseName, testYDBTablesDirectory)

	testData := YDBTestData{
		Cfg:    newYDBConfig(),
		Logger: log.NewZapLogger(zaptest.NewLogger(t)),
	}

	ctx := context.TODO()

	client, err := xydb.NewClient(ctx, testData.Cfg, testData.Logger, metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("Failed to create YDB client: %s", err)
	}

	setupYDBDatabase(t, ctx, client, testYDBExecutionSchema, testYDBPrefix)
	setupYDBDatabase(t, ctx, client, testYDBVisibilitySchema, testYDBPrefix)

	testData.Factory = persistence.NewFactoryFromYDBConfig(
		"test",
		testData.Cfg,
		resolver.NewNoopResolver(),
		testData.Logger,
		metrics.NoopMetricsHandler,
	)

	tearDown := func() {
		testData.Factory.Close()
		tearDownYDBDatabase(t, ctx, client, testYDBPrefix)
	}

	return testData, tearDown
}

func newYDBConfig() xydb.Config {
	return xydb.Config{
		Database: "local",
		Folder:   testYDBTablesDirectory,
		Endpoint: "localhost:2136",
		UseSSL:   false,
	}
}

func setupYDBDatabase(t *testing.T, ctx context.Context, c *xydb.Client, schemaFile, p string) {
	schemaPath, err := filepath.Abs(schemaFile)
	require.NoError(t, err)

	err = c.DB.Scheme().MakeDirectory(ctx, p)
	if err != nil {
		t.Fatalf("Failed to create YDB directory %s: %s", p, err)
	}

	content, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to read from %s: %s", schemaPath, err)
	}

	err = c.DoSchema(ctx, c.AddQueryPrefix(string(content)))
	if err != nil {
		t.Fatalf("Failed to execute scheme query: %s", err)
	}
}

func cleanDir(ctx context.Context, client *xydb.Client, p string) error {
	schemeClient := client.DB.Scheme()
	dir, err := schemeClient.ListDirectory(ctx, p)
	if err != nil {
		if strings.Contains(err.Error(), "Path not found") {
			return nil
		}
		return err
	}
	for _, c := range dir.Children {
		switch c.Type {
		case scheme.EntryDirectory:
			if err = cleanDir(ctx, client, path.Join(p, c.Name)); err != nil {
				return err
			}
		case scheme.EntryTable:
			session, err := client.DB.Table().CreateSession(ctx)
			if err != nil {
				return err
			}
			err = session.DropTable(ctx, path.Join(p, c.Name))
			if err != nil {
				return err
			}
		}
	}
	return schemeClient.RemoveDirectory(ctx, p)
}

func tearDownYDBDatabase(t *testing.T, ctx context.Context, c *xydb.Client, prefix string) {
	if err := cleanDir(ctx, c, prefix); err != nil {
		t.Fatalf("Failed to remove YDB directory %s: %s", prefix, err)
	}
}

// NewTestBaseWithYDB returns a persistence test base backed by YDB
func NewTestBaseWithYDB(options *persistencetests.TestBaseOptions) persistencetests.TestBase {
	if options.DBName == "" {
		options.DBName = "test_" + persistencetests.GenerateRandomDBName(3)
	}
	logger := log.NewTestLogger()
	testCluster := persistence.NewTestCluster(options.DBName, options.DBUsername, options.DBPassword, options.DBHost, options.DBPort, options.SchemaDir, options.FaultInjection, logger)
	tb := persistencetests.NewTestBaseForCluster(testCluster, logger)

	tb.AbstractDataStoreFactory = persistence.NewYDBAbstractDataStoreFactory()
	return tb
}
