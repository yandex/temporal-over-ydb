package persistence

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/tests/testutils"
)

const (
	testYDBDatabaseName    = "local"
	testYDBTablesDirectory = "tests"

	testSchemaDir = "../../schema"

	// YDBSeeds env
	YDBSeeds = "YDB_SEEDS"
	// YDBPort env
	YDBPort = "YDB_PORT"
	// YDBDefaultPort YDB default port
	YDBDefaultPort = 2136
	Localhost      = "127.0.0.1"
)

// GetYDBAddress return the YDB address
func GetYDBAddress() string {
	addr := os.Getenv(YDBSeeds)
	if addr == "" {
		addr = Localhost
	}
	return addr
}

// GetYDBPort return the YDB port
func GetYDBPort() int {
	port := os.Getenv(YDBPort)
	if port == "" {
		return YDBDefaultPort
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("error getting env %v", YDBPort))
	}
	return p
}

func setupYDBDatabase(c *xydb.Client, schemaFile, p string) error {
	ctx := context.Background()
	schemaPath, err := filepath.Abs(schemaFile)
	if err != nil {
		return err
	}

	err = c.DB.Scheme().MakeDirectory(ctx, p)
	if err != nil {
		return fmt.Errorf("failed to create YDB directory %s: %w", p, err)
	}

	content, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("failed to read from %s: %w", schemaPath, err)
	}

	err = c.DoSchema(ctx, c.AddQueryPrefix(string(content)))
	if err != nil {
		return fmt.Errorf("failed to execute scheme query: %w", err)
	}
	return nil
}

func cleanDir(client *xydb.Client, p string) error {
	ctx := context.Background()
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
			if err = cleanDir(client, path.Join(p, c.Name)); err != nil {
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

func ydbConfigToOptions(cfg xydb.Config) map[string]any {
	options := make(map[string]any)
	options["endpoint"] = cfg.Endpoint
	options["database"] = cfg.Database
	options["folder"] = cfg.Folder
	options["token"] = cfg.Token
	if cfg.UseSSL {
		options["use_ssl"] = "true"
	} else {
		options["use_ssl"] = "false"
	}
	return options
}

// TestCluster allows executing YDB operations in testing.
type TestCluster struct {
	Cfg            xydb.Config
	Logger         log.Logger
	Client         *xydb.Client
	database       string
	schemaDir      string
	faultInjection *config.FaultInjection
}

func newYDBConfig(folder string) xydb.Config {
	return xydb.Config{
		Database: "local",
		Folder:   folder,
		Endpoint: "localhost:2136",
		UseSSL:   false,
	}
}

// NewTestCluster returns a new YDB test cluster
func NewTestCluster(database, username, password, host string, port int, schemaDir string, faultInjection *config.FaultInjection, logger log.Logger) *TestCluster {
	cfg := newYDBConfig(path.Join(testYDBTablesDirectory, database))
	ctx := context.TODO()
	client, err := xydb.NewClient(ctx, cfg, logger, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to create YDB client: %s", err))
	}

	if schemaDir == "" {
		schemaDir = testSchemaDir
	}

	if !strings.HasPrefix(schemaDir, "/") && !strings.HasPrefix(schemaDir, "../") {
		temporalPackageDir := testutils.GetRepoRootDirectory()
		schemaDir = path.Join(temporalPackageDir, schemaDir)
	}

	tc := &TestCluster{
		database:       database,
		schemaDir:      schemaDir,
		Cfg:            newYDBConfig(path.Join(testYDBTablesDirectory, database)),
		Client:         client,
		Logger:         logger,
		faultInjection: faultInjection,
	}

	return tc
}

// Config returns the persistence config for connecting to this test cluster
func (s *TestCluster) Config() config.Persistence {
	return config.Persistence{
		DefaultStore:    "test",
		VisibilityStore: "test",
		DataStores: map[string]config.DataStore{
			"test": {
				CustomDataStoreConfig: &config.CustomDatastoreConfig{
					Name:    "test",
					Options: ydbConfigToOptions(s.Cfg),
				},
				FaultInjection: s.faultInjection,
			},
		},
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(common.DefaultTransactionSizeLimit),
	}
}

func (s *TestCluster) DatabaseName() string {
	return s.database
}

func (s *TestCluster) SetupTestDatabase() {
	testYDBPrefix := path.Join(testYDBDatabaseName, testYDBTablesDirectory, s.database)
	schemaYQLPath := path.Join(s.schemaDir, "temporal/schema.yql")
	err := setupYDBDatabase(s.Client, schemaYQLPath, testYDBPrefix)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup YDB temporal database: %s", err))
	}
	schemaYQLPath = path.Join(s.schemaDir, "visibility/schema.yql")
	err = setupYDBDatabase(s.Client, schemaYQLPath, testYDBPrefix)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup YDB visibility database: %s", err))
	}
}

func (s *TestCluster) TearDownTestDatabase() {
	testYDBPrefix := path.Join(testYDBDatabaseName, testYDBTablesDirectory, s.database)
	if err := cleanDir(s.Client, testYDBPrefix); err != nil {
		panic(fmt.Sprintf("Failed to remove YDB directory %s: %s", testYDBPrefix, err))
	}
}
