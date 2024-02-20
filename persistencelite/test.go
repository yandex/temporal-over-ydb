package persistencelite

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rqlite/gorqlite"
	"github.com/yandex/temporal-over-ydb/persistencelite/conn"
	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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

func SetupRQLiteDatabase(f conn.RqliteFactory, schemaFile string) error {
	schemaPath, err := filepath.Abs(schemaFile)
	if err != nil {
		return err
	}
	content, err := os.ReadFile(schemaPath)
	if err != nil {
		return err
	}
	for _, conn := range f.ListConns() {
		_, err = conn.WriteParameterizedContext(context.Background(), []gorqlite.ParameterizedStatement{
			{
				Query:     string(content),
				Arguments: []interface{}{},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to execute scheme rqlite query from %s: %s", schemaFile, err)
		}
	}
	return nil
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

func ydbConfigToOptions(cfg YDBLiteConfig) map[string]any {
	options := make(map[string]any)
	options["endpoint"] = cfg.Endpoint
	options["database"] = cfg.Database
	options["folder"] = cfg.Folder
	options["token"] = cfg.Token
	options["rqlite_conn_url"] = cfg.RqliteConnURL
	options["rqlite_conn_n"] = cfg.RqliteConnN
	options["shard_count"] = cfg.ShardCount
	if cfg.UseSSL {
		options["use_ssl"] = "true"
	} else {
		options["use_ssl"] = "false"
	}
	return options
}

// TestCluster allows executing YDB operations in testing.
type TestCluster struct {
	Cfg            YDBLiteConfig
	Logger         log.Logger
	YDBClient      *xydb.Client
	RqliteFactory  conn.RqliteFactory
	database       string
	schemaDir      string
	faultInjection *config.FaultInjection
}

func newYDBLiteConfig(rqliteConnURL string, folder string) YDBLiteConfig {
	return YDBLiteConfig{
		RqliteConnURL: rqliteConnURL,
		RqliteConnN:   1,
		ShardCount:    1000, // for now it's only used for iterating through all shards in history store, so 1000 is fine
		Config: xydb.Config{
			Database: "local",
			Folder:   folder,
			Endpoint: "localhost:2136",
			UseSSL:   false,
		},
	}
}

// NewTestCluster returns a new YDB test cluster
func NewTestCluster(rqliteConnURL, database, username, password, host string, port int, schemaDir string, faultInjection *config.FaultInjection, logger log.Logger) *TestCluster {
	cfg := newYDBLiteConfig(rqliteConnURL, path.Join(testYDBTablesDirectory, database))
	ctx := context.TODO()
	client, err := xydb.NewClient(ctx, cfg.Config, logger, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to create YDB client: %s", err))
	}

	rqliteFactory, err := conn.NewRqliteFactoryFromConnURL(metrics.NoopMetricsHandler, rqliteConnURL, 1)
	if err != nil {
		panic(fmt.Sprintf("Failed to create RQLite factory: %s", err))
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
		Cfg:            cfg,
		YDBClient:      client,
		RqliteFactory:  rqliteFactory,
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
	err := setupYDBDatabase(s.YDBClient, schemaYQLPath, testYDBPrefix)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup YDB temporal database: %s", err))
	}
	schemaYQLPath = path.Join(s.schemaDir, "visibility/schema.yql")
	err = setupYDBDatabase(s.YDBClient, schemaYQLPath, testYDBPrefix)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup YDB visibility database: %s", err))
	}

	schemaSQLPath := path.Join(s.schemaDir, "temporal/schema.sql")
	err = SetupRQLiteDatabase(s.RqliteFactory, schemaSQLPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup RQLite temporal database: %s", err))
	}
}

func (s *TestCluster) TearDownTestDatabase() {
	testYDBPrefix := path.Join(testYDBDatabaseName, testYDBTablesDirectory, s.database)
	if err := cleanDir(s.YDBClient, testYDBPrefix); err != nil {
		panic(fmt.Sprintf("Failed to remove YDB directory %s: %s", testYDBPrefix, err))
	}
	if err := TearDownTestRQLiteDatabase(s.RqliteFactory); err != nil {
		panic(fmt.Sprintf("Failed to tear down RQLite database%s", err))
	}
}

func TearDownTestRQLiteDatabase(f conn.RqliteFactory) error {
	query := `
DROP TABLE IF EXISTS conditions;
DROP TABLE IF EXISTS shards;
DROP TABLE IF EXISTS current_executions;
DROP TABLE IF EXISTS executions;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS history_node;
DROP TABLE IF EXISTS history_tree;
`
	for _, conn := range f.ListConns() {
		_, err := conn.WriteParameterizedContext(context.Background(), []gorqlite.ParameterizedStatement{
			{
				Query:     query,
				Arguments: []interface{}{},
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}
