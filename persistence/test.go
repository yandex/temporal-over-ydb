package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/tests/testutils"

	"a.yandex-team.ru/contrib/go/patched/goose"
)

const (
	testYDBDatabaseName    = "local"
	testYDBTablesDirectory = "tests"
	testSchemaDir          = "../../schema/temporal"
)

func SetupYDBDatabase(c *xydb.Client, cfg xydb.Config, schemaDir, p string) (err error) {
	err = c.DB.Scheme().MakeDirectory(context.Background(), p)
	if err != nil {
		return fmt.Errorf("failed to create YDB directory %s: %w", p, err)
	}

	// https://blog.ydb.tech/migrations-in-ydb-using-goose-58137bc5c303
	qp := url.Values{}
	qp.Add("go_query_mode", "scripting")
	qp.Add("go_fake_tx", "scripting")
	qp.Add("go_query_bind", fmt.Sprintf("declare,numeric,table_path_prefix(%s)", c.GetPrefix()))
	u := url.URL{
		Scheme:   "grpc",
		Host:     cfg.Endpoint,
		Path:     cfg.Database,
		RawQuery: qp.Encode(),
	}

	db, err := sql.Open("ydb", u.String())
	if err != nil {
		return err
	}
	defer func() {
		if err2 := db.Close(); err2 != nil {
			err = err2
		}
	}()

	err = goose.SetDialect("ydb")
	if err != nil {
		return err
	}
	return goose.Up(db, schemaDir)
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
	if cfg.Token != "" {
		options["token"] = cfg.Token
	}
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
		Database: os.Getenv("YDB_DATABASE"),
		Folder:   folder,
		Endpoint: os.Getenv("YDB_ENDPOINT"),
		UseSSL:   false,
		Token:    "-",
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
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(primitives.DefaultTransactionSizeLimit),
	}
}

func (s *TestCluster) DatabaseName() string {
	return s.database
}

func (s *TestCluster) SetupTestDatabase() {
	testYDBPrefix := path.Join(testYDBDatabaseName, testYDBTablesDirectory, s.database)
	err := SetupYDBDatabase(s.Client, s.Cfg, s.schemaDir, testYDBPrefix)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup YDB temporal database: %s", err))
	}
}

func (s *TestCluster) TearDownTestDatabase() {
	testYDBPrefix := path.Join(testYDBDatabaseName, testYDBTablesDirectory, s.database)
	if err := cleanDir(s.Client, testYDBPrefix); err != nil {
		panic(fmt.Sprintf("Failed to remove YDB directory %s: %s", testYDBPrefix, err))
	}
}
