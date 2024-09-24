package join

import (
	"context"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/mortenson/pg-autojoin/internal/dbinfo"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/stretchr/testify/require"
)

func normalizeString(s string) string {
	return strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, "\n", ""), "; ", ";"))
}

func runTestData(t *testing.T, ctx context.Context, tx pgx.Tx, testDir string) {
	schemaFile, err := os.ReadFile(path.Join(testDir, "schema.sql"))
	require.NoError(t, err)
	queryBefore, err := os.ReadFile(path.Join(testDir, "query_before.sql"))
	require.NoError(t, err)
	queryAfter, err := os.ReadFile(path.Join(testDir, "query_after.sql"))
	require.NoError(t, err)

	_, err = tx.Exec(ctx, string(schemaFile))
	require.NoError(t, err)
	databaseInfo, err := dbinfo.GetDatabaseInfoResult(ctx, tx)
	require.NoError(t, err)
	parsedQuery, err := pg_query.Parse(string(queryBefore))
	require.NoError(t, err)
	_, err = AddMissingJoinsToQuery(parsedQuery, databaseInfo, JoinBehaviorInnerJoin)
	require.NoError(t, err)

	deparse, err := pg_query.Deparse(parsedQuery)
	require.NoError(t, err)
	require.Equal(t, normalizeString(string(queryAfter)), normalizeString(deparse))
}

func TestAutojoin(t *testing.T) {
	envUrl := os.Getenv("PG_AUTOJOIN_TEST_DATABASE_URL")
	if envUrl == "" {
		envUrl = "postgres:///pg-autojoin-test-db"
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, envUrl)
	if err != nil {
		require.NoError(t, err)
	}
	defer conn.Close(ctx)

	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "../../testdata")
	dirEntry, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, e := range dirEntry {
		if !e.IsDir() {
			continue
		}
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)
		runTestData(t, ctx, tx, path.Join(dir, e.Name()))
		err = tx.Rollback(ctx)
		require.NoError(t, err)
	}
}
