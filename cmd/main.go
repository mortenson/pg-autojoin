package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/mortenson/pg_autojoin"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func errAttr(err error) slog.Attr {
	return slog.Any("error", err)
}

func main() {
	verbosePtr := flag.Bool("v", false, "enable verbose output")
	flag.Parse()

	if *verbosePtr {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	args := flag.Args()

	// Connect to database and open transaction.
	dburl := os.Getenv("DATABASE_URL")
	if dburl == "" {
		slog.Error("DATABASE_URL env variable is required")
		os.Exit(1)
	}

	// Parse arbitrary user query that may be missing joins.
	if len(args) == 0 {
		slog.Error("Missing query argument")
		os.Exit(1)
	}
	userQuery := args[0]
	parsedQuery, err := pg_query.Parse(userQuery)
	if err != nil {
		slog.Error("Could not parse query", errAttr(err))
		os.Exit(1)
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dburl)
	if err != nil {
		slog.Error("Could not connect to database", errAttr(err))
		os.Exit(1)
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		slog.Error("Could not create transaction", errAttr(err))
		os.Exit(1)
	}
	defer func() {
		err = tx.Rollback(ctx)
		if err != nil && err != pgx.ErrTxClosed {
			slog.Error("Could not rollback transaction", errAttr(err))
			os.Exit(1)
		}
	}()

	// Gather information on what columns, tables, and fkeys exists.
	databaseInfo, err := pg_autojoin.GetDatabaseInfoResult(ctx, tx)
	if err != nil {
		slog.Error("Could not gather table info", errAttr(err))
		os.Exit(1)
	}

	for _, stmt := range parsedQuery.GetStmts() {
		if stmt.Stmt.GetSelectStmt() == nil {
			slog.Error("Could not rollback transaction")
			os.Exit(1)
		}
		err := pg_autojoin.AddMissingJoinsToSelect(stmt, databaseInfo)
		if err != nil {
			slog.Error("Could not add missing joins to select", errAttr(err))
			os.Exit(1)
		}
	}

	deparse, _ := pg_query.Deparse(parsedQuery)
	fmt.Printf("Old query:\n\t%s \n", userQuery)
	fmt.Printf("New query:\n\t%s \n", deparse)
	rows, err := tx.Query(ctx, deparse)
	if err != nil {
		slog.Error("Could not run generated query", errAttr(err))
		os.Exit(1)
	}
	count := 0
	for rows.Next() {
		count++
		// fmt.Println(rows.Values())
	}
	fmt.Println()
	fmt.Printf("Query returned %d rows\n", count)
}
