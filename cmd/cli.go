package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/jackc/pgx/v5"
	"github.com/mortenson/pg_autojoin"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func errAttr(err error) slog.Attr {
	return slog.Any("error", err)
}

func main() {
	verbosePtr := flag.Bool("v", false, "enable verbose output")
	help := flag.Bool("h", false, "show help")
	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

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

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dburl)
	if err != nil {
		slog.Error("Could not connect to database", errAttr(err))
		os.Exit(1)
	}
	defer conn.Close(ctx)

	// Gather information on what columns, tables, and fkeys exists.
	databaseInfo, err := pg_autojoin.GetDatabaseInfoResult(ctx, conn)
	if err != nil {
		slog.Error("Could not gather table info", errAttr(err))
		os.Exit(1)
	}

	parsedQuery, err := pg_query.Parse(userQuery)
	if err != nil {
		slog.Error("Could not parse query", errAttr(err))
		os.Exit(1)
	}
	err = pg_autojoin.AddMissingJoinsToQuery(parsedQuery, databaseInfo)
	if err != nil {
		slog.Error("Could not add missing joins to query", errAttr(err))
		os.Exit(1)
	}

	// Turn parsed query back into string.
	deparse, err := pg_query.Deparse(parsedQuery)
	if err != nil {
		slog.Error("Could not deparse query after adding joins", errAttr(err))
		os.Exit(1)
	}
	fmt.Printf("Old query:\n\t%s \n", userQuery)
	fmt.Printf("New query:\n\t%s \n", deparse)

	// Execute query.
	rows, err := conn.Query(ctx, deparse)
	if err != nil {
		slog.Error("Could not run generated query", errAttr(err))
		os.Exit(1)
	}
	// Format results in a table.
	count := 0
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
	columns := []string{}
	headers := []string{}
	for _, desc := range rows.FieldDescriptions() {
		columns = append(columns, string(desc.Name))
		headers = append(headers, strings.Repeat("-", len(desc.Name)))
	}
	fmt.Fprintln(w, strings.Join(columns, "\t"))
	fmt.Fprintln(w, strings.Join(headers, "\t"))
	for rows.Next() {
		count++
		columns := []string{}
		for _, column := range rows.RawValues() {
			columns = append(columns, string(column))
		}
		fmt.Fprintln(w, strings.Join(columns, "\t"))
	}
	fmt.Println()
	w.Flush()
	fmt.Println()
	fmt.Printf("Query returned %d rows\n", count)
}
