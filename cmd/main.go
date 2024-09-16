package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/dominikbraun/graph"
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
	tableInfo, err := pg_autojoin.GetTableInfoResult(ctx, tx)
	if err != nil {
		slog.Error("Could not gather table info", errAttr(err))
		os.Exit(1)
	}
	// Add all tables to a graph.
	relationshipGraph := graph.New(graph.StringHash, graph.Directed())
	for tableName := range tableInfo {
		relationshipGraph.AddVertex(tableName)
	}
	for tableName, table := range tableInfo {
		for _, fkey := range table.ForeignKeys {
			relationshipGraph.AddEdge(tableName, fkey.ToTable)
		}
	}

	// Create a map of column name to table name for future use.
	columnToTable := map[string][]string{}
	for tableName, table := range tableInfo {
		for _, column := range table.Columns {
			_, ok := columnToTable[column]
			if !ok {
				columnToTable[column] = []string{}
			}
			columnToTable[column] = append(columnToTable[column], tableName)
		}
	}

	// Take in arbitrary query that is missing joins.
	tree, err := pg_query.Parse(userQuery)
	if err != nil {
		slog.Error("Could not parse query", errAttr(err))
		os.Exit(1)
	}

	for _, stmt := range tree.GetStmts() {
		if stmt.Stmt.GetSelectStmt() == nil {
			slog.Error("Could not rollback transaction")
			os.Exit(1)
		}
		// Parse the query.
		query := pg_autojoin.TraverseQuery(stmt, 0)
		queryTableNames := map[string]string{}
		for _, table := range query.Tables {
			queryTableNames[table.Name] = table.Name
		}
		alreadySeenColumns := map[string]bool{}
		allPaths := [][]string{}
		for _, column := range query.Columns {
			// @todo make data structure better
			_, seen := alreadySeenColumns[column.Name]
			if seen {
				continue
			}
			alreadySeenColumns[column.Name] = true
			if column.Type == pg_autojoin.QueryColumnTypeTableWildcard {
				continue
			}
			tablesThatHaveColumn, ok := columnToTable[column.Name]
			if !ok {
				// @todo log error, we must be parsing wrong or database is wrong.
				continue
			}
			columnExistsInQuery := false
			for _, table := range tablesThatHaveColumn {
				_, tableInQuery := queryTableNames[table]
				if tableInQuery {
					columnExistsInQuery = true
					break
				}
			}
			// Column exists in a table that exists in the query, no need to autojoin!
			if columnExistsInQuery {
				continue
			}
			shortestPath := []string{}
			for _, otherTableName := range tablesThatHaveColumn {
				breakOut := false
				for _, queryTableName := range queryTableNames {
					path, _ := graph.ShortestPath(relationshipGraph, queryTableName, otherTableName)
					if len(path) == 0 {
						path, _ = graph.ShortestPath(relationshipGraph, otherTableName, queryTableName)
						slices.Reverse(path)
					}
					if len(path) == 0 {
						continue
					}
					// Users can provide hints via aliases.
					if column.Alias != nil && *column.Alias == otherTableName {
						slog.Debug(fmt.Sprintf("Using alias to imply join for %s.%s", *column.Alias, column.Name))
						shortestPath = path
						breakOut = true
						break
					}
					if len(shortestPath) == 0 || len(path) <= len(shortestPath) {
						shortestPath = path
					}
				}
				if breakOut {
					break
				}
			}
			if len(shortestPath) == 0 {
				slog.Error("Cannot find shortest path", slog.Any("column", column.Name))
				continue
			} else {
				slog.Debug(fmt.Sprintf("Shortest path for %s is %s", column.Name, strings.Join(shortestPath, ", ")))
				allPaths = append(allPaths, shortestPath)
				for _, pathTableName := range shortestPath {
					queryTableNames[pathTableName] = pathTableName
				}
			}
		}

		tableToAlias := map[string]string{}
		for _, table := range query.Tables {
			if table.Alias != nil {
				tableToAlias[table.Name] = *table.Alias
			}
		}
		alias := func(tableName string) string {
			aliasName, ok := tableToAlias[tableName]
			if ok {
				return aliasName
			}
			return tableName
		}
		numJoins := 0
		for _, path := range allPaths {
			lastTable := ""
			for _, tableName := range path {
				if lastTable == "" {
					lastTable = tableName
					continue
				}
				// Add a join.
				// It's much easier to construct a AST from a string than constructing one ourselves.
				var fromTable string
				var matchingFkey *pg_autojoin.ForeignKey
				for _, fkey := range tableInfo[lastTable].ForeignKeys {
					if fkey.ToTable == tableName {
						matchingFkey = fkey
						fromTable = lastTable
						break
					}
				}
				if matchingFkey == nil {
					for _, fkey := range tableInfo[tableName].ForeignKeys {
						if fkey.ToTable == lastTable {
							matchingFkey = fkey
							fromTable = tableName
							break
						}
					}
				}
				if matchingFkey == nil {
					slog.Error("Could not find matching foreign key", slog.Any("fromTable", lastTable), slog.Any("toTable", tableName))
					break
				}

				joinQuery := "select placeholder FROM foo LEFT JOIN " + alias(tableName) + " ON "
				conditions := []string{}
				for _, fromToPair := range matchingFkey.ColumnConditions {
					conditions = append(conditions, fmt.Sprintf("%s.%s = %s.%s", alias(fromTable), fromToPair[0], alias(matchingFkey.ToTable), fromToPair[1]))
				}
				joinQuery += strings.Join(conditions, " AND ")
				joinParsed, _ := pg_query.Parse(joinQuery)
				// Wrap existing from clause with the new join.
				joinParsed.Stmts[0].Stmt.GetSelectStmt().FromClause[0].GetJoinExpr().Larg = tree.Stmts[0].Stmt.GetSelectStmt().FromClause[0]
				// Replace existing from clause with wrapped from clause.
				tree.Stmts[0].Stmt.GetSelectStmt().FromClause[0] = joinParsed.Stmts[0].Stmt.GetSelectStmt().FromClause[0]
				// The next join will be from this table.
				lastTable = tableName
				numJoins++
			}
		}
		deparse, _ := pg_query.Deparse(tree)
		fmt.Printf("Old query:\n\t%s \n", userQuery)
		fmt.Printf("New query (added %d joins, dank):\n\t%s \n", numJoins, deparse)
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
}
