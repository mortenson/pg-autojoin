package main

import (
	"encoding/json"
	"log"

	"github.com/mortenson/pg_autojoin"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func pprint(jsonStr string) {
	tmp := map[string]interface{}{}
	json.Unmarshal([]byte(jsonStr), &tmp)
	out, _ := json.MarshalIndent(tmp, "", " ")
	log.Print(string(out))
}

func main() {
	query := `
SELECT u.*,column1
FROM users u
JOIN foo on foo.bar = u.bar
WHERE column2 = 'bar'
OR u.column3 = 'foo'
AND (foo.column4 = 'zed');
`
	tree, err := pg_query.Parse(query)
	if err != nil {
		panic(err)
	}
	for _, stmt := range tree.GetStmts() {
		query := pg_autojoin.TraverseQuery(stmt, 0)
		log.Printf("%+v", query)
	}
	joinParsed, _ := pg_query.Parse("SELECT * FROM zed JOIN newshit ON newshit.ok = fuck;")
	// Wrap existing from clause with the new join.
	joinParsed.Stmts[0].Stmt.GetSelectStmt().FromClause[0].GetJoinExpr().Larg = tree.Stmts[0].Stmt.GetSelectStmt().FromClause[0]
	// Replace existing from clause with wrapped from clause.
	tree.Stmts[0].Stmt.GetSelectStmt().FromClause[0] = joinParsed.Stmts[0].Stmt.GetSelectStmt().FromClause[0]
	deparse, _ := pg_query.Deparse(tree)
	log.Print(deparse)
}
