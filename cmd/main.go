package main

import (
	"encoding/json"
	"log"

	"github.com/mortenson/pg_autojoin"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func main() {
	query := "SELECT u.*,column1 FROM users u WHERE column2 = 'bar' OR column3 = 'foo' AND (column4 = 'zed');"

	// pprint
	jsontree, _ := pg_query.ParseToJSON(query)
	tmp := map[string]interface{}{}
	json.Unmarshal([]byte(jsontree), &tmp)
	out, _ := json.MarshalIndent(tmp, "", " ")
	log.Print(string(out))

	tree, err := pg_query.Parse(query)
	if err != nil {
		panic(err)
	}
	for _, stmt := range tree.GetStmts() {
		query := pg_autojoin.TraverseQuery(stmt, 0)
		log.Printf("%+v", query)
	}
}
