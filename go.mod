module github.com/mortenson/pg_autojoin

go 1.23.0

require (
	github.com/dominikbraun/graph v0.23.0
	github.com/jackc/pgx/v5 v5.7.1
	github.com/pganalyze/pg_query_go/v5 v5.1.0
	github.com/rueian/pgbroker v0.0.18
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	golang.org/x/crypto v0.27.0 // indirect
	golang.org/x/text v0.18.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/rueian/pgbroker => github.com/mortenson/pgbroker v0.0.2
