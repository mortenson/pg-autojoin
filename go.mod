module github.com/mortenson/pg_autojoin

go 1.23.0

require (
	github.com/dominikbraun/graph v0.23.0
	github.com/jackc/pgx/v5 v5.7.1
	github.com/pganalyze/pg_query_go/v5 v5.1.0
	github.com/rueian/pgbroker v0.0.18
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	golang.org/x/crypto v0.27.0 // indirect
	golang.org/x/text v0.18.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)

replace github.com/rueian/pgbroker => github.com/mortenson/pgbroker v0.0.1
