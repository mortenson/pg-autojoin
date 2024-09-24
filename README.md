# pg-autojoin

Write PostgreSQL queries that SELECT columns from tables you haven't explicitly
joined yet, and pg-autojoin will join for you by introspecting foreign keys.

For example, given this (minimal) schema:

```sql
CREATE TABLE users (
  id INT NOT NULL PRIMARY KEY,
  email TEXT NOT NULL
);

CREATE TABLE avatars (
  id INT NOT NULL PRIMARY KEY,
  user_id INT NOT NULL REFERENCES users(id),
  image_url TEXT NOT NULL
);
```

You can write a query like:

```sql
SELECT email, image_url FROM users;
```

And it will be transformed into:

```sql
SELECT email, image_url FROM users
 JOIN avatars ON avatars.user_id = users.id
```

(note: deeply nested joins look cooler, just wanted to save space)

## Join behavior

In general, the table that has a missing column and is "closest" to a table
that is already in the query will be joined.

Things can get slightly awkward when tables have duplicate column names. If you
know the table you want to join (at any depth), you can use qualified column
names.

For example, this query tells pg-autojoin that `avatars` should be joined even
though `users` already has an `id` column:

```sql
SELECT avatars.id, email FROM users;
```

## Installation and use

### Using the CLI

The `pg-autojoin` command lets you run a single query and see it joined before
finally executing it against a target (probably local) database. This is useful
for trying out pg-autojoin.

1. Run go install github.com/mortenson/pg-autojoin/cmd/pg-autojoin@latest
2. Set the `DATABASE_URL` env variable to a PostgreSQL connection string
3. Run `pg-autojoin <your query>`

Run `pg-autojoin --help` for information on flags.

Example:

```bash
$ pg-autojoin "SELECT email, image_url FROM users;"
Old query:
	SELECT email, image_url FROM users;
New query:
	SELECT email, image_url FROM users JOIN avatars ON avatars.user_id = users.id

email             image_url
-----             ---------
admin@example.com swaggedout.png

Query returned 1 row
```

### Proxy a PostgreSQL installation

The `pg-autojoin-proxy` command lets you proxy your PostgreSQL server and
add joins to all SELECTs that need them.

1. Run go install github.com/mortenson/pg-autojoin/cmd/pg-autojoin-proxy@latest
2. Set the `DATABASE_URL` env variable to a PostgreSQL connection string
3. Run `pg-autojoin-proxy --listen=<address to listen on> --proxy=<address to proxy>`

Then in your client (ex: `psql`), connect to the proxy instead of the real
PostgreSQL instance.

There are some behaviors that differ from the CLI:

- Returned columns are prefixed with the newly joined table name so clients
have some idea of what happened.
- Queries prefixed with `AUTOJOIN` will just return the joined query without
executing it. `AUTOJOIN VERBOSE` will show you all possible tables to join
for every missing column, which can make it clear what columns you need to
fully qualify.

Run `pg-autojoin-proxy --help` for information on flags, but here are some
useful ones to know:

- `--cachettl=<number>` - How long database schema should be cached in seconds.
- `--onlyjoin=true` - Only respond to queries that use `AUTOJOIN`. Less magical
than always trying to autojoin but lets users copy+paste the joined query
themselves. Defaults to `false`.

## Security

Proxying PostgreSQL connections is unknown territory for me, so please make
sure to do the following if you run `pg-autojoin-proxy` outside of your local
machine:

- Limit the proxy server's ingress/egress
- Only connect to the proxy with read only users
- Enable TLS by setting `PG_AUTOJOIN_CERTFILE` and `PG_AUTOJOIN_KEYFILE` to
your X.509 cert/key files (unless you already don't use TLS/SSL, your call)

Note that while the proxy requires a `DATABASE_URL`, the credentials there are
only used to look up schema. Clients still have to authenticate with the
proxied server using normal means.
