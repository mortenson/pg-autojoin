# pg_autojoin

Write PostgreSQL queries that SELECT columns from tables you haven't explicitly
joined yet.

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

### Explicit target tables with qualified column names

Things can get slightly awkward when tables have duplicate column names. If you
know the table you want to join (at any depth), you can use qualified column
names.

For example, this query tells pg_autojoin that `avatars` should be joined even
though `users` has an `id` column:

```sql
SELECT avatars.id, email FROM users;
```

## Installation and use

### Using the CLI

The `pg_autojoin` command lets you run a single query and see it joined before
finally executing it against a target (probably local) database. This is useful
for trying out pg_autojoin.

1. Run go install github.com/mortenson/pg_autojoin/cmd/pg_autojoin@latest
2. Set the DATABASE_URL env variable to a PostgreSQL connection string
3. Run `pg_autojoin <your query>`

Run `pg_autojoin --help` for information on flags.

### Proxy a PostgreSQL installation

The `pg_autojoin_proxy` command lets you proxy your PostgreSQL server and
add joins to all SELECTs that need them.

1. Run go install github.com/mortenson/pg_autojoin/cmd/pg_autojoin_proxy@latest
3. Run `pg_autojoin_proxy --listen=<address to listen on> --proxy=<address to proxy>`

Then in your client (ex: `psql`), connect to the proxy instead of the real
PostgreSQL instance.

There are some behaviors that differ from the CLI:

- Columns are prefixed with the newly joined table name. Since clients are
assumed to be humans using `psql` or another interactive tool, it's nice to
tell them where data is coming from instead of being completely opaque.
- You can prefix your SELECTs with `AUTOJOIN` to have the proxy just return
your query with joins added. ex: `AUTOJOIN SELECT email, image_url FROM users;`

Run `pg_autojoin_proxy --help` for information on flags, but here are some
useful ones to know:

- `--cachettl=<number>` - How long database schema should be cached in seconds
- `--onlyjoin=true` - Only respond to queries that include `AUTOJOIN`. less
magical than always trying to autojoin but lets users choose what they want,
and copy+paste the joined query which is nice. Defaults to `false`.

## Security

Proxying PostgreSQL connections is unknown territory for me, so please make
sure to do the following if you run `pg_autojoin_proxy` outside of your local
machine:

- Limit the proxy server's egress so that it can only hit the destination
PostgreSQL host
- Only connect to the proxy with read only users
- Enable TLS by setting `PG_AUTOJOIN_CERTFILE` and `PG_AUTOJOIN_KEYFILE`
