package proxy

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestProxy(t *testing.T) {
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	server := NewProxyServer(ProxyServerConfig{
		ProxyAddress: "localhost:5432",
	})

	go server.Serve(ln) //nolint:all
	defer server.Shutdown()

	// @todo use temp database since I can't use transactions
	envUrl := os.Getenv("PG_AUTOJOIN_TEST_DATABASE_URL")
	if envUrl == "" {
		envUrl = "postgres://localhost:5432/pg-autojoin-test-db"
	}
	envUrl = strings.Replace(envUrl, "localhost:5432", ln.Addr().String(), 1)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, envUrl)
	if err != nil {
		require.NoError(t, err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `
		DROP TABLE IF EXISTS proxy_test_avatars;
		DROP TABLE IF EXISTS proxy_test_users;

		CREATE TABLE proxy_test_users (
			id INT NOT NULL PRIMARY KEY,
			email TEXT NOT NULL
		);

		CREATE TABLE proxy_test_avatars (
			id INT NOT NULL PRIMARY KEY,
			user_id INT NOT NULL REFERENCES proxy_test_users(id),
			image_url TEXT NOT NULL
		);

		INSERT INTO proxy_test_users VALUES (1, 'foo@bar.com');
		INSERT INTO proxy_test_avatars VALUES (1, 1, 'image.png');
	`)
	require.NoError(t, err)
	row := conn.QueryRow(ctx, "SELECT email,image_url FROM proxy_test_users;")
	var email string
	var imageUrl string
	err = row.Scan(&email, &imageUrl)
	require.NoError(t, err)
	require.Equal(t, "foo@bar.com", email)
	require.Equal(t, "image.png", imageUrl)
	_, err = conn.Exec(ctx, `
		DROP TABLE IF EXISTS proxy_test_avatars;
		DROP TABLE IF EXISTS proxy_test_users;
	`)
	require.NoError(t, err)
}