package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mortenson/pg-autojoin/internal/join"
	"github.com/mortenson/pg-autojoin/internal/proxy"
)

func main() {
	verbosePtr := flag.Bool("verbose", false, "enable verbose output")
	listenPointer := flag.String("listen", "127.0.0.1:5337", "local listen address")
	proxyPointer := flag.String("proxy", "127.0.0.1:5432", "remote postgres server address")
	prefix := flag.Bool("prefix", true, "prefix row descriptors with the newly joined table (ex: email => users_email)")
	cacheTTL := flag.Int("cachettl", 60*60, "the maximum number of seconds database schema should be cached")
	joinTypePtr := flag.String("jointype", "inner", "default join type (inner or left)")
	onlyJoinGlobalPtr := flag.Bool("onlyjoin", false, "only respond to AUTOJOIN queries, pass all other queries through untouched")
	help := flag.Bool("help", false, "show help")
	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	if *verbosePtr {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	var joinBehavior join.JoinBehavior
	if *joinTypePtr == "left" {
		joinBehavior = join.JoinBehaviorLeftJoin
	} else {
		joinBehavior = join.JoinBehaviorInnerJoin
	}

	dburl := os.Getenv("DATABASE_URL")
	if dburl == "" {
		slog.Error("DATABASE_URL env variable is required")
		os.Exit(1)
	}
	parsedConfigFromDbUrl, err := pgx.ParseConfig(dburl)
	if err != nil || parsedConfigFromDbUrl.Database == "" {
		slog.Error("Could not parse DATABASE_URL to determine what database you want to proxy")
		os.Exit(1)
	}

	var tlsConfig *tls.Config
	certFile := os.Getenv("PG_AUTOJOIN_CERTFILE")
	keyFile := os.Getenv("PG_AUTOJOIN_KEYFILE")
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			slog.Error("Cannot load TLS keypair", slog.Any("error", err))
			os.Exit(1)
		}
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
	}

	ln, err := net.Listen("tcp", *listenPointer)
	if err != nil {
		panic(err)
	}

	server := proxy.NewProxyServer(proxy.ProxyServerConfig{
		DatabaseName:                 parsedConfigFromDbUrl.Database,
		DatabaseUrl:                  dburl,
		OnlyRespondToAutoJoins:       *onlyJoinGlobalPtr,
		ShouldPrefixFieldDescriptors: *prefix,
		ProxyAddress:                 *proxyPointer,
		MaxCacheTTL:                  time.Second * time.Duration(*cacheTTL),
		JoinBehavior:                 joinBehavior,
		TLSConfig:                    tlsConfig,
	})

	go server.Serve(ln) //nolint:all

	slog.Info(fmt.Sprintf("Proxying %s => %s", ln.Addr(), *proxyPointer))

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	server.Shutdown()
}
