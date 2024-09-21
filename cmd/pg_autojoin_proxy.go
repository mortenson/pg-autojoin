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

	"github.com/mortenson/pg_autojoin"
)

func errAttr(err error) slog.Attr {
	return slog.Any("error", err)
}

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

	var joinBehavior pg_autojoin.JoinBehavior
	if *joinTypePtr == "left" {
		joinBehavior = pg_autojoin.JoinBehaviorLeftJoin
	} else {
		joinBehavior = pg_autojoin.JoinBehaviorInnerJoin
	}

	var tlsConfig *tls.Config
	certFile := os.Getenv("PG_AUTOJOIN_CERTFILE")
	keyFile := os.Getenv("PG_AUTOJOIN_KEYFILE")
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			slog.Error("Cannot load TLS keypair", errAttr(err))
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

	server := pg_autojoin.NewProxyServer(pg_autojoin.ProxyServerConfig{
		OnlyRespondToAutoJoins:       *onlyJoinGlobalPtr,
		ShouldPrefixFieldDescriptors: *prefix,
		ProxyAddress:                 *proxyPointer,
		MaxCacheTTL:                  time.Second * time.Duration(*cacheTTL),
		JoinBehavior:                 joinBehavior,
		TlsConfig:                    tlsConfig,
	})

	go server.Serve(ln)

	slog.Info(fmt.Sprintf("Proxying %s => %s", ln.Addr(), *proxyPointer))

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	server.Shutdown()
}
