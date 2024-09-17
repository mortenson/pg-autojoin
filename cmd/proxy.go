package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/mortenson/pg_autojoin"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/rueian/pgbroker/backend"
	"github.com/rueian/pgbroker/message"
	"github.com/rueian/pgbroker/proxy"
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

	ln, err := net.Listen("tcp", "127.0.0.1:5337")
	if err != nil {
		panic(err)
	}
	databaseInfoCache := map[string]*pg_autojoin.DatabaseInfo{}
	infoCacheLocks := map[string]*sync.RWMutex{}
	getDatabaseInfo := func(ctx context.Context, dburl string) *pg_autojoin.DatabaseInfo {
		_, hasLock := infoCacheLocks[dburl]
		if !hasLock {
			infoCacheLocks[dburl] = &sync.RWMutex{}
		}
		infoCacheLocks[dburl].RLock()
		cacheInfo, hasCacheInfo := databaseInfoCache[dburl]
		if hasCacheInfo {
			infoCacheLocks[dburl].RUnlock()
			return cacheInfo
		}
		infoCacheLocks[dburl].RUnlock()
		infoCacheLocks[dburl].Lock()
		defer infoCacheLocks[dburl].Unlock()
		// Load new value into cache.
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
		databaseInfo, err := pg_autojoin.GetDatabaseInfoResult(ctx, tx)
		if err != nil {
			slog.Error("Could not gather table info", errAttr(err))
			return nil
		}
		databaseInfoCache[dburl] = &databaseInfo
		return databaseInfoCache[dburl]
	}

	clientMessageHandlers := proxy.NewClientMessageHandlers()

	clientMessageHandlers.AddHandleQuery(func(ctx *proxy.Ctx, msg *message.Query) (query *message.Query, e error) {
		dburl := "postgres://"
		user, hasUser := ctx.ConnInfo.StartupParameters["user"]
		password, hasPassword := ctx.ConnInfo.StartupParameters["password"]
		database, hasDatabase := ctx.ConnInfo.StartupParameters["database"]
		if hasUser {
			dburl += user
			if hasPassword {
				dburl += ":" + password
			}
			dburl += "@"
		}
		dburl += ctx.ConnInfo.ServerAddress.String()
		if hasDatabase {
			dburl += "/" + database
		}
		databaseInfo := getDatabaseInfo(ctx.Context, dburl)
		if databaseInfo == nil {
			slog.Error("Could not get db info for query")
			return msg, nil
		}
		parsedQuery, err := pg_query.Parse(msg.QueryString)
		if err != nil {
			slog.Error("Could not parse query", errAttr(err))
			return msg, nil
		}
		err = pg_autojoin.AddMissingJoinsToQuery(parsedQuery, *databaseInfo)
		if err != nil {
			slog.Error("Could not add missing joins to query", errAttr(err))
			return msg, nil
		}
		deparse, err := pg_query.Deparse(parsedQuery)
		if err != nil {
			slog.Error("Could not deparse query after adding joins", errAttr(err))
			return msg, nil
		}
		slog.Debug(fmt.Sprintf("Old query:\n\t%s \n", msg.QueryString))
		slog.Debug(fmt.Sprintf("New query:\n\t%s \n", deparse))
		msg.QueryString = deparse
		return msg, nil
	})

	serverStreamCallbackFactories := proxy.NewStreamCallbackFactories()

	server := proxy.Server{
		PGResolver:                    backend.NewStaticPGResolver("127.0.0.1:5432"),
		ConnInfoStore:                 backend.NewInMemoryConnInfoStore(),
		ClientMessageHandlers:         clientMessageHandlers,
		ServerStreamCallbackFactories: serverStreamCallbackFactories,
	}

	go server.Serve(ln)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	server.Shutdown()
}
