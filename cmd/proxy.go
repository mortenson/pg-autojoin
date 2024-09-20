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

// Possibly stupid way to lock individual keys in a map.
var databaseInfoCache = map[string]*pg_autojoin.DatabaseInfo{}
var infoCacheLocks = sync.Map{}

func getDatabaseInfo(ctx context.Context, dburl string) (*pg_autojoin.DatabaseInfo, error) {
	storedLock, _ := infoCacheLocks.LoadOrStore(dburl, &sync.RWMutex{})
	lock := storedLock.(*sync.RWMutex)

	// Read existing cache.
	lock.RLock()
	cacheInfo, hasCacheInfo := databaseInfoCache[dburl]
	lock.RUnlock()
	if hasCacheInfo {
		return cacheInfo, nil
	}

	// Insert new cache.
	lock.Lock()
	defer lock.Unlock()

	// @todo It'd be nice to re-use existing connection we have via the proxy but seems not possible with pgx?
	conn, err := pgx.Connect(ctx, dburl)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	// Gather information on what columns, tables, and fkeys exists.
	databaseInfo, err := pg_autojoin.GetDatabaseInfoResult(ctx, conn)
	if err != nil {
		return nil, err
	}
	databaseInfoCache[dburl] = &databaseInfo
	return databaseInfoCache[dburl], nil
}

// Construct a connection string based on connection parameters.
func buildDbUrl(ctx *proxy.Ctx) string {
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
	return dburl
}

func main() {
	verbosePtr := flag.Bool("v", false, "enable verbose output")
	listenPointer := flag.String("l", "127.0.0.1:5337", "local listen address")
	proxyPointer := flag.String("r", "127.0.0.1:5432", "remote postgres server address")
	prefix := flag.Bool("prefix", false, "prefix row descriptors with the newly joined table")
	help := flag.Bool("h", false, "show help")
	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	shouldPrefixFieldDescriptors := *prefix

	if *verbosePtr {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	ln, err := net.Listen("tcp", *listenPointer)
	if err != nil {
		panic(err)
	}

	clientMessageHandlers := proxy.NewClientMessageHandlers()

	clientMessageHandlers.AddHandleQuery(func(ctx *proxy.Ctx, msg *message.Query) (query *message.Query, e error) {
		databaseInfo, err := getDatabaseInfo(ctx.Context, buildDbUrl(ctx))
		if err != nil {
			slog.Error("Could not get db info for query", errAttr(err))
			return msg, nil
		}
		parsedQuery, err := pg_query.Parse(msg.QueryString)
		if err != nil {
			slog.Error("Could not parse query", errAttr(err))
			return msg, nil
		}
		columnToTableMap, err := pg_autojoin.AddMissingJoinsToQuery(parsedQuery, *databaseInfo)
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

		ctx.ExtraData = map[string]interface{}{}
		ctx.ExtraData["columnToTableMap"] = columnToTableMap

		return msg, nil
	})

	serverMessageHandlers := proxy.NewServerMessageHandlers()

	serverMessageHandlers.AddHandleRowDescription(func(ctx *proxy.Ctx, msg *message.RowDescription) (*message.RowDescription, error) {
		columnToTableMap, ok := ctx.ExtraData["columnToTableMap"].(map[string]string)
		if !ok || !shouldPrefixFieldDescriptors {
			return msg, nil
		}
		for i := range msg.Fields {
			table, hasTable := columnToTableMap[msg.Fields[i].Name]
			if hasTable {
				msg.Fields[i].Name = table + "_" + msg.Fields[i].Name
			}
		}
		return msg, nil
	})

	server := proxy.Server{
		PGResolver:            backend.NewStaticPGResolver(*proxyPointer),
		ConnInfoStore:         backend.NewInMemoryConnInfoStore(),
		ClientMessageHandlers: clientMessageHandlers,
		ServerMessageHandlers: serverMessageHandlers,
		// For some reason this is required for ClientMessageHandlers to work.
		ServerStreamCallbackFactories: proxy.NewStreamCallbackFactories(),
	}

	go server.Serve(ln)

	slog.Info(fmt.Sprintf("Proxying %s => %s", ln.Addr(), *proxyPointer))

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	server.Shutdown()
}
