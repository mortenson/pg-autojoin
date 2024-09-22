package pg_autojoin

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/rueian/pgbroker/backend"
	"github.com/rueian/pgbroker/message"
	"github.com/rueian/pgbroker/proxy"
)

type ProxyServer struct {
	server *proxy.Server
}

type ProxyServerConfig struct {
	OnlyRespondToAutoJoins       bool
	ShouldPrefixFieldDescriptors bool
	ProxyAddress                 string
	MaxCacheTTL                  time.Duration
	JoinBehavior                 JoinBehavior
	TLSConfig                    *tls.Config
}

func errAttr(err error) slog.Attr {
	return slog.Any("error", err)
}

func (s *ProxyServer) Serve(ln net.Listener) error {
	return s.server.Serve(ln)
}

func (s *ProxyServer) Shutdown() {
	s.server.Shutdown()
}

func handleQueryStringMessage(cfg ProxyServerConfig, ctx *proxy.Ctx, queryString string) string {
	defaultReturn := queryString

	onlyJoin := strings.Contains(queryString, "AUTOJOIN")
	if onlyJoin {
		queryString = strings.ReplaceAll(queryString, "AUTOJOIN", "")
		defaultReturn = "SELECT 'unable to autojoin' AS new_query;"
	} else if cfg.OnlyRespondToAutoJoins {
		return defaultReturn
	}

	databaseInfo, err := getDatabaseInfo(ctx.Context, buildDbUrl(ctx), cfg.MaxCacheTTL)
	if err != nil {
		slog.Error("Could not get db info for query", errAttr(err))
		return defaultReturn
	}
	parsedQuery, err := pg_query.Parse(queryString)
	if err != nil {
		slog.Debug("Could not parse query", errAttr(err))
		return defaultReturn
	}
	columnToTableMap, err := AddMissingJoinsToQuery(parsedQuery, *databaseInfo, cfg.JoinBehavior)
	if err != nil {
		slog.Debug("Could not add missing joins to query", errAttr(err))
		return defaultReturn
	}
	deparse, err := pg_query.Deparse(parsedQuery)
	if err != nil {
		slog.Debug("Could not deparse query after adding joins", errAttr(err))
		return defaultReturn
	}
	slog.Debug(fmt.Sprintf("Old query:\n\t%s \n", queryString))
	slog.Debug(fmt.Sprintf("New query:\n\t%s \n", deparse))

	if onlyJoin {
		return fmt.Sprintf("SELECT %s AS new_query;", pq.QuoteLiteral(deparse))
	} else {
		ctx.ExtraData = map[string]interface{}{}
		ctx.ExtraData["columnToTableMap"] = columnToTableMap
		return deparse
	}
}

func NewProxyServer(cfg ProxyServerConfig) *ProxyServer {
	clientMessageHandlers := proxy.NewClientMessageHandlers()

	// I'm not exactly sure when parse and query happen - psql seems to never
	// send requests to parse, but some clients like pgx do, so I guess handle
	// both the same.
	clientMessageHandlers.AddHandleParse(func(ctx *proxy.Ctx, msg *message.Parse) (query *message.Parse, e error) {
		msg.QueryString = handleQueryStringMessage(cfg, ctx, msg.QueryString)
		return msg, nil
	})

	clientMessageHandlers.AddHandleQuery(func(ctx *proxy.Ctx, msg *message.Query) (query *message.Query, e error) {
		msg.QueryString = handleQueryStringMessage(cfg, ctx, msg.QueryString)
		return msg, nil
	})

	serverMessageHandlers := proxy.NewServerMessageHandlers()

	serverMessageHandlers.AddHandleRowDescription(func(ctx *proxy.Ctx, msg *message.RowDescription) (*message.RowDescription, error) {
		columnToTableMap, ok := ctx.ExtraData["columnToTableMap"].(map[string]string)
		if !ok || !cfg.ShouldPrefixFieldDescriptors {
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

	return &ProxyServer{
		server: &proxy.Server{
			PGResolver:            backend.NewStaticPGResolver(cfg.ProxyAddress),
			ConnInfoStore:         backend.NewInMemoryConnInfoStore(),
			ClientMessageHandlers: clientMessageHandlers,
			ServerMessageHandlers: serverMessageHandlers,
			TLSConfig:             cfg.TLSConfig,
			// For some reason this is required for ClientMessageHandlers to work.
			ServerStreamCallbackFactories: proxy.NewStreamCallbackFactories(),
		},
	}
}

type DatabaseInfoCache struct {
	DatabaseInfo *DatabaseInfo
	CreatedAt    time.Time
}

// Possibly stupid way to lock individual keys in a map.
var databaseInfoCache = map[string]*DatabaseInfoCache{}
var infoCacheLocks = sync.Map{}

func getDatabaseInfo(ctx context.Context, dburl string, maxCacheTTL time.Duration) (*DatabaseInfo, error) {
	storedLock, _ := infoCacheLocks.LoadOrStore(dburl, &sync.RWMutex{})
	lock := storedLock.(*sync.RWMutex)

	// Read existing cache.
	lock.RLock()
	cacheInfo, hasCacheInfo := databaseInfoCache[dburl]
	lock.RUnlock()
	if hasCacheInfo && maxCacheTTL != 0 && time.Since(cacheInfo.CreatedAt) < maxCacheTTL {
		return cacheInfo.DatabaseInfo, nil
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
	databaseInfo, err := GetDatabaseInfoResult(ctx, conn)
	if err != nil {
		return nil, err
	}
	databaseInfoCache[dburl] = &DatabaseInfoCache{
		DatabaseInfo: &databaseInfo,
		CreatedAt:    time.Now(),
	}
	return databaseInfoCache[dburl].DatabaseInfo, nil
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
