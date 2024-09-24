package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"
	"github.com/mortenson/pg-autojoin/internal/dbinfo"
	"github.com/mortenson/pg-autojoin/internal/join"
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
	JoinBehavior                 join.JoinBehavior
	TLSConfig                    *tls.Config
}

func (s *ProxyServer) Serve(ln net.Listener) error {
	return s.server.Serve(ln)
}

func (s *ProxyServer) Shutdown() {
	s.server.Shutdown()
}

// Queries can contain multiple statements, so the fact that this looks like
// EXPLAIN and sits next to a SELECT is kind of a ruse.
var autojoinKeywordRegexp = regexp.MustCompile("(?i)^AUTOJOIN( VERBOSE)? ")

// When users use AUTOJOIN, we can surface errors to them via the return value.
// Alternatively, could use RAISE, but that would have to be run on the server
// and could be really messy.
func errorMessageAsSelect(msg string) string {
	return fmt.Sprintf("SELECT %s AS error;", pq.QuoteLiteral(msg))
}

// When a user sends a query to the server, this callback will fetch database
// schema if not already cached and add joins to the query.
// Users can add AUTOJOIN to the start of their query to tell us that they just
// want us to transform the query and return - in this case, we surface errors
// directly to them via a SELECT.
// The function is fairly messy because of the AUTOJOIN behavior, but it's nice
// to have so that users don't have to go to the server.
func handleQueryStringMessage(cfg ProxyServerConfig, ctx *proxy.Ctx, queryString string) string {
	keywordParts := autojoinKeywordRegexp.FindStringSubmatch(queryString)
	keywordAutoJoin := len(keywordParts) > 0
	keywordAutoJoinVerbose := keywordAutoJoin && keywordParts[1] != ""
	if keywordAutoJoin {
		queryString = autojoinKeywordRegexp.ReplaceAllString(queryString, "")
	} else if cfg.OnlyRespondToAutoJoins {
		return queryString
	}

	parsedQuery, err := pg_query.Parse(queryString)
	if err != nil {
		slog.Debug("Could not parse query", slog.Any("error", err))
		// The real server likely has a good error for this, and if we can't parse
		// it it's unlikely that the server can.
		return queryString
	}

	databaseInfo, err := getDatabaseInfo(ctx.Context, buildDbUrl(ctx), cfg.MaxCacheTTL)
	if err != nil {
		slog.Error("Could not get db info for query", slog.Any("error", err))
		if keywordAutoJoin {
			return errorMessageAsSelect("Could not get db info for query, unable to autojoin")
		} else {
			return queryString
		}
	}

	joinPlan, err := join.AddMissingJoinsToQuery(parsedQuery, *databaseInfo, cfg.JoinBehavior)
	if err != nil {
		slog.Debug("Could not add missing joins to query", slog.Any("error", err))
		if keywordAutoJoin {
			return errorMessageAsSelect(fmt.Sprintf("Could not add missing joins to query: %v, unable to autojoin", err))
		} else {
			return queryString
		}
	}

	deparse, err := pg_query.Deparse(parsedQuery)
	if err != nil {
		slog.Debug("Could not deparse query after adding joins", slog.Any("error", err))
		if keywordAutoJoin {
			return errorMessageAsSelect(fmt.Sprintf("Could not deparse query after adding joins: %v, unable to autojoin", err))
		} else {
			return queryString
		}
	}
	slog.Debug(fmt.Sprintf("Old query:\n\t%s", queryString))
	slog.Debug(fmt.Sprintf("New query:\n\t%s", deparse))

	if keywordAutoJoin {
		if keywordAutoJoinVerbose && len(joinPlan.MissingColumnsToPossibleTables) > 0 {
			possibleRows := []string{
				"(" + pq.QuoteLiteral(deparse) + ", '', '')",
			}
			for missingColumn, possibleTableNames := range joinPlan.MissingColumnsToPossibleTables {
				possibleRows = append(possibleRows, fmt.Sprintf(
					"('', %s, %s)",
					pq.QuoteLiteral(missingColumn),
					pq.QuoteLiteral(strings.Join(slices.Sorted(maps.Keys(possibleTableNames)), ","))),
				)
			}
			return fmt.Sprintf("SELECT * FROM (VALUES %s) as t (new_query, missing_column, possible_tables)", strings.Join(possibleRows, ","))
		} else {
			return fmt.Sprintf("SELECT %s AS new_query", pq.QuoteLiteral(deparse))
		}
	} else {
		ctx.ExtraData = map[string]interface{}{}
		ctx.ExtraData["joinPlan"] = joinPlan
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

	clientMessageHandlers.AddHandlePasswordMessage(func(ctx *proxy.Ctx, msg *message.PasswordMessage) (*message.PasswordMessage, error) {
		ctx.ExtraData["password"] = msg.Password
		return msg, nil
	})

	serverMessageHandlers := proxy.NewServerMessageHandlers()

	serverMessageHandlers.AddHandleRowDescription(func(ctx *proxy.Ctx, msg *message.RowDescription) (*message.RowDescription, error) {
		joinPlan, ok := ctx.ExtraData["joinPlan"].(join.MissingJoinResult)
		if !ok || !cfg.ShouldPrefixFieldDescriptors {
			return msg, nil
		}
		// It's fairly useful to prefix columns with the joined table, although
		// what would be really nice is to coerce the joiner to go through specific
		// routes to get what you want.
		for i := range msg.Fields {
			table, hasTable := joinPlan.MissingColumnsToJoinedTables[msg.Fields[i].Name]
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
	DatabaseInfo *dbinfo.DatabaseInfo
	CreatedAt    time.Time
}

// Possibly stupid way to lock individual keys in a map.
var databaseInfoCache = map[string]*DatabaseInfoCache{}
var infoCacheLocks = sync.Map{}

func getDatabaseInfo(ctx context.Context, dburl string, maxCacheTTL time.Duration) (*dbinfo.DatabaseInfo, error) {
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
	databaseInfo, err := dbinfo.GetDatabaseInfoResult(ctx, conn)
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
	database, hasDatabase := ctx.ConnInfo.StartupParameters["database"]
	password, hasPassword := ctx.ExtraData["password"].(string)
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
