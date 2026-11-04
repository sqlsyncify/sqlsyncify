package handler

import (
	"net/http"

	"sqlsyncify/internal/auth"
	"sqlsyncify/internal/svc"

	"github.com/zeromicro/go-zero/rest"
)

func RegisterHandlers(server *rest.Server, serverCtx *svc.ServiceContext) {
	// Token is read per request. SQLSYNCIFY_AUTH_TOKEN overrides AuthToken.
	adminAuth := auth.Middleware(func() string {
		return serverCtx.Config.AdminToken()
	})

	server.AddRoutes(rest.WithMiddlewares(
		[]rest.Middleware{adminAuth},
		rest.Route{
			Method:  http.MethodGet,
			Path:    "/clean/noalias/:site",
			Handler: CleanNoAliasHandler(serverCtx),
		},
		rest.Route{
			Method:  http.MethodGet,
			Path:    "/clean/noalias/v5/:site",
			Handler: CleanNoAliasV5Handler(serverCtx),
		},
		rest.Route{
			Method:  http.MethodGet,
			Path:    "/sync/all/:site",
			Handler: SqlsyncifyAllHandler(serverCtx),
		},
		// POST is not used by the ES synonym plugin. Keep it behind the same middleware
		// so the shared GET handler does not check the token itself.
		rest.Route{
			Method:  http.MethodPost,
			Path:    "/synonym/:site/:lang",
			Handler: SynonymHandler(serverCtx),
		},
	))

	server.AddRoutes(
		[]rest.Route{
			{
				Method:  http.MethodGet,
				Path:    "/",
				Handler: RootHandler(serverCtx),
			},
			{
				// :lang matches sqlsyncify.api, the README, and ES synonym URLs.
				Method:  http.MethodHead,
				Path:    "/synonym/:site/:lang",
				Handler: SynonymHeadHandler(serverCtx),
			},
			{
				Method:  http.MethodGet,
				Path:    "/synonym/:site/:lang",
				Handler: SynonymHandler(serverCtx),
			},
		},
	)
}
