// Package httpapi is the HTTP boundary: routing, access-token verification,
// request validation, error shaping, probes, and RED metrics. Handlers stay
// thin and delegate every business rule to the domain services.
package httpapi

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"social_service/internal/conversation"
	"social_service/internal/friendship"
	"social_service/internal/models"
)

// Dependencies are the collaborators the router needs.
type Dependencies struct {
	Friendship   *friendship.Service
	Conversation *conversation.Service
	Verifier     TokenVerifier
	Logger       *zap.Logger
	Clock        models.Clock
	Metrics      *Metrics
	Probes       *ProbeState
	// Registry exposes /metrics when non-nil.
	Registry *prometheus.Registry
}

// API binds the dependencies to the handlers.
type API struct {
	deps Dependencies
	// clock is used only for latency measurement.
	clock models.Clock
}

// NewRouter builds the Gin engine with every documented route registered.
func NewRouter(deps Dependencies) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	if deps.Logger == nil {
		deps.Logger = zap.NewNop()
	}
	if deps.Clock == nil {
		deps.Clock = models.RealClock{}
	}
	if deps.Probes == nil {
		deps.Probes = NewProbeState(nil, nil)
	}
	api := &API{deps: deps, clock: deps.Clock}

	router := gin.New()
	router.Use(api.recoveryMiddleware(), api.traceMiddleware(), api.observeMiddleware())

	router.GET("/healthz", api.healthz)
	router.GET("/readyz", api.readyz)
	if deps.Registry != nil {
		router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(deps.Registry, promhttp.HandlerOpts{})))
	}

	authed := router.Group("/", api.authMiddleware())
	authed.POST("/friend-requests", api.sendFriendRequest)
	authed.GET("/friend-requests", api.listFriendRequests)
	authed.POST("/friend-requests/:request_id/accept", api.acceptFriendRequest)
	authed.POST("/friend-requests/:request_id/decline", api.declineFriendRequest)
	authed.DELETE("/friend-requests/:request_id", api.cancelFriendRequest)
	authed.GET("/friends", api.listFriends)
	authed.DELETE("/friends/:user_id", api.removeFriend)
	authed.POST("/conversations/direct", api.openDirectConversation)
	authed.POST("/conversations/group", api.createGroupConversation)
	authed.GET("/conversations", api.listConversations)
	authed.POST("/conversations/:conversation_id/participants", api.addConversationParticipants)
	authed.POST("/conversations/:conversation_id/leave", api.leaveConversation)
	authed.PATCH("/conversations/:conversation_id", api.renameConversation)

	router.NoRoute(func(c *gin.Context) {
		c.AbortWithStatusJSON(http.StatusNotFound, models.ErrorResponse{
			Error:  string(models.CodeNotFound),
			Detail: "route not found",
		})
	})
	router.HandleMethodNotAllowed = true
	router.NoMethod(func(c *gin.Context) {
		c.AbortWithStatusJSON(http.StatusMethodNotAllowed, models.ErrorResponse{
			Error:  string(models.CodeInvalidRequest),
			Detail: "method not allowed",
		})
	})
	return router
}
