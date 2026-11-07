package httpapi

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"social_service/internal/models"
	"social_service/internal/repository"
)

// Probe errors.
var (
	errShuttingDown          = errors.New("shutting down")
	errDependencyUnavailable = errors.New("dependency unavailable")
)

// respondError maps a domain or storage error onto the documented error body.
// Internal failures never expose driver detail.
func (a *API) respondError(c *gin.Context, err error) {
	if err == nil {
		return
	}
	if domainErr, ok := models.AsDomainError(err); ok {
		c.AbortWithStatusJSON(domainErr.Status, models.ErrorResponse{
			Error:  string(domainErr.Code),
			Detail: domainErr.Detail,
		})
		return
	}
	if errors.Is(err, repository.ErrUnavailable) {
		a.logInternal(c, err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:  string(models.CodeDatabaseUnavailable),
			Detail: "the database is unavailable",
		})
		return
	}
	a.logInternal(c, err)
	c.AbortWithStatusJSON(http.StatusInternalServerError, models.ErrorResponse{
		Error:  string(models.CodeInternal),
		Detail: "internal error",
	})
}

func (a *API) logInternal(c *gin.Context, err error) {
	route := c.FullPath()
	if route == "" {
		route = "unmatched"
	}
	a.deps.Logger.Error("request failed",
		zap.String("service", "social_service"),
		zap.String("trace_id", models.TraceIDFromContext(c.Request.Context())),
		zap.String("operation", c.Request.Method+" "+route),
		zap.String("status", "error"),
		zap.String("detail", err.Error()),
	)
}

// badRequest writes a 400 invalid_request response.
func (a *API) badRequest(c *gin.Context, detail string) {
	c.AbortWithStatusJSON(http.StatusBadRequest, models.ErrorResponse{
		Error:  string(models.CodeInvalidRequest),
		Detail: detail,
	})
}

// invalidRequest builds the domain error used by query/body parsing helpers.
func invalidRequest(detail string) *models.DomainError {
	return models.NewDomainError(models.CodeInvalidRequest, http.StatusBadRequest, detail)
}
