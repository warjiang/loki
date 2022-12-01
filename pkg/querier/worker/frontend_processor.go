package worker

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"

	querier_stats "github.com/warjiang/loki/pkg/querier/stats"
)

var (
	processorBackoffConfig = backoff.Config{
		MinBackoff: 500 * time.Millisecond,
		MaxBackoff: 5 * time.Second,
	}
)

func newFrontendProcessor(cfg Config, handler RequestHandler, log log.Logger) processor {
	return &frontendProcessor{
		log:            log,
		handler:        handler,
		maxMessageSize: cfg.GRPCClientConfig.MaxSendMsgSize,
		querierID:      cfg.QuerierID,
	}
}

// Handles incoming queries from frontend.
type frontendProcessor struct {
	handler        RequestHandler
	maxMessageSize int
	querierID      string

	log log.Logger
}

// notifyShutdown implements processor.
func (fp *frontendProcessor) notifyShutdown(ctx context.Context, conn *grpc.ClientConn, address string) {

}

// runOne loops, trying to establish a stream to the frontend to begin request processing.
func (fp *frontendProcessor) processQueriesOnSingleStream(ctx context.Context, conn *grpc.ClientConn, address string) {
}

func (fp *frontendProcessor) runRequest(ctx context.Context, request *httpgrpc.HTTPRequest, statsEnabled bool, sendHTTPResponse func(response *httpgrpc.HTTPResponse, stats *querier_stats.Stats) error) {
	var stats *querier_stats.Stats
	if statsEnabled {
		stats, ctx = querier_stats.ContextWithEmptyStats(ctx)
	}

	response, err := fp.handler.Handle(ctx, request)
	if err != nil {
		var ok bool
		response, ok = httpgrpc.HTTPResponseFromError(err)
		if !ok {
			response = &httpgrpc.HTTPResponse{
				Code: http.StatusInternalServerError,
				Body: []byte(err.Error()),
			}
		}
	}

	// Ensure responses that are too big are not retried.
	if len(response.Body) >= fp.maxMessageSize {
		errMsg := fmt.Sprintf("response larger than the max (%d vs %d)", len(response.Body), fp.maxMessageSize)
		response = &httpgrpc.HTTPResponse{
			Code: http.StatusRequestEntityTooLarge,
			Body: []byte(errMsg),
		}
		level.Error(fp.log).Log("msg", "error processing query", "err", errMsg)
	}

	if err := sendHTTPResponse(response, stats); err != nil {
		level.Error(fp.log).Log("msg", "error processing requests", "err", err)
	}
}
