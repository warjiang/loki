package worker

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/grpcclient"
	dskit_middleware "github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func newSchedulerProcessor(cfg Config, handler RequestHandler, log log.Logger, metrics *Metrics) (*schedulerProcessor, []services.Service) {
	p := &schedulerProcessor{
		log:            log,
		handler:        handler,
		maxMessageSize: cfg.GRPCClientConfig.MaxSendMsgSize,
		querierID:      cfg.QuerierID,
		grpcConfig:     cfg.GRPCClientConfig,

		metrics: metrics,
	}

	poolConfig := client.PoolConfig{
		CheckInterval:      5 * time.Second,
		HealthCheckEnabled: true,
		HealthCheckTimeout: 1 * time.Second,
	}

	p.frontendPool = client.NewPool("frontend", poolConfig, nil, p.createFrontendClient, p.metrics.frontendClientsGauge, log)
	return p, []services.Service{p.frontendPool}
}

// Handles incoming queries from query-scheduler.
type schedulerProcessor struct {
	log            log.Logger
	handler        RequestHandler
	grpcConfig     grpcclient.Config
	maxMessageSize int
	querierID      string

	frontendPool *client.Pool
	metrics      *Metrics
}

// notifyShutdown implements processor.
func (sp *schedulerProcessor) notifyShutdown(ctx context.Context, conn *grpc.ClientConn, address string) {

}

func (sp *schedulerProcessor) processQueriesOnSingleStream(ctx context.Context, conn *grpc.ClientConn, address string) {
}

func (sp *schedulerProcessor) runRequest(ctx context.Context, logger log.Logger, queryID uint64, frontendAddress string, statsEnabled bool, request *httpgrpc.HTTPRequest) {

}

func (sp *schedulerProcessor) createFrontendClient(addr string) (client.PoolClient, error) {
	opts, err := sp.grpcConfig.DialOption([]grpc.UnaryClientInterceptor{
		otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
		middleware.ClientUserHeaderInterceptor,
		dskit_middleware.PrometheusGRPCUnaryInstrumentation(sp.metrics.frontendClientRequestDuration),
	}, nil)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &frontendClient{
		HealthClient: grpc_health_v1.NewHealthClient(conn),
		conn:         conn,
	}, nil
}

type frontendClient struct {
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (fc *frontendClient) Close() error {
	return fc.conn.Close()
}
