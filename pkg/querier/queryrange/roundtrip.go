package queryrange

import (
	"flag"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/weaveworks/common/httpgrpc"
	"net/http"
	"strings"

	"github.com/warjiang/loki/pkg/loghttp"
	"github.com/warjiang/loki/pkg/logql"
	"github.com/warjiang/loki/pkg/querier/queryrange/queryrangebase"
	"github.com/warjiang/loki/pkg/tenant"
)

// Config is the configuration for the queryrange tripperware
type Config struct {
	queryrangebase.Config `yaml:",inline"`
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Config.RegisterFlags(f)
}

// Stopper gracefully shutdown resources created
type Stopper interface {
	Stop()
}

type roundTripper struct {
	next, log, metric, series, labels, instantMetric http.RoundTripper

	limits Limits
}

func (r roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	err := req.ParseForm()
	if err != nil {
		return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}

	switch op := getOperation(req.URL.Path); op {
	case QueryRangeOp:
		rangeQuery, err := loghttp.ParseRangeQuery(req)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
		}
		expr, err := logql.ParseExpr(rangeQuery.Query)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
		}
		switch e := expr.(type) {
		case logql.SampleExpr:
			return r.metric.RoundTrip(req)
		case logql.LogSelectorExpr:
			expr, err := transformRegexQuery(req, e)
			if err != nil {
				return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
			}
			if err := validateLimits(req, rangeQuery.Limit, r.limits); err != nil {
				return nil, err
			}
			// Only filter expressions are query sharded
			if !expr.HasFilter() {
				return r.next.RoundTrip(req)
			}
			return r.log.RoundTrip(req)

		default:
			return r.next.RoundTrip(req)
		}
	case SeriesOp:
		_, err := logql.ParseAndValidateSeriesQuery(req)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
		}
		return r.series.RoundTrip(req)
	case LabelNamesOp:
		_, err := loghttp.ParseLabelQuery(req)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
		}
		return r.labels.RoundTrip(req)
	case InstantQueryOp:
		instantQuery, err := loghttp.ParseInstantQuery(req)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
		}
		expr, err := logql.ParseExpr(instantQuery.Query)
		if err != nil {
			return nil, httpgrpc.Errorf(http.StatusBadRequest, err.Error())
		}
		switch expr.(type) {
		case logql.SampleExpr:
			return r.instantMetric.RoundTrip(req)
		default:
			return r.next.RoundTrip(req)
		}
	default:
		return r.next.RoundTrip(req)
	}
}

// transformRegexQuery backport the old regexp params into the v1 query format
func transformRegexQuery(req *http.Request, expr logql.LogSelectorExpr) (logql.LogSelectorExpr, error) {
	regexp := req.Form.Get("regexp")
	if regexp != "" {
		filterExpr, err := logql.AddFilterExpr(expr, labels.MatchRegexp, "", regexp)
		if err != nil {
			return nil, err
		}
		params := req.URL.Query()
		params.Set("query", filterExpr.String())
		req.URL.RawQuery = params.Encode()
		// force the form and query to be parsed again.
		req.Form = nil
		req.PostForm = nil
		return filterExpr, nil
	}
	return expr, nil
}

// validates log entries limits
func validateLimits(req *http.Request, reqLimit uint32, limits Limits) error {
	userID, err := tenant.TenantID(req.Context())
	if err != nil {
		return httpgrpc.Errorf(http.StatusBadRequest, err.Error())
	}

	maxEntriesLimit := limits.MaxEntriesLimitPerQuery(userID)
	if int(reqLimit) > maxEntriesLimit && maxEntriesLimit != 0 {
		return httpgrpc.Errorf(http.StatusBadRequest,
			"max entries limit per query exceeded, limit > max_entries_limit (%d > %d)", reqLimit, maxEntriesLimit)
	}
	return nil
}

const (
	InstantQueryOp = "instant_query"
	QueryRangeOp   = "query_range"
	SeriesOp       = "series"
	LabelNamesOp   = "labels"
)

func getOperation(path string) string {
	switch {
	case strings.HasSuffix(path, "/query_range") || strings.HasSuffix(path, "/prom/query"):
		return QueryRangeOp
	case strings.HasSuffix(path, "/series"):
		return SeriesOp
	case strings.HasSuffix(path, "/labels") || strings.HasSuffix(path, "/label"):
		return LabelNamesOp
	case strings.HasSuffix(path, "/v1/query"):
		return InstantQueryOp
	default:
		return ""
	}
}
