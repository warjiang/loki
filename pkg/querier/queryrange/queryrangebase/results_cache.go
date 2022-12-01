package queryrangebase

import (
	"flag"
	"fmt"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/warjiang/loki/pkg/logproto"
)

var (
	// Value that cacheControlHeader has if the response indicates that the results should not be cached.
	noStoreValue = "no-store"

	// ResultsCacheGenNumberHeaderName holds name of the header we want to set in http response
	ResultsCacheGenNumberHeaderName = "Results-Cache-Gen-Number"
)

type CacheGenNumberLoader interface {
	GetResultsCacheGenNumber(tenantIDs []string) string
}

// ResultsCacheConfig is the config for the results cache.
type ResultsCacheConfig struct {
	Compression string `yaml:"compression"`
}

// RegisterFlags registers flags.
func (cfg *ResultsCacheConfig) RegisterFlags(f *flag.FlagSet) {
}

func (cfg *ResultsCacheConfig) Validate() error {
	return nil
}

// Extractor is used by the cache to extract a subset of a response from a cache entry.
type Extractor interface {
	// Extract extracts a subset of a response from the `start` and `end` timestamps in milliseconds in the `from` response.
	Extract(start, end int64, from Response) Response
	ResponseWithoutHeaders(resp Response) Response
}

// PrometheusResponseExtractor helps extracting specific info from Query Response.
type PrometheusResponseExtractor struct{}

// Extract extracts response for specific a range from a response.
func (PrometheusResponseExtractor) Extract(start, end int64, from Response) Response {
	promRes := from.(*PrometheusResponse)
	return &PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: promRes.Data.ResultType,
			Result:     extractMatrix(start, end, promRes.Data.Result),
		},
		Headers: promRes.Headers,
	}
}

// ResponseWithoutHeaders is useful in caching data without headers since
// we anyways do not need headers for sending back the response so this saves some space by reducing size of the objects.
func (PrometheusResponseExtractor) ResponseWithoutHeaders(resp Response) Response {
	promRes := resp.(*PrometheusResponse)
	return &PrometheusResponse{
		Status: StatusSuccess,
		Data: PrometheusData{
			ResultType: promRes.Data.ResultType,
			Result:     promRes.Data.Result,
		},
	}
}

// CacheSplitter generates cache keys. This is a useful interface for downstream
// consumers who wish to implement their own strategies.
type CacheSplitter interface {
	GenerateCacheKey(userID string, r Request) string
}

// ShouldCacheFn checks whether the current request should go to cache
// or not. If not, just send the request to next handler.
type ShouldCacheFn func(r Request) bool

var errAtModifierAfterEnd = errors.New("at modifier after end")

func extractMatrix(start, end int64, matrix []SampleStream) []SampleStream {
	result := make([]SampleStream, 0, len(matrix))
	for _, stream := range matrix {
		extracted, ok := extractSampleStream(start, end, stream)
		if ok {
			result = append(result, extracted)
		}
	}
	return result
}

func extractSampleStream(start, end int64, stream SampleStream) (SampleStream, bool) {
	result := SampleStream{
		Labels:  stream.Labels,
		Samples: make([]logproto.LegacySample, 0, len(stream.Samples)),
	}
	for _, sample := range stream.Samples {
		if start <= sample.TimestampMs && sample.TimestampMs <= end {
			result.Samples = append(result.Samples, sample)
		}
	}
	if len(result.Samples) == 0 {
		return SampleStream{}, false
	}
	return result, true
}

func (e *Extent) toResponse() (Response, error) {
	msg, err := types.EmptyAny(e.Response)
	if err != nil {
		return nil, err
	}

	if err := types.UnmarshalAny(e.Response, msg); err != nil {
		return nil, err
	}

	resp, ok := msg.(Response)
	if !ok {
		return nil, fmt.Errorf("bad cached type")
	}
	return resp, nil
}
