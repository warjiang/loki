package lazyquery

import (
	"context"
	"github.com/prometheus/prometheus/storage"
)

// LazyQueryable wraps a storage.Queryable
type LazyQueryable struct {
	q storage.Queryable
}

// Querier implements storage.Queryable
func (lq LazyQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return nil, nil
}
