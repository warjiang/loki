package queryrange

import (
	"github.com/pkg/errors"
)

var errInvalidShardingRange = errors.New("Query does not fit in a single sharding configuration")
