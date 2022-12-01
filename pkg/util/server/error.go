package server

import (
	"net/http"
)

// StatusClientClosedRequest is the status code for when a client request cancellation of an http request
const StatusClientClosedRequest = 499

const (
	ErrClientCanceled   = "The request was cancelled by the client."
	ErrDeadlineExceeded = "Request timed out, decrease the duration of the request or add more label matchers (prefer exact match over regex match) to reduce the amount of data processed."
)

// WriteError write a go error with the correct status code.
func WriteError(err error, w http.ResponseWriter) {
}
