package sources

import (
	"context"
	"event-data-pipeline/pkg/payloads"
)

type Source interface {
	// Next fetches the next payload from the source. If no more items are
	// available or an error occurs, calls to Next return false.
	Next(context.Context) bool

	// Payload returns the next payload to be processed.
	Payload() payloads.Payload

	// Error return the last error observed by the source.
	Error() error
}
