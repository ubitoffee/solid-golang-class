package pipelines

import (
	"context"
	"event-data-pipeline/pkg/payloads"
)

// Sink is implemented by types that can operate as the tail of a pipeline.
type Sink interface {
	// Drain processes Drain Payload instance that has been emitted out of
	// Drain Pipeline instance.
	Drain(context.Context, payloads.Payload) error
}
