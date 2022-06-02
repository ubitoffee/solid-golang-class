package processors

import (
	"context"
	"event-data-pipeline/pkg/logger"
	"event-data-pipeline/pkg/payloads"
	"fmt"
	"strings"
)

type jsonObj map[string]interface{}

type ProcessorFactory func(config jsonObj) Processor

var processorFactories = make(map[string]ProcessorFactory)

// Each processor implementation must Register itself
func Register(name string, factory ProcessorFactory) {
	logger.Debugf("Registering processor factory for %s", name)
	if factory == nil {
		logger.Panicf("Processor factory %s does not exist.", name)
	}
	_, registered := processorFactories[name]
	if registered {
		logger.Errorf("Processor factory %s already registered. Ignoring.", name)
	}
	processorFactories[name] = factory
}

// CreateProcessor is a factory method that will create the named processor
func CreateProcessor(name string, config jsonObj) (Processor, error) {

	factory, ok := processorFactories[name]
	if !ok {
		// Factory has not been registered.
		// Make a list of all available datastore factories for logging.
		availableProcessors := make([]string, 0)
		for k := range processorFactories {
			availableProcessors = append(availableProcessors, k)
		}
		return nil, fmt.Errorf("invalid Processor name. Must be one of: %s", strings.Join(availableProcessors, ", "))
	}

	// Run the factory with the configuration.
	return factory(config), nil
}

// Processor is implemented by types that can process Payloads as part of a
// pipeline stage.
type Processor interface {
	// Process operates on the input payload and returns back a new payload
	// to be forwarded to the next pipeline stage. Processors may also opt
	// to prevent the payload from reaching the rest of the pipeline by
	// returning a nil payload value instead.
	Process(context.Context, payloads.Payload) (payloads.Payload, error)
}

// ProcessorFunc is an adapter to allow the use of plain functions as Processor
// instances. If f is a function with the appropriate signature, ProcessorFunc(f)
// is a Processor that calls f.
type ProcessorFunc func(context.Context, payloads.Payload) (payloads.Payload, error)

// Process calls f(ctx, p).
func (f ProcessorFunc) Process(ctx context.Context, p payloads.Payload) (payloads.Payload, error) {
	return f(ctx, p)
}
