package pipelines

import (
	"context"
	"event-data-pipeline/pkg/payloads"
)

// StageRunner is implemented by types that can be strung together to form a
// multi-stage pipeline.
type StageRunner interface {
	// Run implements the processing logic for this stage by reading
	// incoming Payloads from an input channel, processing them and
	// outputting the results to an output channel.
	//
	// Calls to Run are expected to block until:
	// - the stage input channel is closed OR
	// - the provided context expires OR
	// - an error occurs while processing payloads.
	Run(context.Context, StageParams)
}

// StageParams encapsulates the information required for executing a pipeline
// stage. The pipeline passes a StageParams instance to the Run() method of
// each stage.
type StageParams interface {
	// StageIndex returns the position of this stage in the pipeline.
	StageIndex() int

	// Input returns a channel for reading the input payloads for a stage.
	Input() <-chan payloads.Payload

	// Output returns a channel for writing the output payloads for a stage.
	Output() chan<- payloads.Payload

	// Error returns a channel for writing errors that were encountered by
	// a stage while processing payloads.
	Error() chan<- error
}
