package pipelines

import "event-data-pipeline/pkg/payloads"

type workerParams struct {
	stage int

	// Channels for the worker's input, output and errors.
	inCh  <-chan payloads.Payload
	outCh chan<- payloads.Payload
	errCh chan<- error
}

func (p *workerParams) StageIndex() int                 { return p.stage }
func (p *workerParams) Input() <-chan payloads.Payload  { return p.inCh }
func (p *workerParams) Output() chan<- payloads.Payload { return p.outCh }
func (p *workerParams) Error() chan<- error             { return p.errCh }
