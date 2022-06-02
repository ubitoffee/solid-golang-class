package processors

import (
	"context"
	"event-data-pipeline/pkg/logger"
	"event-data-pipeline/pkg/payloads"
)

func init() {
	Register("noop", NewNoopProcessor)
}

type NoopProcessor struct {
	Validator
}

func NewNoopProcessor(config jsonObj) Processor {
	validator := NewValidator()
	return &NoopProcessor{*validator}
}

// Processor return Payload as it is
func (n *NoopProcessor) Process(ctx context.Context, p payloads.Payload) (payloads.Payload, error) {
	logger.Debugf("Processing %v", p)
	n.Validate(ctx, p)
	return p, nil
}

func (n *NoopProcessor) Validate(ctx context.Context, p payloads.Payload) {
	// 기본 Validator 사용
	n.Validator.Validate(ctx, p)
	// NoopProcessor 에서 관심있는 Validate 로직 구현.
}
