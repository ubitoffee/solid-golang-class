package processors

import (
	"context"
	"event-data-pipeline/pkg/payloads"
)

type TimeStampProcessor struct {
	Validator
}

func NewTimeStampProcessor() Processor {
	validator := NewValidator()
	return &TimeStampProcessor{
		*validator,
	}
}
func (tsp *TimeStampProcessor) Process(ctx context.Context, p payloads.Payload) (payloads.Payload, error) {
	// 확장된 Validate 메소드를 사용
	tsp.Validate(ctx, p)
	return p, nil
}

func (tsp *TimeStampProcessor) Validate(ctx context.Context, p payloads.Payload) error {
	// 일반적인 유효성 체크가 아닌
	// TimeStampProcessor 에서만 관심이 있는 데이터 유효성 체크를 위해서 확장
	return nil
}
