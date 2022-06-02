package processors

import (
	"context"
	"event-data-pipeline/pkg/payloads"
)

type Validator struct {
}

func NewValidator() *Validator {

	return &Validator{}
}

func (Validator) Validate(ctx context.Context, p payloads.Payload) error {
	// 데이터 유효성 체크
	// 유효성 통과하지 못하는 경우 에러 반환
	return nil
}
