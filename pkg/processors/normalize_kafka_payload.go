package processors

import (
	"context"
	"encoding/json"
	"event-data-pipeline/pkg/payloads"
	"fmt"
	"strconv"
)

var _ Processor = new(ProcessorFunc)
var _ ProcessorFunc = NormalizeKafkaPayload

func init() {
	Register("normalize_kafka_payload", NewNormalizeKafkaPayloadProcessor)
}

func NewNormalizeKafkaPayloadProcessor(config jsonObj) Processor {

	return ProcessorFunc(NormalizeKafkaPayload)
}

func NormalizeKafkaPayload(ctx context.Context, p payloads.Payload) (payloads.Payload, error) {
	kfkPayload := p.(*payloads.KafkaPayload)

	// 인덱스 생성
	index := fmt.Sprintf("%s-%s", "event-data", kfkPayload.Timestamp.Format("01-02-2006"))
	kfkPayload.Index = index

	// 식별자 생성
	docID := fmt.Sprintf("%s.%s.%v.%s", kfkPayload.Key, kfkPayload.Topic, strconv.FormatFloat(kfkPayload.Partition, 'f', 0, 64), strconv.FormatFloat(kfkPayload.Offset, 'f', 0, 64))
	kfkPayload.DocID = docID

	// 데이터 생성
	data, err := json.Marshal(kfkPayload.Value)
	if err != nil {
		return nil, err
	}
	kfkPayload.Data = data
	return kfkPayload, nil
}
