package sources

import (
	"context"
	"encoding/json"
	"event-data-pipeline/pkg/kafka"
	"event-data-pipeline/pkg/logger"
	"event-data-pipeline/pkg/payloads"
)

type KafkaSource struct {
	kafka.Consumer
}

func NewKafkaSource(kc kafka.Consumer) *KafkaSource {
	return &KafkaSource{kc}
}

func (kc *KafkaSource) Next(ctx context.Context) bool {

	//스트림으로부터 읽어오기
	for {
		select {
		// 스트림이 있을 때
		case p := <-kc.Stream():
			data, err := json.Marshal(p)
			if err != nil {
				logger.Errorf("failed to marshall the stream data")
				return false
			}
			var kfkPayload payloads.KafkaPayload
			err = json.Unmarshal(data, &kfkPayload)
			if err != nil {
				logger.Errorf(err.Error())
			}
			kc.PutPaylod(&kfkPayload)
			return true
		// Shutdown
		case <-ctx.Done():
			logger.Debugf("Context cancelled")
		// 스트림이 없을 때 Sleep 후 다시 읽기 시도.
		default:
			// logger.Debugf("No Next Stream. Sleeping for 2 seconds")
			// time.Sleep(2 * time.Second)
		}
	}

}

// Source 인터페이스 구현
func (kc *KafkaSource) Payload() payloads.Payload {
	return kc.GetPaylod()
}

// Source 인터페이스 구현
func (kc *KafkaSource) Error() error {
	return nil
}
