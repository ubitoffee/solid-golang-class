package sources

import (
	"context"
	"encoding/json"
	"event-data-pipeline/pkg/logger"
	"event-data-pipeline/pkg/payloads"
	"event-data-pipeline/pkg/rabbitmq"
)

type RabbitMQSource struct {
	rabbitmq.Consumer
}

func NewRabbitMQSource(rc rabbitmq.Consumer) *RabbitMQSource {
	return &RabbitMQSource{rc}
}

func (rc *RabbitMQSource) Next(ctx context.Context) bool {

	//스트림으로부터 읽어오기
	for {
		select {
		// 스트림이 있을 때
		case p := <-rc.Stream():
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
			rc.PutPaylod(&kfkPayload)
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
func (rc *RabbitMQSource) Payload() payloads.Payload {
	return rc.GetPaylod()
}

// Source 인터페이스 구현
func (rc *RabbitMQSource) Error() error {
	return nil
}
