package consumers

import (
	"context"
	"encoding/json"
	"event-data-pipeline/pkg/logger"
	"event-data-pipeline/pkg/sources"

	"event-data-pipeline/pkg/kafka"
)

// compile type assertion check
var _ Consumer = new(KafkaConsumerClient)
var _ ConsumerFactory = NewKafkaConsumerClient

// ConsumerFactory 에 kafka 컨슈머를 등록
func init() {
	Register("kafka", NewKafkaConsumerClient)
}

type KafkaClientConfig struct {
	ClientName      string  `json:"client_name,omitempty"`
	Topic           string  `json:"topic,omitempty"`
	ConsumerOptions jsonObj `json:"consumer_options,omitempty"`
}

//Consumer interface 구현체
type KafkaConsumerClient struct {
	kafka.Consumer
	sources.Source
}

func NewKafkaConsumerClient(config jsonObj) Consumer {

	logger.Debugf("%s", config)

	// KafkaClientConfig로 값을 담기 위한 오브젝트
	consumerCfgObj, ok := config["consumerCfg"].(jsonObj)
	if !ok {
		logger.Panicf("no consumer configuration provided")
	}

	// Read config into KafkaClientConfig struct
	var kcCfg KafkaClientConfig
	cfgData, err := json.Marshal(consumerCfgObj)
	if err != nil {
		logger.Errorf(err.Error())
	}
	json.Unmarshal(cfgData, &kcCfg)

	kfkCnsmrCfg := make(jsonObj)
	kfkCnsmrCfg["topic"] = kcCfg.Topic
	kfkCnsmrCfg["consumerOptions"] = kcCfg.ConsumerOptions
	kfkCnsmrCfg["pipeParams"] = config["pipeParams"]
	kfkCnsmr := kafka.NewKafkaConsumer(kfkCnsmrCfg)

	// create a new Consumer concrete type - KafkaConsumerClient
	client := &KafkaConsumerClient{
		Consumer: kfkCnsmr,
		Source:   sources.NewKafkaSource(kfkCnsmr),
	}

	return client

}

// Init implements Consumer
func (kc *KafkaConsumerClient) Init() error {
	var err error

	err = kc.CreateConsumer()
	if err != nil {
		return err
	}
	err = kc.CreateAdminConsumer()
	if err != nil {
		return err
	}
	err = kc.GetPartitions()
	if err != nil {
		return err
	}
	return nil
}

// Consumer 인터페이스 구현
func (kc *KafkaConsumerClient) Consume(ctx context.Context) error {
	err := kc.Read(ctx)
	if err != nil {
		return err
	}
	return nil
}
