package consumers

import (
	"context"
	"encoding/json"
	"event-data-pipeline/pkg/logger"
	"event-data-pipeline/pkg/rabbitmq"
	"event-data-pipeline/pkg/sources"
)

// compile type assertion check
var _ Consumer = new(RabbitMQConsumerClient)
var _ ConsumerFactory = NewRabbitMQConsumerClient

// ConsumerFactory 에 rabbitmq 컨슈머를 등록
func init() {
	Register("rabbitmq", NewRabbitMQConsumerClient)
}

type RabbitMQClientConfig struct {
	ConsumerOptions jsonObj `json:"consumer_options,omitempty"`
}

type RabbitMQConsumerClient struct {
	rabbitmq.Consumer
	sources.Source
}

func NewRabbitMQConsumerClient(config jsonObj) Consumer {
	logger.Debugf("%s", config)

	//TODO: 1주차 과제입니다.
	consumerCfgObj, ok := config["consumerCfg"].(jsonObj)
	if !ok {
		logger.Panicf("no consumer configuration provided")
	}

	var rcCfg RabbitMQClientConfig
	cfgData, err := json.Marshal(consumerCfgObj)

	if err != nil {
		logger.Errorf(err.Error())
	}
	json.Unmarshal(cfgData, &rcCfg)

	rmqCnsmrCfg := make(jsonObj)
	rmqCnsmrCfg["consumerOptions"] = rcCfg.ConsumerOptions
	rmqCnsmrCfg["pipeParams"] = config["pipeParams"]

	rmqCnsmr := rabbitmq.NewRabbitMQConsumer(rmqCnsmrCfg)

	client := &RabbitMQConsumerClient{
		Consumer: rmqCnsmr,
		Source:   sources.NewRabbitMQSource(rmqCnsmr),
	}

	return client
}

// Init implements Consumer
func (rc *RabbitMQConsumerClient) Init() error {
	//TODO: 1주차 과제입니다.
	var err error

	err = rc.CreateConsumer()
	if err != nil {
		return err
	}

	err = rc.InitDeliveryChannel()
	if err != nil {
		return err
	}

	return nil
}

// Consume implements Consumer
func (rc *RabbitMQConsumerClient) Consume(ctx context.Context) error {
	err := rc.Read(ctx)
	if err != nil {
		return err
	}
	return nil
}
