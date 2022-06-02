package kafka

import (
	"context"
	"encoding/json"
	"event-data-pipeline/pkg/logger"
	"event-data-pipeline/pkg/payloads"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var _ Consumer = new(KafkaConsumer)

type Consumer interface {
	CreateConsumer() error
	CreateAdminConsumer() error
	GetPartitions() error
	Read(ctx context.Context) error
	AssignPartition(partition int) error
	Poll(ctx context.Context)
	Stream() chan interface{}
	PutPaylod(p payloads.Payload) error
	GetPaylod() payloads.Payload
}

type KafkaConsumer struct {
	// topic to consume from
	topic string

	//configuration to create confluent-kafka-go consumer
	configMap *kafka.ConfigMap

	//confluent kafka go consumer
	kafkaConsumer *kafka.Consumer

	//adminClient to get partitions
	adminClient *AdminClient

	//partitions response
	partitions *PartitionsResponse

	ctx context.Context

	stream chan interface{}

	errCh chan error

	payload payloads.Payload
}

func NewKafkaConsumer(config jsonObj) *KafkaConsumer {
	topic, ok := config["topic"].(string)
	if !ok {
		logger.Panicf("no topic provided")
	}
	pipeParams, ok := config["pipeParams"].(jsonObj)
	if !ok {
		logger.Panicf("no pipeParams provided")
	}

	//extract context from config
	ctx, ok := pipeParams["context"].(context.Context)
	if !ok {
		logger.Panicf("no topic provided")
	}

	//extract stream chan from config
	stream, ok := pipeParams["stream"].(chan interface{})
	if !ok {
		logger.Panicf("no stream provided")
	}

	//extract error chan from config
	errch, ok := pipeParams["errch"].(chan error)
	if !ok {
		logger.Panicf("no errch provided")
	}

	//consumerOptions
	kfkCnsmrCfg, ok := config["consumerOptions"].(map[string]interface{})
	if !ok {
		logger.Panicf("no errch provided")
	}

	// load Consumer Options to kafka.ConfigMap
	cfgMapData, _ := json.Marshal(kfkCnsmrCfg)
	var kcm kafka.ConfigMap
	json.Unmarshal(cfgMapData, &kcm)

	// create a new KafkaConsumer with configMap fed in
	kafkaConsumer := &KafkaConsumer{
		topic:     topic,
		configMap: &kcm,
		ctx:       ctx,
		stream:    stream,
		errCh:     errch,
	}

	return kafkaConsumer

}

//create KafkaClient instance
func (kc *KafkaConsumer) CreateConsumer() error {

	if kc == nil {
		kc = &KafkaConsumer{configMap: kc.configMap}
	}

	var err error
	// create kafka consumer instance
	kc.kafkaConsumer, err = kafka.NewConsumer(kc.configMap)

	if err != nil {
		return err
	}
	logger.Debugf("Check in consumer creation: %s", kc.kafkaConsumer)
	return nil

}

func (kc *KafkaConsumer) CreateAdminConsumer() error {
	var err error
	kc.adminClient, err = NewAdminClient(kc.topic, kc.kafkaConsumer)
	if err != nil {
		return err
	}
	return nil
}

func (kc *KafkaConsumer) GetPartitions() error {
	var err error
	// get partitions from admin client
	kc.partitions, err = kc.adminClient.GetPartitions()
	if err != nil {
		return err
	}
	return nil
}

func (kc *KafkaConsumer) Read(ctx context.Context) error {
	// 파티션 별로 카프카 컨슈머 생성
	for _, p := range kc.partitions.Partitions {
		// KafkaConsumer 인스턴스 복사
		ckc := kc.Copy()
		// KafkaConsumer 내부 실제 컨슈머 생성
		ckc.CreateConsumer()
		// 데이터를 읽어오기 위한 파티션에 할당
		ckc.AssignPartition(int(p))
		// 실제 데이터를 읽어오는 고루틴 생성
		go ckc.Poll(ctx)
	}
	return nil
}

//Copy KafkaConsumer instance
func (kc *KafkaConsumer) Copy() *KafkaConsumer {
	return &KafkaConsumer{
		topic:     kc.topic,
		configMap: kc.configMap,
		stream:    kc.stream,
		errCh:     kc.errCh,
	}
}

func (kc *KafkaConsumer) AssignPartition(partition int) error {

	var partitions []kafka.TopicPartition

	tp := NewTopicPartition(kc.topic, partition)
	partitions = append(partitions, *tp)

	err := kc.kafkaConsumer.Assign(partitions)
	if err != nil {
		return err
	}
	return err
}

func (kc *KafkaConsumer) Poll(ctx context.Context) {
	cast := func(msg *kafka.Message) map[string]interface{} {
		var record = make(map[string]interface{})

		// dereference to put plain string
		record["topic"] = *msg.TopicPartition.Topic
		record["partition"] = float64(msg.TopicPartition.Partition)
		record["offset"] = float64(msg.TopicPartition.Offset)

		record["key"] = string(msg.Key)

		var valObj map[string]interface{}

		err := json.Unmarshal(msg.Value, &valObj)
		if err != nil {
			logger.Errorf("error in casting value object: %v", err)
		}
		record["value"] = valObj
		record["timestamp"] = msg.Timestamp
		return record
	}
	for {
		select {
		case <-ctx.Done():
			logger.Debugf("shutting down consumer read")
			return
		default:
			ev := kc.kafkaConsumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				record := cast(e)
				kc.stream <- record
				data, _ := json.MarshalIndent(record, "", " ")
				logger.Debugf("%s", string(data))
			case kafka.Error:
				logger.Errorf("Error: %v: %v", e.Code(), e)
			case kafka.PartitionEOF:
				logger.Debugf("[PartitionEOF][Consumer: %s][Topic: %v][Partition: %v][Offset: %d][Message: %v]", kc.kafkaConsumer.String(), *e.Topic, e.Partition, e.Offset, fmt.Sprintf("\"%s\"", e.Error.Error()))
			}
		}
	}
}

// GetPaylod implements Consumer
func (kc *KafkaConsumer) GetPaylod() payloads.Payload {
	return kc.payload
}

// PutPaylod implements Consumer
func (kc *KafkaConsumer) PutPaylod(p payloads.Payload) error {
	kc.payload = p
	return nil
}

// Stream implements Consumer
func (kc *KafkaConsumer) Stream() chan interface{} {
	return kc.stream
}
