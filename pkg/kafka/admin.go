package kafka

import (
	"encoding/json"
	"errors"
	"event-data-pipeline/pkg/logger"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Admin interface {
	GetPartitions() (*PartitionsResponse, error)
}

// Admin Class that implements Admin
type AdminClient struct {
	topic      string
	consumer   *kafka.Consumer
	partitions []kafka.PartitionMetadata
}

func NewAdminClient(topic string, consumer *kafka.Consumer) (*AdminClient, error) {
	if topic == "" {
		return nil, errors.New("topic is empty")
	}
	if consumer == nil {
		return nil, errors.New("consumer is nil")
	}
	return &AdminClient{
		topic:    topic,
		consumer: consumer,
	}, nil
}

func (ac *AdminClient) GetPartitions() (*PartitionsResponse, error) {
	// create admin client from a consumer
	adminClient, err := kafka.NewAdminClientFromConsumer(ac.consumer)
	if err != nil {
		return nil, err
	}
	// close it on return
	defer adminClient.Close()
	md, err := adminClient.GetMetadata(&ac.topic, false, 5000)
	if err != nil {
		return nil, err
	}
	raw, _ := json.Marshal(md)
	logger.Debugf("metadata: %v", string(raw))

	//set partitions data to the admin client
	ac.partitions = md.Topics[ac.topic].Partitions

	partitionsResponse := PartitionsResponse{
		Topic: ac.topic,
	}
	for _, partition := range ac.partitions {
		partitionsResponse.Partitions = append(partitionsResponse.Partitions, float64(partition.ID))
	}
	return &partitionsResponse, err
}
