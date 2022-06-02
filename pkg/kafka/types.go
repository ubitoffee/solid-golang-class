package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

type jsonObj = map[string]interface{}

type PartitionsResponse struct {
	Topic      string    `json:"topic"`
	Partitions []float64 `json:"partitions"`
}

func NewTopicPartition(topic string, partition int) *kafka.TopicPartition {
	return &kafka.TopicPartition{
		Topic:     &topic,
		Partition: int32(partition),
	}
}
