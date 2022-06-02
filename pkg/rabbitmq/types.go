package rabbitmq

type jsonObj map[string]interface{}
type RabbitMQConsumerConfig struct {
	Host         string `json:"host,omitempty"`
	ExchangeName string `json:"exchange_name,omitempty"`
	ExchangeType string `json:"exchange_type,omitempty"`
	QueueName    string `json:"queue_name,omitempty"`
	RoutingKey   string `json:"routing_key,omitempty"`
}
