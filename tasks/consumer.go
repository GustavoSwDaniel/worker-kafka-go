package tasks

import (
	"log"

	"github.com/Shopify/sarama"
)

type Consumer struct {
	config            *sarama.Config
	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	topic             string
}

func NewConsumer() *Consumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	brokers := []string{"localhost:9092"}
	topic := "meu-topico"

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatal("Erro ao criar o consumidor do Kafka:", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal("Erro ao criar a partição do consumidor:", err)
	}

	return &Consumer{
		config:            config,
		consumer:          consumer,
		partitionConsumer: partitionConsumer,
		topic:             topic,
	}
}

func (c *Consumer) Run() {
	for {
		select {
		case msg := <-c.partitionConsumer.Messages():
			log.Println("Mensagem consumida do Kafka:", string(msg.Value))

		case err := <-c.partitionConsumer.Errors():
			log.Println("Erro no consumidor do Kafka:", err.Err)
		}
	}
}
