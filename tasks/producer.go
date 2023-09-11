package tasks

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/Shopify/sarama"
)

type Producer struct {
	config   *sarama.Config
	producer sarama.AsyncProducer
	topic    string
	interval int
}

type Response struct {
	Data   []interface{} `json:"data"`
	Total  int           `json:"total"`
	Offset int           `json:"offset"`
	Count  int           `json:"count"`
}

func NewProducer(config *sarama.Config) *Producer {
	brokers := []string{"pkc-n3603.us-central1.gcp.confluent.cloud:9092"}
	topic := "purchases"
	interval := 10

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatal("Erro ao criar o produtor do Kafka:", err)
	}

	return &Producer{
		config:   config,
		producer: producer,
		topic:    topic,
		interval: interval,
	}
}

func getOrders() Response {
	var response Response
	resp, err := http.Get("http://0.0.0.0:8081/orders?status=pending&expirate=True")
	if err != nil {
		log.Panic("erro")
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)

	if err != nil {
		log.Panic("erro de novo")
	}

	err = json.Unmarshal([]byte(body), &response)

	if err != nil {
		println("Erro ao deserializar o JSON:", err)
	}
	return response
}

func (p *Producer) Run() {

	ticker := time.NewTicker(time.Duration(p.interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			orders := getOrders()
			for _, value := range orders.Data {
				fmt.Printf("Valor: %s\n", value)

				strValue, ok := value.(string)
				if !ok || strValue == "" {
					log.Printf("Erro ao converter valor para string")
					continue
				}

				message := &sarama.ProducerMessage{
					Topic: p.topic,
					Value: sarama.StringEncoder(strValue),
				}
				p.producer.Input() <- message
				log.Printf("mensagem enviada com sucesso")
			}

		case err := <-p.producer.Errors():
			log.Println("Erro no produtor do Kafka:", err.Err)
		}
	}
}
