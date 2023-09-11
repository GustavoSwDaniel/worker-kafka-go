package main

import (
	"crypto/tls"
	"log"

	"e-nowshop-worker/tasks"

	"github.com/Shopify/sarama"
)

func main() {
	mode := "producer"

	if mode == "producer" {
		log.Println("Modo: Produtor")
		config := createSaramaConfig()
		producer := tasks.NewProducer(config)

		producer.Run()
	} else if mode == "consumer" {
		log.Println("Modo: Consumidor")
		consumer := tasks.NewConsumer()
		consumer.Run()
	} else {
		log.Println("Modo não definido. Encerrando o programa.")
	}
}

func createSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()

	// Configurar propriedades do Sarama com as configurações desejadas
	config.Version = sarama.V2_6_0_0
	config.Net.SASL.Enable = true
	config.Net.SASL.User = "UZWYI6MZL2ATPD2J"
	config.Net.SASL.Password = "25xBRbPd/7UrfmstzAmKxptJAdugn232ALkCPeDbfksFw3ngs/qajW8f7bfeFtr3"
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{
		InsecureSkipVerify: true,
	}

	return config
}
