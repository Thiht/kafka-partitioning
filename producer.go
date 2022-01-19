package main

import (
	"github.com/Shopify/sarama"
)

func NewProducer(kafkaUsername, kafkaPassword string, brokerAddrs []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()

	// TLS
	config.Net.TLS.Enable = true
	config.Net.SASL.Enable = true
	config.Net.SASL.User = kafkaUsername
	config.Net.SASL.Password = kafkaPassword

	config.Version = sarama.V2_7_0_0
	config.ClientID = kafkaUsername

	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewHashPartitioner

	if err := config.Validate(); err != nil {
		return nil, err
	}
	return sarama.NewSyncProducer(brokerAddrs, config)
}
