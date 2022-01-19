package main

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
)

func NewConsumer(kafkaUsername, kafkaPassword string, brokerAddrs []string, groupID string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()

	// TLS
	config.Net.TLS.Enable = true
	config.Net.SASL.Enable = true
	config.Net.SASL.User = kafkaUsername
	config.Net.SASL.Password = kafkaPassword

	config.Version = sarama.V2_7_0_0
	config.ClientID = kafkaUsername

	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Group.Session.Timeout = 30 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.CommitInterval = time.Second

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return sarama.NewConsumerGroup(brokerAddrs, groupID, config)
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		log.Printf("Message claimed from partition %d", message.Partition)
		session.MarkMessage(message, "")
	}
	return nil
}
