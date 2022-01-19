package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
)

const (
	kafkaUsername = "experimentation.admin"
	kafkaPassword = ""
	kafkaGroupID  = kafkaUsername + ".kafka-test"
	kafkaTopic    = "experimentation.test-partitions"
)

var kafkaBrokerAddrs = []string{
	"n1.de1.m1stg.queue.ovh.net:9091",
	"n2.de1.m1stg.queue.ovh.net:9091",
	"n3.de1.m1stg.queue.ovh.net:9091",
}

var keys = []string{"foo", "bar", "baz"}

func main() {
	if len(os.Args) != 1 && os.Args[1] != "producer" && os.Args[1] != "consumer" {
		log.Fatal("1 argument expected: producer or consumer")
	}

	// sarama.Logger = log.New(os.Stderr, "[Sarama] ", log.LstdFlags)

	switch os.Args[1] {
	case "producer":
		producerClient, err := NewProducer(kafkaUsername, kafkaPassword, kafkaBrokerAddrs)
		if err != nil {
			log.Fatalf("Failed to init producer: %v\n", err)
		}

		for i := 0; i < 1000; i++ {
			partition, _, err := producerClient.SendMessage(&sarama.ProducerMessage{
				Topic: kafkaTopic,
				Key:   sarama.StringEncoder(keys[i%len(keys)]),
				Value: sarama.StringEncoder(fmt.Sprintf("Hello #%d", i)),
			})
			if err != nil {
				log.Printf("Failed to send message: %v\n", err)
				continue
			}
			log.Printf("Message sent on partition %d\n", partition)
		}
		_ = producerClient.Close()

	case "consumer":
		consumerClient, err := NewConsumer(kafkaUsername, kafkaPassword, kafkaBrokerAddrs, kafkaGroupID)
		if err != nil {
			log.Fatalf("Failed to init consumer: %v\n", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		consumer := Consumer{
			ready: make(chan bool),
		}
		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// `Consume` should be called inside an infinite loop, when a
				// server-side rebalance happens, the consumer session will need to be
				// recreated to get the new claims
				if err := consumerClient.Consume(ctx, []string{kafkaTopic}, &consumer); err != nil {
					log.Panicf("Error from consumer: %v", err)
				}
				// check if context was cancelled, signaling that the consumer should stop
				if ctx.Err() != nil {
					return
				}
				consumer.ready = make(chan bool)
			}
		}()

		<-consumer.ready // Await till the consumer has been set up
		log.Println("Sarama consumer up and running!...")

		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
		case <-sigterm:
			log.Println("terminating: via signal")
		}

		cancel()
		wg.Wait()
		if err = consumerClient.Close(); err != nil {
			log.Panicf("Error closing client: %v", err)
		}
	}
}
