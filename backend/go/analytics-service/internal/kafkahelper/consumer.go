package kafkahelper

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func NewReader(topic string, groupID string, broker string) *kafka.Reader {
	log.Printf("Starting Kafka reader. Topic=%s Group=%s Broker=%s\n", topic, groupID, broker)

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
	})
}

func ReadLoop(
	ctx context.Context,
	reader *kafka.Reader,
	topic string,
	handler func(message kafka.Message) error,
) {
	for {
		message, err := reader.ReadMessage(ctx)

		if err != nil {
			log.Printf("Kafka read error. Topic=%s Error=%v\n", topic, err)
			time.Sleep(3 * time.Second)
			continue
		}

		if err := handler(message); err != nil {
			log.Printf("Kafka handler error. Topic=%s Error=%v\n", topic, err)
			continue
		}
	}
}