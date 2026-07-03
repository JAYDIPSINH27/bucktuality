package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

type MatchCreatedEvent struct {
	EventType    string    `json:"EventType"`
	RoomId       string    `json:"RoomId"`
	User1Id      string    `json:"User1Id"`
	User2Id      string    `json:"User2Id"`
	CreatedAtUtc time.Time `json:"CreatedAtUtc"`
}

type MessageSentEvent struct {
	EventType    string    `json:"EventType"`
	RoomId       string    `json:"RoomId"`
	SenderUserId string    `json:"SenderUserId"`
	SentAtUtc    time.Time `json:"SentAtUtc"`
}

type AnalyticsSummary struct {
	TotalMatches  int `json:"totalMatches"`
	TotalMessages int `json:"totalMessages"`
}

var (
	summary AnalyticsSummary
	mu      sync.Mutex
)

func main() {
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:9092")

	ctx := context.Background()

	go consumeMatchCreated(ctx, kafkaBroker)
	go consumeMessageSent(ctx, kafkaBroker)

	router := gin.Default()

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"service": "analytics-service",
			"status":  "healthy",
		})
	})

	router.GET("/analytics/summary", func(c *gin.Context) {
		mu.Lock()
		defer mu.Unlock()

		c.JSON(http.StatusOK, summary)
	})

	router.Run(":8083")
}

func consumeMatchCreated(ctx context.Context, broker string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "match-created",
		GroupID:     "analytics-match-created-group",
		StartOffset: kafka.FirstOffset,
	})

	log.Println("Listening to topic: match-created")

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println("Error reading match-created:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		var event MatchCreatedEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Println("Invalid match-created event:", err)
			continue
		}

		mu.Lock()
		summary.TotalMatches++
		mu.Unlock()

		log.Printf("MatchCreated consumed. RoomId=%s User1=%s User2=%s\n",
			event.RoomId,
			event.User1Id,
			event.User2Id,
		)
	}
}

func consumeMessageSent(ctx context.Context, broker string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "message-sent",
		GroupID:     "analytics-message-sent-group",
		StartOffset: kafka.FirstOffset,
	})

	log.Println("Listening to topic: message-sent")

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println("Error reading message-sent:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		var event MessageSentEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Println("Invalid message-sent event:", err)
			continue
		}

		mu.Lock()
		summary.TotalMessages++
		mu.Unlock()

		log.Printf("MessageSent consumed. RoomId=%s Sender=%s\n",
			event.RoomId,
			event.SenderUserId,
		)
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	return value
}