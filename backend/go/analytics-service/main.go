package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
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
	TotalMatches  int       `json:"totalMatches"`
	TotalMessages int       `json:"totalMessages"`
	UpdatedAtUtc  time.Time `json:"updatedAtUtc"`
}

var (
	db *sql.DB
	mu sync.Mutex
)

func main() {
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:9092")

	connectionString := getEnv(
		"SQL_CONNECTION_STRING",
		"sqlserver://sa:Bucktuality@12345@localhost:1433?database=BucktualityAnalyticsDb&encrypt=disable",
	)

	db = connectSqlWithRetry(connectionString)

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
		summary, err := getSummary()

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, summary)
	})

	router.Run(":8083")
}

func createKafkaReader(topic string, groupID string, broker string) *kafka.Reader {

	for {

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{broker},
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		_, err := reader.ReadLag(ctx)

		cancel()

		if err == nil {

			log.Printf("Connected to Kafka. Topic=%s\n", topic)

			return reader
		}

		log.Printf("Kafka not ready for topic %s. Retrying in 5 seconds...\n", topic)

		reader.Close()

		time.Sleep(5 * time.Second)
	}
}

func consumeMatchCreated(ctx context.Context, broker string) {
	
	reader := createKafkaReader(
	"match-created",
	"analytics-match-created-group",
	broker,
)

	log.Println("Listening to topic: match-created")

	for {
		message, err := reader.ReadMessage(ctx)

		if err != nil {

			log.Println("Kafka connection lost:", err)

			reader.Close()

			reader = createKafkaReader(
				"match-created",
				"analytics-match-created-group",
				broker,
			)

			continue
		}

		var event MatchCreatedEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Println("Invalid match-created event:", err)
			continue
		}

		err = incrementMatches()

		if err != nil {
			log.Println("Failed to update match analytics:", err)
			continue
		}

		log.Printf("MatchCreated consumed and saved. RoomId=%s\n", event.RoomId)
	}
}

func consumeMessageSent(ctx context.Context, broker string) {
	reader := createKafkaReader(
	"message-sent",
	"analytics-message-sent-group",
	broker,
)
	log.Println("Listening to topic: message-sent")

	for {
		message, err := reader.ReadMessage(ctx)

		if err != nil {

			log.Println("Kafka connection lost:", err)

			reader.Close()

			reader = createKafkaReader(
				"message-sent",
				"analytics-message-sent-group",
				broker,
			)

			continue
		}
		var event MessageSentEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Println("Invalid message-sent event:", err)
			continue
		}

		err = incrementMessages()

		if err != nil {
			log.Println("Failed to update message analytics:", err)
			continue
		}

		log.Printf("MessageSent consumed and saved. RoomId=%s Sender=%s\n",
			event.RoomId,
			event.SenderUserId,
		)
	}
}

func incrementMatches() error {
	mu.Lock()
	defer mu.Unlock()

	_, err := db.Exec(`
		UPDATE AnalyticsSummary
		SET TotalMatches = TotalMatches + 1,
		    UpdatedAtUtc = SYSUTCDATETIME()
		WHERE Id = 1
	`)

	return err
}

func incrementMessages() error {
	mu.Lock()
	defer mu.Unlock()

	_, err := db.Exec(`
		UPDATE AnalyticsSummary
		SET TotalMessages = TotalMessages + 1,
		    UpdatedAtUtc = SYSUTCDATETIME()
		WHERE Id = 1
	`)

	return err
}

func getSummary() (AnalyticsSummary, error) {
	var summary AnalyticsSummary

	err := db.QueryRow(`
		SELECT TotalMatches, TotalMessages, UpdatedAtUtc
		FROM AnalyticsSummary
		WHERE Id = 1
	`).Scan(
		&summary.TotalMatches,
		&summary.TotalMessages,
		&summary.UpdatedAtUtc,
	)

	return summary, err
}

func connectSqlWithRetry(connectionString string) *sql.DB {
	for {
		database, err := sql.Open("sqlserver", connectionString)

		if err == nil {
			err = database.Ping()

			if err == nil {
				log.Println("Connected to SQL Server.")
				return database
			}
		}

		log.Println("SQL Server not ready, retrying in 5 seconds...")
		time.Sleep(5 * time.Second)
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	return value
}