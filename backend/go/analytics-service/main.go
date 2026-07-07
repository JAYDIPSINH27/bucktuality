package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"bucktuality/analytics-service/internal/kafkahelper"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

type MatchCreatedEvent struct {
	EventType    string    `json:"EventType"`
	RoomId      string    `json:"RoomId"`
	User1Id     string    `json:"User1Id"`
	User2Id     string    `json:"User2Id"`
	CreatedAtUtc time.Time `json:"CreatedAtUtc"`
}

type MessageSentEvent struct {
	EventType    string    `json:"EventType"`
	RoomId      string    `json:"RoomId"`
	SenderUserId string   `json:"SenderUserId"`
	SentAtUtc   time.Time `json:"SentAtUtc"`
}

type AnalyticsSummary struct {
	TotalMatches  int       `json:"totalMatches"`
	TotalMessages int       `json:"totalMessages"`
	UpdatedAtUtc  time.Time `json:"updatedAtUtc"`
}

var db *sql.DB

func main() {
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:9092")

	connectionString := getEnv(
		"SQL_CONNECTION_STRING",
		"sqlserver://sa:Bucktuality%4012345@localhost:1433?database=BucktualityAnalyticsDb&encrypt=disable",
	)

	db = connectSqlWithRetry(connectionString)

	ctx := context.Background()

	go consumeMatchCreated(ctx, kafkaBroker)
	go consumeMessageSent(ctx, kafkaBroker)

	router := gin.Default()
	router.Use(corsMiddleware())

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

func consumeMatchCreated(ctx context.Context, broker string) {
	reader := kafkahelper.NewReader(
		"match-created",
		"analytics-match-created-group",
		broker,
	)

	kafkahelper.ReadLoop(ctx, reader, "match-created", func(message kafka.Message) error {
		log.Println("RAW match-created:", string(message.Value))

		var event MatchCreatedEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			return err
		}

		return incrementMatches()
	})
}

func consumeMessageSent(ctx context.Context, broker string) {
	reader := kafkahelper.NewReader(
		"message-sent",
		"analytics-message-sent-group",
		broker,
	)

	kafkahelper.ReadLoop(ctx, reader, "message-sent", func(message kafka.Message) error {
		log.Println("RAW message-sent:", string(message.Value))

		var event MessageSentEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			return err
		}

		return incrementMessages()
	})
}

func incrementMatches() error {
	_, err := db.Exec(`
		UPDATE AnalyticsSummary
		SET TotalMatches = TotalMatches + 1,
		    UpdatedAtUtc = SYSUTCDATETIME()
		WHERE Id = 1
	`)

	return err
}

func incrementMessages() error {
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

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	return value
}