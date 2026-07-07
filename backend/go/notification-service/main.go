package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

type Notification struct {
	Id           int       `json:"id"`
	Type         string    `json:"type"`
	Message      string    `json:"message"`
	SourceTopic  string    `json:"sourceTopic"`
	CreatedAtUtc time.Time `json:"createdAtUtc"`
}

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

var db *sql.DB

func main() {
	kafkaBroker := getEnv("KAFKA_BROKER", "localhost:9092")

	connectionString := getEnv(
		"SQL_CONNECTION_STRING",
		"sqlserver://sa:Bucktuality%4012345@localhost:1433?database=BucktualityNotificationsDb&encrypt=disable",
	)

	db = connectSqlWithRetry(connectionString)

	ctx := context.Background()

	go consumeMatchCreated(ctx, kafkaBroker)
	go consumeMessageSent(ctx, kafkaBroker)

	router := gin.Default()
	router.Use(corsMiddleware())

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"service": "notification-service",
			"status":  "healthy",
		})
	})

	router.GET("/notifications", func(c *gin.Context) {
		notifications, err := getNotifications()

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, notifications)
	})

	router.Run(":8085")
}

func consumeMatchCreated(ctx context.Context, broker string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "match-created",
		GroupID:     "notification-match-created-sql-group-v1",
		StartOffset: kafka.FirstOffset,
	})

	log.Println("Notification service listening to match-created")

	for {
		message, err := reader.ReadMessage(ctx)

		if err != nil {
			log.Println("Error reading match-created:", err)
			time.Sleep(3 * time.Second)
			continue
		}

		log.Println("RAW match-created event:", string(message.Value))

		var event MatchCreatedEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Println("Invalid match-created event:", err)
			continue
		}

		err = saveNotification(
			"MatchCreated",
			"New match created in room "+event.RoomId,
			"match-created",
		)

		if err != nil {
			log.Println("Failed to save MatchCreated notification:", err)
			continue
		}

		log.Println("Saved MatchCreated notification:", event.RoomId)
	}
}

func consumeMessageSent(ctx context.Context, broker string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       "message-sent",
		GroupID:     "notification-message-sent-sql-group-v1",
		StartOffset: kafka.FirstOffset,
	})

	log.Println("Notification service listening to message-sent")

	for {
		message, err := reader.ReadMessage(ctx)

		if err != nil {
			log.Println("Error reading message-sent:", err)
			time.Sleep(3 * time.Second)
			continue
		}

		log.Println("RAW message-sent event:", string(message.Value))

		var event MessageSentEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Println("Invalid message-sent event:", err)
			continue
		}

		err = saveNotification(
			"MessageSent",
			"Message sent in room "+event.RoomId,
			"message-sent",
		)

		if err != nil {
			log.Println("Failed to save MessageSent notification:", err)
			continue
		}

		log.Println("Saved MessageSent notification:", event.RoomId)
	}
}

func saveNotification(notificationType string, message string, sourceTopic string) error {
	_, err := db.Exec(`
		INSERT INTO Notifications
		(Type, Message, SourceTopic, CreatedAtUtc)
		VALUES (@p1, @p2, @p3, SYSUTCDATETIME())
	`,
		notificationType,
		message,
		sourceTopic,
	)

	return err
}

func getNotifications() ([]Notification, error) {
	rows, err := db.Query(`
		SELECT TOP 100
			Id,
			Type,
			Message,
			ISNULL(SourceTopic, '') AS SourceTopic,
			CreatedAtUtc
		FROM Notifications
		ORDER BY CreatedAtUtc DESC
	`)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	notifications := make([]Notification, 0)

	for rows.Next() {
		var item Notification

		err := rows.Scan(
			&item.Id,
			&item.Type,
			&item.Message,
			&item.SourceTopic,
			&item.CreatedAtUtc,
		)

		if err != nil {
			return nil, err
		}

		notifications = append(notifications, item)
	}

	return notifications, nil
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