package main

import (
	"context"
	"net/http"
	"os"
	"time"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type MatchRequest struct {
	UserId       string `json:"userId"`
	ConnectionId string `json:"connectionId"`
	Vibe         string `json:"vibe"`
}

type MatchResponse struct {
	IsMatched           bool   `json:"isMatched"`
	Status              string `json:"status"`
	RoomId              string `json:"roomId,omitempty"`
	PartnerUserId       string `json:"partnerUserId,omitempty"`
	PartnerConnectionId string `json:"partnerConnectionId,omitempty"`
}

func main() {
	redisAddress := getEnv("REDIS_ADDRESS", "localhost:6379")

	rdb := connectRedis(redisAddress)

	router := gin.Default()

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"service": "matchmaking-service",
			"status":  "healthy",
		})
	})

	router.POST("/match/start", func(c *gin.Context) {
		var req MatchRequest

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "invalid request",
			})
			return
		}

		if req.UserId == "" || req.ConnectionId == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "userId and connectionId are required",
			})
			return
		}

		// Save current user's connection data.
		rdb.HSet(ctx, "connection_user:"+req.ConnectionId, map[string]interface{}{
			"userId": req.UserId,
			"vibe":   req.Vibe,
		})

		// Try to get another waiting user.
		partnerConnectionId, err := rdb.RPop(ctx, "waiting_users").Result()

		if err == redis.Nil {
			// Nobody waiting, so add this user to queue.
			rdb.LPush(ctx, "waiting_users", req.ConnectionId)

			c.JSON(http.StatusOK, MatchResponse{
				IsMatched: false,
				Status:    "waiting",
			})
			return
		}

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "redis error",
			})
			return
		}

		// Avoid matching user with themselves.
		if partnerConnectionId == req.ConnectionId {
			rdb.LPush(ctx, "waiting_users", req.ConnectionId)

			c.JSON(http.StatusOK, MatchResponse{
				IsMatched: false,
				Status:    "waiting",
			})
			return
		}

		partnerData, _ := rdb.HGetAll(ctx, "connection_user:"+partnerConnectionId).Result()
		partnerUserId := partnerData["userId"]

		roomId := "room-" + uuid.New().String()

		// Store room data.
		rdb.SAdd(ctx, "room:"+roomId, req.ConnectionId, partnerConnectionId)
		rdb.Set(ctx, "user_room:"+req.ConnectionId, roomId, 0)
		rdb.Set(ctx, "user_room:"+partnerConnectionId, roomId, 0)

		c.JSON(http.StatusOK, MatchResponse{
			IsMatched:           true,
			Status:              "matched",
			RoomId:              roomId,
			PartnerUserId:       partnerUserId,
			PartnerConnectionId: partnerConnectionId,
		})
	})

	router.POST("/match/leave", func(c *gin.Context) {
		var body struct {
			ConnectionId string `json:"connectionId"`
			RoomId       string `json:"roomId"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "invalid request",
			})
			return
		}

		if body.RoomId != "" {
			rdb.SRem(ctx, "room:"+body.RoomId, body.ConnectionId)
		}

		rdb.Del(ctx, "user_room:"+body.ConnectionId)
		rdb.Del(ctx, "connection_user:"+body.ConnectionId)
		rdb.LRem(ctx, "waiting_users", 0, body.ConnectionId)

		c.JSON(http.StatusOK, gin.H{
			"status": "left",
		})
	})

	router.Run(":8081")
}

func connectRedis(address string) *redis.Client {
	var client *redis.Client

	for {
		client = redis.NewClient(&redis.Options{
			Addr: address,
		})

		err := client.Ping(ctx).Err()

		if err == nil {
			fmt.Println("Connected to Redis.")
			return client
		}

		fmt.Println("Redis not ready, retrying in 3 seconds...")
		time.Sleep(3 * time.Second)
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	return value
}