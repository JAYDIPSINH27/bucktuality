package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type PresenceRequest struct {
	UserId       string `json:"userId"`
	ConnectionId string `json:"connectionId"`
	Status       string `json:"status"`
	RoomId       string `json:"roomId"`
	CameraOn     bool   `json:"cameraOn"`
	MicOn        bool   `json:"micOn"`
}

func main() {
	redisAddress := getEnv("REDIS_ADDRESS", "localhost:6379")

	rdb := connectRedis(redisAddress)

	router := gin.Default()

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"service": "presence-service",
			"status":  "healthy",
		})
	})

	router.POST("/presence/online", func(c *gin.Context) {
		var req PresenceRequest

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
			return
		}

		if req.UserId == "" || req.ConnectionId == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "userId and connectionId are required"})
			return
		}

		key := "presence:user:" + req.UserId

		rdb.HSet(ctx, key, map[string]interface{}{
			"userId":       req.UserId,
			"connectionId": req.ConnectionId,
			"status":       "online",
			"roomId":       req.RoomId,
			"cameraOn":     req.CameraOn,
			"micOn":        req.MicOn,
			"lastSeenUtc":  time.Now().UTC().Format(time.RFC3339),
		})

		rdb.SAdd(ctx, "presence:online_users", req.UserId)
		rdb.Set(ctx, "presence:connection:"+req.ConnectionId, req.UserId, 0)

		c.JSON(http.StatusOK, gin.H{
			"status": "online",
			"userId": req.UserId,
		})
	})

	router.POST("/presence/offline", func(c *gin.Context) {
		var req PresenceRequest

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
			return
		}

		if req.UserId == "" && req.ConnectionId != "" {
			userId, _ := rdb.Get(ctx, "presence:connection:"+req.ConnectionId).Result()
			req.UserId = userId
		}

		if req.UserId == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "userId or valid connectionId is required"})
			return
		}

		key := "presence:user:" + req.UserId

		rdb.HSet(ctx, key, map[string]interface{}{
			"status":      "offline",
			"roomId":      "",
			"cameraOn":    false,
			"micOn":       false,
			"lastSeenUtc": time.Now().UTC().Format(time.RFC3339),
		})

		rdb.SRem(ctx, "presence:online_users", req.UserId)
		rdb.SRem(ctx, "presence:waiting_users", req.UserId)
		rdb.SRem(ctx, "presence:matched_users", req.UserId)

		if req.ConnectionId != "" {
			rdb.Del(ctx, "presence:connection:"+req.ConnectionId)
		}

		c.JSON(http.StatusOK, gin.H{
			"status": "offline",
			"userId": req.UserId,
		})
	})

	router.POST("/presence/status", func(c *gin.Context) {
		var req PresenceRequest

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
			return
		}

		if req.UserId == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "userId is required"})
			return
		}

		key := "presence:user:" + req.UserId

		rdb.HSet(ctx, key, map[string]interface{}{
			"userId":      req.UserId,
			"status":      req.Status,
			"roomId":      req.RoomId,
			"cameraOn":    req.CameraOn,
			"micOn":       req.MicOn,
			"lastSeenUtc": time.Now().UTC().Format(time.RFC3339),
		})

		rdb.SRem(ctx, "presence:waiting_users", req.UserId)
		rdb.SRem(ctx, "presence:matched_users", req.UserId)

		if req.Status == "waiting" {
			rdb.SAdd(ctx, "presence:waiting_users", req.UserId)
		}

		if req.Status == "matched" || req.Status == "in-call" {
			rdb.SAdd(ctx, "presence:matched_users", req.UserId)
		}

		c.JSON(http.StatusOK, gin.H{
			"status": req.Status,
			"userId": req.UserId,
		})
	})

	router.GET("/presence/user/:userId", func(c *gin.Context) {
		userId := c.Param("userId")

		data, err := rdb.HGetAll(ctx, "presence:user:"+userId).Result()

		if err != nil || len(data) == 0 {
			c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
			return
		}

		c.JSON(http.StatusOK, data)
	})

	router.GET("/presence/summary", func(c *gin.Context) {
		onlineCount, _ := rdb.SCard(ctx, "presence:online_users").Result()
		waitingCount, _ := rdb.SCard(ctx, "presence:waiting_users").Result()
		matchedCount, _ := rdb.SCard(ctx, "presence:matched_users").Result()

		c.JSON(http.StatusOK, gin.H{
			"onlineUsers":  onlineCount,
			"waitingUsers": waitingCount,
			"matchedUsers": matchedCount,
		})
	})

	router.Run(":8084")
}

func connectRedis(address string) *redis.Client {
	for {
		client := redis.NewClient(&redis.Options{
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