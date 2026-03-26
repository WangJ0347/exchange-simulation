package exchange

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

// InitializeRedisClient creates and returns a Redis client instance
func InitializeRedisClient() *redis.Client {
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "host.docker.internal"
	}

	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		redisPort = "6379"
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")

	redisDB := 0
	if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
		if db, err := strconv.Atoi(dbStr); err == nil {
			redisDB = db
		}
	}

	addr := redisHost + ":" + redisPort

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     redisPassword,
		DB:           redisDB,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		PoolSize:     15,
		PoolTimeout:  30 * time.Second,
	})

	// Test connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Printf("Failed to connect to Redis: %v", err)
		os.Exit(1)
	}

	log.Println("Successfully connected to Redis")
	return rdb
}

// func InitializeRedisClient() *redis.Client {
// 	rdb := redis.NewClient(&redis.Options{
// 		Addr:         "localhost:6379",
// 		Password:     "",
// 		DB:           0,
// 		DialTimeout:  10 * time.Second,
// 		ReadTimeout:  10 * time.Second,
// 		WriteTimeout: 10 * time.Second,
// 		PoolSize:     15,
// 		PoolTimeout:  30 * time.Second,
// 	})

// 	// Test connection
// 	_, err := rdb.Ping(ctx).Result()
// 	if err != nil {
// 		log.Printf("Failed to connect to Redis: %v", err)
// 		os.Exit(1)
// 	}

// 	log.Println("Successfully connected to Redis")
// 	return rdb
// }

// ExtractInternalOrderID extracts internalOrderID from remark or falls back to orderGroupID
// Format: {strategy}:{basketID}:{internalOrderID}
func ExtractInternalOrderID(remark string, orderGroupID string) string {
	if remark == "" {
		return orderGroupID
	}

	parts := strings.Split(remark, ":")
	if len(parts) >= 3 {
		// Return the third part (index 2) as internalOrderID
		return parts[2]
	}

	// Fallback to OrderGroupID if format is incorrect
	return orderGroupID
}
