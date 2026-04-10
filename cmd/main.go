package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"exchange"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
)

var hcmcLoc *time.Location

func init() {
	loc, err := time.LoadLocation("Asia/Ho_Chi_Minh")
	if err != nil {
		loc = time.FixedZone("Asia/Ho_Chi_Minh", 7*60*60)
	}
	hcmcLoc = loc
}

// isTradingDay checks if today is a trading day
func isTradingDay(redisClient *redis.Client) bool {
	ctx := context.Background()
	now := time.Now().In(hcmcLoc)
	currentDate := now.Format("2006-01-02")
	dayOfWeek := now.Weekday()

	// Check if it's Saturday or Sunday
	if dayOfWeek == time.Saturday || dayOfWeek == time.Sunday {
		return false
	}

	// Check if current date is in holidays set
	isMember, err := redisClient.SIsMember(ctx, "Holidays", currentDate).Result()
	if err != nil {
		log.Printf("Error checking holidays: %v", err)
		return true // Default to trading day if can't check
	}

	return !isMember
}

// purgeRedisMemory attempts to purge memory fragmentation after cleanup operations
// This helps Redis return freed memory to the OS
func purgeRedisMemory(redisClient *redis.Client) {
	ctx := context.Background()
	// Try MEMORY PURGE command (available in Redis 4.0+)
	// This purges memory fragmentation and returns memory to OS
	err := redisClient.Do(ctx, "MEMORY", "PURGE").Err()
	if err != nil {
		// MEMORY PURGE might not be available in older Redis versions
		// This is not critical, just log it
		log.Printf("MEMORY PURGE not available or failed (this is OK for Redis < 4.0): %v", err)
	} else {
		log.Println("MEMORY PURGE completed successfully")
	}
}

// resetStates clears streams and resets orderbooks (called at 8:45)
func resetStates(redisClient *redis.Client) {
	log.Println("Resetting states: clearing streams and orderbooks...")

	ctx := context.Background()

	// Clear streams (same as order-processor)
	if err := redisClient.XTrimMaxLen(ctx, "eqtOrder", 0).Err(); err != nil {
		log.Printf("Error trimming eqtOrder stream: %v", err)
	}
	if err := redisClient.XTrimMaxLen(ctx, "fnoOrder", 0).Err(); err != nil {
		log.Printf("Error trimming fnoOrder stream: %v", err)
	}
	if err := redisClient.XTrimMaxLen(ctx, "tradeEvent", 0).Err(); err != nil {
		log.Printf("Error trimming tradeEvent stream: %v", err)
	}

	// Delete all *_exchangeOrders hashes
	var cursor uint64
	for {
		keys, nextCursor, err := redisClient.Scan(ctx, cursor, "*_exchangeOrders", 200).Result()
		if err != nil {
			log.Printf("Error scanning _exchangeOrders keys: %v", err)
			break
		}
		if len(keys) > 0 {
			if err := redisClient.Del(ctx, keys...).Err(); err != nil {
				log.Printf("Error deleting _exchangeOrders keys: %v", err)
			} else {
				log.Printf("Deleted %d _exchangeOrders keys", len(keys))
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	// Purge memory fragmentation after cleanup
	log.Println("Purging Redis memory fragmentation...")
	purgeRedisMemory(redisClient)

	// Reset all orderbooks (OrderBookManager handles this internally)
	// We just need to create a new manager which starts with empty orderbooks
	log.Println("States reset complete")
}

// orchestrate runs the main orchestrator during trading hours (9:15-14:50)
func orchestrate(manager *exchange.OrderBookManager, queueManager *exchange.TickerQueueManager, redisClient *redis.Client, done chan struct{}, interrupt <-chan os.Signal) error {
	log.Println("Starting matching engine orchestrator")

	var wg sync.WaitGroup
	ingressStop := make(chan struct{})
	var tradeWg sync.WaitGroup

	// Start HTTP server in goroutine
	router := mux.NewRouter()
	api := router.PathPrefix("/services").Subrouter()

	// Register endpoints (now using queue manager)
	api.HandleFunc("/eqt/send", exchange.HandleEqtSend(queueManager)).Methods("POST")
	api.HandleFunc("/eqt/modify", exchange.HandleEqtModify(queueManager)).Methods("POST")
	api.HandleFunc("/eqt/cancel", exchange.HandleEqtCancel(queueManager)).Methods("POST")
	api.HandleFunc("/fno/send", exchange.HandleFnoSend(queueManager)).Methods("POST")
	api.HandleFunc("/fno/modify", exchange.HandleFnoModify(queueManager)).Methods("POST")
	api.HandleFunc("/fno/cancel", exchange.HandleFnoCancel(queueManager)).Methods("POST")

	port := getEnv("PORT", "8003")
	server := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	// Start HTTP server
	go func() {
		log.Printf("Matching engine HTTP server starting on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	tradeWg.Add(1)
	go exchange.ListenToTradeEvent(queueManager, redisClient, ingressStop, done, &tradeWg)

	exchange.StartLiquidityProviderV2(manager, queueManager, redisClient, done, &wg)

	// Wait until 14:50 to run market close task and shutdown
	now := time.Now().In(hcmcLoc)
	shutdownTimeStr := "14:50:00"
	today := now.Format("2006-01-02")
	shutdownDateTime, err := time.ParseInLocation("2006-01-02 15:04:05", today+" "+shutdownTimeStr, hcmcLoc)
	if err != nil {
		log.Printf("Error parsing 14:50 time: %v", err)
		shutdownDateTime = now.Add(6 * time.Hour) // Default to 6 hours from now
	}

	// If 14:50 has already passed, set for tomorrow
	if now.After(shutdownDateTime) {
		shutdownDateTime = shutdownDateTime.Add(24 * time.Hour)
	}

	duration := shutdownDateTime.Sub(now)
	timer := time.NewTimer(duration)

	select {
	case <-timer.C:
		log.Println("Shutdown time (14:50) reached, running market close task...")

		log.Println("Shutting down HTTP server before market close...")
		shctx, shcancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := server.Shutdown(shctx); err != nil {
			log.Printf("Error shutting down HTTP server: %v", err)
		}
		shcancel()

		log.Println("Stopping trade event ingress...")
		close(ingressStop)
		tradeWg.Wait()

		log.Println("Executing market close task via per-ticker queues...")
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 120*time.Second)
		manager.SetRedisContext(flushCtx)
		queueManager.HandleMarketCloseViaQueues()
		flushCancel()
		manager.SetRedisContext(context.Background())

		manager.StopRedisFlushWorker()

		log.Println("Stopping all ticker queues...")
		queueManager.StopAllQueues()

		log.Println("Stopping event listeners and LP...")
		close(done)

		doneChan := make(chan struct{})
		go func() {
			wg.Wait()
			close(doneChan)
		}()

		select {
		case <-doneChan:
			log.Println("All goroutines stopped, orchestrator shutdown complete")
		case <-time.After(5 * time.Second):
			log.Println("Warning: Shutdown timeout, some goroutines may not have finished")
		}

		return nil
	case <-interrupt:
		log.Println("Interrupt received, shutting down orchestrator...")
		timer.Stop()

		shctx, shcancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := server.Shutdown(shctx); err != nil {
			log.Printf("Error shutting down HTTP server: %v", err)
		}
		shcancel()

		close(ingressStop)
		tradeWg.Wait()

		now := time.Now().In(hcmcLoc)
		if now.Hour() >= 14 && now.Minute() >= 50 {
			log.Println("Running market close task before shutdown...")
			flushCtx, flushCancel := context.WithTimeout(context.Background(), 120*time.Second)
			manager.SetRedisContext(flushCtx)
			queueManager.HandleMarketCloseViaQueues()
			flushCancel()
			manager.SetRedisContext(context.Background())
			log.Println("Market close task completed")
		}

		manager.StopRedisFlushWorker()

		log.Println("Stopping all ticker queues...")
		queueManager.StopAllQueues()

		log.Println("Stopping event listeners...")
		close(done)

		doneChan := make(chan struct{})
		go func() {
			wg.Wait()
			close(doneChan)
		}()

		select {
		case <-doneChan:
			log.Println("All goroutines stopped gracefully")
		case <-time.After(5 * time.Second):
			log.Println("Warning: Shutdown timeout, some goroutines may not have finished")
		}

		return errors.New("interrupted by user")
	}
}

// secondsToNextTime calculates seconds until target time (today or tomorrow)
func secondsToNextTime(targetTime string) int {
	now := time.Now().In(hcmcLoc)
	today := now.Format("2006-01-02")
	targetDateTime, err := time.ParseInLocation("2006-01-02 15:04:05", today+" "+targetTime, hcmcLoc)

	if err != nil {
		log.Printf("Error parsing target time: %v", err)
		return 60 // Default to 1 minute
	}

	// If target time has passed today, set it for tomorrow
	if now.After(targetDateTime) {
		targetDateTime = targetDateTime.Add(24 * time.Hour)
	}

	duration := targetDateTime.Sub(now)
	seconds := int(duration.Seconds())

	// Ensure minimum 1 second
	if seconds < 1 {
		seconds = 1
	}

	return seconds
}

// sleepWithInterrupt sleeps for the specified duration while allowing interrupt handling
func sleepWithInterrupt(duration time.Duration, interrupt <-chan os.Signal) bool {
	timer := time.NewTimer(duration)
	select {
	case <-timer.C:
		return true
	case <-interrupt:
		timer.Stop()
		log.Println("Program interrupted, exiting...")
		return false
	}
}

func main() {
	// Initialize Redis client
	redisClient := exchange.InitializeRedisClient()
	defer redisClient.Close()

	// Set up channel to handle interrupt signal
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	for {
		// Check if today is a trading day
		if !isTradingDay(redisClient) {
			sleepTime := secondsToNextTime("09:00:00")
			log.Printf("Not a trading day. Sleeping until next check time (in %v seconds)", sleepTime)
			if !sleepWithInterrupt(time.Duration(sleepTime)*time.Second, interrupt) {
				return
			}
			continue
		}

		// Wait until 9:00 to reset states
		now := time.Now().In(hcmcLoc)
		currentTime := now.Format("15:04:05")

		if currentTime < "09:15:00" {
			// Reset states (orderbooks will be reset when new manager is created)
			resetStates(redisClient)
			// Wait until 9:15
			sleepTime := secondsToNextTime("09:15:05")
			log.Printf("Waiting for 9:15 to reset states. Sleeping %v seconds", sleepTime)
			if !sleepWithInterrupt(time.Duration(sleepTime)*time.Second, interrupt) {
				return
			}
		}

		// Reset states at 8:45
		now = time.Now().In(hcmcLoc)
		currentTime = now.Format("15:04:05")

		// if currentTime >= "08:45:00" && currentTime < "09:15:00" {

		// 	// Wait until 9:15 (market open)
		// 	sleepTime := secondsToNextTime("09:15:05")
		// 	log.Printf("States reset. Waiting for market open (9:15). Sleeping %v seconds", sleepTime)
		// 	if !sleepWithInterrupt(time.Duration(sleepTime)*time.Second, interrupt) {
		// 		return
		// 	}
		// }

		// // Check if we're in trading window (9:15 to 14:50)
		// now = time.Now().In(hcmcLoc)
		// currentTime = now.Format("15:04:05")

		if currentTime >= "09:15:00" && currentTime < "14:50:00" {
			log.Println("Trading window detected (9:15-14:50), starting orchestrator...")

			// Create OrderBookManager
			manager := exchange.NewOrderBookManager(redisClient)

			// Create done channel for queue manager (will be closed in orchestrate)
			done := make(chan struct{})
			// Create TickerQueueManager (queue size: 1000 per ticker)
			queueManager := exchange.NewTickerQueueManager(manager, 1000, done)

			// Restore orderbook state from Redis (in case of restart during trading hours)
			log.Println("Restoring orderbook state from Redis...")
			if err := manager.RestoreOrderbookState(); err != nil {
				log.Printf("Error restoring orderbook state: %v", err)
				log.Println("Continuing with empty orderbook...")
			}

			// Run orchestrator (will shutdown at 14:50)
			// Pass done channel so orchestrate can close it
			err := orchestrate(manager, queueManager, redisClient, done, interrupt)
			if err != nil {
				if err.Error() == "interrupted by user" {
					log.Println("Program interrupted, exiting...")
					return
				}
				log.Printf("Orchestrator error: %v", err)
			}

			log.Println("Orchestrator shutdown complete")
		}

		// Outside trading window, sleep until next trading day
		sleepTime := secondsToNextTime("09:00:00")
		log.Printf("Outside trading window. Next trading day start: 08:45:00 (in %v seconds)", sleepTime)

		// Force GC and return heap memory to OS before sleeping until next trading day
		debug.FreeOSMemory()

		if !sleepWithInterrupt(time.Duration(sleepTime)*time.Second, interrupt) {
			return
		}
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
