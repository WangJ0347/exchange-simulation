package exchange

import (
	"context"
	"log"
	"sync"
	"time"

	"exchange/streamspb"

	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"
)

// ListenToTradeEvent listens to tradeEvent stream and processes trade events
func ListenToTradeEvent(queueManager *TickerQueueManager, redisClient *redis.Client, done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Println("Starting to listen to tradeEvent stream")
	lastID := "$" // Start from newest messages only

	// Create a cancellable context
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Cancel context when done channel is closed
	go func() {
		<-done
		cancel()
	}()

	for {
		select {
		case <-done:
			log.Println("Stopped listening to tradeEvent stream")
			return
		default:
		}

		// Read from Redis Stream using XREAD
		streams, err := redisClient.XRead(streamCtx, &redis.XReadArgs{
			Streams: []string{"tradeEvent", lastID},
			Block:   0,   // Block indefinitely until message arrives
			Count:   100, // Batch read to reduce Redis roundtrips (performance optimization)
		}).Result()

		if err != nil {
			if err == context.Canceled || streamCtx.Err() == context.Canceled {
				return
			}
			if err == redis.Nil {
				continue
			}
			log.Printf("TradeEvent XRead error: %v", err)
			select {
			case <-done:
				return
			case <-time.After(1 * time.Second):
				continue
			}
		}

		// Process messages
		for _, stream := range streams {
			if stream.Stream == "tradeEvent" {
				for _, message := range stream.Messages {
					select {
					case <-done:
						return
					default:
					}

					lastID = message.ID

					// tradeEvent stream carries a single "payload" field (protobuf); market-feed publishes pb.TradeEventPayload
					payloadVal, ok := message.Values["payload"]
					if !ok {
						log.Printf("Missing payload in tradeEvent message %s", message.ID)
						continue
					}
					payloadStr, ok := payloadVal.(string)
					if !ok {
						log.Printf("Invalid payload type in tradeEvent message %s", message.ID)
						continue
					}
					var ev streamspb.TradeEventPayload
					if err := proto.Unmarshal([]byte(payloadStr), &ev); err != nil {
						log.Printf("Failed to decode tradeEvent payload %s: %v", message.ID, err)
						continue
					}

					ticker := ev.GetTicker()
					priceStr := ev.GetPrice()
					volumeStr := ev.GetVolume()
					var side Side = Buy
					if ev.GetSide() == "sell" {
						side = Sell
					}

					// Enqueue trade event request
					tickerReq := TickerRequest{
						Type:   "TradeEvent",
						Ticker: ticker,
						TradeEvent: &TradeEventData{
							Ticker: ticker,
							Price:  priceStr,
							Volume: volumeStr,
							Side:   side,
						},
						Source: "trade_event",
						Async:  true, // Don't need to wait for result
					}

					queueManager.EnqueueRequestAsync(tickerReq)
				}
			}
		}
	}
}
