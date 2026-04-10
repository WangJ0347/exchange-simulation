package exchange

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	defaultEnqueueWaitTimeout = 45 * time.Second
	enqueueSendWaitTimeout    = 60 * time.Second
)

// TickerRequest represents a request to be processed for a specific ticker
type TickerRequest struct {
	Type       string // "Send", "Modify", "Cancel", "TradeEvent"
	Ticker     string
	Request    OrderRequest // For Send/Modify/Cancel
	TradeEvent *TradeEventData
	Source     string           // "user" or "liquidity_provider" - for tracking
	ResultChan chan OrderResult // For synchronous responses
	ErrorChan  chan error       // For errors
	Async      bool             // If true, don't wait for result (for LP bulk operations)
}

// TradeEventData represents trade event information
type TradeEventData struct {
	Ticker string
	Price  string
	Volume string
	Side   Side
}

// TickerQueueManager manages FIFO queues for each ticker
type TickerQueueManager struct {
	manager   *OrderBookManager
	queues    map[string]chan TickerRequest // ticker -> request queue
	queuesMu  sync.RWMutex
	queueSize int // Buffer size for each queue
	done      <-chan struct{}
}

// NewTickerQueueManager creates a new TickerQueueManager
func NewTickerQueueManager(manager *OrderBookManager, queueSize int, done <-chan struct{}) *TickerQueueManager {
	tqm := &TickerQueueManager{
		manager:   manager,
		queues:    make(map[string]chan TickerRequest),
		queueSize: queueSize,
		done:      done,
	}
	return tqm
}

// getOrCreateQueue gets or creates a queue for a ticker
func (tqm *TickerQueueManager) getOrCreateQueue(ticker string) chan TickerRequest {
	tqm.queuesMu.RLock()
	if queue, exists := tqm.queues[ticker]; exists {
		tqm.queuesMu.RUnlock()
		return queue
	}
	tqm.queuesMu.RUnlock()

	// Create new queue
	tqm.queuesMu.Lock()
	defer tqm.queuesMu.Unlock()

	// Double-check after acquiring write lock
	if queue, exists := tqm.queues[ticker]; exists {
		return queue
	}

	queue := make(chan TickerRequest, tqm.queueSize)
	tqm.queues[ticker] = queue

	// Start processor goroutine for this ticker
	go tqm.processTickerQueue(ticker, queue)

	return queue
}

// EnqueueRequest enqueues a request for processing (blocks until a queue slot is available, up to enqueueSendWaitTimeout).
func (tqm *TickerQueueManager) EnqueueRequest(req TickerRequest) OrderResult {
	return tqm.EnqueueRequestWithContext(context.Background(), req)
}

// EnqueueRequestWithContext enqueues like EnqueueRequest but aborts if ctx is canceled.
func (tqm *TickerQueueManager) EnqueueRequestWithContext(c context.Context, req TickerRequest) OrderResult {
	reqChan := tqm.getOrCreateQueue(req.Ticker)

	resultChan := make(chan OrderResult, 1)
	req.ResultChan = resultChan

	sendTimer := time.NewTimer(enqueueSendWaitTimeout)
	defer sendTimer.Stop()

	select {
	case reqChan <- req:
	case <-tqm.done:
		return OrderResult{
			Ticker:       req.Ticker,
			Status:       "ERR",
			ErrorMessage: "Service shutting down",
		}
	case <-c.Done():
		return OrderResult{
			Ticker:       req.Ticker,
			Status:       "ERR",
			ErrorMessage: "Request canceled",
		}
	case <-sendTimer.C:
		log.Printf("Timeout waiting for queue slot for ticker %s", req.Ticker)
		return OrderResult{
			Ticker:       req.Ticker,
			Status:       "ERR",
			ErrorMessage: "Timeout waiting to enqueue (queue saturated)",
		}
	}

	waitTimer := time.NewTimer(defaultEnqueueWaitTimeout)
	defer waitTimer.Stop()

	select {
	case result := <-resultChan:
		return result
	case <-tqm.done:
		return OrderResult{
			Ticker:       req.Ticker,
			Status:       "ERR",
			ErrorMessage: "Service shutting down",
		}
	case <-c.Done():
		return OrderResult{
			Ticker:       req.Ticker,
			Status:       "ERR",
			ErrorMessage: "Request canceled",
		}
	case <-waitTimer.C:
		log.Printf("Timeout waiting for result for ticker %s", req.Ticker)
		return OrderResult{
			Ticker:       req.Ticker,
			Status:       "ERR",
			ErrorMessage: "Timeout waiting for processing",
		}
	}
}

// HandleMarketCloseViaQueues runs end-of-day expiry and book reset on each ticker's queue (serialized with trading).
func (tqm *TickerQueueManager) HandleMarketCloseViaQueues() {
	log.Println("Starting market close task (per-ticker queues)...")
	tickers := tqm.manager.SnapshotTickersFromOrderbooks()
	if len(tickers) == 0 {
		log.Println("No active orderbooks, market close task complete")
		return
	}
	log.Printf("Processing %d tickers for market close", len(tickers))
	for _, ticker := range tickers {
		res := tqm.EnqueueRequestWithContext(context.Background(), TickerRequest{
			Type:   "MarketClose",
			Ticker: ticker,
			Source: "market_close",
		})
		if res.Status == "ERR" && res.ErrorMessage != "" {
			log.Printf("Market close step for %s: %s", ticker, res.ErrorMessage)
		}
	}
	log.Println("Market close task completed: all tickers processed")
}

// CancelAllLPOrders cancels all LP orders via per-ticker queues (safe concurrent with matching).
func (tqm *TickerQueueManager) CancelAllLPOrders() int {
	log.Println("Scanning for orphaned LP orders to cancel...")
	targets := tqm.manager.CollectLPCancelTargets()
	if len(targets) == 0 {
		log.Println("No orphaned LP orders found")
		return 0
	}
	log.Printf("Found %d orphaned LP orders, canceling via queues...", len(targets))
	canceled := 0
	for _, t := range targets {
		res := tqm.EnqueueRequest(TickerRequest{
			Type:   "Cancel",
			Ticker: t.Ticker,
			Request: OrderRequest{
				OrderID:      t.OrderID,
				OrderGroupID: t.OrderGroupID,
				Ticker:       t.Ticker,
				Remark:       t.Remark,
			},
			Source: "lp_bulk_cleanup",
		})
		if res.Status == "CAN" {
			canceled++
			continue
		}
		if res.Status == "ERR" && strings.Contains(res.ErrorMessage, "already removed") {
			canceled++
		}
	}
	log.Printf("Canceled %d orphaned LP orders (via queue)", canceled)
	return canceled
}

// EnqueueRequestAsync enqueues a request without waiting for result (blocks up to enqueueSendWaitTimeout for a slot).
func (tqm *TickerQueueManager) EnqueueRequestAsync(req TickerRequest) {
	reqChan := tqm.getOrCreateQueue(req.Ticker)
	req.Async = true

	sendTimer := time.NewTimer(enqueueSendWaitTimeout)
	defer sendTimer.Stop()
	select {
	case reqChan <- req:
	case <-tqm.done:
		return
	case <-sendTimer.C:
		log.Printf("Queue full for ticker %s, async enqueue timed out", req.Ticker)
	}
}

// processTickerQueue processes requests sequentially for a ticker
func (tqm *TickerQueueManager) processTickerQueue(ticker string, reqChan chan TickerRequest) {
	log.Printf("Queue processor started for ticker %s", ticker)
	defer log.Printf("Queue processor stopped for ticker %s", ticker)

	for {
		select {
		case <-tqm.done:
			return
		case req, ok := <-reqChan:
			if !ok {
				// Channel closed, exit gracefully
				return
			}
			// Process request sequentially
			result := tqm.handleRequest(ticker, req)

			// Send result back if not async
			if !req.Async && req.ResultChan != nil {
				select {
				case req.ResultChan <- result:
				default:
					log.Printf("Result channel full for ticker %s", ticker)
				}
			}
		}
	}
}

// handleRequest handles a single request
func (tqm *TickerQueueManager) handleRequest(ticker string, req TickerRequest) OrderResult {
	switch req.Type {
	case "Send", "Modify":
		// Validate request - Send/Modify need Price and Quantity
		if err := validateRequest(req.Request); err != nil {
			return OrderResult{
				Ticker:       ticker,
				Status:       "ERR",
				ErrorMessage: err.Error(),
			}
		}

		// Process order (no ticker lock needed - queue ensures sequential processing)
		result := tqm.manager.ProcessOrder(ticker, req.Type, req.Request)

		return result

	case "Cancel":
		// Cancel only needs OrderID and Ticker - skip price/quantity validation
		if req.Request.Ticker == "" {
			return OrderResult{
				Ticker:       ticker,
				Status:       "ERR",
				ErrorMessage: "Ticker is required",
			}
		}
		if req.Request.OrderID == "" {
			return OrderResult{
				Ticker:       ticker,
				Status:       "ERR",
				ErrorMessage: "OrderID is required for Cancel",
			}
		}

		// Process cancel (no ticker lock needed - queue ensures sequential processing)
		result := tqm.manager.ProcessOrder(ticker, req.Type, req.Request)

		return result

	case "TradeEvent":
		if req.TradeEvent == nil {
			return OrderResult{
				Ticker:       ticker,
				Status:       "ERR",
				ErrorMessage: "TradeEvent data is nil",
			}
		}

		// Process trade event (no ticker lock needed)
		err := tqm.manager.MatchTradeEvent(
			req.TradeEvent.Ticker,
			req.TradeEvent.Price,
			req.TradeEvent.Volume,
			req.TradeEvent.Side,
		)
		if err != nil {
			return OrderResult{
				Ticker:       ticker,
				Status:       "ERR",
				ErrorMessage: err.Error(),
			}
		}
		return OrderResult{Status: "OK"}

	case "MarketClose":
		tqm.manager.processMarketCloseTicker(ticker)
		return OrderResult{Ticker: ticker, Status: "OK", ErrorMessage: "Market close applied"}

	default:
		return OrderResult{
			Ticker:       ticker,
			Status:       "ERR",
			ErrorMessage: "Unknown request type: " + req.Type,
		}
	}
}

// validateRequest validates an order request
func validateRequest(req OrderRequest) error {
	if req.Ticker == "" {
		return &ValidationError{Message: "Ticker is required"}
	}
	if req.Price <= 0 {
		return &ValidationError{Message: "Price must be greater than 0"}
	}
	if req.Quantity <= 0 {
		return &ValidationError{Message: "Quantity must be greater than 0"}
	}
	return nil
}

// ValidationError represents a validation error
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

// StopAllQueues gracefully stops all ticker queue processors
// This closes all queue channels, which will cause processors to exit
func (tqm *TickerQueueManager) StopAllQueues() {
	tqm.queuesMu.Lock()
	defer tqm.queuesMu.Unlock()

	// Close all queue channels to unblock processors
	for ticker, q := range tqm.queues {
		log.Printf("Closing queue for ticker %s", ticker)
		close(q)
	}
	// Clear the map
	tqm.queues = make(map[string]chan TickerRequest)
}
