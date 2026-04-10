package exchange

import (
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// LiquidityProviderOrder tracks a liquidity provider order
type LiquidityProviderOrderV2 struct {
	OrderID      string
	OrderGroupID string
	PriceLevel   string // e.g., "b1", "a1"
}

// TickerRefreshConfig holds per-ticker state for distributed refresh
type TickerRefreshConfig struct {
	ticker       string
	lastQuotes   map[string]string // Cache of last known quotes
	activeOrders map[string]string // priceLevel -> orderID mapping
	mu           sync.RWMutex
}

// LiquidityProviderServiceV2 manages liquidity provider orders with improved architecture
type LiquidityProviderServiceV2 struct {
	manager       *OrderBookManager
	queueManager  *TickerQueueManager
	redisClient   *redis.Client
	activeOrders  map[string]map[string]*LiquidityProviderOrderV2 // ticker -> orderID -> order info (for compatibility)
	tickerConfigs map[string]*TickerRefreshConfig                 // NEW: per-ticker configs
	mu            sync.RWMutex
	configMu      sync.RWMutex
	done          <-chan struct{}
}

// NewLiquidityProviderServiceV2 creates a new improved liquidity provider service
func NewLiquidityProviderServiceV2(manager *OrderBookManager, queueManager *TickerQueueManager, redisClient *redis.Client, done <-chan struct{}) *LiquidityProviderServiceV2 {
	return &LiquidityProviderServiceV2{
		manager:       manager,
		queueManager:  queueManager,
		redisClient:   redisClient,
		activeOrders:  make(map[string]map[string]*LiquidityProviderOrderV2),
		tickerConfigs: make(map[string]*TickerRefreshConfig),
		done:          done,
	}
}

// discoverTickers finds all tickers with {ticker}_quotes pattern (exact match, not {ticker}_odd_quotes)
func (lp *LiquidityProviderServiceV2) discoverTickers() ([]string, error) {
	seen := make(map[string]struct{})

	// 1. Find all tickers from *_weight hash fields (e.g., stocks)
	weightKeys, err := lp.redisClient.Keys(ctx, "*_weight").Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if len(weightKeys) > 0 {
		pipe := lp.redisClient.Pipeline()
		hkeysCmds := make([]*redis.StringSliceCmd, 0, len(weightKeys))
		for _, key := range weightKeys {
			hkeysCmds = append(hkeysCmds, pipe.HKeys(ctx, key))
		}
		_, _ = pipe.Exec(ctx)

		for _, cmd := range hkeysCmds {
			tickers, err := cmd.Result()
			if err != nil && err != redis.Nil {
				continue
			}
			for _, t := range tickers {
				t = strings.TrimSpace(t)
				if t != "" {
					seen[t] = struct{}{}
				}
			}
		}
	}

	// 2. Add single futures tickers from FuturesTickerKeys in queueManager if available
	// Use exchange/config.go's LoadConfig to access FuturesTickerKeys and ETFTickers
	cfg := LoadConfig()

	// 2. Add single futures tickers from FuturesTickerKeys in config if available
	if len(cfg.FuturesTickerKeys) > 0 {
		pipe := lp.redisClient.Pipeline()
		getCmds := make([]*redis.StringCmd, 0, len(cfg.FuturesTickerKeys))
		for _, key := range cfg.FuturesTickerKeys {
			getCmds = append(getCmds, pipe.Get(ctx, key))
		}
		_, _ = pipe.Exec(ctx)

		for _, cmd := range getCmds {
			val, err := cmd.Result()
			if err != nil {
				if err != redis.Nil {
					continue
				}
			}
			t := strings.TrimSpace(val)
			if t != "" {
				seen[t] = struct{}{}
			}
		}
	}

	// 3. Add ETF tickers if ETFTickers in config is available
	if cfg.ETFTickers != nil {
		for _, t := range cfg.ETFTickers {
			seen[t] = struct{}{}
		}
	}

	tickerList := make([]string, 0, len(seen))
	for t := range seen {
		tickerList = append(tickerList, t)
	}
	return tickerList, nil
}

// readQuotes reads quotes from Redis for a ticker
func (lp *LiquidityProviderServiceV2) readQuotes(ticker string) (map[string]string, error) {
	quotesKey := ticker + "_quotes"
	quotes, err := lp.redisClient.HGetAll(ctx, quotesKey).Result()
	if err != nil {
		return nil, err
	}
	return quotes, nil
}

// sendOrder sends a liquidity provider order
func (lp *LiquidityProviderServiceV2) sendOrder(ticker, side, priceLevel string, price, quantity float64) (string, error) {
	remark := "LIQUIDITY_PROVIDER:" + ticker + ":" + priceLevel

	req := OrderRequest{
		Ticker:   ticker,
		BS:       side,
		Price:    price,
		Quantity: quantity,
		Remark:   remark,
	}

	// Enqueue request through queue manager
	tickerReq := TickerRequest{
		Type:    "Send",
		Ticker:  ticker,
		Request: req,
		Source:  "liquidity_provider",
		Async:   false, // Need result to track orderID
	}

	result := lp.queueManager.EnqueueRequest(tickerReq)
	if result.Status == "ERR" {
		return "", nil // Return nil error but empty orderID to indicate failure
	}

	// Track order (use orderID and orderGroupID from result)
	lp.mu.Lock()
	if lp.activeOrders[ticker] == nil {
		lp.activeOrders[ticker] = make(map[string]*LiquidityProviderOrderV2)
	}
	lp.activeOrders[ticker][result.OrderID] = &LiquidityProviderOrderV2{
		OrderID:      result.OrderID,
		OrderGroupID: result.OrderGroupID,
		PriceLevel:   priceLevel,
	}
	lp.mu.Unlock()

	return result.OrderID, nil
}

// cancelOrder cancels a liquidity provider order
func (lp *LiquidityProviderServiceV2) cancelOrder(ticker, orderID string) error {
	// Get order info from active orders
	lp.mu.RLock()
	orderInfo, exists := lp.activeOrders[ticker][orderID]
	lp.mu.RUnlock()

	if !exists {
		log.Printf("LP Cancel: Order %s not found in lp.activeOrders[%s], skipping cancel", orderID, ticker)
		return nil // Order not found, skip
	}

	remark := "LIQUIDITY_PROVIDER:" + ticker + ":" + orderInfo.PriceLevel

	req := OrderRequest{
		OrderID:      orderID,
		OrderGroupID: orderInfo.OrderGroupID,
		Ticker:       ticker,
		Remark:       remark,
	}

	// Enqueue cancel request through queue manager (SYNC - must wait for completion)
	// This is critical: new orders must not be sent until old orders are fully cancelled
	// Otherwise we get order pile-up (e.g., 15 LP orders at same price instead of 1)
	tickerReq := TickerRequest{
		Type:    "Cancel",
		Ticker:  ticker,
		Request: req,
		Source:  "liquidity_provider",
		Async:   false, // MUST wait for result to prevent order pile-up
	}

	// Use synchronous enqueue to ensure cancel completes before we send new orders
	lp.queueManager.EnqueueRequest(tickerReq)

	// Remove from tracking
	lp.mu.Lock()
	if lp.activeOrders[ticker] != nil {
		delete(lp.activeOrders[ticker], orderID)
		if len(lp.activeOrders[ticker]) == 0 {
			delete(lp.activeOrders, ticker)
		}
	}
	lp.mu.Unlock()

	return nil
}

// getPriceLevelFromOrderID finds the price level for a given orderID
func getPriceLevelFromOrderID(orderID string, activeOrders map[string]string) string {
	for priceLevel, oid := range activeOrders {
		if oid == orderID {
			return priceLevel
		}
	}
	return ""
}

// refreshTickerOrders performs smart diff-based refresh for a single ticker
// Only cancels and resends orders when quotes actually change
func (lp *LiquidityProviderServiceV2) refreshTickerOrders(cfg *TickerRefreshConfig) error {
	ticker := cfg.ticker

	// 1. Read current quotes from Redis
	newQuotes, err := lp.readQuotes(ticker)
	if err != nil {
		return err
	}

	// 2. Get cached state
	cfg.mu.RLock()
	lastQuotes := make(map[string]string)
	for k, v := range cfg.lastQuotes {
		lastQuotes[k] = v
	}
	activeOrders := make(map[string]string)
	for k, v := range cfg.activeOrders {
		activeOrders[k] = v
	}
	cfg.mu.RUnlock()

	quoteLevels := getQuoteLevels(ticker)
	ordersToCancel := []string{}

	type orderToSend struct {
		ticker     string
		side       string
		priceLevel string
		price      float64
		quantity   float64
	}
	ordersToSend := []orderToSend{}

	// 3. Diff logic - check each price level
	for i := 1; i <= quoteLevels; i++ {
		// Check bid levels (b1, b2, ...)
		bPriceKey := "b" + strconv.Itoa(i)
		bVolKey := bPriceKey + "_vol"

		oldBidPrice := lastQuotes[bPriceKey]
		oldBidVol := lastQuotes[bVolKey]
		newBidPrice := newQuotes[bPriceKey]
		newBidVol := newQuotes[bVolKey]

		// If quote changed or disappeared
		if oldBidPrice != newBidPrice || oldBidVol != newBidVol {
			// Cancel old order at this level (if exists)
			if oldOrderID, exists := activeOrders[bPriceKey]; exists && oldOrderID != "" {
				ordersToCancel = append(ordersToCancel, oldOrderID)
			}

			// Send new order (if quote still valid)
			if newBidPrice != "" && newBidVol != "" && newBidPrice != "0" && newBidVol != "0" {
				price, err := strconv.ParseFloat(newBidPrice, 64)
				if err == nil && price > 0 {
					qty, err := strconv.ParseFloat(newBidVol, 64)
					if err == nil && qty > 0 {
						ordersToSend = append(ordersToSend, orderToSend{
							ticker:     ticker,
							side:       "B",
							priceLevel: bPriceKey,
							price:      price,
							quantity:   qty,
						})
					}
				}
			}
		}

		// Check ask levels (a1, a2, ...)
		aPriceKey := "a" + strconv.Itoa(i)
		aVolKey := aPriceKey + "_vol"

		oldAskPrice := lastQuotes[aPriceKey]
		oldAskVol := lastQuotes[aVolKey]
		newAskPrice := newQuotes[aPriceKey]
		newAskVol := newQuotes[aVolKey]

		// If quote changed or disappeared
		if oldAskPrice != newAskPrice || oldAskVol != newAskVol {
			// Cancel old order at this level (if exists)
			if oldOrderID, exists := activeOrders[aPriceKey]; exists && oldOrderID != "" {
				ordersToCancel = append(ordersToCancel, oldOrderID)
			}

			// Send new order (if quote still valid)
			if newAskPrice != "" && newAskVol != "" && newAskPrice != "0" && newAskVol != "0" {
				price, err := strconv.ParseFloat(newAskPrice, 64)
				if err == nil && price > 0 {
					qty, err := strconv.ParseFloat(newAskVol, 64)
					if err == nil && qty > 0 {
						ordersToSend = append(ordersToSend, orderToSend{
							ticker:     ticker,
							side:       "S",
							priceLevel: aPriceKey,
							price:      price,
							quantity:   qty,
						})
					}
				}
			}
		}
	}

	// 4. Execute updates - cancel old orders
	for _, orderID := range ordersToCancel {
		lp.cancelOrder(ticker, orderID)
		// Also remove from cfg.activeOrders
		cfg.mu.Lock()
		delete(cfg.activeOrders, getPriceLevelFromOrderID(orderID, activeOrders))
		cfg.mu.Unlock()
	}

	// 5. Send new orders (filter out duplicate prices - important!)
	// Multiple price levels (a1, a2, a3) might map to the same price
	// Only send ONE order per unique price per side
	newOrderIDs := make(map[string]string) // priceLevel -> new orderID
	sentBidPrices := make(map[float64]bool)
	sentAskPrices := make(map[float64]bool)

	for _, req := range ordersToSend {
		// Check for duplicate price
		if req.side == "B" {
			if sentBidPrices[req.price] {
				// Already sent order at this bid price, skip
				continue
			}
			sentBidPrices[req.price] = true
		} else {
			if sentAskPrices[req.price] {
				// Already sent order at this ask price, skip
				continue
			}
			sentAskPrices[req.price] = true
		}

		orderID, err := lp.sendOrder(req.ticker, req.side, req.priceLevel, req.price, req.quantity)
		if err == nil && orderID != "" {
			newOrderIDs[req.priceLevel] = orderID
		}
	}

	// 6. Update cached state
	cfg.mu.Lock()
	cfg.lastQuotes = newQuotes
	// Update activeOrders map with new orderIDs
	for priceLevel, orderID := range newOrderIDs {
		cfg.activeOrders[priceLevel] = orderID
	}
	cfg.mu.Unlock()

	// if len(ordersToCancel) > 0 || len(ordersToSend) > 0 {
	// 	log.Printf("Liquidity provider: Refreshed %s - canceled %d, sent %d orders", ticker, len(ordersToCancel), len(ordersToSend))
	// }

	return nil
}

// runTickerRefreshLoop runs the refresh loop for a single ticker with randomized intervals
func (lp *LiquidityProviderServiceV2) runTickerRefreshLoop(cfg *TickerRefreshConfig) {
	ticker := cfg.ticker

	for {
		// Calculate random interval (3-7 seconds for futures, 3-180 seconds for others)
		var randomSeconds int
		if strings.HasPrefix(ticker, "41I") {
			// Futures: 3-7 seconds
			randomSeconds = 3 + rand.Intn(5)
		} else {
			// Others: 3–300 seconds
			randomSeconds = 3 + rand.Intn(298)
		}
		interval := time.Duration(randomSeconds) * time.Second

		timer := time.NewTimer(interval)

		select {
		case <-timer.C:
			// Check if in trading window
			if !isInTradingWindow() {
				continue
			}

			// Perform refresh
			if err := lp.refreshTickerOrders(cfg); err != nil {
				log.Printf("Error refreshing %s: %v", ticker, err)
			}

		case <-lp.done:
			timer.Stop()
			log.Printf("Stopping refresh loop for %s", ticker)
			return
		}
	}
}

// sendInitialOrdersForTicker sends initial orders for a single ticker
func (lp *LiquidityProviderServiceV2) sendInitialOrdersForTicker(ticker string, quotes map[string]string) {
	quoteLevels := getQuoteLevels(ticker)
	ordersSent := 0

	// Create config for this ticker
	cfg := &TickerRefreshConfig{
		ticker:       ticker,
		lastQuotes:   make(map[string]string),
		activeOrders: make(map[string]string),
	}

	// Track sent prices to avoid duplicates (multiple price levels can map to same price)
	sentBidPrices := make(map[float64]bool)
	sentAskPrices := make(map[float64]bool)

	// Send bid orders (b1, b2, ...)
	for i := 1; i <= quoteLevels; i++ {
		priceKey := "b" + strconv.Itoa(i)
		volKey := "b" + strconv.Itoa(i) + "_vol"

		priceStr := quotes[priceKey]
		volStr := quotes[volKey]

		if priceStr == "" || volStr == "" {
			continue
		}

		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			continue
		}

		quantity, err := strconv.ParseFloat(volStr, 64)
		if err != nil {
			continue
		}

		if price <= 0 || quantity <= 0 {
			continue
		}

		// Skip if we already sent at this price (prevents duplicate LP orders)
		if sentBidPrices[price] {
			continue
		}
		sentBidPrices[price] = true

		orderID, err := lp.sendOrder(ticker, "B", priceKey, price, quantity)
		if err == nil && orderID != "" {
			cfg.activeOrders[priceKey] = orderID
			ordersSent++
		}
	}

	// Send ask orders (a1, a2, ...)
	for i := 1; i <= quoteLevels; i++ {
		priceKey := "a" + strconv.Itoa(i)
		volKey := "a" + strconv.Itoa(i) + "_vol"

		priceStr := quotes[priceKey]
		volStr := quotes[volKey]

		if priceStr == "" || volStr == "" {
			continue
		}

		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			continue
		}

		quantity, err := strconv.ParseFloat(volStr, 64)
		if err != nil {
			continue
		}

		if price <= 0 || quantity <= 0 {
			continue
		}

		// Skip if we already sent at this price (prevents duplicate LP orders)
		if sentAskPrices[price] {
			continue
		}
		sentAskPrices[price] = true

		orderID, err := lp.sendOrder(ticker, "S", priceKey, price, quantity)
		if err == nil && orderID != "" {
			cfg.activeOrders[priceKey] = orderID
			ordersSent++
		}
	}

	// Store initial quotes
	cfg.lastQuotes = quotes

	// Register config
	lp.configMu.Lock()
	lp.tickerConfigs[ticker] = cfg
	lp.configMu.Unlock()

	if ordersSent > 0 {
		log.Printf("Liquidity provider: Sent %d initial orders for %s", ordersSent, ticker)
	}
}

// sendInitialOrders sends initial orders at 9:16 based on current quotes
func (lp *LiquidityProviderServiceV2) sendInitialOrders() error {
	tickers, err := lp.discoverTickers()
	if err != nil {
		return err
	}

	log.Printf("Liquidity provider: Found %d tickers, sending initial orders...", len(tickers))

	if len(tickers) == 0 {
		return nil
	}

	// Pipeline all quote reads
	quoteCmds := make([]*redis.StringStringMapCmd, len(tickers))
	_, err = lp.redisClient.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, ticker := range tickers {
			quoteCmds[i] = pipe.HGetAll(ctx, ticker+"_quotes")
		}
		return nil
	})

	if err != nil {
		log.Printf("Error reading quotes in pipeline: %v", err)
		return err
	}

	// Process each ticker's quotes
	for i, ticker := range tickers {
		quotes, err := quoteCmds[i].Result()
		if err != nil {
			log.Printf("Error getting quotes for %s: %v", ticker, err)
			continue
		}

		if len(quotes) == 0 {
			continue
		}

		lp.sendInitialOrdersForTicker(ticker, quotes)
	}

	return nil
}

// cancelAllOrders cancels all active liquidity provider orders
func (lp *LiquidityProviderServiceV2) cancelAllOrders() error {
	lp.mu.Lock()
	allOrders := make(map[string]map[string]*LiquidityProviderOrderV2) // ticker -> orderID -> order info
	for ticker, orders := range lp.activeOrders {
		allOrders[ticker] = make(map[string]*LiquidityProviderOrderV2)
		for orderID, orderInfo := range orders {
			allOrders[ticker][orderID] = orderInfo
		}
	}
	lp.activeOrders = make(map[string]map[string]*LiquidityProviderOrderV2) // Clear active orders
	lp.mu.Unlock()

	for ticker, orders := range allOrders {
		for orderID := range orders {
			lp.cancelOrder(ticker, orderID)
		}
	}

	log.Println("Liquidity provider: All orders canceled")
	return nil
}

// sleepWithDone sleeps for the specified duration while allowing done channel handling
func (lp *LiquidityProviderServiceV2) sleepWithDone(duration time.Duration) bool {
	timer := time.NewTimer(duration)
	select {
	case <-timer.C:
		return true
	case <-lp.done:
		timer.Stop()
		log.Println("Liquidity provider service interrupted, exiting...")
		return false
	}
}

// Start starts the improved liquidity provider service
func (lp *LiquidityProviderServiceV2) Start() {
	log.Println("Liquidity provider service V2 starting...")

	now := time.Now().In(hcmcLoc)
	currentTime := now.Format("15:04:05")

	// If time passes 14:30, wait until 9:00 tomorrow
	if currentTime >= "14:30:00" {
		sleepTime := secondsToNextTime("09:00:05")
		log.Printf("Time passed 14:30. Waiting until 9:00 tomorrow (in %v seconds)", sleepTime)
		if !lp.sleepWithDone(time.Duration(sleepTime) * time.Second) {
			return
		}
		// After sleep, update now
		now = time.Now().In(hcmcLoc)
		currentTime = now.Format("15:04:05")
	}

	// Wait until 9:16 if before 9:16
	if currentTime < "09:16:00" {
		sleepTime := secondsToNextTime("09:16:05")
		log.Printf("Waiting until 9:16 (in %v seconds)", sleepTime)
		if !lp.sleepWithDone(time.Duration(sleepTime) * time.Second) {
			return
		}
	}

	// Send initial orders at 9:16 (if in trading window)
	if isInTradingWindow() {
		// CRITICAL: Clean up orphaned LP orders from previous instances first
		// Without this, orders from crashed/restarted LP services pile up
		log.Println("Liquidity provider: Cleaning up orphaned orders from previous instances...")
		canceledCount := lp.queueManager.CancelAllLPOrders()
		if canceledCount > 0 {
			log.Printf("Liquidity provider: Cleaned up %d orphaned orders", canceledCount)
		}

		log.Println("Liquidity provider: Sending initial orders at 9:16...")
		if err := lp.sendInitialOrders(); err != nil {
			log.Printf("Error sending initial orders: %v", err)
		}
	}

	// Launch per-ticker refresh goroutines
	lp.configMu.RLock()
	for _, cfg := range lp.tickerConfigs {
		go lp.runTickerRefreshLoop(cfg)
	}
	lp.configMu.RUnlock()

	// Set cancel timer for 14:30
	now = time.Now().In(hcmcLoc)
	today := now.Format("2006-01-02")
	cancelTime, err := time.ParseInLocation("2006-01-02 15:04:05", today+" 14:30:00", hcmcLoc)
	if err != nil {
		log.Printf("Error parsing 14:30 time: %v", err)
		cancelTime = now.Add(6 * time.Hour) // Fallback
	}
	if now.After(cancelTime) {
		cancelTime = cancelTime.Add(24 * time.Hour)
	}
	duration := cancelTime.Sub(now)
	if duration < 0 {
		duration = 24 * time.Hour // Fallback if calculation is negative
	}

	// Create timer with correct duration
	cancelTimer := time.NewTimer(duration)
	defer cancelTimer.Stop()

	// Wait for shutdown or market close
	select {
	case <-lp.done:
		log.Println("Liquidity provider service stopping...")
		lp.cancelAllOrders()
		return

	case <-cancelTimer.C:
		log.Println("Liquidity provider: 14:30 reached, canceling all orders...")
		lp.cancelAllOrders()
		// Wait for next day or shutdown
		<-lp.done
	}
}

// StartLiquidityProviderV2 starts the improved liquidity provider service in a goroutine
func StartLiquidityProviderV2(manager *OrderBookManager, queueManager *TickerQueueManager, redisClient *redis.Client, done <-chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		service := NewLiquidityProviderServiceV2(manager, queueManager, redisClient, done)
		service.Start()
	}()
}

// isInTradingWindow checks if current time is in trading window (9:15-11:30 or 13:00-14:30)
func isInTradingWindow() bool {
	now := time.Now().In(hcmcLoc)
	hour, min := now.Hour(), now.Minute()
	totalMins := hour*60 + min

	// Morning session: 9:15-11:30 (555-690 mins)
	if totalMins >= 555 && totalMins <= 690 {
		return true
	}

	// Afternoon session: 13:00-14:30 (780-870 mins)
	if totalMins >= 780 && totalMins <= 870 {
		return true
	}

	return false
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

// getQuoteLevels returns the number of quote levels based on ticker type
// Stocks: 3 levels (b1-b3, a1-a3)
// Futures: 10 levels (b1-b10, a1-a10)
func getQuoteLevels(ticker string) int {
	if isFuturesOrder(ticker) {
		return 10
	}
	return 3
}
