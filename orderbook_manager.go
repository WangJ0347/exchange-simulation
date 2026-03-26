package exchange

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"exchange/pb"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"
)

// OrderRequest represents the request from order-processor
type OrderRequest struct {
	OrderID      string  `json:"orderID,omitempty"`      // Required for modify/cancel
	OrderGroupID string  `json:"orderGroupID,omitempty"` // Required for modify/cancel
	Ticker       string  `json:"ticker"`
	BS           string  `json:"bs"` // "B" or "S"
	Price        float64 `json:"price"`
	Quantity     float64 `json:"quantity"`
	Remark       string  `json:"remark,omitempty"`
}

// OrderResult represents the response to order-processor
type OrderResult struct {
	OrderID      string  `json:"orderID"`
	OrderGroupID string  `json:"orderGroupID"`
	Ticker       string  `json:"ticker"`
	BS           string  `json:"bs"`
	Price        float64 `json:"price"`
	Qty          float64 `json:"qty"`
	PendingQty   float64 `json:"pendingQty"`
	FilledQty    float64 `json:"filledQty"`
	AvgPrice     float64 `json:"avgPrice"`
	Status       string  `json:"status"`
	Remark       string  `json:"remark,omitempty"`
	TradeTime    string  `json:"tradeTime"`
	ErrorMessage string  `json:"errorMessage"`
}

// OrderMetadata stores additional metadata for orders in the orderbook
type OrderMetadata struct {
	Remark       string
	OrderGroupID string
	Ticker       string
	IsLP         bool // Cached: isLiquidityProviderOrder(remark) - performance optimization
	IsFutures    bool // Cached: isFuturesOrder(ticker) - performance optimization
}

// OrderBookManager manages multiple orderbooks (one per ticker)
type OrderBookManager struct {
	orderbooks   map[string]*OrderBook          // ticker -> OrderBook
	orderMeta    map[string]*OrderMetadata      // orderID -> metadata (remark, orderGroupID)
	tickerOrders map[string]map[string]struct{} // ticker -> set of orderIDs (reverse index for fast market close)
	redisClient  *redis.Client
	mu           sync.RWMutex // Protects orderbooks, orderMeta, and tickerOrders maps
	// Removed: tickerLocks - no longer needed with FIFO queue
}

// NewOrderBookManager creates a new OrderBookManager
func NewOrderBookManager(redisClient *redis.Client) *OrderBookManager {
	return &OrderBookManager{
		orderbooks:   make(map[string]*OrderBook),
		orderMeta:    make(map[string]*OrderMetadata),
		tickerOrders: make(map[string]map[string]struct{}),
		redisClient:  redisClient,
	}
}

// getOrderbook gets or creates an orderbook for a ticker
// Queue ensures sequential processing, so no ticker lock needed
func (m *OrderBookManager) getOrderbook(ticker string) *OrderBook {
	m.mu.RLock()
	ob, exists := m.orderbooks[ticker]
	m.mu.RUnlock()

	if exists {
		return ob
	}

	// Create new orderbook (need write lock)
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if ob, exists := m.orderbooks[ticker]; exists {
		return ob
	}
	ob = NewOrderBook()
	m.orderbooks[ticker] = ob
	return ob
}

// GetOrderState retrieves order state from order-manager's Redis (legacy, for compatibility)
// DEPRECATED: Use GetExchangeOrderState instead
func (m *OrderBookManager) GetOrderState(ticker, remark, orderGroupID string) (*OrderState, error) {
	key := ticker + "_orders"
	internalOrderID := ExtractInternalOrderID(remark, orderGroupID)
	orderJSON, err := m.redisClient.HGet(ctx, key, internalOrderID).Result()
	if err != nil {
		return nil, err
	}
	var state OrderState
	err = json.Unmarshal([]byte(orderJSON), &state)
	return &state, err
}

// GetExchangeOrderState retrieves order state from exchange's own Redis cache using orderID
func (m *OrderBookManager) GetExchangeOrderState(ticker, orderID string) (*OrderState, error) {
	key := ticker + "_exchangeOrders"
	orderJSON, err := m.redisClient.HGet(ctx, key, orderID).Result()
	if err != nil {
		return nil, err
	}
	var state OrderState
	err = json.Unmarshal([]byte(orderJSON), &state)
	return &state, err
}

// UpdateExchangeOrderState updates order state in exchange's own Redis cache
func (m *OrderBookManager) UpdateExchangeOrderState(state OrderState) error {
	key := state.Ticker + "_exchangeOrders"
	orderJSON, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return m.redisClient.HSet(ctx, key, state.OrderID, string(orderJSON)).Err()
}

// StreamOrderUpdate publishes order update to eqtOrder or fnoOrder stream
func (m *OrderBookManager) StreamOrderUpdate(state OrderState) error {
	// Skip streaming for liquidity provider orders (only update in-memory)
	if isLiquidityProviderOrder(state.Remark) {
		return nil
	}

	streamName := "eqtOrder"
	if isFuturesOrder(state.Ticker) {
		streamName = "fnoOrder"
	}

	payload := &pb.OrderResult{
		OrderID:      state.OrderID,
		OrderGroupID: state.OrderGroupID,
		Ticker:       state.Ticker,
		Bs:           state.BS,
		Price:        state.Price,
		Qty:          state.Qty,
		PendingQty:   state.PendingQty,
		FilledQty:    state.FilledQty,
		AvgPrice:     state.AvgPrice,
		Status:       state.Status,
		Remark:       state.Remark,
		TradeTime:    state.TradeTime,
		ErrorMessage: state.ErrorMessage,
	}
	protoBytes, err := proto.Marshal(payload)
	if err != nil {
		return err
	}

	_, err = m.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{
			"order": string(protoBytes),
		},
	}).Result()
	return err
}

// streamMatchedOrders streams updates for matched opposite side orders
// done: fully filled opposite side orders (may include synthetic incoming order at end)
// partial: partially filled order (could be incoming order or opposite side order)
// partialQty: quantity matched from the partial order
// incomingOrderID: ID of the incoming order (to exclude synthetic order from done)
func (m *OrderBookManager) streamMatchedOrders(done []*Order, partial *Order, partialQty decimal.Decimal, incomingOrderID string) {
	// Collect all orders that need state retrieval and streaming
	type orderToProcess struct {
		order     *Order
		meta      *OrderMetadata
		fillQty   float64
		fillPrice float64
		isPartial bool
	}

	var ordersToProcess []orderToProcess

	// Collect done orders (fully filled opposite side orders)
	m.mu.RLock()
	for _, matchedOrder := range done {
		// Skip synthetic order (incoming order fully matched)
		if matchedOrder.ID() == incomingOrderID {
			continue
		}

		meta, exists := m.orderMeta[matchedOrder.ID()]
		if !exists {
			continue
		}

		// Skip streaming for liquidity provider orders (only update in-memory)
		if isLiquidityProviderOrder(meta.Remark) {
			continue
		}

		fillQty, _ := matchedOrder.Quantity().Float64()
		fillPrice, _ := matchedOrder.Price().Float64()

		ordersToProcess = append(ordersToProcess, orderToProcess{
			order:     matchedOrder,
			meta:      meta,
			fillQty:   fillQty,
			fillPrice: fillPrice,
			isPartial: false,
		})
	}

	// Handle partial matched order (if partial is an opposite side order)
	if partial != nil && partial.ID() != incomingOrderID {
		meta, exists := m.orderMeta[partial.ID()]
		if exists && !isLiquidityProviderOrder(meta.Remark) {
			fillQty, _ := partialQty.Float64()
			fillPrice, _ := partial.Price().Float64()

			ordersToProcess = append(ordersToProcess, orderToProcess{
				order:     partial,
				meta:      meta,
				fillQty:   fillQty,
				fillPrice: fillPrice,
				isPartial: true,
			})
		}
	}
	m.mu.RUnlock()

	if len(ordersToProcess) == 0 {
		return
	}

	// Pipeline all GetExchangeOrderState calls (using orderID directly)
	stateCmds := make([]*redis.StringCmd, len(ordersToProcess))
	_, err := m.redisClient.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, orderInfo := range ordersToProcess {
			key := orderInfo.meta.Ticker + "_exchangeOrders"
			stateCmds[i] = pipe.HGet(ctx, key, orderInfo.order.ID())
		}
		return nil
	})

	if err != nil {
		log.Printf("Error executing pipeline for order states: %v", err)
		// Fallback to individual calls
		for _, orderInfo := range ordersToProcess {
			state, err := m.GetExchangeOrderState(orderInfo.meta.Ticker, orderInfo.order.ID())
			if err != nil {
				log.Printf("Error getting order state for matched order %s: %v", orderInfo.order.ID(), err)
				continue
			}

			// Calculate and update state
			newFilledQty := state.FilledQty + orderInfo.fillQty
			newAvgPrice := CalculateAvgPrice(state.AvgPrice, state.FilledQty, orderInfo.fillPrice, orderInfo.fillQty)
			state.FilledQty = newFilledQty
			state.AvgPrice = newAvgPrice
			state.PendingQty = state.Qty - newFilledQty

			// Validate state
			if state.PendingQty < 0 {
				state.PendingQty = 0
				state.FilledQty = state.Qty
			}
			if state.FilledQty > state.Qty {
				state.FilledQty = state.Qty
				state.PendingQty = 0
			}

			state.UpdateStatus()

			// Update exchange cache
			if err := m.UpdateExchangeOrderState(*state); err != nil {
				log.Printf("Error updating exchange order state: %v", err)
			}

			// Stream update
			m.StreamOrderUpdate(*state)
		}
		return
	}

	// Process results and prepare states for streaming
	var statesToStream []OrderState
	for i, orderInfo := range ordersToProcess {
		orderJSON, err := stateCmds[i].Result()
		if err != nil {
			log.Printf("Error getting order state for matched order %s: %v", orderInfo.order.ID(), err)
			continue
		}

		var state OrderState
		if err := json.Unmarshal([]byte(orderJSON), &state); err != nil {
			log.Printf("Error unmarshaling order state for %s: %v", orderInfo.order.ID(), err)
			continue
		}

		// Calculate and update state
		newFilledQty := state.FilledQty + orderInfo.fillQty
		newAvgPrice := CalculateAvgPrice(state.AvgPrice, state.FilledQty, orderInfo.fillPrice, orderInfo.fillQty)
		state.FilledQty = newFilledQty
		state.AvgPrice = newAvgPrice
		state.PendingQty = state.Qty - newFilledQty

		// Validate state
		if state.PendingQty < 0 {
			state.PendingQty = 0
			state.FilledQty = state.Qty
		}
		if state.FilledQty > state.Qty {
			state.FilledQty = state.Qty
			state.PendingQty = 0
		}

		state.UpdateStatus()

		statesToStream = append(statesToStream, state)
	}

	// Update exchange cache and stream updates
	if len(statesToStream) > 0 {
		// Pipeline exchange cache updates
		_, err = m.redisClient.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, state := range statesToStream {
				key := state.Ticker + "_exchangeOrders"
				orderJSON, _ := json.Marshal(state)
				pipe.HSet(ctx, key, state.OrderID, string(orderJSON))
			}
			return nil
		})

		if err != nil {
			log.Printf("Error executing pipeline for exchange cache updates: %v", err)
			// Fallback to individual calls
			for _, state := range statesToStream {
				m.UpdateExchangeOrderState(state)
			}
		}

		// Pipeline stream updates (protobuf; order-manager expects proto)
		_, err = m.redisClient.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, state := range statesToStream {
				streamName := "eqtOrder"
				if isFuturesOrder(state.Ticker) {
					streamName = "fnoOrder"
				}

				payload := &pb.OrderResult{
					OrderID:      state.OrderID,
					OrderGroupID: state.OrderGroupID,
					Ticker:       state.Ticker,
					Bs:           state.BS,
					Price:        state.Price,
					Qty:          state.Qty,
					PendingQty:   state.PendingQty,
					FilledQty:    state.FilledQty,
					AvgPrice:     state.AvgPrice,
					Status:       state.Status,
					Remark:       state.Remark,
					TradeTime:    state.TradeTime,
					ErrorMessage: state.ErrorMessage,
				}
				protoBytes, protoErr := proto.Marshal(payload)
				if protoErr != nil {
					log.Printf("Error marshaling order state for streaming: %v", protoErr)
					continue
				}

				pipe.XAdd(ctx, &redis.XAddArgs{
					Stream: streamName,
					Values: map[string]interface{}{
						"order": string(protoBytes),
					},
				})
			}
			return nil
		})

		if err != nil {
			log.Printf("Error executing pipeline for stream updates: %v", err)
			// Fallback to individual calls
			for _, state := range statesToStream {
				m.StreamOrderUpdate(state)
			}
		}
	}
}

// ProcessOrder processes Send/Modify/Cancel orders
// Queue ensures sequential processing per ticker, so no ticker lock needed
func (m *OrderBookManager) ProcessOrder(ticker, action string, req OrderRequest) OrderResult {
	ob := m.getOrderbook(ticker)

	switch action {
	case "Send":
		return m.processSend(ob, ticker, req)
	case "Modify":
		return m.processModify(ob, ticker, req)
	case "Cancel":
		return m.processCancel(ob, ticker, req)
	}

	return OrderResult{
		Ticker:       ticker,
		OrderID:      req.OrderID,
		OrderGroupID: req.OrderGroupID,
		BS:           req.BS,
		Price:        req.Price,
		Qty:          req.Quantity,
		PendingQty:   0,
		FilledQty:    0,
		AvgPrice:     0,
		Remark:       req.Remark,
		TradeTime:    getCurrentTradeTime(),
		ErrorMessage: "Unknown action",
		Status:       "ERR",
	}
}

// processSend handles Send order action
func (m *OrderBookManager) processSend(ob *OrderBook, ticker string, req OrderRequest) OrderResult {
	// Generate OrderID and OrderGroupID
	orderID := uuid.New().String()
	orderGroupID := uuid.New().String()

	// Set TradeTime
	tradeTime := getCurrentTradeTime()

	// Convert to orderbook.Order format
	side := Buy
	if req.BS == "S" {
		side = Sell
	}

	price := decimal.NewFromFloat(req.Price)
	quantity := decimal.NewFromFloat(req.Quantity)

	// Process limit order
	done, partial, partialQty, err := ob.ProcessLimitOrder(side, orderID, quantity, price)
	if err != nil {
		return OrderResult{
			OrderID:      orderID,
			OrderGroupID: orderGroupID,
			Ticker:       ticker,
			BS:           req.BS,
			Price:        req.Price,
			Qty:          req.Quantity,
			Status:       "ERR",
			TradeTime:    tradeTime,
			ErrorMessage: err.Error(),
		}
	}

	// Store metadata (protected by ticker lock, but orderMeta map needs global lock)
	m.mu.Lock()
	m.orderMeta[orderID] = &OrderMetadata{
		Remark:       req.Remark,
		OrderGroupID: orderGroupID,
		Ticker:       ticker,
		IsLP:         isLiquidityProviderOrder(req.Remark),
		IsFutures:    isFuturesOrder(ticker),
	}
	// Add to ticker reverse index for fast market close
	if m.tickerOrders[ticker] == nil {
		m.tickerOrders[ticker] = make(map[string]struct{})
	}
	m.tickerOrders[ticker][orderID] = struct{}{}
	m.mu.Unlock()

	// Calculate FilledQty and AvgPrice
	// ProcessLimitOrder behavior:
	// - If fully matched, it appends a synthetic order (with incoming orderID) to `done`
	//   whose price is already the weighted average of all fills.
	// - If partially matched, `done` has only opposite-side orders, and `partial` is the
	//   remaining piece of the incoming order with `partialQty` already matched.

	var avgPrice float64
	var filledQty float64

	// Fully matched: use the synthetic order's price/quantity directly
	if len(done) > 0 && done[len(done)-1].ID() == orderID {
		synth := done[len(done)-1]
		filledQty, _ = synth.Quantity().Float64()
		avgPrice, _ = synth.Price().Float64()
	} else if partial != nil && partial.ID() == orderID {
		// Partially matched incoming order: partialQty is the filled amount
		// (partialQty = original quantity - remaining quantity)
		// The done orders are the opposite-side orders consumed, their quantities sum to partialQty
		// So we use partialQty directly to avoid double-counting
		filledQty, _ = partialQty.Float64()

		// Calculate avgPrice from the opposite-side orders that were consumed
		if len(done) > 0 {
			var totalValue decimal.Decimal
			for _, matchedOrder := range done {
				totalValue = totalValue.Add(matchedOrder.Price().Mul(matchedOrder.Quantity()))
			}
			avgPriceValue := totalValue.Div(partialQty)
			avgPrice, _ = avgPriceValue.Float64()
		} else {
			// No done orders means no matching happened (edge case)
			avgPrice = req.Price
		}
	} else {
		// No matching at all - order goes directly to orderbook
		filledQty = 0
		avgPrice = 0
	}

	pendingQty := req.Quantity - filledQty

	// Validate: prevent over-fill
	if pendingQty < 0 {
		log.Printf("WARNING: Over-fill detected in processSend for order %s: attempted fill %f > qty %f, clamping", orderID, filledQty, req.Quantity)
		pendingQty = 0
		filledQty = req.Quantity
	}

	// Determine status
	status := "PEX"
	if pendingQty == 0 && filledQty > 0 {
		status = "FLL"
	}

	// Stream updates for matched opposite side orders
	m.streamMatchedOrders(done, partial, partialQty, orderID)

	// Create order state
	state := OrderState{
		OrderID:      orderID,
		OrderGroupID: orderGroupID,
		Ticker:       ticker,
		BS:           req.BS,
		Price:        req.Price,
		Qty:          req.Quantity,
		PendingQty:   pendingQty,
		FilledQty:    filledQty,
		AvgPrice:     avgPrice,
		Status:       status,
		Remark:       req.Remark,
		TradeTime:    tradeTime,
	}

	// Update exchange's Redis cache (only for non-LP orders)
	// Note: Queue manager will also update, but we do it here for immediate consistency
	if !isLiquidityProviderOrder(req.Remark) {
		if err := m.UpdateExchangeOrderState(state); err != nil {
			log.Printf("Error updating exchange order state: %v", err)
		}
	}

	// Stream update for incoming order
	m.StreamOrderUpdate(state)

	// Return result
	return OrderResult{
		OrderID:      orderID,
		OrderGroupID: orderGroupID,
		Ticker:       ticker,
		BS:           req.BS,
		Price:        req.Price,
		Qty:          req.Quantity,
		PendingQty:   pendingQty,
		FilledQty:    filledQty,
		AvgPrice:     avgPrice,
		Status:       status,
		Remark:       req.Remark,
		TradeTime:    tradeTime,
		ErrorMessage: "Success",
	}
}

// processModify handles Modify order action
func (m *OrderBookManager) processModify(ob *OrderBook, ticker string, req OrderRequest) OrderResult {
	// Check if this is a liquidity provider order
	isLP := isLiquidityProviderOrder(req.Remark)

	var state *OrderState
	var err error

	if isLP {
		// For LP orders, get state from orderMeta and orderbook (not Redis)
		// Simplified: no need to track filledQty/pendingQty for LP orders
		m.mu.RLock()
		meta, exists := m.orderMeta[req.OrderID]
		m.mu.RUnlock()

		if !exists {
			return OrderResult{
				Ticker:       ticker,
				OrderID:      req.OrderID,
				OrderGroupID: req.OrderGroupID,
				BS:           req.BS,
				Price:        req.Price,
				Qty:          req.Quantity,
				Status:       "ERR",
				ErrorMessage: "Order not found",
			}
		}

		// Verify orderGroupID matches
		if meta.OrderGroupID != req.OrderGroupID {
			return OrderResult{
				Ticker:       ticker,
				OrderID:      req.OrderID,
				OrderGroupID: req.OrderGroupID,
				BS:           req.BS,
				Price:        req.Price,
				Qty:          req.Quantity,
				Status:       "ERR",
				ErrorMessage: "OrderGroupID mismatch",
			}
		}

		// Get order from orderbook to verify it exists
		orderInBook := ob.Order(req.OrderID)
		if orderInBook == nil {
			// Order not in book (fully filled or canceled)
			return OrderResult{
				Ticker:       ticker,
				OrderID:      req.OrderID,
				OrderGroupID: req.OrderGroupID,
				BS:           req.BS,
				Price:        req.Price,
				Qty:          req.Quantity,
				Status:       "ERR",
				ErrorMessage: "Order not found in orderbook",
			}
		}

		// Build minimal state for LP orders - no need to track filledQty/pendingQty
		price, _ := orderInBook.Price().Float64()
		pendingQty, _ := orderInBook.Quantity().Float64()

		// Determine BS from order side
		bs := "B"
		if orderInBook.Side() == Sell {
			bs = "S"
		}

		state = &OrderState{
			OrderID:      req.OrderID,
			OrderGroupID: meta.OrderGroupID,
			Ticker:       ticker,
			BS:           bs,
			Price:        price,
			Qty:          req.Quantity, // Use new quantity as baseline
			PendingQty:   pendingQty,   // Current pending from orderbook
			FilledQty:    0,            // Don't track for LP orders
			AvgPrice:     0,            // LP orders don't track avg price
			Status:       "PEX",
			Remark:       meta.Remark,
			TradeTime:    getCurrentTradeTime(),
		}
	} else {
		// For regular orders, get state from exchange's own Redis cache
		state, err = m.GetExchangeOrderState(ticker, req.OrderID)
		if err != nil {
			return OrderResult{
				Ticker:       ticker,
				OrderID:      req.OrderID,
				OrderGroupID: req.OrderGroupID,
				BS:           req.BS,
				Price:        req.Price,
				Qty:          req.Quantity,
				Status:       "ERR",
				ErrorMessage: "Order not found in exchange cache",
			}
		}

		// Verify orderGroupID matches
		if state.OrderGroupID != req.OrderGroupID {
			return OrderResult{
				Ticker:       ticker,
				OrderID:      req.OrderID,
				OrderGroupID: req.OrderGroupID,
				BS:           req.BS,
				Price:        req.Price,
				Qty:          req.Quantity,
				Status:       "ERR",
				TradeTime:    state.TradeTime,
				ErrorMessage: "OrderGroupID mismatch",
			}
		}

		// Check if in terminal status
		if state.IsTerminalStatus() {
			return OrderResult{} // Ignore
		}
	}

	// Get latest FilledQty from orderbook (re-fetch to ensure current value)
	orderID := state.OrderID
	orderInBook := ob.Order(orderID)
	currentFilledQty := state.FilledQty
	if orderInBook != nil && !isLP {
		// Order still in book, get current filled qty from state
		// (we need to track this separately as orderbook doesn't track filled qty)
		// For now, use the state's FilledQty which should be up-to-date
	}

	// Priority logic
	priceChanged := req.Price != state.Price
	qtyIncreased := req.Quantity > state.Qty
	isLiquidityProvider := isLP

	side := Buy
	if state.BS == "S" {
		side = Sell
	}

	var newFilledQty float64
	var newAvgPrice float64
	var newPendingQty float64

	// Liquidity provider orders always lose priority (cancel and resend)
	if priceChanged || qtyIncreased || isLiquidityProvider {
		// Lose priority - remove and re-add
		ob.CancelOrder(orderID)
		m.mu.Lock()
		delete(m.orderMeta, orderID)
		// Remove from ticker index
		if m.tickerOrders[ticker] != nil {
			delete(m.tickerOrders[ticker], orderID)
		}
		m.mu.Unlock()

		// Calculate new pending qty
		// For LP orders, simplified: treat as if nothing filled (currentFilledQty = 0)
		if isLiquidityProvider {
			currentFilledQty = 0 // Simplified: don't track fills for LP orders
		}
		newPendingQty = req.Quantity - currentFilledQty
		if newPendingQty < 0 {
			newPendingQty = 0
		}

		// Re-add to orderbook
		price := decimal.NewFromFloat(req.Price)
		pendingQtyDecimal := decimal.NewFromFloat(newPendingQty)

		done, partial, partialQty, err := ob.ProcessLimitOrder(side, orderID, pendingQtyDecimal, price)
		if err != nil {
			return OrderResult{
				OrderID:      orderID,
				OrderGroupID: state.OrderGroupID,
				Ticker:       ticker,
				BS:           req.BS,
				Price:        req.Price,
				Qty:          req.Quantity,
				Status:       "ERR",
				ErrorMessage: err.Error(),
			}
		}

		// Store metadata
		m.mu.Lock()
		m.orderMeta[orderID] = &OrderMetadata{
			Remark:       req.Remark,
			OrderGroupID: state.OrderGroupID,
			Ticker:       ticker,
			IsLP:         isLiquidityProviderOrder(req.Remark),
			IsFutures:    isFuturesOrder(ticker),
		}
		// Re-add to ticker index
		if m.tickerOrders[ticker] == nil {
			m.tickerOrders[ticker] = make(map[string]struct{})
		}
		m.tickerOrders[ticker][orderID] = struct{}{}
		m.mu.Unlock()

		// Calculate new fills if any
		// ProcessLimitOrder returns:
		// - done: opposite side orders matched, plus synthetic order at end if fully matched
		// - partial: incoming order if partially matched, or opposite side order if fully matched with partial last match
		// - partialQty: quantity matched from the partial order

		newFillQty := 0.0
		newFillValue := 0.0

		// Check if fully matched: last order in done should be the synthetic incoming order
		if len(done) > 0 && done[len(done)-1].ID() == orderID {
			// Fully matched: use the synthetic order's price and quantity
			syntheticOrder := done[len(done)-1]
			newFillQty, _ = syntheticOrder.Quantity().Float64()
			avgPrice, _ := syntheticOrder.Price().Float64()
			newFillValue = avgPrice * newFillQty
		} else if partial != nil && partial.ID() == orderID {
			// Partially matched incoming order: partialQty is the new fill amount
			// Use partialQty directly to avoid double-counting
			newFillQty, _ = partialQty.Float64()

			// Calculate avgPrice from the opposite-side orders consumed
			if len(done) > 0 {
				for _, matchedOrder := range done {
					price, _ := matchedOrder.Price().Float64()
					qty, _ := matchedOrder.Quantity().Float64()
					newFillValue += price * qty
				}
			} else {
				// No done orders (edge case)
				newFillValue = req.Price * newFillQty
			}
		} else {
			// No matching happened
			newFillQty = 0
			newFillValue = 0
		}

		// Recalculate AvgPrice using weighted average with existing fills
		if newFillQty > 0 {
			if isLiquidityProvider {
				// For LP orders, don't track avg price, just update filled quantity
				newFilledQty = currentFilledQty + newFillQty
				newAvgPrice = 0
			} else {
				newFilledQty = currentFilledQty + newFillQty
				newAvgPrice = CalculateAvgPrice(state.AvgPrice, currentFilledQty, newFillValue/newFillQty, newFillQty)
			}
		} else {
			newFilledQty = currentFilledQty
			if isLiquidityProvider {
				newAvgPrice = 0
			} else {
				newAvgPrice = state.AvgPrice
			}
		}

		newPendingQty = req.Quantity - newFilledQty

		// Validate: prevent over-fill
		if newPendingQty < 0 {
			log.Printf("WARNING: Over-fill detected in processModify for order %s: attempted fill %f > qty %f, clamping", orderID, newFilledQty, req.Quantity)
			newPendingQty = 0
			newFilledQty = req.Quantity
		}

		// Stream updates for matched opposite side orders
		m.streamMatchedOrders(done, partial, partialQty, orderID)
	} else {
		// Keep priority - update in place
		// Get the list.Element for this order (preserves position in FIFO queue)
		element, exists := ob.orders[orderID]
		if !exists {
			return OrderResult{
				OrderID:      orderID,
				OrderGroupID: state.OrderGroupID,
				BS:           req.BS,
				Price:        req.Price,
				Qty:          req.Quantity,
				Ticker:       ticker,
				Status:       "ERR",
				ErrorMessage: "Order not found in orderbook",
			}
		}

		// Get current order from element
		currentOrder := element.Value.(*Order)

		// Calculate new pending quantity
		newPendingQty = req.Quantity - currentFilledQty
		if newPendingQty < 0 {
			// Invalid modification: new quantity is less than already filled quantity
			return OrderResult{
				OrderID:      orderID,
				OrderGroupID: state.OrderGroupID,
				BS:           req.BS,
				Price:        req.Price,
				Qty:          req.Quantity,
				Ticker:       ticker,
				Status:       "ERR",
				ErrorMessage: "New quantity cannot be less than already filled quantity",
			}
		}

		// If new pending quantity is 0, order is fully filled - remove it
		if newPendingQty == 0 {
			ob.CancelOrder(orderID)
			m.mu.Lock()
			delete(m.orderMeta, orderID)
			// Remove from ticker index
			if m.tickerOrders[ticker] != nil {
				delete(m.tickerOrders[ticker], orderID)
			}
			m.mu.Unlock()

			newFilledQty = currentFilledQty
			newAvgPrice = state.AvgPrice
		} else {
			// Update order in place (preserves FIFO position)
			// Get the OrderSide (bids or asks)
			orderSide := ob.GetOrderSide(side)

			// Get the OrderQueue for this price level
			price := decimal.NewFromFloat(state.Price)
			orderQueue := orderSide.GetOrderQueue(price)
			if orderQueue == nil {
				return OrderResult{
					OrderID:      orderID,
					OrderGroupID: state.OrderGroupID,
					Ticker:       ticker,
					BS:           req.BS,
					Price:        req.Price,
					Qty:          req.Quantity,
					Status:       "ERR",
					ErrorMessage: "OrderQueue not found for price",
				}
			}

			// Create updated order with new quantity (keep same timestamp to preserve priority)
			updatedOrder := NewOrder(orderID, side, decimal.NewFromFloat(newPendingQty), price, currentOrder.Time())

			// Update in place using OrderQueue.Update()
			// This updates the order quantity while keeping the same list.Element position
			orderQueue.Update(element, updatedOrder)

			// No new matching happens - order is already in book, just quantity changed
			newFilledQty = currentFilledQty
			newAvgPrice = state.AvgPrice
		}
	}

	// Update state with new values
	state.Qty = req.Quantity
	state.FilledQty = newFilledQty
	state.AvgPrice = newAvgPrice
	state.PendingQty = newPendingQty
	state.Price = req.Price
	state.Remark = req.Remark
	state.TradeTime = getCurrentTradeTime() // Update trade time to reflect modification
	state.UpdateStatus()

	// Update exchange's Redis cache (only for non-LP orders)
	if !isLiquidityProviderOrder(req.Remark) {
		if err := m.UpdateExchangeOrderState(*state); err != nil {
			log.Printf("Error updating exchange order state: %v", err)
		}
	}

	// Stream updated order state (will skip for LP orders)
	m.StreamOrderUpdate(*state)

	return OrderResult{
		OrderID:      orderID,
		OrderGroupID: state.OrderGroupID,
		Ticker:       ticker,
		BS:           state.BS,
		Price:        req.Price,
		Qty:          req.Quantity,
		PendingQty:   newPendingQty,
		FilledQty:    newFilledQty,
		AvgPrice:     newAvgPrice,
		Status:       state.Status,
		Remark:       req.Remark,
		TradeTime:    state.TradeTime,
		ErrorMessage: "Success",
	}
}

// processCancel handles Cancel order action
func (m *OrderBookManager) processCancel(ob *OrderBook, ticker string, req OrderRequest) OrderResult {
	// Handle liquidity provider orders without Redis (they are not stored in Redis)
	if isLiquidityProviderOrder(req.Remark) {
		m.mu.RLock()
		meta, exists := m.orderMeta[req.OrderID]
		m.mu.RUnlock()

		if !exists {
			return OrderResult{
				Ticker:       ticker,
				OrderID:      req.OrderID,
				OrderGroupID: req.OrderGroupID,
				BS:           req.BS,
				Price:        req.Price,
				Qty:          req.Quantity,
				Status:       "ERR",
				ErrorMessage: "Order not found (LP)",
			}
		}

		// Verify orderGroupID matches
		if meta.OrderGroupID != req.OrderGroupID {
			return OrderResult{
				Ticker:       ticker,
				OrderID:      req.OrderID,
				OrderGroupID: req.OrderGroupID,
				BS:           req.BS,
				Price:        req.Price,
				Qty:          req.Quantity,
				Status:       "ERR",
				ErrorMessage: "OrderGroupID mismatch (LP)",
			}
		}

		// Get order from orderbook
		orderInBook := ob.Order(req.OrderID)
		if orderInBook == nil {
			// Treat as already removed; clean up metadata and consider cancel successful
			m.mu.Lock()
			delete(m.orderMeta, req.OrderID)
			m.mu.Unlock()

			return OrderResult{
				OrderID:      req.OrderID,
				OrderGroupID: req.OrderGroupID,
				Ticker:       ticker,
				BS:           req.BS,
				Price:        req.Price,
				Qty:          req.Quantity,
				PendingQty:   0,
				FilledQty:    0,
				AvgPrice:     0,
				Status:       "CAN",
				Remark:       meta.Remark,
				TradeTime:    getCurrentTradeTime(),
				ErrorMessage: "Success (already removed)",
			}
		}

		// Cancel from orderbook and remove metadata
		ob.CancelOrder(req.OrderID)
		m.mu.Lock()
		delete(m.orderMeta, req.OrderID)
		// Remove from ticker index
		if m.tickerOrders[ticker] != nil {
			delete(m.tickerOrders[ticker], req.OrderID)
		}
		m.mu.Unlock()

		price, _ := orderInBook.Price().Float64()
		qty, _ := orderInBook.Quantity().Float64()
		bs := "B"
		if orderInBook.Side() == Sell {
			bs = "S"
		}

		// For LP orders we skip streaming because they are not stored in Redis
		return OrderResult{
			OrderID:      req.OrderID,
			OrderGroupID: req.OrderGroupID,
			Ticker:       ticker,
			BS:           bs,
			Price:        price,
			Qty:          qty,
			PendingQty:   0,
			FilledQty:    0,
			AvgPrice:     0,
			Status:       "CAN",
			Remark:       meta.Remark,
			TradeTime:    getCurrentTradeTime(),
			ErrorMessage: "Success",
		}
	}

	// Get existing order state from exchange's own Redis cache
	state, err := m.GetExchangeOrderState(ticker, req.OrderID)
	if err != nil {
		return OrderResult{
			Ticker:       ticker,
			OrderID:      req.OrderID,
			OrderGroupID: req.OrderGroupID,
			BS:           req.BS,
			Price:        req.Price,
			Qty:          req.Quantity,
			Status:       "ERR",
			ErrorMessage: "Order not found in exchange cache",
		}
	}

	// Verify orderGroupID matches
	if state.OrderGroupID != req.OrderGroupID {
		return OrderResult{
			Ticker:       ticker,
			OrderID:      req.OrderID,
			OrderGroupID: req.OrderGroupID,
			BS:           req.BS,
			Price:        req.Price,
			Qty:          req.Quantity,
			Status:       "ERR",
			ErrorMessage: "OrderGroupID mismatch",
		}
	}

	// Check if in terminal status
	if state.IsTerminalStatus() {
		return OrderResult{} // Ignore
	}

	// Remove from orderbook
	orderID := state.OrderID
	ob.CancelOrder(orderID)
	m.mu.Lock()
	delete(m.orderMeta, orderID)
	// Remove from ticker index
	if m.tickerOrders[ticker] != nil {
		delete(m.tickerOrders[ticker], orderID)
	}
	m.mu.Unlock()

	// Determine status
	status := "CAN"
	if state.FilledQty > 0 {
		status = "FLL"
	}

	state.Status = status
	state.PendingQty = 0

	// Update exchange's Redis cache
	if err := m.UpdateExchangeOrderState(*state); err != nil {
		log.Printf("Error updating exchange order state: %v", err)
	}

	// Stream update
	m.StreamOrderUpdate(*state)

	return OrderResult{
		OrderID:      orderID,
		OrderGroupID: state.OrderGroupID,
		Ticker:       ticker,
		BS:           state.BS,
		Price:        state.Price,
		Qty:          state.Qty,
		PendingQty:   0,
		FilledQty:    state.FilledQty,
		AvgPrice:     state.AvgPrice,
		Status:       status,
		Remark:       state.Remark,
		TradeTime:    state.TradeTime,
		ErrorMessage: "Success",
	}
}

// MatchTradeEvent handles tradeEvent matching
// Queue ensures sequential processing per ticker, so no ticker lock needed
func (m *OrderBookManager) MatchTradeEvent(ticker, priceStr, volumeStr string, side Side) error {
	// Parse price and volume
	price, err := decimal.NewFromString(priceStr)
	if err != nil {
		return err
	}

	volume, err := decimal.NewFromString(volumeStr)
	if err != nil {
		return err
	}

	// Get orderbook
	ob := m.getOrderbook(ticker)

	// Treat trade event as a limit order with FOK behavior (cancel even if partially filled)
	// Generate a temporary order ID for the trade event order
	tempOrderID := "trade_event_" + uuid.New().String()

	// Process as limit order with the trade event price
	done, partial, partialQty, err := ob.ProcessLimitOrder(side, tempOrderID, volume, price)
	if err != nil {
		return err
	}

	// FOK behavior: if trade event order is not fully matched, it disappears
	// ProcessLimitOrder appends a synthetic order to the end of `done` if fully matched
	// If not fully matched, the order may have been added to the orderbook - we need to cancel it
	tradeEventFullyMatched := len(done) > 0 && done[len(done)-1].ID() == tempOrderID

	// If not fully matched, cancel the trade event order from the orderbook (FOK behavior)
	if !tradeEventFullyMatched {
		// Check if order exists in orderbook (it will be there if partially matched or not matched)
		if ob.Order(tempOrderID) != nil {
			ob.CancelOrder(tempOrderID)
		}
		// Note: tempOrderID is not in orderMeta, so no need to clean that up
	}

	// We still need to update and stream opposite-side orders that were matched
	// (even if the trade event order itself disappears)

	// Update states for matched orders (only opposite-side orders, not the trade event order itself)
	for _, order := range done {
		// Skip the trade event order itself (synthetic order at end if fully matched)
		if order.ID() == tempOrderID {
			continue
		}
		m.mu.RLock()
		meta, exists := m.orderMeta[order.ID()]
		m.mu.RUnlock()
		if !exists {
			continue
		}

		// Skip LP orders - they're not stored in Redis
		if isLiquidityProviderOrder(meta.Remark) {
			continue
		}

		// Get current order state from exchange cache
		state, err := m.GetExchangeOrderState(meta.Ticker, order.ID())
		if err != nil {
			log.Printf("Error getting order state for %s: %v", order.ID(), err)
			continue
		}

		// Calculate fill
		fillQty, _ := order.Quantity().Float64()
		fillPrice, _ := order.Price().Float64()

		// Recalculate AvgPrice
		newFilledQty := state.FilledQty + fillQty
		newAvgPrice := CalculateAvgPrice(state.AvgPrice, state.FilledQty, fillPrice, fillQty)

		// Update state
		state.FilledQty = newFilledQty
		state.AvgPrice = newAvgPrice
		state.PendingQty = state.Qty - newFilledQty

		// Validate state
		if state.PendingQty < 0 {
			state.PendingQty = 0
			state.FilledQty = state.Qty
		}
		if state.FilledQty > state.Qty {
			state.FilledQty = state.Qty
			state.PendingQty = 0
		}

		state.UpdateStatus()

		// Update exchange cache
		if err := m.UpdateExchangeOrderState(*state); err != nil {
			log.Printf("Error updating exchange order state: %v", err)
		}

		// Stream update
		m.StreamOrderUpdate(*state)
	}

	// Handle partial matched opposite-side order (similar to processModify)
	// If partial is an opposite-side order (not the trade_event order), update and stream it
	if partial != nil && partial.ID() != tempOrderID {
		m.mu.RLock()
		meta, exists := m.orderMeta[partial.ID()]
		m.mu.RUnlock()
		if exists {
			// Skip LP orders - they're not stored in Redis
			if !isLiquidityProviderOrder(meta.Remark) {
				// Get current order state from exchange cache
				state, err := m.GetExchangeOrderState(meta.Ticker, partial.ID())
				if err != nil {
					log.Printf("Error getting order state for partial order %s: %v", partial.ID(), err)
				} else {
					// Calculate fill from partial order
					fillQty, _ := partialQty.Float64()
					fillPrice, _ := partial.Price().Float64()

					// Recalculate AvgPrice
					newFilledQty := state.FilledQty + fillQty
					newAvgPrice := CalculateAvgPrice(state.AvgPrice, state.FilledQty, fillPrice, fillQty)

					// Update state
					state.FilledQty = newFilledQty
					state.AvgPrice = newAvgPrice
					state.PendingQty = state.Qty - newFilledQty

					// Validate state
					if state.PendingQty < 0 {
						state.PendingQty = 0
						state.FilledQty = state.Qty
					}
					if state.FilledQty > state.Qty {
						state.FilledQty = state.Qty
						state.PendingQty = 0
					}

					state.UpdateStatus()

					// Update exchange cache
					if err := m.UpdateExchangeOrderState(*state); err != nil {
						log.Printf("Error updating exchange order state: %v", err)
					}

					// Stream update
					m.StreamOrderUpdate(*state)
				}
			}
		}
	}

	return nil
}

// HandleMarketClose expires orders and resets orderbooks
func (m *OrderBookManager) HandleMarketClose() {
	log.Println("Starting market close task...")

	// Get all tickers with orderbooks
	m.mu.RLock()
	tickers := make([]string, 0, len(m.orderbooks))
	for ticker := range m.orderbooks {
		tickers = append(tickers, ticker)
	}
	m.mu.RUnlock()

	if len(tickers) == 0 {
		log.Println("No active orderbooks, market close task complete")
		return
	}

	log.Printf("Processing %d tickers for market close", len(tickers))

	for _, ticker := range tickers {
		// Get all orders from exchange's Redis cache
		orders, _ := m.redisClient.HGetAll(ctx, ticker+"_exchangeOrders").Result()

		for orderID, orderJSON := range orders {
			var state OrderState
			if err := json.Unmarshal([]byte(orderJSON), &state); err != nil {
				log.Printf("Error unmarshaling order %s: %v", orderID, err)
				continue
			}

			// Update status
			if state.FilledQty == 0 {
				state.Status = "EXP"
			} else {
				state.Status = "PXP"
			}
			state.PendingQty = 0

			// Update exchange cache
			if err := m.UpdateExchangeOrderState(state); err != nil {
				log.Printf("Error updating exchange order state: %v", err)
			}

			// Stream update
			m.StreamOrderUpdate(state)
		}

		// Reset orderbook
		m.mu.Lock()
		m.orderbooks[ticker] = NewOrderBook()
		m.mu.Unlock()

		// Clear order metadata for this ticker
		m.mu.Lock()
		// Use reverse index for O(1) lookup instead of O(N) scan
		if orderSet, exists := m.tickerOrders[ticker]; exists {
			for orderID := range orderSet {
				delete(m.orderMeta, orderID)
			}
			// Clear the ticker's order set
			delete(m.tickerOrders, ticker)
		}
		m.mu.Unlock()
	}

	log.Println("Market close task completed: all orders expired and orderbooks reset")
}

// CancelAllLPOrders cancels all liquidity provider orders from all orderbooks
// This should be called on LP service startup to clean up orphaned orders from previous instances
func (m *OrderBookManager) CancelAllLPOrders() int {
	log.Println("Scanning for orphaned LP orders to cancel...")

	m.mu.RLock()
	// Collect all LP order metadata
	lpOrders := make(map[string]*OrderMetadata) // orderID -> metadata
	for orderID, meta := range m.orderMeta {
		if isLiquidityProviderOrder(meta.Remark) {
			lpOrders[orderID] = meta
		}
	}
	m.mu.RUnlock()

	if len(lpOrders) == 0 {
		log.Println("No orphaned LP orders found")
		return 0
	}

	log.Printf("Found %d orphaned LP orders, canceling...", len(lpOrders))

	// Cancel each LP order
	canceledCount := 0
	for orderID, meta := range lpOrders {
		// Get orderbook
		ob := m.getOrderbook(meta.Ticker)

		// Check if order exists in orderbook
		order := ob.Order(orderID)
		if order != nil {
			// Cancel from orderbook
			ob.CancelOrder(orderID)
			canceledCount++
		}

		// Remove from metadata and ticker index
		m.mu.Lock()
		delete(m.orderMeta, orderID)
		if m.tickerOrders[meta.Ticker] != nil {
			delete(m.tickerOrders[meta.Ticker], orderID)
		}
		m.mu.Unlock()
	}

	log.Printf("Canceled %d orphaned LP orders", canceledCount)
	return canceledCount
}

// RestoreOrderbookState restores pending orders from exchange's Redis cache into the orderbook
// This is called when the service restarts during trading hours to recover state
func (m *OrderBookManager) RestoreOrderbookState() error {
	log.Println("Starting orderbook state restoration from exchange Redis cache...")

	// Find all tickers by scanning Redis keys matching pattern "*_exchangeOrders"
	// Use SCAN to avoid blocking Redis
	var cursor uint64
	var tickers = make(map[string]bool) // Use map to deduplicate tickers

	for {
		keys, nextCursor, err := m.redisClient.Scan(ctx, cursor, "*_exchangeOrders", 100).Result()
		if err != nil {
			log.Printf("Error scanning Redis keys: %v", err)
			return err
		}

		// Extract tickers from keys (format: {ticker}_exchangeOrders)
		for _, key := range keys {
			if len(key) > 15 && key[len(key)-15:] == "_exchangeOrders" {
				ticker := key[:len(key)-15]
				tickers[ticker] = true
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break // Scan complete
		}
	}

	if len(tickers) == 0 {
		log.Println("No tickers found in exchange cache, nothing to restore")
		return nil
	}

	log.Printf("Found %d tickers to restore", len(tickers))

	// Convert ticker map to slice for ordered processing
	tickerList := make([]string, 0, len(tickers))
	for ticker := range tickers {
		tickerList = append(tickerList, ticker)
	}

	// Pipeline all HGetAll calls to reduce round-trips
	// This reduces N Redis calls to 1 batch operation
	orderCmds := make([]*redis.StringStringMapCmd, len(tickerList))
	_, err := m.redisClient.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, ticker := range tickerList {
			orderCmds[i] = pipe.HGetAll(ctx, ticker+"_exchangeOrders")
		}
		return nil
	})

	if err != nil {
		log.Printf("Error executing pipeline for order restoration: %v", err)
		return err
	}

	// Store orders by ticker for processing
	tickerOrders := make(map[string]map[string]string, len(tickerList))
	for i, ticker := range tickerList {
		if orderCmds[i] != nil {
			orders, cmdErr := orderCmds[i].Result()
			if cmdErr != nil {
				log.Printf("Error getting orders for ticker %s: %v", ticker, cmdErr)
				continue
			}
			if len(orders) > 0 {
				tickerOrders[ticker] = orders
			}
		}
	}

	totalRestored := 0
	totalSkipped := 0

	// Restore orders for each ticker
	for _, ticker := range tickerList {
		orders, exists := tickerOrders[ticker]
		if !exists || len(orders) == 0 {
			continue
		}

		// Get or create orderbook for this ticker
		// No ticker lock needed - restoration happens before queue starts
		ob := m.getOrderbook(ticker)

		// Process each order (orderID is the hash key in exchangeOrders)
		for orderID, orderJSON := range orders {
			var state OrderState
			if err := json.Unmarshal([]byte(orderJSON), &state); err != nil {
				log.Printf("Error unmarshaling order %s for ticker %s: %v", orderID, ticker, err)
				totalSkipped++
				continue
			}

			// Only restore orders that are pending execution (PEX) with pending quantity
			if state.Status != "PEX" || state.PendingQty <= 0 {
				continue
			}

			// Convert side
			side := Buy
			if state.BS == "S" {
				side = Sell
			}

			// Restore order to orderbook using original OrderID and PendingQty
			price := decimal.NewFromFloat(state.Price)
			pendingQty := decimal.NewFromFloat(state.PendingQty)

			// ProcessLimitOrder will add the order to the orderbook
			// Note: If opposite-side orders were already restored, matching may occur
			done, partial, partialQty, err := ob.ProcessLimitOrder(side, state.OrderID, pendingQty, price)
			if err != nil {
				// If order already exists (shouldn't happen during restore), skip it
				if err.Error() == "order with given ID already exists" {
					log.Printf("Order %s already exists in orderbook for ticker %s, skipping", state.OrderID, ticker)
					totalSkipped++
					continue
				}
				log.Printf("Error restoring order %s for ticker %s: %v", state.OrderID, ticker, err)
				totalSkipped++
				continue
			}

			// Restore metadata first (needed for streamMatchedOrders)
			m.mu.Lock()
			m.orderMeta[state.OrderID] = &OrderMetadata{
				Remark:       state.Remark,
				OrderGroupID: state.OrderGroupID,
				Ticker:       ticker,
				IsLP:         isLiquidityProviderOrder(state.Remark),
				IsFutures:    isFuturesOrder(ticker),
			}
			// Add to ticker reverse index
			if m.tickerOrders[ticker] == nil {
				m.tickerOrders[ticker] = make(map[string]struct{})
			}
			m.tickerOrders[ticker][state.OrderID] = struct{}{}
			m.mu.Unlock()

			// If order was matched during restoration, handle it properly
			if len(done) > 0 || (partial != nil && partial.ID() == state.OrderID) {
				log.Printf("Order %s for ticker %s was matched during restoration (done=%d, partial=%v)",
					state.OrderID, ticker, len(done), partial != nil)

				// Stream updates for matched opposite side orders
				m.streamMatchedOrders(done, partial, partialQty, state.OrderID)

				// Calculate new fills for the restored order
				var newFilledQty float64
				var newAvgPrice float64
				var newPendingQty float64

				// Check if fully matched: synthetic order at end of done
				if len(done) > 0 && done[len(done)-1].ID() == state.OrderID {
					synth := done[len(done)-1]
					newFilledQty, _ = synth.Quantity().Float64()
					newAvgPrice, _ = synth.Price().Float64()
					newPendingQty = 0
				} else if partial != nil && partial.ID() == state.OrderID {
					// Partially matched incoming order: partialQty is the new fill amount
					// Use partialQty directly to avoid double-counting
					newFilledQty, _ = partialQty.Float64()

					// Calculate avgPrice from the opposite-side orders consumed
					if len(done) > 0 {
						var totalValue decimal.Decimal
						for _, matchedOrder := range done {
							totalValue = totalValue.Add(matchedOrder.Price().Mul(matchedOrder.Quantity()))
						}
						avgPriceValue := totalValue.Div(partialQty)
						newAvgPrice, _ = avgPriceValue.Float64()
					} else {
						// No done orders (edge case)
						newAvgPrice = state.Price
					}

					// Combine with existing fills from Redis state
					if state.FilledQty > 0 {
						newAvgPrice = CalculateAvgPrice(state.AvgPrice, state.FilledQty, newAvgPrice, newFilledQty)
						newFilledQty = state.FilledQty + newFilledQty
					}

					newPendingQty = state.Qty - newFilledQty
					if newPendingQty < 0 {
						newPendingQty = 0
					}
				} else {
					// No matching happened
					newFilledQty = state.FilledQty
					newAvgPrice = state.AvgPrice
					newPendingQty = state.PendingQty
				}

				// Update state
				state.FilledQty = newFilledQty
				state.AvgPrice = newAvgPrice
				state.PendingQty = newPendingQty
				state.UpdateStatus()

				// Update exchange cache
				if err := m.UpdateExchangeOrderState(state); err != nil {
					log.Printf("Error updating exchange order state: %v", err)
				}

				// Stream update for the restored order
				m.StreamOrderUpdate(state)
			}

			totalRestored++
		}
	}

	log.Printf("Orderbook restoration complete: %d orders restored, %d skipped", totalRestored, totalSkipped)
	return nil
}

// Helper functions
func getCurrentTradeTime() string {
	return time.Now().In(hcmcLoc).Format("2006-01-02 15:04:05")
}

func isFuturesOrder(ticker string) bool {
	return len(ticker) > 3 && ticker[:3] == "41I"
}

// isLiquidityProviderOrder checks if an order is a liquidity provider order
// Liquidity provider orders have remark starting with "LIQUIDITY_PROVIDER:"
func isLiquidityProviderOrder(remark string) bool {
	return strings.HasPrefix(remark, "LIQUIDITY_PROVIDER:")
}

// isLiquidityProviderOrderID checks if an orderID belongs to a liquidity provider order
func (m *OrderBookManager) isLiquidityProviderOrderID(orderID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta, exists := m.orderMeta[orderID]
	if !exists {
		return false
	}
	return isLiquidityProviderOrder(meta.Remark)
}

var hcmcLoc *time.Location

func init() {
	loc, err := time.LoadLocation("Asia/Ho_Chi_Minh")
	if err != nil {
		loc = time.FixedZone("Asia/Ho_Chi_Minh", 7*60*60)
	}
	hcmcLoc = loc
}
