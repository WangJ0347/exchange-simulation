package exchange

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"exchange/pb"

	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"
)

// In-memory order state cache (non-LP orders) for the hot matching path.
// Redis is updated asynchronously via redisFlushWorker; callers that need
// immediate external visibility still use UpdateExchangeOrderState + StreamOrderUpdate synchronously.
// Crash/restart before flush may lose the last updates (see plan P2).

func (m *OrderBookManager) cacheGetOrderState(ticker, orderID string) (*OrderState, bool) {
	m.orderStateCacheMu.RLock()
	defer m.orderStateCacheMu.RUnlock()
	st, ok := m.orderStateCache[orderID]
	if !ok || st == nil || st.Ticker != ticker {
		return nil, false
	}
	cp := *st
	return &cp, true
}

func (m *OrderBookManager) cachePutOrderState(state *OrderState) {
	if state == nil || isLiquidityProviderOrder(state.Remark) {
		return
	}
	m.orderStateCacheMu.Lock()
	defer m.orderStateCacheMu.Unlock()
	cp := *state
	m.orderStateCache[state.OrderID] = &cp
}

func (m *OrderBookManager) cacheDeleteOrderState(orderID string) {
	m.orderStateCacheMu.Lock()
	defer m.orderStateCacheMu.Unlock()
	delete(m.orderStateCache, orderID)
}

func (m *OrderBookManager) cacheInvalidateTicker(ticker string) {
	m.orderStateCacheMu.Lock()
	defer m.orderStateCacheMu.Unlock()
	for id, st := range m.orderStateCache {
		if st != nil && st.Ticker == ticker {
			delete(m.orderStateCache, id)
		}
	}
}

// enqueueStateFlush schedules HSET + stream publish for the given state (non-LP only).
func (m *OrderBookManager) enqueueStateFlush(state OrderState) {
	if isLiquidityProviderOrder(state.Remark) {
		return
	}
	s := state
	select {
	case <-m.flushClose:
		if err := m.persistOrderStateFull(m.redisOpCtx(), s); err != nil {
			log.Printf("exchange: sync flush after worker stop for order %s: %v", s.OrderID, err)
		}
	default:
		select {
		case m.flushCh <- s:
		case <-m.flushClose:
			if err := m.persistOrderStateFull(m.redisOpCtx(), s); err != nil {
				log.Printf("exchange: sync flush after worker stop for order %s: %v", s.OrderID, err)
			}
		case <-time.After(5 * time.Second):
			if err := m.persistOrderStateFull(m.redisOpCtx(), s); err != nil {
				log.Printf("exchange: sync flush enqueue timeout for order %s: %v", s.OrderID, err)
			}
		}
	}
}

// persistOrderStateFull writes exchange hash and order stream entry (same as Update + Stream).
func (m *OrderBookManager) persistOrderStateFull(ctx context.Context, state OrderState) error {
	if isLiquidityProviderOrder(state.Remark) {
		return nil
	}
	key := state.Ticker + "_exchangeOrders"
	orderJSON, err := json.Marshal(state)
	if err != nil {
		return err
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
	_, err = m.redisClient.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, key, state.OrderID, string(orderJSON))
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			Values: map[string]interface{}{
				"order": string(protoBytes),
			},
		})
		return nil
	})
	return err
}

func (m *OrderBookManager) redisFlushWorker() {
	defer m.flushWG.Done()
	batch := make([]OrderState, 0, 64)
	tick := time.NewTicker(15 * time.Millisecond)
	defer tick.Stop()

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		ctx := m.redisOpCtx()
		_, err := m.redisClient.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, state := range batch {
				if isLiquidityProviderOrder(state.Remark) {
					continue
				}
				key := state.Ticker + "_exchangeOrders"
				orderJSON, jerr := json.Marshal(state)
				if jerr != nil {
					log.Printf("exchange: flush marshal order %s: %v", state.OrderID, jerr)
					continue
				}
				pipe.HSet(ctx, key, state.OrderID, string(orderJSON))

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
				protoBytes, perr := proto.Marshal(payload)
				if perr != nil {
					log.Printf("exchange: flush proto order %s: %v", state.OrderID, perr)
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
			log.Printf("exchange: flush pipeline error: %v", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-m.flushClose:
			flushBatch()
			for {
				select {
				case s := <-m.flushCh:
					batch = append(batch, s)
					if len(batch) >= 64 {
						flushBatch()
					}
				default:
					flushBatch()
					return
				}
			}
		case s := <-m.flushCh:
			batch = append(batch, s)
			if len(batch) >= 64 {
				flushBatch()
			}
		case <-tick.C:
			flushBatch()
		}
	}
}

// StopRedisFlushWorker stops the background flusher and waits until pending writes complete.
// Safe to call once during process shutdown.
func (m *OrderBookManager) StopRedisFlushWorker() {
	m.flushStopOnce.Do(func() {
		close(m.flushClose)
	})
	m.flushWG.Wait()
}
