package exchange

import (
	"log"
	"net/http"
)

// HandleEqtSend handles sending equity orders (JSON or application/x-protobuf)
func HandleEqtSend(queueManager *TickerQueueManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := decodeOrderRequest(r)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Validate required fields for Send (orderID and orderGroupID are ignored if provided)
		if req.Ticker == "" || req.BS == "" || req.Price <= 0 || req.Quantity <= 0 {
			http.Error(w, "Invalid request: missing required fields", http.StatusBadRequest)
			return
		}

		// Clear orderID and orderGroupID if provided (they will be generated)
		req.OrderID = ""
		req.OrderGroupID = ""

		// Enqueue request
		tickerReq := TickerRequest{
			Type:    "Send",
			Ticker:  req.Ticker,
			Request: req,
			Source:  "user",
			Async:   false,
		}

		result := queueManager.EnqueueRequestWithContext(r.Context(), tickerReq)

		if err := encodeOrderResult(w, r, result); err != nil {
			log.Printf("Encode order result: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}
}

// HandleEqtModify handles modifying equity orders (JSON or application/x-protobuf)
func HandleEqtModify(queueManager *TickerQueueManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := decodeOrderRequest(r)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Validate required fields for Modify
		if req.OrderID == "" || req.OrderGroupID == "" || req.Ticker == "" || req.Price <= 0 || req.Quantity <= 0 {
			http.Error(w, "Invalid request: orderID, orderGroupID, ticker, price, and quantity are required", http.StatusBadRequest)
			return
		}

		// Enqueue request
		tickerReq := TickerRequest{
			Type:    "Modify",
			Ticker:  req.Ticker,
			Request: req,
			Source:  "user",
			Async:   false,
		}

		result := queueManager.EnqueueRequestWithContext(r.Context(), tickerReq)

		if err := encodeOrderResult(w, r, result); err != nil {
			log.Printf("Encode order result: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}
}

// HandleEqtCancel handles canceling equity orders (JSON or application/x-protobuf)
func HandleEqtCancel(queueManager *TickerQueueManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := decodeOrderRequest(r)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		log.Printf("Request for Cancel: %v", req)

		// Validate required fields for Cancel
		if req.OrderID == "" || req.OrderGroupID == "" || req.Ticker == "" {
			http.Error(w, "Invalid request: orderID, orderGroupID, and ticker are required", http.StatusBadRequest)
			return
		}

		// Enqueue request
		tickerReq := TickerRequest{
			Type:    "Cancel",
			Ticker:  req.Ticker,
			Request: req,
			Source:  "user",
			Async:   false,
		}

		result := queueManager.EnqueueRequestWithContext(r.Context(), tickerReq)

		if err := encodeOrderResult(w, r, result); err != nil {
			log.Printf("Encode order result: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}
}

// HandleFnoSend handles sending futures orders (JSON or application/x-protobuf)
func HandleFnoSend(queueManager *TickerQueueManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := decodeOrderRequest(r)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Validate required fields for Send (orderID and orderGroupID are ignored if provided)
		if req.Ticker == "" || req.BS == "" || req.Price <= 0 || req.Quantity <= 0 {
			http.Error(w, "Invalid request: missing required fields", http.StatusBadRequest)
			return
		}

		// Clear orderID and orderGroupID if provided (they will be generated)
		req.OrderID = ""
		req.OrderGroupID = ""

		// Enqueue request
		tickerReq := TickerRequest{
			Type:    "Send",
			Ticker:  req.Ticker,
			Request: req,
			Source:  "user",
			Async:   false,
		}

		result := queueManager.EnqueueRequestWithContext(r.Context(), tickerReq)

		if err := encodeOrderResult(w, r, result); err != nil {
			log.Printf("Encode order result: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}
}

// HandleFnoModify handles modifying futures orders (JSON or application/x-protobuf)
func HandleFnoModify(queueManager *TickerQueueManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := decodeOrderRequest(r)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Validate required fields for Modify
		if req.OrderID == "" || req.OrderGroupID == "" || req.Ticker == "" || req.Price <= 0 || req.Quantity <= 0 {
			http.Error(w, "Invalid request: orderID, orderGroupID, ticker, price, and quantity are required", http.StatusBadRequest)
			return
		}

		// Enqueue request
		tickerReq := TickerRequest{
			Type:    "Modify",
			Ticker:  req.Ticker,
			Request: req,
			Source:  "user",
			Async:   false,
		}

		result := queueManager.EnqueueRequestWithContext(r.Context(), tickerReq)

		if err := encodeOrderResult(w, r, result); err != nil {
			log.Printf("Encode order result: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}
}

// HandleFnoCancel handles canceling futures orders (JSON or application/x-protobuf)
func HandleFnoCancel(queueManager *TickerQueueManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := decodeOrderRequest(r)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Validate required fields for Cancel
		if req.OrderID == "" || req.OrderGroupID == "" || req.Ticker == "" {
			http.Error(w, "Invalid request: orderID, orderGroupID, and ticker are required", http.StatusBadRequest)
			return
		}

		// Enqueue request
		tickerReq := TickerRequest{
			Type:    "Cancel",
			Ticker:  req.Ticker,
			Request: req,
			Source:  "user",
			Async:   false,
		}

		result := queueManager.EnqueueRequestWithContext(r.Context(), tickerReq)

		if err := encodeOrderResult(w, r, result); err != nil {
			log.Printf("Encode order result: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}
}
