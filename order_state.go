package exchange

// OrderState represents the full state of an order in the matching engine
type OrderState struct {
	OrderID      string
	OrderGroupID string
	Ticker       string
	BS           string // "B" or "S"
	Price        float64
	Qty          float64 // Original quantity (used for priority logic, updated after modify)
	PendingQty   float64
	FilledQty    float64
	AvgPrice     float64 // Weighted average of all fills: (sum of fillPrice * fillQty) / totalFilledQty
	Status       string  // PEX, PXP, FLL, CAN, EXP
	Remark       string
	TradeTime    string // "2006-01-02 15:04:05"
	ErrorMessage string // Error message if order failed
}

// IsTerminalStatus checks if the order is in a terminal status
func (os *OrderState) IsTerminalStatus() bool {
	return os.Status == "FLL" || os.Status == "CAN" || os.Status == "EXP" || os.Status == "PXP"
}

// UpdateStatus updates the order status based on filled quantity and pending quantity
func (os *OrderState) UpdateStatus() {
	if os.PendingQty == 0 && os.FilledQty > 0 {
		os.Status = "FLL"
	} else if os.PendingQty > 0 && os.FilledQty > 0 {
		os.Status = "PEX"
	} else if os.PendingQty > 0 && os.FilledQty == 0 {
		os.Status = "PEX"
	}
}

// CalculateAvgPrice calculates weighted average price for fills
func CalculateAvgPrice(existingAvgPrice, existingFilledQty, fillPrice, fillQty float64) float64 {
	if existingFilledQty == 0 {
		return fillPrice // First fill
	}
	totalValue := (existingAvgPrice * existingFilledQty) + (fillPrice * fillQty)
	totalFilledQty := existingFilledQty + fillQty
	if totalFilledQty == 0 {
		return existingAvgPrice
	}
	return totalValue / totalFilledQty
}
