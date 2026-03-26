package exchange

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"exchange/pb"

	"google.golang.org/protobuf/proto"
)

const contentTypeProtobuf = "application/x-protobuf"

func wantsProtobufResponse(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), contentTypeProtobuf)
}

func isProtobufRequest(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Content-Type"), contentTypeProtobuf)
}

func decodeOrderRequest(r *http.Request) (OrderRequest, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return OrderRequest{}, err
	}
	if isProtobufRequest(r) {
		var preq pb.OrderRequest
		if err := proto.Unmarshal(body, &preq); err != nil {
			return OrderRequest{}, err
		}
		return OrderRequest{
			OrderID:      preq.GetOrderID(),
			OrderGroupID: preq.GetOrderGroupID(),
			Ticker:       preq.GetTicker(),
			BS:           preq.GetBs(),
			Price:        preq.GetPrice(),
			Quantity:     preq.GetQuantity(),
			Remark:       preq.GetRemark(),
		}, nil
	}
	var req OrderRequest
	err = json.Unmarshal(body, &req)
	return req, err
}

func encodeOrderResult(w http.ResponseWriter, r *http.Request, result OrderResult) error {
	if wantsProtobufResponse(r) {
		pres := &pb.OrderResult{
			OrderID:      result.OrderID,
			OrderGroupID: result.OrderGroupID,
			Ticker:       result.Ticker,
			Bs:           result.BS,
			Price:        result.Price,
			Qty:          result.Qty,
			PendingQty:   result.PendingQty,
			FilledQty:    result.FilledQty,
			AvgPrice:     result.AvgPrice,
			Status:       result.Status,
			Remark:       result.Remark,
			TradeTime:    result.TradeTime,
			ErrorMessage: result.ErrorMessage,
		}
		data, err := proto.Marshal(pres)
		if err != nil {
			return err
		}
		w.Header().Set("Content-Type", contentTypeProtobuf)
		_, err = w.Write(data)
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(result)
}
