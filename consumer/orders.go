package main

import (
	"encoding/json"
)

// Order is the canonical schema for events on the "orders" topic.
// The Go field names are idiomatic (public, capitalised); the `json:"..."`
// tags pin the wire keys to their defined form (orderId, product, price).
// It must match the struct produced by the producer service.
type Order struct {
	OrderId string  `json:"orderId"`
	Product string  `json:"product"`
	Price   float64 `json:"price"`
}

// parse decodes an order payload from the wire into an Order.
func parse(b []byte) (Order, error) {
	var o Order
	if err := json.Unmarshal(b, &o); err != nil {
		return Order{}, err
	}
	return o, nil
}
