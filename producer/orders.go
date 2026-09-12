package main

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/google/uuid"
)

// Order is the canonical schema for events on the "orders" topic.
// The Go field names are idiomatic (public, capitalised); the `json:"..."`
// tags pin the wire keys to their defined form (orderId, product, price).
type Order struct {
	OrderId string  `json:"orderId"`
	Product string  `json:"product"`
	Price   float64 `json:"price"`
}

// productCatalog returns the finite set of products an order may reference.
func productCatalog() []string {
	return []string{
		"laptop", "keyboard", "mouse", "monitor", "webcam", "headset",
		"desk", "chair", "cable", "router", "speaker", "tablet",
	}
}

func randomProduct() string {
	catalog := productCatalog()
	return catalog[rand.Intn(len(catalog))]
}

func randomPrice() float64 {
	return 5.0 + float64(rand.Int63n(99900))/100.0
}

// newRandomOrder generates an order with random orderId, product and price.
func newRandomOrder() Order {
	return Order{
		OrderId: fmt.Sprint(uuid.New()),
		Product: randomProduct(),
		Price:   randomPrice(),
	}
}

// encode serializes an order to the JSON payload written to kafka.
func encode(o Order) ([]byte, error) {
	return json.Marshal(o)
}
