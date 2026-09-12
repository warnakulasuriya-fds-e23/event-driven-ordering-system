package main

import (
	"fmt"
	"math/rand"

	"github.com/google/uuid"
)

// Order is the canonical schema for events on the "orders" topic.
// Go field names are idiomatic (public, capitalised); the `avro:"..."` tags
// bind each field to its Avro record field in schema/order.avsc, which keeps
// the wire keys at their defined form (orderId, product, price).
type Order struct {
	OrderId string  `avro:"orderId"`
	Product string  `avro:"product"`
	Price   float64 `avro:"price"`
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
