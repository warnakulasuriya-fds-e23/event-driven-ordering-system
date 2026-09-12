package main

// Order is the canonical schema for events on the "orders" topic.
// Go field names are idiomatic (public, capitalised); the `avro:"..."` tags
// bind each field to its Avro record field in schema/order.avsc, which keeps
// the wire keys at their defined form (orderId, product, price).
// It must match the struct produced by the producer service.
type Order struct {
	OrderId string  `avro:"orderId"`
	Product string  `avro:"product"`
	Price   float64 `avro:"price"`
}
