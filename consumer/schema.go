package main

import (
	"os"

	"github.com/hamba/avro/v2"
)

// OrderSchema wraps the parsed Avro schema for order events and the codec
// built on top of it, so both services share a single source of truth
// (schema/order.avsc) rather than hand-rolled deserialization.
type OrderSchema struct {
	schema avro.Schema
}

// loadOrderSchema reads and parses the Avro schema file at `path`.
func loadOrderSchema(path string) (*OrderSchema, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	parsed, err := avro.ParseBytes(b)
	if err != nil {
		return nil, err
	}

	return &OrderSchema{schema: parsed}, nil
}

// Encode serializes an order into the Avro payload written to kafka.
func (s *OrderSchema) Encode(o Order) ([]byte, error) {
	return avro.Marshal(s.schema, o)
}

// Decode parses an Avro payload read from kafka into an Order.
func (s *OrderSchema) Decode(data []byte) (Order, error) {
	var o Order
	if err := avro.Unmarshal(s.schema, data, &o); err != nil {
		return Order{}, err
	}
	return o, nil
}