package main

import (
	"os"
	"strings"
)

// envOrDefault returns the value of the environment variable `key`, or
// `fallback` when it is unset/empty.
func envOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value != "" {
		return value
	}
	return fallback
}

// brokers parses KAFKA_BOOTSTRAP_SERVERS (comma-separated) into a list.
func brokers() []string {
	return strings.Split(envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ",")
}

// topic is the Kafka topic orders are published to.
func topic() string {
	return envOrDefault("KAFKA_TOPIC", "orders")
}

// orderAvscPath is the location of the order Avro schema file.
func orderAvscPath() string {
	return envOrDefault("ORDER_AVSC_PATH", "schema/order.avsc")
}
