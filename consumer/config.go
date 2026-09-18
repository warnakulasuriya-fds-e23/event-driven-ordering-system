package main

import (
	"os"
	"strconv"
	"strings"
	"time"
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

// topic is the Kafka topic orders are consumed from.
func topic() string {
	return envOrDefault("KAFKA_TOPIC", "orders")
}

// groupID is the consumer group used to track committed offsets.
func groupID() string {
	return envOrDefault("KAFKA_GROUP_ID", "orders-consumer")
}

// orderAvscPath is the location of the order Avro schema file.
func orderAvscPath() string {
	return envOrDefault("ORDER_AVSC_PATH", "schema/order.avsc")
}

// aggregationPort is the TCP port the consumer exposes its aggregation HTTP API on.
func aggregationPort() string {
	return envOrDefault("AGGREGATION_PORT", "8080")
}

// retryConfig returns the consumer retry configuration from environment.
func retryConfig() RetryConfig {
	maxRetries := parseIntEnv("CONSUMER_MAX_RETRIES", 3)
	baseDelayMs := parseIntEnv("CONSUMER_BASE_DELAY_MS", 1000)
	return RetryConfig{
		MaxRetries: maxRetries,
		BaseDelay:  time.Duration(baseDelayMs) * time.Millisecond,
	}
}

// dlqFilePath returns the path to the consumer's file-based DLQ.
func dlqFilePath() string {
	return envOrDefault("DLQ_FILE_PATH", "dlq/consumer-dlq.jsonl")
}

// parseIntEnv reads an integer from an environment variable, falling back to
// the provided default when the variable is unset, empty, or non-numeric.
func parseIntEnv(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}
