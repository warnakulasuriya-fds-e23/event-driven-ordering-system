package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

// Environment configuration. Defaults match the values wired in docker-compose.
func envOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value != "" {
		return value
	}
	return fallback
}

func brokers() []string {
	return strings.Split(envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ",")
}

func topic() string {
	return envOrDefault("KAFKA_TOPIC", "orders")
}

// entry is a single structured-log field. Str fields become JSON strings,
// Num fields are emitted as raw JSON tokens (e.g. numbers).
type entry struct {
	Key string
	Val string
	Str bool
}

func strEntry(key, val string) entry {
	return entry{Key: key, Val: val, Str: true}
}

func numEntry(key, val string) entry {
	return entry{Key: key, Val: val, Str: false}
}

func nowUTC() string {
	// RFC3339 (ISO8601) UTC timestamp, e.g. 2026-09-10T07:14:00Z.
	// Using UTC so logs from any host share a common, unambiguous timestamp.
	return time.Now().UTC().Format(time.RFC3339)
}

// logRecord emits one line of structured JSON to stdout. The schema is
// consistent across services and fields are lowercase/snake_case so that the
// lines can be shipped to Loki / Grafana / Alloy and indexed without reshaping.
func logRecord(level, message string, fields ...entry) {
	parts := make([]string, 0)
	parts = append(parts, fmt.Sprintf("%q:%q", "@timestamp", nowUTC()))
	parts = append(parts, fmt.Sprintf("%q:%q", "level", level))
	parts = append(parts, fmt.Sprintf("%q:%q", "logger", "orders-producer"))
	parts = append(parts, fmt.Sprintf("%q:%q", "@message", message))
	parts = append(parts, fmt.Sprintf("%q:%q", "kafka_topic", topic()))
	for _, f := range fields {
		if f.Str {
			parts = append(parts, fmt.Sprintf("%q:%q", f.Key, f.Val))
		} else {
			parts = append(parts, fmt.Sprintf("%q:%s", f.Key, f.Val))
		}
	}
	fmt.Println("{" + strings.Join(parts, ",") + "}")
}

// orderJSON encodes an order event payload. Order events are small documents
// carrying orderId, product and price.
func orderJSON(orderID, product string, price float64) []byte {
	return []byte(fmt.Sprintf(`{"orderId":%q,"product":%q,"price":%.2f}`, orderID, product, price))
}

func main() {
	products := []string{
		"laptop", "keyboard", "mouse", "monitor", "webcam", "headset",
		"desk", "chair", "cable", "router", "speaker", "tablet",
	}

	brokerList := strings.Join(brokers(), ",")

	logRecord("INFO", "producer starting",
		strEntry("kafka_brokers", brokerList))

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers(),
		Topic:   topic(),
	})

	defer writer.Close()

	logRecord("INFO", "producer ready",
		strEntry("kafka_brokers", brokerList))

	count := 0
	for {
		orderID := fmt.Sprint(uuid.New())
		product := products[rand.Intn(len(products))]
		price := 5.0 + float64(rand.Int63n(99900))/100.0

		msg := kafka.Message{
			Key:   []byte(orderID),
			Value: orderJSON(orderID, product, price),
		}

		count += 1
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			// WriteMessages only returns without error once the message has
			// been acknowledged by the broker, so a non-nil error means the
			// order was NOT accepted.
			logRecord("ERROR", "order publish failed",
				numEntry("sequence", fmt.Sprint(count)),
				strEntry("order_id", orderID),
				strEntry("product", product),
				numEntry("price", fmt.Sprintf("%.2f", price)),
				strEntry("error", err.Error()))
		} else {
			logRecord("INFO", "order published",
				numEntry("sequence", fmt.Sprint(count)),
				strEntry("order_id", orderID),
				strEntry("product", product),
				numEntry("price", fmt.Sprintf("%.2f", price)),
				strEntry("kafka_delivery", "success"))
		}

		time.Sleep(1 * time.Second)
	}
}
