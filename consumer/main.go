package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

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

func groupID() string {
	return envOrDefault("KAFKA_GROUP_ID", "orders-consumer")
}

// ---- structured JSON logging (consistent schema shared with the producer) ----
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
	return time.Now().UTC().Format(time.RFC3339)
}

func logRecord(level, message string, fields ...entry) {
	parts := make([]string, 0)
	parts = append(parts, fmt.Sprintf("%q:%q", "@timestamp", nowUTC()))
	parts = append(parts, fmt.Sprintf("%q:%q", "level", level))
	parts = append(parts, fmt.Sprintf("%q:%q", "logger", "orders-consumer"))
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

func main() {
	brokerList := strings.Join(brokers(), ",")

	logRecord("INFO", "consumer starting",
		strEntry("kafka_brokers", brokerList),
		strEntry("kafka_group_id", groupID()))

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers(),
		GroupID:  groupID(),
		Topic:    topic(),
		MinBytes: 10e3, // 10 KB
		MaxBytes: 10e6, // 10 MB
	})

	defer reader.Close()

	logRecord("INFO", "consumer ready",
		strEntry("kafka_brokers", brokerList),
		strEntry("kafka_group_id", groupID()))

	// Throttle reporting of transient read errors so an idle consumer does not
	// spam the log when there are simply no new events.
	lastErrLog := time.Now()

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			// No new data (or a transient error). The reader backs off on its
			// own; log at a low cadence and keep polling.
			now := time.Now()
			if now.Sub(lastErrLog) > 15*time.Second {
				logRecord("WARNING", "message read error",
					strEntry("error", err.Error()))
				lastErrLog = now
			}
			time.Sleep(1 * time.Second)
			continue
		}

		// Log the order event. m.Value is the JSON payload produced upstream.
		logRecord("INFO", "order received",
			numEntry("kafka_partition", fmt.Sprint(m.Partition)),
			numEntry("kafka_offset", fmt.Sprint(m.Offset)),
			strEntry("event", string(m.Value)))
	}
}
