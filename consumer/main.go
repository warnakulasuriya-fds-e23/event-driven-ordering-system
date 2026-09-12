package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	logger := newLogger("orders-consumer", topic())
	brokerList := strings.Join(brokers(), ",")

	logger.log("INFO", "consumer starting",
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

	logger.log("INFO", "consumer ready",
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
				logger.log("WARNING", "message read error",
					strEntry("error", err.Error()))
				lastErrLog = now
			}
			time.Sleep(1 * time.Second)
			continue
		}

		order, perr := parse(m.Value)
		if perr != nil {
			logger.log("WARNING", "unparseable order event",
				numEntry("kafka_offset", fmt.Sprint(m.Offset)),
				strEntry("error", perr.Error()))
			continue
		}

		// Log the event using the decoded Order schema, plus the raw payload.
		logger.log("INFO", "order received",
			numEntry("kafka_partition", fmt.Sprint(m.Partition)),
			numEntry("kafka_offset", fmt.Sprint(m.Offset)),
			strEntry("order_id", order.OrderId),
			strEntry("product", order.Product),
			numEntry("price", fmt.Sprint(order.Price)),
			strEntry("event", string(m.Value)))
	}
}
