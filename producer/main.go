package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	logger := newLogger("orders-producer", topic())
	brokerList := strings.Join(brokers(), ",")

	logger.log("INFO", "producer starting",
		strEntry("kafka_brokers", brokerList))

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers(),
		Topic:   topic(),
	})

	defer writer.Close()

	logger.log("INFO", "producer ready",
		strEntry("kafka_brokers", brokerList))

	count := 0
	for {
		order := newRandomOrder()

		payload, perr := encode(order)
		if perr != nil {
			logger.log("ERROR", "order encode failed",
				strEntry("error", perr.Error()))
			time.Sleep(1 * time.Second)
			continue
		}

		msg := kafka.Message{
			Key:   []byte(order.OrderId),
			Value: payload,
		}

		count += 1
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			// WriteMessages only returns without error once the broker has
			// acknowledged the message, so a non-nil error means the order was
			// NOT accepted.
			logger.log("ERROR", "order publish failed",
				numEntry("sequence", fmt.Sprint(count)),
				strEntry("order_id", order.OrderId),
				strEntry("product", order.Product),
				numEntry("price", fmt.Sprint(order.Price)),
				strEntry("error", err.Error()))
		} else {
			logger.log("INFO", "order published",
				numEntry("sequence", fmt.Sprint(count)),
				strEntry("order_id", order.OrderId),
				strEntry("product", order.Product),
				numEntry("price", fmt.Sprint(order.Price)),
				strEntry("kafka_delivery", "success"))
		}

		time.Sleep(1 * time.Second)
	}
}
