package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	logger := newLogger("orders-producer", topic())
	brokerList := strings.Join(brokers(), ",")

	logger.log("INFO", "producer starting",
		strEntry("kafka_brokers", brokerList))

	// The Avro schema is the contract for the events this service publishes.
	schema, serr := loadOrderSchema(orderAvscPath())
	if serr != nil {
		logger.log("ERROR", "failed to load order avro schema",
			strEntry("avro_schema", orderAvscPath()),
			strEntry("error", serr.Error()))
		os.Exit(1)
	}
	logger.log("INFO", "order avro schema loaded",
		strEntry("avro_schema", orderAvscPath()))

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

		payload, perr := schema.Encode(order)
		if perr != nil {
			logger.log("ERROR", "order avro encode failed",
				strEntry("order_id", order.OrderId),
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
				numEntry("payload_bytes", fmt.Sprint(len(payload))),
				strEntry("kafka_delivery", "success"))
		}

		time.Sleep(1 * time.Second)
	}
}
