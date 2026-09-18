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

	// Set up the file-based DLQ writer for orders that fail after retries.
	dlqFile, derr := openDLQFile(dlqFilePath())
	if derr != nil {
		logger.log("ERROR", "failed to open DLQ file",
			strEntry("dlq_path", dlqFilePath()),
			strEntry("error", derr.Error()))
		os.Exit(1)
	}
	defer dlqFile.Close()
	dlq := &fileDLQ{file: dlqFile, path: dlqFilePath()}

	logger.log("INFO", "dlq file ready",
		strEntry("dlq_path", dlqFilePath()))

	logger.log("INFO", "producer ready",
		strEntry("kafka_brokers", brokerList),
		strEntry("dlq_path", dlqFilePath()))

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
		err := writeOrderWithRetry(writer, msg, order, retryConfig(), dlq, logger)
		if err != nil {
			// Write failed even after retries. The order has been written to
			// the DLQ file by writeOrderWithRetry, so we just log and move on.
			logger.log("ERROR", "order publish failed after retries, sent to DLQ",
				numEntry("sequence", fmt.Sprint(count)),
				strEntry("order_id", order.OrderId),
				strEntry("product", order.Product),
				numEntry("price", fmt.Sprint(order.Price)),
				strEntry("error", err.Error()),
				strEntry("dlq_path", dlqFilePath()))
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

// writeOrderWithRetry attempts to write msg to Kafka with retries. If all
// retries are exhausted, the order is written to the DLQ file. Returns nil on
// success, or the final error if the order was DLQ'd.
func writeOrderWithRetry(
	writer *kafka.Writer,
	msg kafka.Message,
	order Order,
	cfg RetryConfig,
	dlq *fileDLQ,
	logger *Logger,
) error {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		lastErr = writer.WriteMessages(context.Background(), msg)
		if lastErr == nil {
			return nil
		}
		if !kafkaTransientError(lastErr) {
			// Permanent error; don't retry, send to DLQ.
			break
		}
		if attempt < cfg.MaxRetries-1 {
			delay := cfg.BaseDelay * time.Duration(1<<attempt)
			logger.log("WARNING", "order publish retry",
				numEntry("attempt", fmt.Sprint(attempt+1)),
				numEntry("max_retries", fmt.Sprint(cfg.MaxRetries)),
				strEntry("order_id", order.OrderId),
				strEntry("error", lastErr.Error()))
			time.Sleep(delay)
		}
	}

	// All retries exhausted or permanent error — write to DLQ.
	dlqEntry := ProducerDLQEntry{
		Timestamp:  nowUTC(),
		OrderID:    order.OrderId,
		Product:    order.Product,
		Price:      order.Price,
		Error:      lastErr.Error(),
		RetryCount: cfg.MaxRetries,
	}
	if derr := dlq.Append(dlqEntry); derr != nil {
		logger.log("ERROR", "failed to write to DLQ file",
			strEntry("dlq_path", dlq.path),
			strEntry("error", derr.Error()),
			strEntry("order_id", order.OrderId))
	}
	return fmt.Errorf("publish failed after %d retries: %s", cfg.MaxRetries, lastErr.Error())
}
