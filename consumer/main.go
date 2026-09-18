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
	logger := newLogger("orders-consumer", topic())
	brokerList := strings.Join(brokers(), ",")

	logger.log("INFO", "consumer starting",
		strEntry("kafka_brokers", brokerList),
		strEntry("kafka_group_id", groupID()))

	// The Avro schema is the contract for the events this service consumes.
	schema, serr := loadOrderSchema(orderAvscPath())
	if serr != nil {
		logger.log("ERROR", "failed to load order avro schema",
			strEntry("avro_schema", orderAvscPath()),
			strEntry("error", serr.Error()))
		os.Exit(1)
	}
	logger.log("INFO", "order avro schema loaded",
		strEntry("avro_schema", orderAvscPath()))

	// In-memory aggregation state shared by the Kafka reader and the HTTP API.
	aggregator := NewAggregator()

	// Start the aggregation HTTP server before entering the Kafka loop so the
	// endpoint is available as soon as the process is ready.
	StartAggregationServer(aggregator, aggregationPort(), logger)

	// Set up the file-based DLQ writer for unparseable messages.
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
		strEntry("kafka_group_id", groupID()),
		strEntry("aggregation_port", aggregationPort()),
		strEntry("aggregation_endpoint", "/metrics"),
		strEntry("dlq_path", dlqFilePath()))

	// Throttle reporting of transient read errors so an idle consumer does not
	// spam the log when there are simply no new events.
	lastErrLog := time.Now()

	for {
		m, err := readMessageWithRetry(reader, consumerRetryConfig(), logger)
		if err != nil {
			// Retry exhausted or permanent error. No specific message to DLQ
			// (we didn't receive one), so just log and keep polling.
			now := time.Now()
			if now.Sub(lastErrLog) > 15*time.Second {
				logger.log("ERROR", "message read failed after retries",
					strEntry("error", err.Error()))
				lastErrLog = now
			}
			time.Sleep(1 * time.Second)
			continue
		}

		order, perr := schema.Decode(m.Value)
		if perr != nil {
			// Poison message: log, DLQ with metadata, commit offset, continue.
			logger.log("ERROR", "unparseable order event sent to DLQ",
				numEntry("kafka_offset", fmt.Sprint(m.Offset)),
				numEntry("kafka_partition", fmt.Sprint(m.Partition)),
				strEntry("error", perr.Error()),
				strEntry("dlq_path", dlqFilePath()))

			dlqErr := dlq.Append(ConsumerDLQEntry{
				Timestamp:       nowUTC(),
				Topic:           topic(),
				Partition:       m.Partition,
				Offset:          m.Offset,
				Error:           perr.Error(),
				RawBytesLength:  len(m.Value),
			})
			if dlqErr != nil {
				logger.log("ERROR", "failed to write to DLQ file",
					strEntry("dlq_path", dlqFilePath()),
					strEntry("error", dlqErr.Error()))
			}

			// Commit the offset so we don't re-read and re-DLQ this message.
			reader.CommitMessages(context.Background(), m)
			continue
		}

		// Feed the in-memory aggregator with every successfully decoded event.
		aggregator.Record(order)

		// Log the decoded event. The raw payload is binary Avro, so only the
		// deserialized fields are logged.
		logger.log("INFO", "order received",
			numEntry("kafka_partition", fmt.Sprint(m.Partition)),
			numEntry("kafka_offset", fmt.Sprint(m.Offset)),
			numEntry("payload_bytes", fmt.Sprint(len(m.Value))),
			strEntry("order_id", order.OrderId),
			strEntry("product", order.Product),
			numEntry("price", fmt.Sprint(order.Price)))
	}
}

// readMessageWithRetry wraps reader.ReadMessage with retry logic for transient errors.
func readMessageWithRetry(reader *kafka.Reader, cfg RetryConfig, logger *Logger) (kafka.Message, error) {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		m, err := reader.ReadMessage(context.Background())
		if err == nil {
			return m, nil
		}
		lastErr = err
		if !kafkaTransientError(err) {
			// Permanent error; don't retry.
			return kafka.Message{}, err
		}
		if attempt < cfg.MaxRetries-1 {
			delay := cfg.BaseDelay * time.Duration(1<<attempt)
			logger.log("WARNING", "message read retry",
				numEntry("attempt", fmt.Sprint(attempt+1)),
				numEntry("max_retries", fmt.Sprint(cfg.MaxRetries)),
				strEntry("error", err.Error()))
			time.Sleep(delay)
		}
	}
	return kafka.Message{}, lastErr
}

// consumerRetryConfig returns the consumer retry configuration from environment.
// This is a package-level alias for retryConfig() to keep main.go readable.
func consumerRetryConfig() RetryConfig {
	return retryConfig()
}
