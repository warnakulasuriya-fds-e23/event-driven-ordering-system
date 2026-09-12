package main

import (
	"fmt"
	"strings"
	"time"
)

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
	return time.Now().UTC().Format(time.RFC3339)
}

// Logger emits one line of structured JSON per record with a stable schema, so
// log lines can be shipped to Loki / Grafana / Alloy and indexed without
// reshaping.
type Logger struct {
	Service string
	Topic   string
}

func newLogger(service, topic string) *Logger {
	return &Logger{Service: service, Topic: topic}
}

func (l *Logger) log(level, message string, fields ...entry) {
	parts := make([]string, 0)
	parts = append(parts, fmt.Sprintf("%q:%q", "@timestamp", nowUTC()))
	parts = append(parts, fmt.Sprintf("%q:%q", "level", level))
	parts = append(parts, fmt.Sprintf("%q:%q", "logger", l.Service))
	parts = append(parts, fmt.Sprintf("%q:%q", "@message", message))
	parts = append(parts, fmt.Sprintf("%q:%q", "kafka_topic", l.Topic))
	for _, f := range fields {
		if f.Str {
			parts = append(parts, fmt.Sprintf("%q:%q", f.Key, f.Val))
		} else {
			parts = append(parts, fmt.Sprintf("%q:%s", f.Key, f.Val))
		}
	}
	fmt.Println("{" + strings.Join(parts, ",") + "}")
}
