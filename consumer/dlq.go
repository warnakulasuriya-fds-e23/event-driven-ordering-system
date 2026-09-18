package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// ConsumerDLQEntry records a single poison message that was sent to the DLQ.
type ConsumerDLQEntry struct {
	Timestamp      string `json:"timestamp"`
	Topic          string `json:"topic"`
	Partition      int    `json:"partition"`
	Offset         int64  `json:"offset"`
	Error          string `json:"error"`
	RawBytesLength int    `json:"raw_bytes_length,omitempty"`
}

// fileDLQ appends ConsumerDLQEntry records to a JSONL file on disk. It is safe
// for concurrent use from a single writer goroutine.
type fileDLQ struct {
	file *os.File
	path string
	mu   sync.Mutex
}

// openDLQFile opens (or creates) the JSONL DLQ file for appending. It ensures
// the parent directory exists.
func openDLQFile(path string) (*os.File, error) {
	dir := dirname(path)
	if dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create DLQ directory %s: %w", dir, err)
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open DLQ file %s: %w", path, err)
	}
	return f, nil
}

// Append writes a single DLQ entry as a JSON line, synchronously synced to disk.
func (d *fileDLQ) Append(entry ConsumerDLQEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	body, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal DLQ entry: %w", err)
	}
	body = append(body, '\n')

	if _, err := d.file.Write(body); err != nil {
		return fmt.Errorf("write DLQ entry: %w", err)
	}
	if err := d.file.Sync(); err != nil {
		return fmt.Errorf("sync DLQ file: %w", err)
	}
	return nil
}

// dirname returns the directory portion of a path, or empty string for bare filenames.
func dirname(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return ""
}
