package main

import (
	"encoding/json"
	"net/http"
)

// StartAggregationServer launches the HTTP server that exposes the aggregation
// endpoint. It runs in a goroutine and does not block. If the port is already
// in use the server logs an error and continues (the Kafka loop remains the
// primary workload); the only fatal case is when the process cannot bind at all,
// which is surfaced via the error log line below.
func StartAggregationServer(aggregator *Aggregator, port string, logger *Logger) {
	addr := ":" + port
	logger.log("INFO", "aggregation http server starting",
		strEntry("http_addr", addr),
		strEntry("http_endpoint", "/metrics"))

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		snap := aggregator.Snapshot()
		body, err := json.Marshal(snap)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	})

	go func() {
		err := http.ListenAndServe(addr, mux)
		if err != nil {
			logger.log("ERROR", "aggregation http server failed",
				strEntry("http_addr", addr),
				strEntry("error", err.Error()))
		}
	}()
}
