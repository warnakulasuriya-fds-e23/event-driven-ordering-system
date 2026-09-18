package main

import "sync"

// productAgg holds the running aggregates for a single product.
type productAgg struct {
	Sum   float64
	Count int64
}

// AggregationResult is the point-in-time snapshot returned by the HTTP API.
type AggregationResult struct {
	Overall struct {
		TotalSum       float64 `json:"total_sum"`
		OverallAverage float64 `json:"overall_average"`
		OrderCount     int64   `json:"order_count"`
	} `json:"overall"`
	ByProduct map[string]struct {
		Sum     float64 `json:"sum"`
		Average float64 `json:"average"`
		Count   int64   `json:"count"`
	} `json:"by_product"`
}

// Aggregator maintains in-memory rolling aggregates for order events. It is safe
// for concurrent use: the Kafka reader calls Record, and the HTTP handler calls
// Snapshot — both are protected by an internal RWMutex.
type Aggregator struct {
	mu       sync.RWMutex
	totalSum float64
	totalCnt int64
	byProduct map[string]*productAgg
}

// NewAggregator returns an empty Aggregator ready to record events.
func NewAggregator() *Aggregator {
	return &Aggregator{
		byProduct: make(map[string]*productAgg),
	}
}

// Record incorporates a single order event into the running aggregates.
func (a *Aggregator) Record(o Order) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.totalSum += o.Price
	a.totalCnt++

	pa, ok := a.byProduct[o.Product]
	if !ok {
		pa = &productAgg{}
		a.byProduct[o.Product] = pa
	}
	pa.Sum += o.Price
	pa.Count++
}

// Snapshot returns a consistent point-in-time view of all current aggregates.
// It is safe to call concurrently with Record.
func (a *Aggregator) Snapshot() AggregationResult {
	a.mu.RLock()
	defer a.mu.RUnlock()

	res := AggregationResult{
		ByProduct: make(map[string]struct {
			Sum     float64 `json:"sum"`
			Average float64 `json:"average"`
			Count   int64   `json:"count"`
		}),
	}
	res.Overall.TotalSum = a.totalSum
	res.Overall.OrderCount = a.totalCnt
	if a.totalCnt > 0 {
		res.Overall.OverallAverage = a.totalSum / float64(a.totalCnt)
	}

	for prod, pa := range a.byProduct {
		pb := struct {
			Sum     float64 `json:"sum"`
			Average float64 `json:"average"`
			Count   int64   `json:"count"`
		}{
			Sum:   pa.Sum,
			Count: pa.Count,
		}
		if pa.Count > 0 {
			pb.Average = pa.Sum / float64(pa.Count)
		}
		res.ByProduct[prod] = pb
	}

	return res
}
