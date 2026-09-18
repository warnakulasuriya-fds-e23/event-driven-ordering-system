# Consumer Aggregation API Specification

## Overview

The consumer service exposes a read-only HTTP API for real-time aggregation metrics. The data is held in-memory (updated by the Kafka reader loop) and served as a JSON snapshot on demand.

| Property          | Value                          |
|-------------------|--------------------------------|
| Base URL          | `http://<host>:<port>`         |
| Default Port      | `8080` (configurable via `AGGREGATION_PORT`) |
| Protocol          | HTTP/1.1                       |
| Content Type      | `application/json`             |
| Authentication    | None (internal service use)    |

---

## Endpoints

### GET /metrics

Returns a point-in-time snapshot of all current aggregation metrics.

#### Request

| Aspect      | Detail                                      |
|-------------|---------------------------------------------|
| Method      | `GET`                                       |
| URL         | `/metrics`                                  |
| Headers     | `Accept: application/json` (optional)       |
| Query Params| None                                        |
| Body        | None                                        |

#### Successful Response

**Status:** `200 OK`

**Headers:**
- `Content-Type: application/json`

**Body:**

```json
{
  "overall": {
    "total_sum": 21345.67,
    "overall_average": 38.72,
    "order_count": 551
  },
  "by_product": {
    "laptop":  { "sum": 5000.00, "average": 1000.00, "count": 5 },
    "keyboard": { "sum": 250.00,  "average": 25.00,  "count": 10 }
  }
}
```

#### Response Fields

##### `overall` object

| Field             | Type   | Description                                           |
|-------------------|--------|-------------------------------------------------------|
| `total_sum`       | number | Sum of all order prices seen so far                   |
| `overall_average` | number | Mean price across all orders (`total_sum / order_count`). `0` when no orders yet |
| `order_count`     | integer| Total number of orders consumed                       |

##### `by_product` object

Map of product name → product aggregates.

| Field       | Type   | Description                                           |
|-------------|--------|-------------------------------------------------------|
| `sum`       | number | Sum of prices for this product                        |
| `average`   | number | Mean price for this product (`sum / count`). `0` when count is 0 |
| `count`     | integer| Number of orders for this product                     |

Keys are product names as they appear in the `product` field of the order event (e.g., `laptop`, `keyboard`, `mouse`, `monitor`, `webcam`, `headset`, `desk`, `chair`, `cable`, `router`, `speaker`, `tablet`).

#### Error Responses

**Status:** `405 Method Not Allowed`

Returned when the request method is not `GET`.

```json
{
  "error": "method not allowed"
}
```

**Status:** `500 Internal Server Error`

Returned if JSON marshaling of the snapshot fails (extremely unlikely).

```json
{
  "error": "internal error"
}
```

---

## Behavior Notes

- **In-memory state:** All aggregate values are held in-memory and reset on service restart. There is no persistence.
- **Thread safety:** The snapshot is computed under a read lock, so it is always internally consistent even while the Kafka loop is recording new events.
- **Bounded growth:** The `by_product` map only grows with new distinct product names. Since the product catalog is finite and bounded, memory usage is stable.
- **Polling model:** This is a request/response endpoint. There is no streaming, no WebSocket, and no Server-Sent Events. Clients poll at their desired interval.
- **Empty state:** When no orders have been consumed yet, the endpoint returns zeros (`total_sum: 0`, `overall_average: 0`, `order_count: 0`, empty `by_product` map).

---

## Configuration

| Environment Variable   | Default   | Description                                |
|------------------------|-----------|--------------------------------------------|
| `AGGREGATION_PORT`     | `8080`    | TCP port the HTTP server listens on        |

---

## Example Usage

```bash
# Query the metrics endpoint
curl -s http://localhost:8080/metrics

# With pretty-printing
curl -s http://localhost:8080/metrics | jq

# From inside the Docker network
docker compose exec consumer curl -s http://localhost:8080/metrics
```

---

## Integration Notes

- The endpoint is available as soon as the consumer process starts and the HTTP server goroutine begins.
- No Kafka consumer group coordination is involved in serving this endpoint; it reads from the in-memory `Aggregator` state.
- The producer service is unaffected by this API.
