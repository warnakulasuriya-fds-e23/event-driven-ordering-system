# Ordering System

An event-driven ordering system built with Go services and Apache Kafka.

## Structure

| Directory  | Purpose                                                         |
|------------|-----------------------------------------------------------------|
| `producer` | Go service that generates random order events and publishes them to the Kafka `orders` topic. |
| `consumer` | Go service that consumes events from the Kafka `orders` topic and logs them. |
| `infra`    | Kafka broker configuration and monitoring / log-auditing concerns (placeholder). |

## Stack (planned / staged)

- **Kafka** broker for event streaming (single node, `orders` topic with 1 partition).
- **Producer / Consumer**: Go services.
- **Observability** (future): Alloy / Loki / Grafana log shipping stack. Services already emit
  structured JSON logs to stdout so they can be shipped and indexed later.

## Quick start

```bash
docker compose up --build
```

This brings up:

- `kafka` — a single-node Apache Kafka broker (KRaft mode, no Zookeeper).
- `kafka-init` — a one-shot helper that pre-creates the `orders` topic with exactly
  **1 partition** (replication factor 1) so consumers never race with auto-creation.
  It exits after creating the topic; the other services wait for it.
- `producer` — publishes a random order event (`orderId`, `product`, `price`) to
  `orders` roughly once per second. Its logs show each produced event and whether
  Kafka acknowledged it.
- `consumer` — consumes events from `orders` and logs them when received.

View logs with:

```bash
docker compose logs -f producer consumer
```

## Logging format

All services write one structured JSON object per line to stdout with a consistent
schema (`@timestamp`, `level`, `logger`, `@message`, plus event fields). This keeps
logs parseable and indexable by the planned Alloy / Loki / Grafana shipping stack
without reshaping.

## Aggregation API

The consumer exposes a read-only HTTP API for real-time aggregation metrics.
The data is held in-memory (updated by the Kafka reader loop) and served as a
JSON snapshot on demand. No separate aggregation service is required.

**Endpoint:** `GET /metrics`

**Example:**

```bash
curl -s http://localhost:8080/metrics | jq
```

**Response shape:**

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

- `overall.total_sum` — sum of all order prices seen so far.
- `overall.overall_average` — mean price across all orders (`total_sum / order_count`, or `0` when no orders yet).
- `overall.order_count` — total number of orders consumed.
- `by_product` — per-product breakdown keyed by the `product` field of the order (`laptop`, `keyboard`, `mouse`, etc.).

All monetary values are in the same unit as the `price` field of the Avro schema (float64).

The product catalog is finite and bounded, so the in-memory state never grows
unbounded. Orders with a product not seen before are still tracked dynamically.

### Configuration

Services are configured through environment variables (see `docker-compose.yml`):

| Variable                 | Producer | Consumer | Default         |
|--------------------------|----------|----------|-----------------|
| `KAFKA_BOOTSTRAP_SERVERS`| yes      | yes      | `localhost:9092`|
| `KAFKA_TOPIC`            | yes      | yes      | `orders`        |
| `KAFKA_GROUP_ID`         |          | yes      | `orders-consumer`|
| `AGGREGATION_PORT`       |          | yes      | `8080`          |

## Inspecting the topic

```bash
# from the host, exec into the kafka container
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
```

Output confirms `PartitionCount: 1` for the `orders` topic.