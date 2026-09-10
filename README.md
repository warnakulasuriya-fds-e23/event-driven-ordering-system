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

This starts the Kafka broker, then the producer (publishes random order events) and the
consumer (logs them).

## Configuration

Services are configured through environment variables. See `docker-compose.yml` for defaults.