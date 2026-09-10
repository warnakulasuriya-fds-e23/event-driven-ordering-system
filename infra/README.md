# infra

This directory holds infrastructure related to Kafka, monitoring, and log auditing.

Planned contents (not yet added):

- Kafka broker configuration / tuning (production profiles, multi-node topic configs).
- Observability stack: Alloy / Loki / Grafana log shipping & dashboards.
- Log audit / retention policies.

Current state:

- The Kafka broker is declared in the top-level `docker-compose.yml` (single-node KRaft).
- A one-shot `kafka-init` service in the same compose pre-creates the `orders` topic
  with a single partition (replication factor 1) before the producer/consumer start.
- Both Go services emit structured JSON logs (one record per line on stdout), ready to
  be shipped to the monitoring stack once it is introduced here.