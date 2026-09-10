# infra

This directory holds infrastructure related to Kafka, monitoring, and log auditing.

Planned contents (not yet added):

- Kafka broker configuration / tuning (production profiles, topic configs).
- Observability stack: Alloy / Loki / Grafana log shipping & dashboards.
- Log audit / retention policies.

Currently the Kafka broker itself is declared in the top-level `docker-compose.yml`.
This directory will grow as monitoring and log-auditing tooling is introduced.