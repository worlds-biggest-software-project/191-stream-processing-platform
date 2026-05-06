# Stream Processing Platform

> Part of the [worlds-biggest-software-project](https://github.com/worlds-biggest-software-project) initiative.
>
> An AI-native, open-source platform for real-time event processing with a SQL-like query interface.

The Stream Processing Platform delivers real-time event processing built around a familiar SQL-like surface, targeted at data engineers, platform teams, and analytics builders. It aims to combine the maturity of established streaming engines with an AI-native authoring and operations experience, lowering the barrier for teams that today must choose between heavyweight Java-based engines or restrictive managed services.

---

## Why Stream Processing Platform?

- **Operational complexity of incumbents:** Apache Flink is mature and battle-tested but carries steep operational complexity and is JVM-heavy, requiring deep expertise in watermarks, state backends, and Kubernetes tuning.
- **Non-standard or restricted SQL:** ksqlDB uses a non-standard SQL dialect with limited subquery and JOIN support, and is licensed under the Confluent Community Licence, which restricts competitive SaaS use.
- **Vendor lock-in and cost at scale:** Confluent Cloud and Amazon Managed Service for Apache Flink offer convenience but lag open-source releases, lock users into a single cloud, and become expensive at scale (Confluent Flink billed per CFU; Kinesis ~$0.11/KPU-hr).
- **Micro-batch latency limits:** Apache Spark Structured Streaming is not true streaming — minimum micro-batch latency is around 100 ms, unsuitable for sub-10 ms use cases.
- **Underserved authoring experience:** Most platforms require SQL or Java, excluding non-technical users; watermark and window strategy tuning still demands deep domain expertise.

---

## Key Features

### SQL-First Streaming Core

- SQL interface for streaming transformations, materialized views, and windowed aggregations
- Tumbling, sliding, and session windows with event-time watermarks
- Exactly-once semantics with durable checkpointing to object storage
- CDC source support for PostgreSQL and MySQL at minimum

### Connectivity and Schema

- Kafka source and sink connectors with Kafka protocol compatibility
- Schema registry integration for Avro, Protobuf, and JSON Schema with compatibility enforcement
- Object storage integration for tiered state and lakehouse delivery

### AI-Assisted Authoring and Operations

- Natural language to streaming SQL translation using an embedded LLM
- AI-driven anomaly detection on streams without hand-authored CEP rules
- AI-assisted watermark strategy recommendation based on historical event-time distributions
- LLM-powered root-cause explanation for pipeline latency spikes and data quality incidents
- Intelligent predictive auto-scaling using upstream signals such as campaign schedules

### Cloud-Native Architecture

- Disaggregated state backend separating compute and storage for independent scaling
- Web UI for pipeline management and observability
- Declarative, snapshot-based pipeline regression testing
- Multi-engine targeting (e.g., Flink and/or Spark runners) from a unified SQL surface

### Real-Time AI Workloads (Backlog)

- Native vector storage and similarity search for real-time AI/RAG pipelines
- WASM in-engine transforms for lightweight stateless processing
- Multi-tenant namespace isolation for internal platform teams

---

## AI-Native Advantage

The platform pushes AI into both authoring and operations rather than treating it as a downstream consumer of streams. Analysts describe transformations in plain English and receive streaming SQL; watermark strategies and windowing are auto-suggested from historical data latency profiles; unsupervised models detect anomalous event patterns without hand-authored rules; and predictive auto-scaling acts on upstream signals before lag accumulates. LLM-driven observability explains latency spikes and data quality regressions in natural language.

---

## Tech Stack & Deployment

The recommended foundation is Apache 2.0-licenced open-source components — Apache Flink, RisingWave, Apache Kafka or Redpanda Connect, and Apache Beam — to avoid licence compatibility concerns with ksqlDB (Confluent Community Licence) or the Redpanda broker (BSL). The platform is expected to support self-hosted deployments via a Kubernetes operator and managed cloud delivery. Standards alignment includes the Apache Kafka protocol, ANSI SQL, the Apache Beam model, CloudEvents (CNCF), OpenTelemetry, and Avro/Protobuf/JSON Schema for event contracts.

---

## Market Context

The streaming analytics market is estimated at USD 4.34–18 billion in 2025 (depending on scope), projected to reach USD 7.75 billion (conservative) to USD 65+ billion (broad) by 2030–2035 at CAGRs of 12–28%. Self-hosted open-source tools are free but require significant engineering investment, while managed services charge $0.10–$0.55/hr per compute unit and full-stack vendors such as Confluent can reach $100K–$1M+/yr for large enterprises. Primary buyers are data engineers and platform teams at mid-to-large enterprises, fintech and e-commerce companies needing real-time fraud detection or personalisation, IoT platform builders, and analytics teams running live dashboards.

---

## Project Status

> This project is in the **research and specification phase**.  
> Contributions, feedback, and domain expertise are welcome.

---

## Contributing

We welcome contributions from developers, domain experts, and potential users.
See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

**Important:** All contributions must be your own original work or clearly attributed
open-source material with a compatible licence. Copyright infringement and licence
violations will not be tolerated and will result in immediate removal of the offending
contribution. If you are unsure whether a piece of code, text, or other material is
safe to contribute, open an issue and ask before submitting.

---

## Licence

Licence to be determined. See [discussion](#) for context.
