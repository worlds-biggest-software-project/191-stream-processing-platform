# Stream Processing Platform — Feature & Functionality Survey

> Candidate #191 · Researched: 2026-05-03

## Solutions Analysed

| Tool | Type | Licence / Model | URL |
|------|------|-----------------|-----|
| Apache Flink | Open-source stream processor | Apache 2.0 | https://flink.apache.org |
| Apache Kafka / ksqlDB | Open-source broker + stream-SQL layer | Apache 2.0 / Confluent Community Licence | https://ksqldb.io |
| RisingWave | Open-source streaming database | Apache 2.0 | https://risingwave.com |
| Apache Spark Structured Streaming | Open-source batch/stream engine | Apache 2.0 | https://spark.apache.org |
| Materialize | Commercial SaaS streaming database | Proprietary | https://materialize.com |
| Confluent Cloud (Flink) | Commercial managed streaming platform | Proprietary | https://confluent.io |
| Amazon Managed Service for Apache Flink | Commercial managed Flink (AWS) | Proprietary | https://aws.amazon.com/managed-flink |
| Redpanda | Open-source Kafka-compatible broker | BSL / Apache 2.0 (Connect) | https://redpanda.com |
| Timeplus / Proton | Open-source stream-SQL engine | Apache 2.0 | https://timeplus.com |
| DeltaStream | Commercial serverless managed Flink | Proprietary | https://deltastream.io |

---

## Feature Analysis by Solution

### Apache Flink

**Core features**
- True streaming with sub-second latency; exactly-once semantics via distributed snapshots
- Rich windowing: tumbling, sliding, session, and custom windows with event-time and processing-time support
- Stateful processing with RocksDB or heap-based state backends; Flink 2.x introduces disaggregated state (decoupled compute and storage)
- DataStream API (Java/Scala/Python) and Table/SQL API (ANSI SQL) on a unified runtime
- Complex Event Processing (CEP) library for pattern detection on event streams
- Flink 2.0 (March 2025): native AI/ML model inference in SQL, disaggregated state backend, Process Table Functions bridging SQL and DataStream
- Flink 2.2 (December 2025): Materialized Tables, Delta Joins, direct ML model inference via OpenAI and other provider integrations
- Connectors: Kafka, Kinesis, S3, JDBC, HBase, Elasticsearch, Iceberg, and many others

**Differentiating features**
- Decades of battle-hardening at companies like Alibaba, Uber, and Netflix
- Unified batch and stream processing on a single engine (DataSet API removed in 2.x, fully unified)
- Process Table Functions enable server-side stateful logic reusable in SQL queries

**UX patterns**
- Primarily programmatic (Java/Python SDK); SQL API lowers barrier but still requires JVM toolchain familiarity
- Flink dashboard for job monitoring; Flink SQL Gateway for JDBC/REST-based SQL access
- Steep initial learning curve for watermarks, state, and exactly-once configuration

**Integration points**
- REST API for job management and monitoring
- SQL Gateway (REST endpoint) for JDBC/HTTP SQL execution
- Confluent Cloud and AWS Managed Flink provide managed control planes
- Apache Iceberg, Delta Lake, and Hudi integration for lakehouse patterns

**Known gaps**
- Operational complexity remains high for self-hosted deployments (Kubernetes, ZooKeeper/KRaft, state backend tuning)
- JVM memory management is non-trivial; Python support is second-class compared to Java/Scala
- Schema management is not built-in; requires Confluent Schema Registry or equivalent
- Watermark strategy tuning requires deep domain expertise

**Licence / IP notes**
- Apache 2.0; no patent concerns for core engine. Confluent's managed Flink uses proprietary control-plane code.

---

### Apache Kafka / ksqlDB

**Core features**
- Kafka: distributed, partitioned, replicated commit-log with producer/consumer APIs and consumer group semantics
- Kafka Streams: Java library for stream processing embedded in applications
- ksqlDB: SQL engine over Kafka topics with push and pull queries; stream-table duality
- Schema Registry integration (Avro, Protobuf, JSON Schema)
- Kafka Connect framework: 200+ pre-built connectors for sources and sinks
- Kafka 3.x+ introduces KRaft (ZooKeeper-free metadata quorum)
- Share groups (2025): queue-like semantics with per-message acknowledgement within consumer groups

**Differentiating features**
- De-facto event streaming backbone; all major cloud providers and stream processors integrate with Kafka protocol
- ksqlDB pull queries allow low-latency point-in-time lookups on materialised tables
- Kafka Connect runs connectors inside the Kafka cluster, eliminating separate ingestion infrastructure

**UX patterns**
- Kafka: requires significant operational knowledge (partitioning, replication factor, retention)
- ksqlDB: SQL-first experience accessible via CLI, REST API, and Confluent Control Center UI
- Confluent Control Center provides a web GUI for topic management, consumer lag, and connector management

**Integration points**
- REST Proxy for HTTP-based produce/consume
- ksqlDB REST API for stream query submission
- Confluent Cloud API (OpenAPI-documented) for cluster management
- Connectors for RDBMS, SaaS, cloud storage, search engines, and data warehouses

**Known gaps**
- ksqlDB SQL dialect is non-standard (not ANSI SQL); limited subquery and JOIN support compared to full SQL engines
- ksqlDB stateful queries require careful partition alignment; complex joins are error-prone
- Confluent Community Licence for ksqlDB restricts commercial SaaS use without Confluent agreement
- Consumer lag monitoring and exactly-once semantics across multi-cluster topologies is complex

**Licence / IP notes**
- Kafka broker: Apache 2.0. ksqlDB: Confluent Community Licence (restricts competitive SaaS use). Schema Registry: Confluent Community Licence.

---

### RisingWave

**Core features**
- PostgreSQL-compatible SQL interface for defining sources, sinks, and materialized views
- Decoupled compute-storage architecture; state persisted to object storage (S3, GCS, MinIO)
- Exactly-once semantics via barrier-based checkpointing (snapshot consistency)
- Materialized views are always consistent across multi-way joins and complex pipelines
- Supports CDC ingestion (PostgreSQL, MySQL), Kafka, Kinesis, S3, Iceberg, and more
- v2.6 (September 2025): vector(n) data type and vector similarity search for real-time AI/RAG workloads
- Copy-on-Write (CoW) mode for Apache Iceberg sinks (high-frequency updates)
- Integrates with dbt Core for streaming transformations

**Differentiating features**
- Single system unifying CDC, SQL transformation, and lakehouse delivery (no separate sink ETL)
- Outperforms Flink on 22 of 27 Nexmark benchmarks while using PostgreSQL-compatible SQL
- Native vector storage eliminates the need for a separate vector database in AI pipelines

**UX patterns**
- psql-compatible client; any PostgreSQL tool (DBeaver, Metabase, Grafana) works out of the box
- No JVM, no separate schema registry required for simple deployments
- Kubernetes operator available for production deployments

**Integration points**
- PostgreSQL wire protocol (psql, JDBC, ODBC)
- Python SDK (risingwave-connect-py) for CDC sources and sinks
- Kubernetes operator API for infrastructure management
- Apache Iceberg, Delta Lake, and Hudi sinks
- dbt adapter for dbt-native transformations

**Known gaps**
- Younger ecosystem; fewer production case studies than Flink or Kafka Streams
- Connector library smaller than Kafka Connect's 200+
- Self-hosted operational documentation less mature than established tools
- No native web UI; monitoring relies on third-party Prometheus/Grafana

**Licence / IP notes**
- Apache 2.0 for core engine; no known patent concerns.

---

### Apache Spark Structured Streaming

**Core features**
- Micro-batch and continuous streaming modes over the Spark engine
- Unified batch/streaming API: same code handles both bounded and unbounded datasets
- Delta Lake integration: exactly-once guarantees with transaction log, Change Data Feed (CDF), and schema enforcement
- Auto-scaling via Databricks serverless compute
- SQL, Python (PySpark), Java, Scala, and R APIs
- Deep integration with Delta Lake for ACID transactions, schema evolution, and time travel on streaming writes

**Differentiating features**
- Single platform for ETL, ML training, batch analytics, and streaming (Lambda and Kappa architectures)
- Delta Lake Change Data Feed tracks row-level changes for downstream incremental processing
- Databricks ecosystem provides MLflow, Unity Catalog governance, and Photon execution engine

**UX patterns**
- Databricks notebooks provide interactive development with live query results
- Declarative SQL interface via Spark SQL; Python DataFrame API for programmatic pipelines
- Unified governance through Unity Catalog (data lineage, access control, audit)

**Integration points**
- Kafka, Kinesis, S3, GCS, ADLS sources and sinks
- Delta Lake, Apache Iceberg, and Hudi table formats
- MLflow for model registry and serving
- REST API via Databricks Jobs API and Connect for embedded use

**Known gaps**
- Micro-batch latency: minimum ~100ms (not suitable for sub-10ms use cases)
- Not true streaming: windowing semantics differ from native event-time engines
- Databricks managed service is expensive at scale; open-source Spark requires significant cluster management
- Complex stateful aggregations can cause state store bloat and long checkpoint times

**Licence / IP notes**
- Apache Spark: Apache 2.0. Delta Lake: Apache 2.0. Databricks platform: proprietary.

---

### Materialize

**Core features**
- Incrementally-maintained SQL views: results update as input data changes, not on query execution
- PostgreSQL wire-compatible (psql, JDBC, and any PG-compatible tool)
- Sources: Kafka/Redpanda topics, PostgreSQL CDC, MySQL CDC, webhooks
- Sinks: Kafka topics and object storage (S3/GCS via Kafka Connect)
- SUBSCRIBE command for live-streaming result changes to applications
- Complex joins, aggregations, and window functions over live data
- Strongly consistent: exactly-once semantics with linearisable reads

**Differentiating features**
- True incremental computation (differential dataflow): only recomputes affected rows on input change
- Acts as the live data layer for applications; downstream apps query Materialize like a database, not a stream
- SUBSCRIBE enables server-sent event patterns without polling

**UX patterns**
- SQL-only interaction; no custom SDK required — any PostgreSQL client works
- HTTP API accepts SQL queries via JSON POST (for non-PG-native integrations)
- Materialize Cloud provides a managed web console for source/sink configuration

**Integration points**
- PostgreSQL wire protocol
- HTTP REST API (https://materialize.com/docs/integrations/http-api/)
- Tools and integrations page covers Metabase, dbt, Grafana, Redash, and others
- CREATE CONNECTION syntax for declarative source/sink configuration

**Known gaps**
- Connector set more limited than Kafka Connect or Flink (primarily Kafka and PG/MySQL CDC)
- No native support for processing-time windows or session windows
- Credit-based pricing can become expensive at high data volumes
- Limited self-hosted option; primarily a managed cloud product

**Licence / IP notes**
- Proprietary SaaS; core differential dataflow engine is open source (Apache 2.0) but Materialize product is not.

---

### Confluent Cloud (Flink)

**Core features**
- Fully managed Apache Flink integrated with Confluent's Kafka platform
- Flink SQL environment accessible via Confluent Control Center and REST API
- Managed connectors: 120+ Kafka Connect connectors fully hosted
- Schema Registry integration (Avro, Protobuf, JSON Schema) with compatibility enforcement
- ksqlDB available alongside Flink for simpler SQL use cases
- Kafka REST Proxy for HTTP-based produce/consume without Kafka client libraries
- Role-based access control and end-to-end encryption

**Differentiating features**
- Tightest Kafka-Flink integration available: same control plane for broker, connectors, schema, and compute
- No JVM or Kubernetes management required for users
- Confluent Flink REST API (OpenAPI) for programmatic job management

**UX patterns**
- Web console (Control Center) for topic management, connector setup, and Flink query authoring
- Stream Designer: visual pipeline builder for drag-and-drop Kafka connector and Flink SQL pipelines
- Guided onboarding with pre-built pipeline templates

**Integration points**
- Kafka REST Proxy API (https://docs.confluent.io/cloud/current/kafka-rest/)
- ksqlDB REST API and Cluster API
- Flink REST API for job management
- Terraform provider and Confluent CLI for infrastructure-as-code

**Known gaps**
- Vendor lock-in; migrating Flink jobs and Kafka topics off Confluent is operationally heavy
- Expensive at scale: Confluent Flink billed per Confluent Flink Unit (CFU), Kafka billed per GB ingested
- ksqlDB not ANSI SQL; ksqlDB and Flink SQL require context switching
- Flink version lags slightly behind Apache Flink open-source releases

**Licence / IP notes**
- Platform is proprietary; underlying Flink and Kafka are Apache 2.0. ksqlDB under Confluent Community Licence.

---

### Amazon Managed Service for Apache Flink

**Core features**
- Fully managed Flink (formerly Kinesis Data Analytics for Apache Flink)
- Native AWS service integrations: Kinesis Data Streams, MSK (Kafka), S3, DynamoDB, Lambda
- 40+ source and destination connectors via Flink connector library
- Auto-scaling: adjusts Kinesis Processing Units (KPUs) based on CPU and throughput
- Exactly-once processing semantics; durable checkpointing to S3
- Flink Studio (Apache Zeppelin notebooks) for interactive SQL and Python development

**Differentiating features**
- Deep AWS IAM integration: no separate credential management for AWS services
- Flink SQL and Python APIs exposed in notebook environment for rapid iteration
- Integrated with AWS Glue Data Catalog for schema management

**UX patterns**
- AWS Console UI for application configuration, monitoring (CloudWatch), and scaling
- Managed Service for Apache Flink API (REST) for programmatic application lifecycle management
- AWS CDK and CloudFormation support for infrastructure-as-code

**Integration points**
- AWS SDK (Java, Python, Go, .NET) for application management
- AWS MSF API Reference: https://docs.aws.amazon.com/managed-flink/latest/java/api-link.html
- CloudWatch Metrics for operational monitoring
- Kinesis Data Streams, MSK, S3, Firehose connectors

**Known gaps**
- AWS-only; no multi-cloud deployment
- Lags open-source Flink releases by several months
- SQL-only applications limited compared to full DataStream API (notebook environment required for SQL-first workflows)
- Kinesis Data Analytics for SQL (legacy) discontinued January 2026

**Licence / IP notes**
- Managed service is proprietary AWS; underlying Flink is Apache 2.0.

---

### Redpanda

**Core features**
- Kafka-compatible broker written in C++ (no JVM); claims up to 10x lower latency vs. Kafka
- WebAssembly (WASM) in-broker data transforms: filter, scrub, redact, transcode without a separate stream processor
- Redpanda Connect: 300+ connectors packaged as a single Go binary (128 MiB)
- Single binary deployment with no ZooKeeper dependency (built-in Raft consensus)
- Tiered storage: hot data in local NVMe, cold data offloaded to object storage
- Transactions and exactly-once semantics compatible with Kafka producer/consumer APIs
- Shadow indexing for S3 cost-optimised long-term retention

**Differentiating features**
- WASM transforms run co-located with partition leaders: no separate compute cluster needed for simple transforms
- C++ implementation avoids JVM GC pauses; more predictable tail latency
- Redpanda Connect (formerly Benthos) provides a declarative YAML-based pipeline DSL

**UX patterns**
- rpk CLI for cluster management; compatible with Kafka CLI tools
- Redpanda Console (web UI) for topic browsing, consumer lag, and schema management
- Declarative YAML-based transform and connect pipeline definitions

**Integration points**
- Kafka wire protocol (100% API compatible for core operations)
- HTTP/Websocket sources and sinks via Redpanda Connect
- Wasm transform SDK (Go, Rust, JavaScript/TypeScript)
- Redpanda Cloud API for managed deployments

**Known gaps**
- Not 100% Kafka API coverage: some advanced Kafka features (e.g., full ACL parity, all configs) lag
- BSL licence for self-hosted Redpanda restricts competitive use; Apache 2.0 only for Redpanda Connect
- WASM transforms limited to per-message stateless transformations; no windowing or stateful aggregation
- Smaller managed cloud footprint than Confluent Cloud or AWS MSK

**Licence / IP notes**
- Redpanda broker: Business Source Licence (BSL); converts to Apache 2.0 after 4 years. Redpanda Connect: Apache 2.0. WASM SDK: Apache 2.0.

---

### Timeplus / Proton

**Core features**
- Single C++ binary (<500 MB) embedding ClickHouse query engine for OLAP plus streaming SQL extensions
- ANSI-compatible streaming SQL: tumble/hop/session windows, watermarks, materialized views, CDC
- Benchmarks: 90 million events/second EPS on Apple M2 Max; 4 ms end-to-end latency; 50 GB/s throughput (Timeplus 3.0)
- Proton Ingest REST API (port 3218) for HTTP-based data ingestion
- Kafka-native source integration; ClickHouse-compatible sinks
- Python, Java, Go, and REST SDK support

**Differentiating features**
- Combines streaming and OLAP in a single binary: ad-hoc historical queries and live stream queries over the same engine
- No JVM, no ZooKeeper: minimal operational footprint
- Open-source ksqlDB alternative with broader ANSI SQL support

**UX patterns**
- SQL-first; Proton connects to BI tools via JDBC/ClickHouse HTTP interface
- Interactive CLI for local development
- Timeplus Enterprise adds a web UI, RBAC, and managed cloud

**Integration points**
- Proton Ingest REST API: https://docs.timeplus.com/proton-ingest-api
- ClickHouse HTTP interface for queries
- Kafka source for reading from Kafka-compatible brokers
- Python, Java, Go SDK bindings

**Known gaps**
- Early-stage; smaller community and ecosystem than Flink or Kafka
- Limited connector library compared to Kafka Connect
- Clustering and high-availability for self-hosted is less documented than enterprise tools
- Timeplus Enterprise (web UI, RBAC) is commercial; Proton lacks a native web UI

**Licence / IP notes**
- Proton: Apache 2.0. Timeplus Enterprise: proprietary.

---

### DeltaStream

**Core features**
- Serverless managed stream processing built on Apache Flink
- SQL-based interface: create databases, streams, continuous queries, and materialized views via SQL
- Integrates with Kafka, Kinesis, and Redpanda as event sources/sinks
- DeltaStream Fusion (late 2025): unified streaming, real-time, and batch analytics in a single serverless platform
- Unified engine orchestration across Flink, Spark, and ClickHouse; no multi-tool management
- AI-assisted pipeline creation: natural-language-to-SQL pipeline authoring
- BYOC (bring your own cloud) and fully managed deployment options
- Available on AWS and Microsoft Azure

**Differentiating features**
- First commercially production-ready platform to unify Flink + Spark + ClickHouse under one SQL surface
- AI-assisted pipeline generation: describe requirements in English, receive SQL pipeline
- Multi-cloud and BYOC model reduces vendor lock-in compared to single-cloud managed services

**UX patterns**
- SQL-first web console; no Flink or Spark expertise required from end users
- Serverless: auto-provisioning and auto-scaling without cluster management
- Fusion platform provides a single surface for streaming, historical, and analytical queries

**Integration points**
- Kafka, Kinesis, Redpanda source/sink connectors
- REST API for pipeline management
- dbt integration for transformation workflows
- Terraform provider for infrastructure-as-code

**Known gaps**
- Relatively new product; production case studies limited
- Connector library smaller than established platforms
- Pricing information not fully public; enterprise-tier required for BYOC
- Dependency on Flink/Spark/ClickHouse versions managed by DeltaStream, not user-controlled

**Licence / IP notes**
- Proprietary SaaS; built on Apache-licenced open-source engines.

---

## Cross-Cutting Feature Themes

### Table-Stakes Features
- Exactly-once or at-least-once processing semantics with durable checkpointing
- Kafka protocol compatibility for source and sink connectivity
- SQL interface (at minimum SQL-like) for defining streaming transformations
- Tumbling, sliding, and session window aggregations with event-time watermarks
- Schema management (Avro, Protobuf, or JSON Schema) with compatibility enforcement
- Horizontal scalability and auto-scaling for variable workloads
- Monitoring and observability integration (metrics, lag tracking, job status)
- At least basic CDC (Change Data Capture) source support

### Differentiating Features
- Disaggregated compute-storage (RisingWave, Flink 2.x): enables independent scaling of compute and state
- In-broker WASM transforms (Redpanda): eliminates separate compute cluster for simple transformations
- Incrementally-maintained views (Materialize): results always up-to-date without polling
- Unified streaming + OLAP in a single binary (Timeplus/Proton): reduces infrastructure complexity
- AI-assisted pipeline authoring (DeltaStream, Flink 2.2): natural language to SQL
- Multi-engine orchestration (DeltaStream Fusion): Flink + Spark + ClickHouse under one surface
- Native vector storage for real-time AI/RAG workloads (RisingWave v2.6)

### Underserved Areas / Opportunities
- Natural language query interface: most platforms require SQL or Java; non-technical users are excluded
- Intelligent watermark and window strategy auto-configuration from data latency profiles
- Cross-pipeline observability with root-cause explanation (why is this pipeline late or producing bad data?)
- Unified schema evolution management across heterogeneous connectors without separate schema registry tooling
- Cost optimisation intelligence: auto-tiering of state to object storage based on access frequency
- Declarative pipeline testing: snapshot-based regression tests for streaming pipelines (analogous to dbt tests)
- Multi-tenancy and namespace isolation for platform teams serving many internal consumers
- Out-of-the-box integration with vector databases and embedding models for real-time RAG

### AI-Augmentation Candidates
- Natural language to streaming SQL translation (replacing manual SQL authoring for analysts)
- Automated anomaly detection on streams without hand-authored CEP rules
- AI-driven watermark strategy recommendation from historical event-time distributions
- Intelligent auto-scaling using upstream signals (campaign schedules, seasonality) rather than reactive lag-based scaling
- LLM-powered root-cause analysis for pipeline latency spikes and data quality incidents
- AI-assisted connector configuration and schema mapping

---

## Legal & IP Summary

The core open-source tools in this space (Apache Flink, RisingWave, Apache Kafka, Apache Spark, Timeplus Proton, Redpanda Connect) are all available under Apache 2.0 or equivalent permissive licences. The main IP risks are: (1) ksqlDB is under the Confluent Community Licence, which prohibits building a competing SaaS product without a commercial agreement; (2) Redpanda broker uses the Business Source Licence (BSL), which similarly restricts competitive SaaS use until the code converts to Apache 2.0 after 4 years; (3) Materialize's commercial product is proprietary, though its underlying differential dataflow library is Apache 2.0. No patents covering core stream processing algorithms (windowing, watermarks, exactly-once via distributed snapshots) were identified in public literature; these techniques derive from the academic Dataflow Model and MillWheel papers, which pre-date current commercial implementations. An AI-native open-source stream processing platform built on Apache 2.0-licenced components (Flink, RisingWave, Kafka/Redpanda Connect, Apache Beam) faces no licence compatibility concerns.

---

## Recommended Feature Scope

**Must-have (MVP)**
- SQL interface for defining streaming transformations, materialized views, and windowed aggregations
- Kafka source and sink connectors (Kafka protocol compatibility)
- Exactly-once semantics with durable checkpointing to object storage
- CDC source support (PostgreSQL, MySQL minimum)
- Basic monitoring: job status, consumer lag, checkpoint health via metrics API
- Schema registry integration (Avro/Protobuf/JSON Schema with compatibility enforcement)

**Should-have (v1.1)**
- Natural language to SQL translation using an embedded LLM for pipeline authoring
- AI-driven anomaly detection on streaming data without hand-authored rules
- Disaggregated state backend (compute-storage separation) for cloud-native scalability
- Declarative pipeline testing framework (snapshot-based regression tests)
- Web UI for pipeline management and observability
- Multi-engine support: Flink and/or Spark runner targets from a unified SQL surface

**Nice-to-have (backlog)**
- Native vector storage and similarity search for real-time AI/RAG workloads
- WASM in-engine transforms for lightweight stateless processing without a separate compute cluster
- AI-assisted watermark strategy recommendation
- Intelligent predictive auto-scaling from upstream signals
- LLM-powered root-cause explanation for pipeline incidents
- Multi-tenant namespace isolation for internal platform teams
