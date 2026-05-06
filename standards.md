# Standards & API Reference

> Project: Stream Processing Platform · Generated: 2026-05-03

## Industry Standards & Specifications

### ISO/IEC Standards

**ISO/IEC 19464 — Advanced Message Queuing Protocol (AMQP) 1.0**
- URL: https://www.iso.org/standard/64955.html
- Approved as an ISO/IEC International Standard in April 2014. AMQP 1.0 is a binary, application-layer, message-queuing protocol providing flow-controlled, message-oriented communication with at-most-once, at-least-once, and exactly-once delivery guarantees. Directly relevant as a transport-layer standard for stream processing event ingestion.

**ISO 8000 — Data Quality**
- URL: https://www.iso.org/standard/60805.html
- Defines requirements for data quality and data portability in master data. The principles of data availability via real-time APIs are referenced in this standard. Relevant to stream processing platforms ensuring data quality and correctness guarantees in event pipelines.

**ISO/IEC 23009-1 — Dynamic Adaptive Streaming over HTTP (DASH)**
- URL: https://www.iso.org/standard/57623.html
- Specifies Media Presentation Description (MPD) and segment formats for adaptive streaming over HTTP. Covers event message streams signalled with InbandEventStream elements. Relevant for media and IoT streaming use cases built on stream processing platforms.

---

### W3C & IETF Standards

**CloudEvents v1.0 (CNCF Graduated Standard)**
- URL: https://cloudevents.io / https://github.com/cloudevents/spec
- A CNCF-graduated specification (January 2024) defining a standard envelope schema for event metadata (context attributes) to provide interoperability across services, platforms, and systems. CloudEvents SQL (V1, approved June 2024) provides a standardised way to query and filter CloudEvents. Highly relevant as the canonical event format for cross-system stream processing interoperability.

**RFC 5277 — NETCONF Event Notifications**
- URL: https://datatracker.ietf.org/doc/rfc5277/
- Defines NETCONF protocol operations for subscribing to and receiving event notifications from network devices. Relevant for IoT and network telemetry streaming pipelines that ingest NETCONF event streams.

**RFC 9431 — MQTT and TLS Profile for ACE Framework**
- URL: https://datatracker.ietf.org/doc/rfc9431/
- Specifies an authentication and authorisation profile for publish-subscribe messaging using MQTT over TLS. Directly relevant for IoT edge-to-cloud streaming data pipelines using MQTT as the transport layer.

**RFC 8895 — ALTO Incremental Updates Using Server-Sent Events (SSE)**
- URL: https://datatracker.ietf.org/doc/html/rfc8895
- Defines use of Server-Sent Events (SSE) for streaming incremental data updates over HTTP. Relevant for platforms that expose live streaming results to web clients (analogous to Materialize's SUBSCRIBE and ksqlDB push queries).

**RFC 7826 — Real-Time Streaming Protocol 2.0 (RTSP)**
- URL: https://datatracker.ietf.org/doc/html/rfc7826
- Defines the application-level protocol for controlling delivery of real-time data. Relevant for media and multimedia stream processing use cases.

---

### Data Model & API Specifications

**Apache Kafka Wire Protocol**
- URL: https://kafka.apache.org/protocol / https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
- De-facto binary wire protocol for event streaming; implemented by Kafka, Redpanda, AWS MSK, Azure Event Hubs in Kafka mode, and many others. Defines producer, consumer, admin, and streams APIs. Any stream processing platform targeting broad ecosystem compatibility must support the Kafka wire protocol.

**Apache Avro Specification**
- URL: https://avro.apache.org/docs/current/spec.html
- JSON-based schema definition language with compact binary serialisation. The standard schema format for Kafka-based streaming pipelines in data lake and warehouse contexts. Supports rich schema evolution (forward, backward, full compatibility).

**Protocol Buffers (Protobuf) v3**
- URL: https://protobuf.dev/programming-guides/proto3/
- Google's language-neutral, platform-neutral binary serialisation format. Most compact and highest-performance serialisation format for streaming data; ~20–30% smaller than Avro. Widely used in microservice-to-stream integrations.

**JSON Schema (Draft 2020-12)**
- URL: https://json-schema.org/specification
- Vocabulary for annotating and validating JSON documents. Used in schema registries alongside Avro and Protobuf for teams preferring human-readable event payloads. All three formats are supported by Confluent Schema Registry and compatible systems.

**Apache Beam Model**
- URL: https://beam.apache.org/documentation/programming-guide/
- Unified programming model for defining batch and streaming data pipelines that run on multiple execution engines (Flink, Spark, Google Dataflow, Hazelcast Jet). Core concepts: PCollection (bounded or unbounded data), PTransform, Pipeline. Relevant as an abstraction layer for multi-runner stream processing platforms.

**OpenAPI Specification 3.1**
- URL: https://spec.openapis.org/oas/v3.1.0
- Industry-standard format for documenting RESTful APIs. Confluent Cloud, Materialize, RisingWave, and Timeplus Proton all expose HTTP management or ingest APIs; OpenAPI 3.1 documentation is the appropriate standard for these interfaces.

**AsyncAPI Specification 3.0**
- URL: https://www.asyncapi.com/docs/reference/specification/v3.0.0
- Emerging standard for documenting event-driven and asynchronous APIs (Kafka, MQTT, AMQP, WebSockets). Analogous to OpenAPI but for message-based interfaces. Increasingly relevant for documenting streaming platform connectors and schemas.

---

### Security & Authentication Standards

**OAuth 2.0 (RFC 6749) and OpenID Connect 1.0**
- URL: https://datatracker.ietf.org/doc/html/rfc6749 / https://openid.net/connect/
- Industry-standard authorisation and authentication protocols. Confluent Cloud, Materialize, AWS Managed Flink, and DeltaStream all use OAuth 2.0 / OIDC for API authentication. Required for enterprise-grade stream processing platform security.

**SASL/SCRAM and SASL/GSSAPI (Kerberos)**
- URL: https://kafka.apache.org/documentation/#security_sasl
- Kafka's built-in authentication mechanisms. SASL/SCRAM (username/password) is the standard for cloud deployments; SASL/GSSAPI (Kerberos) is required for on-premises enterprise deployments with Active Directory.

**TLS 1.3 (RFC 8446)**
- URL: https://datatracker.ietf.org/doc/html/rfc8446
- Mandatory transport encryption standard. All production stream processing deployments must encrypt data in transit using TLS 1.3. Kafka, Flink REST API, and all cloud-managed services enforce TLS.

**OWASP API Security Top 10 (2023)**
- URL: https://owasp.org/www-project-api-security/
- Defines the most critical API security risks. Relevant for the REST management and ingest APIs exposed by stream processing platforms (injection, broken authentication, excessive data exposure).

**GDPR (EU) 2016/679 — Data Protection Regulation**
- URL: https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A32016R0679
- European data protection regulation. Stream processing platforms that process personal data must implement data minimisation, right-to-erasure (compaction/deletion in event logs), and audit logging. Redpanda's WASM transforms are explicitly cited for GDPR use cases (PII redaction).

---

### MCP Server Specifications

**Model Context Protocol (MCP) — Anthropic**
- URL: https://modelcontextprotocol.io/specification
- Open protocol enabling LLMs to connect to external data sources and tools. Relevant for building an AI-native stream processing platform that exposes pipeline status, data quality metrics, and transformation suggestions to LLM agents via MCP tools and resources. A stream processing MCP server could expose: current pipeline topology, consumer lag metrics, schema registry contents, and a SQL execution tool.

---

## Similar Products — Developer Documentation & APIs

### Apache Flink
- **Description:** Stateful stream processing engine with Java, Python, and SQL APIs; supports exactly-once semantics, windowing, and CEP. Flink 2.x adds native AI/ML inference.
- **API Documentation:** https://nightlies.apache.org/flink/flink-docs-stable/
- **REST API:** https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/
- **SQL Gateway REST API:** https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql-gateway/rest/
- **SDKs/Libraries:** Java (native), Python (PyFlink), Scala; Maven: `org.apache.flink:flink-java`
- **Developer Guide:** https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/overview/
- **Standards:** REST/JSON for job management; SQL Gateway exposes JDBC; OpenAPI-documented
- **Authentication:** Kerberos/SASL for cluster auth; REST API uses basic auth or token

### Apache Kafka
- **Description:** Distributed event streaming platform; de-facto broker for stream processing pipelines; provides producer, consumer, streams, connect, and admin APIs.
- **API Documentation:** https://kafka.apache.org/documentation/
- **Protocol Specification:** https://kafka.apache.org/protocol
- **SDKs/Libraries:** Java (librdkafka, kafka-clients), Python (confluent-kafka, kafka-python), Go (confluent-kafka-go), .NET (Confluent.Kafka), Node.js (kafkajs)
- **Developer Guide:** https://kafka.apache.org/quickstart
- **Standards:** Custom binary wire protocol; KRaft (Raft-based metadata quorum, ZooKeeper-free); Kafka Connect REST API
- **Authentication:** SASL/SCRAM, SASL/GSSAPI (Kerberos), TLS mutual auth

### Confluent Cloud
- **Description:** Fully managed Kafka + Flink + ksqlDB platform with 120+ connectors, Schema Registry, and stream governance.
- **API Documentation:** https://docs.confluent.io/cloud/current/api.html
- **Kafka REST API:** https://docs.confluent.io/cloud/current/kafka-rest/kafka-rest-cc.html
- **ksqlDB REST API:** https://docs.confluent.io/cloud/current/ksqldb/ksqldb-cluster-api.html
- **SDKs/Libraries:** Confluent CLI, Terraform provider, Python/Java/Go/Node.js Kafka clients
- **Developer Guide:** https://developer.confluent.io/
- **Standards:** REST/JSON; OpenAPI-documented; OAuth 2.0 + API key/secret authentication
- **Authentication:** Basic (API key + secret), OAuth 2.0

### RisingWave
- **Description:** PostgreSQL-compatible streaming database; ingests, transforms, and serves real-time event streams via materialized views; includes native vector storage.
- **API Documentation:** https://docs.risingwave.com/
- **Developer Guide:** https://risingwavelabs.github.io/risingwave/
- **SDKs/Libraries:** PostgreSQL wire protocol (any PG client); Python SDK: https://github.com/risingwavelabs/risingwave-connect-py; Kubernetes operator: https://github.com/risingwavelabs/risingwave-operator
- **Connectors Documentation:** https://tutorials.risingwave.com/docs/basics/connector/
- **Standards:** PostgreSQL wire protocol (JDBC, ODBC, psql); Apache Iceberg sink spec
- **Authentication:** PostgreSQL password authentication; Kubernetes RBAC for operator

### Materialize
- **Description:** Streaming SQL database maintaining incrementally-updated materialized views over Kafka and CDC sources; PostgreSQL wire-compatible.
- **API Documentation:** https://materialize.com/docs/
- **HTTP API:** https://materialize.com/docs/integrations/http-api/
- **Tools & Integrations:** https://materialize.com/docs/integrations/
- **SDKs/Libraries:** Any PostgreSQL client (psql, JDBC, ODBC); no dedicated SDK required
- **Developer Guide:** https://materialize.com/docs/get-started/
- **Standards:** PostgreSQL wire protocol; REST/JSON HTTP API
- **Authentication:** Basic auth (email + app password) for HTTP API; PostgreSQL password auth for wire protocol

### Timeplus / Proton
- **Description:** Open-source C++ stream processing engine combining ClickHouse OLAP with streaming SQL; single binary, no JVM.
- **API Documentation:** https://docs.timeplus.com/proton
- **Ingest REST API:** https://docs.timeplus.com/proton-ingest-api
- **GitHub:** https://github.com/timeplus-io/proton
- **SDKs/Libraries:** REST API (port 3218); Python, Java, Go SDK bindings; ClickHouse HTTP interface for queries
- **Developer Guide:** https://www.timeplus.com/proton
- **Standards:** ClickHouse HTTP interface; REST/JSON ingest API
- **Authentication:** HTTP basic auth for REST ingest API

### Amazon Managed Service for Apache Flink
- **Description:** Fully managed Apache Flink on AWS; integrates with Kinesis, MSK, S3, and other AWS services; auto-scales KPUs.
- **API Documentation:** https://docs.aws.amazon.com/managed-flink/latest/java/how-it-works.html
- **API Reference:** https://docs.aws.amazon.com/managed-flink/latest/java/api-link.html
- **Connectors:** https://docs.aws.amazon.com/managed-flink/latest/java/how-connectors.html
- **SDKs/Libraries:** AWS SDK for Java 2.x, Python (boto3), Go, .NET, CLI
- **Developer Guide:** https://docs.aws.amazon.com/managed-flink/latest/java/getting-started.html
- **Standards:** Flink REST API; AWS SDK REST; OpenAPI via AWS service model
- **Authentication:** AWS IAM (SigV4 signatures); no separate credential management for AWS service connectors

### Redpanda
- **Description:** Kafka-compatible C++ broker with embedded WASM transforms and 300+ Redpanda Connect connectors; claims 10x lower latency than Kafka.
- **API Documentation:** https://docs.redpanda.com/
- **WASM Transforms Docs:** https://docs.redpanda.com/current/develop/data-transforms/how-transforms-work/
- **Redpanda Connect (Benthos):** https://docs.redpanda.com/redpanda-connect/
- **SDKs/Libraries:** Kafka-compatible clients (all languages); rpk CLI; WASM SDK (Go, Rust, JavaScript/TypeScript)
- **Developer Guide:** https://www.redpanda.com/what-is-redpanda
- **Standards:** Kafka wire protocol; Wasm binary standard (WASI); YAML-based Connect DSL
- **Authentication:** SASL/SCRAM, mTLS, Redpanda Cloud API key

### DeltaStream
- **Description:** Serverless managed stream processing platform built on Flink + Spark + ClickHouse (DeltaStream Fusion); SQL-first with AI-assisted pipeline authoring.
- **API Documentation:** https://docs.deltastream.io/
- **Product Page:** https://www.deltastream.io/serverless-stream-processing/
- **SDKs/Libraries:** REST API, Terraform provider, dbt integration
- **Developer Guide:** https://www.deltastream.io/blog/
- **Standards:** SQL (Flink SQL dialect); REST/JSON management API
- **Authentication:** API key + OAuth 2.0

---

## Notes

**Emerging Standards to Watch**
- **AsyncAPI 3.0**: Rapidly gaining adoption as the OpenAPI equivalent for event-driven architectures. Not yet universally supported but recommended for documenting Kafka-based APIs in new projects.
- **Apache Iceberg REST Catalog Specification**: Becoming the standard table format REST interface for lakehouse integration. Flink, Spark, RisingWave, and others are converging on this for streaming-to-lakehouse delivery.
- **OpenTelemetry Semantic Conventions for Messaging**: The OTel project is standardising metric names, span attributes, and trace context propagation for messaging systems (Kafka, RabbitMQ, AMQP). Adopting these conventions ensures observability data is portable across monitoring vendors.
- **Model Context Protocol (MCP)**: Still young but strategically important for AI-native platforms. Defining an MCP server interface for a stream processing platform would enable LLM agents to introspect pipeline topology, query metrics, and author SQL transformations autonomously.

**Areas Where Standards Are Still Evolving**
- No single ISO/IEC standard governs stream processing semantics (watermarks, exactly-once, windowing). These concepts remain de-facto standards derived from academic papers (the Dataflow Model, MillWheel).
- Schema registry interoperability: Confluent Schema Registry has a de-facto REST API that Redpanda, AWS Glue, and Apicurio Registry all implement, but it has not been formalised as an open standard.
- Cross-platform pipeline portability: Apache Beam addresses this at the SDK level, but there is no standard serialisation format for streaming pipeline definitions equivalent to OpenAPI for REST APIs.
