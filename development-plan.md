# Stream Processing Platform — Phased Development Plan

> Project: 191-stream-processing-platform · Created: 2026-05-25
> Purpose: Provide sufficient detail for Claude Code (Opus) to implement each phase end-to-end.

---

## Technology Decisions

| Concern | Choice | Rationale |
|---------|--------|-----------|
| Primary language | **Rust** (engine core) + **TypeScript** (API/UI) | Rust provides the low-latency, zero-GC performance needed for sub-10ms stream processing — matching Redpanda/Timeplus C++ performance without the memory-safety risks. TypeScript handles the control plane API and web UI where developer velocity matters more than nanosecond latency. |
| Stream engine runtime | **Custom Rust engine** with Apache Arrow in-memory format | Arrow columnar format enables vectorized SQL execution on event batches. Building on Arrow (Apache 2.0) avoids JVM overhead that plagues Flink and the licence restrictions of ksqlDB/Redpanda broker. |
| SQL parser & planner | **sqlparser-rs** + **DataFusion** query planner | DataFusion (Apache 2.0) provides a production-grade SQL query planner, optimizer, and execution engine in Rust. sqlparser-rs handles ANSI SQL parsing. Together they deliver the SQL-native interface without building a query engine from scratch. |
| API framework | **Axum** (Rust HTTP API) + **Fastify** (TypeScript control plane) | Axum integrates with Tokio for async Rust; Fastify is the fastest Node.js framework and supports OpenAPI 3.1 schema generation via @fastify/swagger. |
| Database | **PostgreSQL 16** | The control plane metadata store uses the Hybrid Relational + JSONB model (Data Model Suggestion 3). PostgreSQL provides typed columns for identity/relationships and JSONB for variable connector/pipeline configs. GIN indexes enable fast containment queries. |
| Message broker | **Apache Kafka** (protocol-compatible; Redpanda as alternative) | Kafka protocol is the de-facto standard. The engine implements Kafka consumer/producer APIs directly. Redpanda is a drop-in alternative for teams wanting lower latency. |
| Schema registry | **Embedded schema registry** (Confluent Schema Registry API-compatible) | Implements the Confluent Schema Registry REST API for Avro, Protobuf, and JSON Schema compatibility. Avoids external dependency while maintaining ecosystem compatibility. |
| Object storage | **S3-compatible** (AWS S3, MinIO, GCS via S3 API) | Checkpoint state and tiered storage use the S3 API. MinIO provides a local development option. |
| Frontend | **React 19** + **Vite** + **TanStack Router** + **Tailwind CSS** | Modern React stack with server components support. TanStack Router for type-safe routing. Tailwind for rapid UI development. |
| State management (UI) | **TanStack Query** | Server-state management with caching, background refetching, and optimistic updates — ideal for the dashboard-heavy observability UI. |
| LLM integration | **Anthropic Claude API** (primary) + **OpenAI-compatible API** (fallback) | Claude for NL-to-SQL translation and root-cause analysis. OpenAI-compatible interface allows self-hosted model substitution. |
| Containerisation | **Docker** + **Docker Compose** (dev) + **Kubernetes Operator** (prod) | Docker Compose for local development with all dependencies. Custom Kubernetes operator for production deployment with auto-scaling. |
| Testing (Rust) | **cargo test** + **proptest** (property-based) + **criterion** (benchmarks) | Standard Rust testing with property-based tests for SQL engine correctness and criterion for latency benchmarks. |
| Testing (TypeScript) | **Vitest** + **Playwright** (E2E) + **Supertest** (API) | Vitest for unit/integration, Playwright for browser E2E, Supertest for HTTP API testing. |
| Code quality | **clippy** + **rustfmt** (Rust) · **ESLint** + **Prettier** (TypeScript) · **oxlint** (fast lint) | Standard tooling for both language ecosystems. |
| CI/CD | **GitHub Actions** | Multi-stage pipeline: lint, test, build Docker images, publish to container registry. |
| Observability | **OpenTelemetry SDK** (Rust + TypeScript) | OTel traces, metrics, and logs from both the engine and control plane. Exports to Prometheus/Jaeger/OTLP-compatible backends. |
| Package managers | **Cargo** (Rust) + **pnpm** (TypeScript) | Cargo is the standard Rust package manager. pnpm for fast, disk-efficient Node.js dependency management. |
| Event format | **CloudEvents v1.0** envelope | All internal platform events use CloudEvents context attributes for cross-system interoperability per CNCF standard. |
| Serialisation | **Apache Avro** (primary) + **Protobuf** + **JSON Schema** | Avro for schema-evolved streaming data; Protobuf for high-throughput internal RPC; JSON Schema for human-readable payloads. |

---

### Project Structure

```
stream-processing-platform/
├── Cargo.toml                          # Rust workspace root
├── Dockerfile                          # Multi-stage: engine + control-plane
├── docker-compose.yml                  # Local dev: Kafka, PostgreSQL, MinIO, engine, API, UI
├── .github/
│   └── workflows/
│       ├── ci.yml                      # Lint, test, build on every PR
│       └── release.yml                 # Build + push Docker images on tag
├── crates/
│   ├── engine-core/                    # Stream processing engine
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── runtime/                # Task scheduling, checkpointing, watermarks
│   │       │   ├── mod.rs
│   │       │   ├── task_manager.rs
│   │       │   ├── checkpoint.rs
│   │       │   └── watermark.rs
│   │       ├── sql/                    # SQL parsing, planning, optimisation
│   │       │   ├── mod.rs
│   │       │   ├── parser.rs
│   │       │   ├── planner.rs
│   │       │   └── optimizer.rs
│   │       ├── execution/              # Operators: filter, project, aggregate, join, window
│   │       │   ├── mod.rs
│   │       │   ├── operator.rs
│   │       │   ├── filter.rs
│   │       │   ├── aggregate.rs
│   │       │   ├── window.rs
│   │       │   └── join.rs
│   │       ├── connectors/             # Source and sink connectors
│   │       │   ├── mod.rs
│   │       │   ├── kafka.rs
│   │       │   ├── cdc_postgres.rs
│   │       │   ├── cdc_mysql.rs
│   │       │   └── s3.rs
│   │       ├── state/                  # State backend (RocksDB local + S3 tiered)
│   │       │   ├── mod.rs
│   │       │   ├── rocksdb_backend.rs
│   │       │   └── s3_backend.rs
│   │       ├── schema/                 # Schema registry client + serde
│   │       │   ├── mod.rs
│   │       │   ├── avro.rs
│   │       │   ├── protobuf.rs
│   │       │   └── json_schema.rs
│   │       └── metrics/                # OTel metrics + tracing
│   │           ├── mod.rs
│   │           └── otel.rs
│   ├── engine-api/                     # Engine gRPC/REST API (job submission, status)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs
│   │       ├── grpc.rs
│   │       └── rest.rs
│   └── schema-registry/               # Embedded schema registry
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs
│           ├── api.rs                  # Confluent-compatible REST API
│           ├── storage.rs
│           └── compatibility.rs
├── control-plane/                      # TypeScript control plane
│   ├── package.json
│   ├── tsconfig.json
│   ├── src/
│   │   ├── index.ts                    # Fastify server entry
│   │   ├── config.ts                   # Environment + YAML config
│   │   ├── db/
│   │   │   ├── schema.ts              # Drizzle ORM schema (matches data model)
│   │   │   ├── migrations/            # SQL migration files
│   │   │   └── client.ts
│   │   ├── routes/
│   │   │   ├── tenants.ts
│   │   │   ├── pipelines.ts
│   │   │   ├── connectors.ts
│   │   │   ├── deployments.ts
│   │   │   ├── schemas.ts
│   │   │   ├── alerts.ts
│   │   │   └── ai.ts
│   │   ├── services/
│   │   │   ├── pipeline.service.ts
│   │   │   ├── connector.service.ts
│   │   │   ├── deployment.service.ts
│   │   │   ├── schema-registry.service.ts
│   │   │   ├── alert.service.ts
│   │   │   └── ai.service.ts
│   │   ├── auth/
│   │   │   ├── jwt.ts
│   │   │   ├── api-key.ts
│   │   │   └── rbac.ts
│   │   └── integrations/
│   │       ├── engine-client.ts        # gRPC client to engine-api
│   │       ├── llm-client.ts           # Claude/OpenAI API client
│   │       └── otel.ts
│   └── tests/
│       ├── unit/
│       ├── integration/
│       └── fixtures/
├── web-ui/                             # React frontend
│   ├── package.json
│   ├── vite.config.ts
│   ├── src/
│   │   ├── main.tsx
│   │   ├── routes/
│   │   │   ├── dashboard.tsx
│   │   │   ├── pipelines/
│   │   │   ├── connectors/
│   │   │   ├── schemas/
│   │   │   ├── deployments/
│   │   │   ├── alerts/
│   │   │   └── settings/
│   │   ├── components/
│   │   │   ├── sql-editor/             # Monaco-based SQL editor
│   │   │   ├── pipeline-graph/         # DAG visualisation
│   │   │   ├── metrics-charts/         # Real-time metrics
│   │   │   └── nl-query/               # Natural language input
│   │   ├── hooks/
│   │   ├── lib/
│   │   │   ├── api-client.ts
│   │   │   └── types.ts
│   │   └── styles/
│   └── tests/
│       ├── unit/
│       └── e2e/
├── k8s/                                # Kubernetes operator and manifests
│   ├── operator/
│   │   ├── Cargo.toml
│   │   └── src/
│   └── manifests/
│       ├── crds/
│       └── helm/
├── tests/                              # Cross-component integration tests
│   ├── pipeline-e2e/
│   ├── connector-e2e/
│   └── fixtures/
│       ├── avro-schemas/
│       ├── sample-events/
│       └── sql-queries/
└── docs/
    ├── architecture.md
    ├── api-reference.md
    └── deployment-guide.md
```

---

## Phase 1: Foundation — Project Scaffolding and Core Abstractions

### Purpose

Establish the monorepo workspace, build tooling, CI pipeline, and core data types shared across all components. After this phase, every subsequent phase can compile, lint, test, and build Docker images from day one. This phase also implements the PostgreSQL database schema and migrations.

### Tasks

#### 1.1 — Rust Workspace and Build Configuration

**What**: Create the Cargo workspace with three crates (engine-core, engine-api, schema-registry) and configure build tooling.

**Design**:

```toml
# Cargo.toml (workspace root)
[workspace]
resolver = "2"
members = [
    "crates/engine-core",
    "crates/engine-api",
    "crates/schema-registry",
]

[workspace.dependencies]
tokio = { version = "1.38", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
uuid = { version = "1.10", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
opentelemetry = "0.24"
opentelemetry-otlp = "0.17"
thiserror = "2.0"
anyhow = "1.0"
arrow = "53"
datafusion = "42"
sqlparser = "0.50"
rdkafka = { version = "0.37", features = ["cmake-build"] }
tonic = "0.12"
prost = "0.13"
axum = "0.8"
rocksdb = "0.22"
aws-sdk-s3 = "1.50"
```

Core shared types across crates:

```rust
// crates/engine-core/src/lib.rs

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for a pipeline deployment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeploymentId(pub Uuid);

/// Unique identifier for a pipeline
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PipelineId(pub Uuid);

/// Unique identifier for a tenant
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantId(pub Uuid);

/// Processing semantics guarantee
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryGuarantee {
    AtLeastOnce,
    ExactlyOnce,
}

/// Lifecycle state of a pipeline deployment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeploymentStatus {
    Pending,
    Deploying,
    Running,
    Stopping,
    Stopped,
    Failed,
    Cancelled,
}

/// CloudEvents v1.0 envelope for all platform events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudEvent<T: Serialize> {
    pub specversion: String,   // always "1.0"
    pub id: Uuid,
    pub source: String,        // e.g. "/tenants/{id}/pipelines/{id}"
    #[serde(rename = "type")]
    pub event_type: String,    // e.g. "com.streamplatform.pipeline.created"
    pub time: DateTime<Utc>,
    pub datacontenttype: String,
    pub data: T,
}

/// Window type for streaming aggregations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowType {
    Tumbling { size_ms: u64 },
    Sliding { size_ms: u64, slide_ms: u64 },
    Session { gap_ms: u64 },
}

/// Watermark strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WatermarkStrategy {
    BoundedOutOfOrderness { max_delay_ms: u64 },
    Monotonous,
    AiRecommended { max_delay_ms: u64, confidence: f64 },
}

/// Checkpoint trigger configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointConfig {
    pub interval_ms: u64,
    pub min_pause_ms: u64,
    pub timeout_ms: u64,
    pub max_concurrent: u32,
    pub storage_path: String,  // s3://bucket/prefix
}
```

**Testing**:
- `Unit: DeploymentStatus serializes to lowercase JSON string`
- `Unit: CloudEvent<T> serializes with correct specversion "1.0"`
- `Unit: WindowType::Tumbling round-trips through serde_json`
- `Unit: CheckpointConfig defaults are sane (interval=60s, timeout=600s)`
- `Build: cargo build --workspace succeeds with no warnings`
- `Lint: cargo clippy --workspace -- -D warnings passes`

#### 1.2 — TypeScript Control Plane Scaffolding

**What**: Create the Fastify-based control plane with Drizzle ORM, OpenAPI generation, and structured logging.

**Design**:

```json
// control-plane/package.json (key dependencies)
{
  "dependencies": {
    "fastify": "^5.0.0",
    "@fastify/swagger": "^9.0.0",
    "@fastify/swagger-ui": "^5.0.0",
    "@fastify/cors": "^10.0.0",
    "@fastify/jwt": "^9.0.0",
    "drizzle-orm": "^0.35.0",
    "drizzle-kit": "^0.28.0",
    "pg": "^8.13.0",
    "zod": "^3.23.0",
    "pino": "^9.5.0",
    "@opentelemetry/sdk-node": "^0.55.0",
    "@opentelemetry/exporter-trace-otlp-http": "^0.55.0"
  }
}
```

```typescript
// control-plane/src/config.ts
import { z } from 'zod';

export const ConfigSchema = z.object({
  port: z.number().default(8080),
  host: z.string().default('0.0.0.0'),
  database: z.object({
    url: z.string(),
    maxConnections: z.number().default(20),
    ssl: z.boolean().default(false),
  }),
  engine: z.object({
    grpcUrl: z.string().default('localhost:50051'),
  }),
  auth: z.object({
    jwtSecret: z.string().min(32),
    jwtExpiresIn: z.string().default('24h'),
    apiKeyHashRounds: z.number().default(12),
  }),
  otel: z.object({
    endpoint: z.string().optional(),
    serviceName: z.string().default('stream-platform-control-plane'),
  }),
  llm: z.object({
    provider: z.enum(['anthropic', 'openai']).default('anthropic'),
    apiKey: z.string(),
    model: z.string().default('claude-sonnet-4-20250514'),
    maxTokens: z.number().default(4096),
  }),
});

export type Config = z.infer<typeof ConfigSchema>;
```

```typescript
// control-plane/src/index.ts
import Fastify from 'fastify';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import cors from '@fastify/cors';

export async function buildApp(config: Config) {
  const app = Fastify({
    logger: {
      level: 'info',
      transport: { target: 'pino-pretty' },
    },
    genReqId: () => crypto.randomUUID(),
  });

  await app.register(cors, { origin: true });
  await app.register(swagger, {
    openapi: {
      info: {
        title: 'Stream Processing Platform API',
        version: '0.1.0',
        description: 'Control plane API for the stream processing platform',
      },
      servers: [{ url: `http://${config.host}:${config.port}` }],
      components: {
        securitySchemes: {
          bearerAuth: { type: 'http', scheme: 'bearer', bearerFormat: 'JWT' },
          apiKey: { type: 'apiKey', in: 'header', name: 'X-API-Key' },
        },
      },
    },
  });
  await app.register(swaggerUi, { routePrefix: '/docs' });

  // Health check
  app.get('/health', async () => ({ status: 'ok', timestamp: new Date().toISOString() }));

  return app;
}
```

**Testing**:
- `Unit: ConfigSchema validates correct config object`
- `Unit: ConfigSchema rejects missing jwtSecret`
- `Unit: ConfigSchema applies correct defaults for port, host, maxConnections`
- `Integration: GET /health returns 200 with status "ok"`
- `Integration: GET /docs returns Swagger UI HTML`
- `Integration: GET /docs/json returns valid OpenAPI 3.1 JSON`

#### 1.3 — Database Schema and Migrations

**What**: Implement the PostgreSQL schema based on Data Model Suggestion 3 (Hybrid Relational + JSONB) using Drizzle ORM migrations.

**Design**:

The database schema follows Data Model Suggestion 3 verbatim. Key tables and their Drizzle ORM definitions:

```typescript
// control-plane/src/db/schema.ts
import { pgTable, uuid, varchar, text, boolean, integer, bigint,
         doublePrecision, timestamp, jsonb, uniqueIndex, index, inet } from 'drizzle-orm/pg-core';

export const tenants = pgTable('tenants', {
  id: uuid('id').primaryKey().defaultRandom(),
  name: varchar('name', { length: 255 }).notNull(),
  slug: varchar('slug', { length: 100 }).notNull().unique(),
  plan: varchar('plan', { length: 50 }).notNull().default('free'),
  dataResidency: varchar('data_residency', { length: 10 }).notNull().default('us'),
  settings: jsonb('settings').notNull().default({}),
  createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
});

export const users = pgTable('users', {
  id: uuid('id').primaryKey().defaultRandom(),
  email: varchar('email', { length: 320 }).notNull().unique(),
  displayName: varchar('display_name', { length: 255 }).notNull(),
  passwordHash: varchar('password_hash', { length: 255 }),
  authProvider: varchar('auth_provider', { length: 50 }).notNull().default('local'),
  authMetadata: jsonb('auth_metadata').notNull().default({}),
  isActive: boolean('is_active').notNull().default(true),
  lastLoginAt: timestamp('last_login_at', { withTimezone: true }),
  createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
});

export const tenantMemberships = pgTable('tenant_memberships', {
  id: uuid('id').primaryKey().defaultRandom(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id, { onDelete: 'cascade' }),
  userId: uuid('user_id').notNull().references(() => users.id, { onDelete: 'cascade' }),
  role: varchar('role', { length: 50 }).notNull().default('viewer'),
  permissions: jsonb('permissions').notNull().default([]),
  invitedBy: uuid('invited_by').references(() => users.id),
  joinedAt: timestamp('joined_at', { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  uniqueIndex('idx_tenant_memberships_unique').on(table.tenantId, table.userId),
]);

export const apiKeys = pgTable('api_keys', {
  id: uuid('id').primaryKey().defaultRandom(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id, { onDelete: 'cascade' }),
  userId: uuid('user_id').notNull().references(() => users.id, { onDelete: 'cascade' }),
  name: varchar('name', { length: 255 }).notNull(),
  keyPrefix: varchar('key_prefix', { length: 10 }).notNull(),
  keyHash: varchar('key_hash', { length: 255 }).notNull(),
  scopes: jsonb('scopes').notNull().default([]),
  expiresAt: timestamp('expires_at', { withTimezone: true }),
  lastUsedAt: timestamp('last_used_at', { withTimezone: true }),
  createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  index('idx_api_keys_prefix').on(table.keyPrefix),
]);

export const namespaces = pgTable('namespaces', {
  id: uuid('id').primaryKey().defaultRandom(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id, { onDelete: 'cascade' }),
  name: varchar('name', { length: 255 }).notNull(),
  slug: varchar('slug', { length: 100 }).notNull(),
  description: text('description'),
  labels: jsonb('labels').notNull().default({}),
  isDefault: boolean('is_default').notNull().default(false),
  createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  uniqueIndex('idx_namespaces_tenant_slug').on(table.tenantId, table.slug),
]);

export const environments = pgTable('environments', {
  id: uuid('id').primaryKey().defaultRandom(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id, { onDelete: 'cascade' }),
  name: varchar('name', { length: 100 }).notNull(),
  slug: varchar('slug', { length: 50 }).notNull(),
  isProduction: boolean('is_production').notNull().default(false),
  config: jsonb('config').notNull().default({}),
  createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  uniqueIndex('idx_environments_tenant_slug').on(table.tenantId, table.slug),
]);

export const pipelines = pgTable('pipelines', {
  id: uuid('id').primaryKey().defaultRandom(),
  tenantId: uuid('tenant_id').notNull().references(() => tenants.id, { onDelete: 'cascade' }),
  namespaceId: uuid('namespace_id').notNull().references(() => namespaces.id, { onDelete: 'cascade' }),
  name: varchar('name', { length: 255 }).notNull(),
  description: text('description'),
  sqlText: text('sql_text').notNull(),
  currentVersion: integer('current_version').notNull().default(1),
  engineTarget: varchar('engine_target', { length: 50 }).notNull().default('flink'),
  config: jsonb('config').notNull().default({}),
  sources: jsonb('sources').notNull().default([]),
  sinks: jsonb('sinks').notNull().default([]),
  labels: jsonb('labels').notNull().default({}),
  createdBy: uuid('created_by').notNull().references(() => users.id),
  createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  uniqueIndex('idx_pipelines_tenant_ns_name').on(table.tenantId, table.namespaceId, table.name),
]);

// Remaining tables follow the same pattern from Data Model Suggestion 3:
// connectorTypes, connectors, pipelineVersions, deployments, checkpoints,
// materializedViews, schemaSubjects, schemaVersions, pipelineMetrics,
// alerts, alertIncidents, aiInteractions, auditLog, pipelineTests, pipelineTestRuns
```

**Testing**:
- `Integration: drizzle-kit push applies all migrations to empty PostgreSQL database`
- `Integration: all 22 tables created with correct column types and constraints`
- `Integration: UNIQUE constraints reject duplicate tenant slugs`
- `Integration: CASCADE deletes remove child records when tenant is deleted`
- `Integration: JSONB columns accept and return valid JSON objects`
- `Integration: GIN indexes exist on connectors.config and pipelines.labels`

#### 1.4 — Docker Compose Development Environment

**What**: Create a Docker Compose stack with PostgreSQL, Kafka (KRaft mode), MinIO, and placeholder services for engine and control plane.

**Design**:

```yaml
# docker-compose.yml
version: '3.9'

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: streamplatform
      POSTGRES_USER: streamplatform
      POSTGRES_PASSWORD: dev-password
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U streamplatform"]
      interval: 5s
      timeout: 5s
      retries: 5

  kafka:
    image: apache/kafka:3.9.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      CLUSTER_ID: "stream-platform-dev-01"
    ports:
      - "9092:9092"
    volumes:
      - kafka_data:/var/lib/kafka/data

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_data:/data

  minio-init:
    image: minio/mc:latest
    depends_on:
      - minio
    entrypoint: |
      sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin &&
      mc mb local/checkpoints --ignore-existing &&
      mc mb local/state --ignore-existing
      "

volumes:
  pg_data:
  kafka_data:
  minio_data:
```

**Testing**:
- `E2E: docker compose up -d starts all services without errors`
- `E2E: PostgreSQL accepts connections on localhost:5432`
- `E2E: Kafka broker is ready and accepts topic creation on localhost:9092`
- `E2E: MinIO console accessible at localhost:9001`
- `E2E: MinIO "checkpoints" and "state" buckets exist after init`
- `E2E: docker compose down --volumes cleans up all state`

#### 1.5 — CI Pipeline

**What**: GitHub Actions workflow for linting, testing, and building all components.

**Design**:

```yaml
# .github/workflows/ci.yml
name: CI
on:
  push:
    branches: [main]
  pull_request:

jobs:
  rust:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --check
      - run: cargo clippy --workspace -- -D warnings
      - run: cargo test --workspace

  typescript:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        project: [control-plane, web-ui]
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
        with:
          version: 9
      - uses: actions/setup-node@v4
        with:
          node-version: 22
          cache: 'pnpm'
      - run: pnpm install --frozen-lockfile
        working-directory: ${{ matrix.project }}
      - run: pnpm lint
        working-directory: ${{ matrix.project }}
      - run: pnpm typecheck
        working-directory: ${{ matrix.project }}
      - run: pnpm test
        working-directory: ${{ matrix.project }}

  docker:
    runs-on: ubuntu-latest
    needs: [rust, typescript]
    steps:
      - uses: actions/checkout@v4
      - run: docker compose build
```

**Testing**:
- `CI: Push to main triggers workflow`
- `CI: PR triggers workflow`
- `CI: Rust formatting, clippy, and tests pass`
- `CI: TypeScript lint, typecheck, and tests pass for both control-plane and web-ui`
- `CI: Docker build succeeds`

---

## Phase 2: Stream Engine Core — SQL Parsing, Planning, and In-Memory Execution

### Purpose

Build the heart of the platform: a Rust-based streaming SQL engine that can parse ANSI SQL, plan a query execution graph, and execute operators (filter, project, aggregate) over Arrow record batches in memory. After this phase, the engine can accept a SQL query and process a finite batch of events through it — the foundation for all streaming features.

### Tasks

#### 2.1 — SQL Parser and Streaming Extensions

**What**: Parse ANSI SQL with streaming extensions (TUMBLE, HOP, SESSION window functions; EMIT CHANGES; CREATE STREAM / CREATE MATERIALIZED VIEW) using sqlparser-rs.

**Design**:

```rust
// crates/engine-core/src/sql/parser.rs

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Streaming SQL statement types beyond standard SQL
#[derive(Debug, Clone)]
pub enum StreamStatement {
    /// CREATE STREAM orders (user_id VARCHAR, amount DECIMAL, event_time TIMESTAMP)
    /// WITH (connector = 'kafka', topic = 'orders', format = 'avro')
    CreateStream {
        name: String,
        columns: Vec<ColumnDef>,
        with_options: HashMap<String, String>,
    },
    /// CREATE MATERIALIZED VIEW fraud_alerts AS
    /// SELECT user_id, COUNT(*) as tx_count
    /// FROM orders
    /// GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE), user_id
    /// HAVING COUNT(*) > 100
    CreateMaterializedView {
        name: String,
        query: Box<StreamQuery>,
    },
    /// Standard SELECT with streaming extensions
    Query(StreamQuery),
    /// INSERT INTO sink_stream SELECT ... FROM source_stream
    InsertIntoSelect {
        target: String,
        query: Box<StreamQuery>,
    },
}

/// Parsed window specification
#[derive(Debug, Clone)]
pub enum WindowSpec {
    Tumble { time_column: String, interval_ms: u64 },
    Hop { time_column: String, size_ms: u64, slide_ms: u64 },
    Session { time_column: String, gap_ms: u64 },
}

/// A streaming query with optional window and watermark configuration
#[derive(Debug, Clone)]
pub struct StreamQuery {
    pub projections: Vec<Projection>,
    pub from: Vec<StreamSource>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub window: Option<WindowSpec>,
    pub having: Option<Expr>,
    pub watermark: Option<WatermarkSpec>,
}

#[derive(Debug, Clone)]
pub struct WatermarkSpec {
    pub column: String,
    pub max_delay_ms: u64,
}

/// Parse a SQL string into a StreamStatement
pub fn parse_stream_sql(sql: &str) -> Result<StreamStatement, ParseError> {
    // 1. Pre-process streaming keywords (TUMBLE, HOP, SESSION) into function calls
    // 2. Parse with sqlparser using GenericDialect
    // 3. Extract window specs from GROUP BY clauses
    // 4. Build StreamStatement from parsed AST
    todo!()
}
```

**Testing**:
- `Unit: parse_stream_sql("SELECT * FROM orders") → Query with single wildcard projection`
- `Unit: parse_stream_sql with TUMBLE → WindowSpec::Tumble with correct interval`
- `Unit: parse_stream_sql with HOP(event_time, INTERVAL '5' MINUTE, INTERVAL '1' MINUTE) → WindowSpec::Hop`
- `Unit: parse_stream_sql with SESSION(event_time, INTERVAL '30' MINUTE) → WindowSpec::Session`
- `Unit: CREATE STREAM with WITH clause → CreateStream with parsed options`
- `Unit: CREATE MATERIALIZED VIEW → CreateMaterializedView with inner query`
- `Unit: invalid SQL → ParseError with line/column info`
- `Unit: nested subquery → correctly parsed inner and outer queries`
- `Unit: multi-way JOIN → from list with join conditions`

#### 2.2 — Query Planner and Logical Plan

**What**: Convert parsed SQL into a logical plan (DAG of operators) using DataFusion's query planner with custom streaming extensions.

**Design**:

```rust
// crates/engine-core/src/sql/planner.rs

use datafusion::logical_expr::{LogicalPlan, Extension};
use arrow::datatypes::Schema;

/// A streaming-aware logical plan node
#[derive(Debug, Clone)]
pub enum StreamNode {
    /// Read from an unbounded source (Kafka topic, CDC stream)
    StreamScan {
        stream_name: String,
        schema: Arc<Schema>,
        connector_config: ConnectorConfig,
    },
    /// Filter rows
    Filter {
        predicate: Expr,
        input: Box<StreamNode>,
    },
    /// Project columns
    Projection {
        expressions: Vec<Expr>,
        input: Box<StreamNode>,
    },
    /// Windowed aggregation
    WindowAggregate {
        window: WindowSpec,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        input: Box<StreamNode>,
    },
    /// Stream-to-stream join
    StreamJoin {
        join_type: JoinType,
        left: Box<StreamNode>,
        right: Box<StreamNode>,
        on: Vec<(Expr, Expr)>,
        window_bound_ms: u64,  // time-bound for join buffer
    },
    /// Write to a sink (Kafka topic, database)
    StreamSink {
        sink_name: String,
        connector_config: ConnectorConfig,
        input: Box<StreamNode>,
    },
    /// Maintain a materialized view
    MaterializedViewSink {
        view_name: String,
        input: Box<StreamNode>,
    },
}

/// Aggregation function types
#[derive(Debug, Clone)]
pub enum AggregateExpr {
    Count(Option<Expr>),
    Sum(Expr),
    Avg(Expr),
    Min(Expr),
    Max(Expr),
    CountDistinct(Expr),
}

/// Convert a StreamStatement into a StreamNode DAG
pub fn plan_stream_query(
    statement: &StreamStatement,
    catalog: &dyn StreamCatalog,  // resolves stream names to schemas
) -> Result<StreamNode, PlanError> {
    todo!()
}

/// Catalog trait for resolving stream metadata
pub trait StreamCatalog: Send + Sync {
    fn get_stream_schema(&self, name: &str) -> Option<Arc<Schema>>;
    fn get_connector_config(&self, name: &str) -> Option<ConnectorConfig>;
}
```

**Testing**:
- `Unit: SELECT * FROM s → StreamScan for "s"`
- `Unit: SELECT a, b FROM s WHERE x > 10 → StreamScan → Filter → Projection`
- `Unit: GROUP BY TUMBLE(...) → WindowAggregate with Tumble spec`
- `Unit: JOIN with time bound → StreamJoin with correct window_bound_ms`
- `Unit: unknown stream name → PlanError::StreamNotFound`
- `Unit: type mismatch in WHERE clause → PlanError::TypeError`
- `Unit: nested aggregates rejected → PlanError::InvalidAggregation`
- `Fixture: 10 SQL queries from tests/fixtures/sql-queries/ all plan correctly`

#### 2.3 — In-Memory Operator Execution

**What**: Implement physical operators that process Arrow RecordBatches: Filter, Projection, HashAggregate, and TumbleWindow.

**Design**:

```rust
// crates/engine-core/src/execution/operator.rs

use arrow::record_batch::RecordBatch;
use arrow::array::*;

/// A physical operator that processes record batches
#[async_trait::async_trait]
pub trait Operator: Send + Sync {
    /// Process an input batch and return zero or more output batches
    async fn process(&mut self, input: RecordBatch) -> Result<Vec<RecordBatch>, ExecutionError>;

    /// Called when a watermark advances (triggers window emissions)
    async fn on_watermark(&mut self, watermark_ms: u64) -> Result<Vec<RecordBatch>, ExecutionError> {
        Ok(vec![])
    }

    /// Return the output schema
    fn output_schema(&self) -> Arc<Schema>;

    /// Checkpoint current operator state
    async fn checkpoint(&self) -> Result<Vec<u8>, ExecutionError>;

    /// Restore operator state from checkpoint
    async fn restore(&mut self, state: &[u8]) -> Result<(), ExecutionError>;
}

// crates/engine-core/src/execution/filter.rs

pub struct FilterOperator {
    predicate: PhysicalExpr,
    schema: Arc<Schema>,
}

impl FilterOperator {
    pub fn new(predicate: PhysicalExpr, input_schema: Arc<Schema>) -> Self {
        Self { predicate, schema: input_schema }
    }
}

#[async_trait::async_trait]
impl Operator for FilterOperator {
    async fn process(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>, ExecutionError> {
        let mask = self.predicate.evaluate(&batch)?.into_boolean()?;
        let filtered = arrow::compute::filter_record_batch(&batch, &mask)?;
        Ok(if filtered.num_rows() > 0 { vec![filtered] } else { vec![] })
    }

    fn output_schema(&self) -> Arc<Schema> { self.schema.clone() }
    async fn checkpoint(&self) -> Result<Vec<u8>, ExecutionError> { Ok(vec![]) } // stateless
    async fn restore(&mut self, _state: &[u8]) -> Result<(), ExecutionError> { Ok(()) }
}

// crates/engine-core/src/execution/window.rs

pub struct TumbleWindowOperator {
    window_size_ms: u64,
    time_column: String,
    aggregates: Vec<AggregateAccumulator>,
    /// Pending windows: window_start_ms → accumulated state
    pending_windows: BTreeMap<u64, Vec<AggregateAccumulator>>,
    output_schema: Arc<Schema>,
}

#[async_trait::async_trait]
impl Operator for TumbleWindowOperator {
    async fn process(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>, ExecutionError> {
        // 1. Extract event timestamps from time_column
        // 2. Assign each row to a window: window_start = (timestamp / window_size) * window_size
        // 3. Accumulate into pending_windows
        // 4. Do NOT emit — wait for watermark
        Ok(vec![])
    }

    async fn on_watermark(&mut self, watermark_ms: u64) -> Result<Vec<RecordBatch>, ExecutionError> {
        // Emit all windows where window_end <= watermark_ms
        let mut emitted = vec![];
        let cutoff = watermark_ms;
        let to_emit: Vec<u64> = self.pending_windows.keys()
            .take_while(|&&start| start + self.window_size_ms <= cutoff)
            .copied()
            .collect();

        for window_start in to_emit {
            if let Some(accumulators) = self.pending_windows.remove(&window_start) {
                let batch = self.finalize_window(window_start, &accumulators)?;
                emitted.push(batch);
            }
        }
        Ok(emitted)
    }

    async fn checkpoint(&self) -> Result<Vec<u8>, ExecutionError> {
        // Serialize pending_windows to bytes
        bincode::serialize(&self.pending_windows).map_err(ExecutionError::Serialization)
    }

    async fn restore(&mut self, state: &[u8]) -> Result<(), ExecutionError> {
        self.pending_windows = bincode::deserialize(state).map_err(ExecutionError::Deserialization)?;
        Ok(())
    }

    fn output_schema(&self) -> Arc<Schema> { self.output_schema.clone() }
}
```

**Testing**:
- `Unit: FilterOperator with "amount > 100" filters correct rows from a 1000-row batch`
- `Unit: FilterOperator with always-true predicate returns all rows`
- `Unit: FilterOperator with always-false predicate returns empty vec`
- `Unit: ProjectionOperator selects subset of columns`
- `Unit: ProjectionOperator computes expression "a + b AS total"`
- `Unit: TumbleWindowOperator accumulates rows without emitting (no watermark)`
- `Unit: TumbleWindowOperator emits window on watermark advance past window_end`
- `Unit: TumbleWindowOperator with COUNT(*) returns correct count per window`
- `Unit: TumbleWindowOperator with SUM(amount) returns correct sum`
- `Unit: TumbleWindowOperator checkpoint → restore preserves pending window state`
- `Unit: late events (before watermark) still assigned to correct window if window not yet emitted`
- `Benchmark (criterion): FilterOperator processes 1M-row batch in <10ms`
- `Benchmark (criterion): TumbleWindowOperator processes 1M events with 1-minute windows in <50ms`

#### 2.4 — Pipeline Execution Runtime

**What**: Build the runtime that chains operators into a pipeline DAG, manages data flow between them, and drives execution via Tokio tasks.

**Design**:

```rust
// crates/engine-core/src/runtime/task_manager.rs

use tokio::sync::mpsc;

/// A running pipeline instance
pub struct PipelineRuntime {
    deployment_id: DeploymentId,
    /// Operators chained via channels
    stages: Vec<StageHandle>,
    /// Cancellation token
    cancel: CancellationToken,
    /// Metrics collector
    metrics: Arc<PipelineMetrics>,
}

/// A single stage in the pipeline (one or more parallel operator instances)
struct StageHandle {
    operator_name: String,
    parallelism: usize,
    task_handles: Vec<tokio::task::JoinHandle<Result<(), ExecutionError>>>,
}

/// Message passed between pipeline stages
#[derive(Debug)]
enum StageMessage {
    Data(RecordBatch),
    Watermark(u64),        // watermark timestamp in ms
    Barrier(BarrierId),    // checkpoint barrier
    End,                   // end of stream (for bounded sources)
}

/// Checkpoint barrier ID
#[derive(Debug, Clone, Copy)]
pub struct BarrierId {
    pub checkpoint_id: u64,
    pub timestamp_ms: u64,
}

/// Metrics tracked per pipeline
pub struct PipelineMetrics {
    pub records_in: AtomicU64,
    pub records_out: AtomicU64,
    pub bytes_processed: AtomicU64,
    pub watermark_ms: AtomicU64,
    pub last_checkpoint_ms: AtomicU64,
    pub processing_time_ns: AtomicU64,
}

impl PipelineRuntime {
    /// Build a pipeline from a StreamNode DAG
    pub async fn from_plan(
        deployment_id: DeploymentId,
        plan: StreamNode,
        config: &PipelineConfig,
    ) -> Result<Self, ExecutionError> {
        // 1. Topologically sort the StreamNode DAG
        // 2. For each node, create the physical Operator
        // 3. Connect operators via mpsc channels (channel capacity = config.buffer_size)
        // 4. Spawn each operator as a Tokio task
        todo!()
    }

    /// Start pipeline execution
    pub async fn start(&mut self) -> Result<(), ExecutionError> {
        // Signal source operators to begin reading
        todo!()
    }

    /// Gracefully stop the pipeline
    pub async fn stop(&mut self) -> Result<(), ExecutionError> {
        self.cancel.cancel();
        for stage in &mut self.stages {
            for handle in stage.task_handles.drain(..) {
                handle.await??;
            }
        }
        Ok(())
    }

    /// Trigger a checkpoint across all operators
    pub async fn checkpoint(&self, checkpoint_id: u64) -> Result<CheckpointResult, ExecutionError> {
        // Inject a Barrier into source operators
        // Each operator: on Barrier, checkpoint state, forward Barrier downstream
        // Collect all operator states into CheckpointResult
        todo!()
    }
}

#[derive(Debug)]
pub struct PipelineConfig {
    pub buffer_size: usize,       // channel capacity between stages (default: 1024)
    pub batch_size: usize,        // max rows per RecordBatch (default: 4096)
    pub checkpoint_interval_ms: u64,
    pub parallelism: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1024,
            batch_size: 4096,
            checkpoint_interval_ms: 60_000,
            parallelism: 1,
        }
    }
}
```

**Testing**:
- `Unit: PipelineRuntime::from_plan builds correct stage chain from simple filter→project plan`
- `Integration: pipeline with in-memory source → filter → projection produces correct output`
- `Integration: pipeline with tumble window emits results on watermark advance`
- `Integration: pipeline stop() completes gracefully within 5 seconds`
- `Integration: checkpoint() collects state from all operators`
- `Integration: pipeline with parallelism=4 produces same results as parallelism=1`
- `Proptest: random RecordBatch data through filter→aggregate pipeline always produces valid output`

---

## Phase 3: Kafka Connectivity — Sources, Sinks, and Schema Registry

### Purpose

Connect the engine to the outside world via Kafka. After this phase, the platform can consume events from Kafka topics, process them through SQL pipelines, and write results back to Kafka topics. The embedded schema registry enables schema evolution for Avro, Protobuf, and JSON Schema payloads.

### Tasks

#### 3.1 — Kafka Source Connector

**What**: Implement a Kafka consumer that reads from one or more topics and feeds deserialized RecordBatches into the pipeline.

**Design**:

```rust
// crates/engine-core/src/connectors/kafka.rs

use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::config::ClientConfig;

/// Configuration for a Kafka source
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSourceConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub group_id: String,
    pub auto_offset_reset: AutoOffsetReset,
    pub security_protocol: SecurityProtocol,
    pub sasl_mechanism: Option<SaslMechanism>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub schema_subject: Option<String>,         // schema registry subject for deserialization
    pub max_poll_records: u32,                   // default: 500
    pub event_time_field: Option<String>,        // field to extract event time from
    pub max_out_of_orderness_ms: u64,            // watermark delay, default: 5000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AutoOffsetReset { Earliest, Latest }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SecurityProtocol { Plaintext, SaslSsl, Ssl }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SaslMechanism { Plain, ScramSha256, ScramSha512 }

/// Kafka source operator that feeds events into the pipeline
pub struct KafkaSourceOperator {
    consumer: StreamConsumer,
    deserializer: Box<dyn EventDeserializer>,
    schema: Arc<Schema>,
    config: KafkaSourceConfig,
    /// Track max event time for watermark generation
    max_event_time_ms: u64,
    /// Partition offsets for checkpoint
    committed_offsets: HashMap<i32, i64>,
}

/// Deserialize raw Kafka bytes into Arrow RecordBatch
pub trait EventDeserializer: Send + Sync {
    fn deserialize(&self, key: Option<&[u8]>, value: &[u8]) -> Result<RecordBatch, DeserError>;
    fn schema(&self) -> Arc<Schema>;
}

impl KafkaSourceOperator {
    pub async fn new(config: KafkaSourceConfig, registry: &SchemaRegistryClient) -> Result<Self, ConnectorError> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("group.id", &config.group_id)
            .set("auto.offset.reset", config.auto_offset_reset.as_str())
            .set("enable.auto.commit", "false");  // manual commit for exactly-once

        // Configure security
        if let SecurityProtocol::SaslSsl = config.security_protocol {
            client_config
                .set("security.protocol", "SASL_SSL")
                .set("sasl.mechanism", config.sasl_mechanism.as_ref().unwrap().as_str())
                .set("sasl.username", config.sasl_username.as_ref().unwrap())
                .set("sasl.password", config.sasl_password.as_ref().unwrap());
        }

        let consumer: StreamConsumer = client_config.create()?;
        consumer.subscribe(&[&config.topic])?;

        // Resolve deserializer from schema registry
        let deserializer = if let Some(ref subject) = config.schema_subject {
            registry.get_deserializer(subject).await?
        } else {
            Box::new(JsonDeserializer::new())
        };

        Ok(Self {
            schema: deserializer.schema(),
            consumer,
            deserializer,
            config,
            max_event_time_ms: 0,
            committed_offsets: HashMap::new(),
        })
    }
}
```

**Testing**:
- `Integration (real Kafka): consume 1000 JSON messages from topic → correct RecordBatches`
- `Integration (real Kafka): consume Avro-encoded messages with schema registry → correct deserialization`
- `Integration (real Kafka): SASL_SSL authentication succeeds`
- `Integration (real Kafka): consumer group rebalance completes without data loss`
- `Integration (real Kafka): checkpoint saves partition offsets; restore resumes from correct offset`
- `Unit (mocked): invalid bootstrap_servers → ConnectorError::ConnectionFailed`
- `Unit (mocked): deserialization of corrupt payload → skips record, increments error metric`
- `Unit: watermark advances as max(event_time) - max_out_of_orderness_ms`

#### 3.2 — Kafka Sink Connector

**What**: Implement a Kafka producer that serializes output RecordBatches and writes them to a target topic with exactly-once semantics via Kafka transactions.

**Design**:

```rust
// crates/engine-core/src/connectors/kafka.rs (continued)

use rdkafka::producer::{FutureProducer, FutureRecord};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSinkConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    pub key_field: Option<String>,              // field to use as Kafka key
    pub security_protocol: SecurityProtocol,
    pub sasl_mechanism: Option<SaslMechanism>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub schema_subject: Option<String>,
    pub delivery_guarantee: DeliveryGuarantee,
    pub transactional_id: Option<String>,       // required for exactly-once
    pub linger_ms: u64,                         // default: 5
    pub batch_size: u32,                        // default: 16384
    pub compression: CompressionType,           // default: lz4
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionType { None, Gzip, Snappy, Lz4, Zstd }

pub struct KafkaSinkOperator {
    producer: FutureProducer,
    serializer: Box<dyn EventSerializer>,
    config: KafkaSinkConfig,
    records_written: u64,
    bytes_written: u64,
}

pub trait EventSerializer: Send + Sync {
    fn serialize(&self, batch: &RecordBatch) -> Result<Vec<(Option<Vec<u8>>, Vec<u8>)>, SerError>;
}
```

**Testing**:
- `Integration (real Kafka): write 1000 records → all readable from topic with correct values`
- `Integration (real Kafka): exactly-once with transactions → no duplicates after producer restart`
- `Integration (real Kafka): key_field extracts correct column as Kafka key`
- `Integration (real Kafka): Avro serialization + schema registry → records deserializable by external consumer`
- `Unit (mocked): empty RecordBatch → no records produced`
- `Unit (mocked): producer delivery failure → records buffered for retry`

#### 3.3 — Embedded Schema Registry

**What**: Implement a Confluent Schema Registry-compatible REST API backed by PostgreSQL for storing and validating Avro, Protobuf, and JSON Schema definitions.

**Design**:

```rust
// crates/schema-registry/src/api.rs

use axum::{Router, Json, extract::Path};

/// Schema Registry REST API endpoints (Confluent-compatible)
pub fn router() -> Router {
    Router::new()
        // Subject operations
        .route("/subjects", get(list_subjects))
        .route("/subjects/:subject/versions", get(list_versions).post(register_schema))
        .route("/subjects/:subject/versions/:version", get(get_schema_by_version))
        .route("/subjects/:subject/versions/latest", get(get_latest_schema))
        .route("/subjects/:subject", delete(delete_subject))
        // Compatibility checks
        .route("/compatibility/subjects/:subject/versions/:version", post(check_compatibility))
        .route("/config", get(get_global_config).put(set_global_config))
        .route("/config/:subject", get(get_subject_config).put(set_subject_config))
        // Schema by ID
        .route("/schemas/ids/:id", get(get_schema_by_id))
}

/// Schema registration request (matches Confluent API)
#[derive(Debug, Deserialize)]
pub struct RegisterSchemaRequest {
    pub schema: String,
    #[serde(rename = "schemaType")]
    pub schema_type: Option<String>,  // AVRO (default), PROTOBUF, JSON
    pub references: Option<Vec<SchemaReference>>,
}

/// Schema registration response
#[derive(Debug, Serialize)]
pub struct RegisterSchemaResponse {
    pub id: i32,
}

/// Compatibility levels per Confluent Schema Registry API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompatibilityLevel {
    #[serde(rename = "NONE")]
    None,
    #[serde(rename = "BACKWARD")]
    Backward,
    #[serde(rename = "FORWARD")]
    Forward,
    #[serde(rename = "FULL")]
    Full,
    #[serde(rename = "BACKWARD_TRANSITIVE")]
    BackwardTransitive,
    #[serde(rename = "FORWARD_TRANSITIVE")]
    ForwardTransitive,
    #[serde(rename = "FULL_TRANSITIVE")]
    FullTransitive,
}
```

```rust
// crates/schema-registry/src/compatibility.rs

/// Check if a new schema is compatible with existing versions
pub fn check_avro_compatibility(
    new_schema: &str,
    existing_schemas: &[String],
    level: CompatibilityLevel,
) -> Result<bool, CompatibilityError> {
    // Backward: new schema can read data written by last/all old schemas
    // Forward: old schema(s) can read data written by new schema
    // Full: both backward and forward
    // Transitive: check against ALL previous versions, not just the latest
    todo!()
}
```

**Testing**:
- `Integration: POST /subjects/orders-value/versions registers Avro schema → returns ID`
- `Integration: GET /subjects/orders-value/versions/1 returns registered schema`
- `Integration: POST duplicate schema → returns same ID (content-addressed)`
- `Integration: BACKWARD compatibility → adding optional field succeeds`
- `Integration: BACKWARD compatibility → removing required field fails`
- `Integration: FORWARD compatibility → adding required field with default succeeds`
- `Integration: GET /subjects lists all registered subjects`
- `Integration: DELETE /subjects/orders-value soft-deletes subject`
- `Unit: check_avro_compatibility with backward-incompatible change returns false`
- `Unit: check_avro_compatibility with NONE level always returns true`

---

## Phase 4: CDC Sources and Checkpointing

### Purpose

Add Change Data Capture sources for PostgreSQL and MySQL, and implement distributed checkpointing with durable state persistence to S3. After this phase, the platform supports the three core source types (Kafka, PostgreSQL CDC, MySQL CDC) and can recover from failures without data loss.

### Tasks

#### 4.1 — PostgreSQL CDC Source

**What**: Implement a PostgreSQL logical replication source using the `pgoutput` plugin to capture INSERT, UPDATE, and DELETE events.

**Design**:

```rust
// crates/engine-core/src/connectors/cdc_postgres.rs

use tokio_postgres::Client;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresCdcConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub schema: String,                  // default: "public"
    pub tables: Vec<String>,             // tables to replicate
    pub slot_name: String,               // replication slot name
    pub publication_name: String,        // PostgreSQL publication
    pub snapshot_mode: SnapshotMode,     // initial, never
    pub ssl_mode: SslMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotMode {
    Initial,   // snapshot existing data, then stream changes
    Never,     // stream changes only (from current LSN)
}

/// CDC event representing a row change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    pub operation: CdcOperation,
    pub table: String,
    pub schema: String,
    pub lsn: u64,                       // Log Sequence Number
    pub timestamp_ms: u64,
    pub before: Option<serde_json::Value>,  // previous row state (UPDATE/DELETE)
    pub after: Option<serde_json::Value>,   // new row state (INSERT/UPDATE)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CdcOperation {
    Insert,
    Update,
    Delete,
    Snapshot,  // initial snapshot row
}

pub struct PostgresCdcSource {
    client: Client,
    config: PostgresCdcConfig,
    current_lsn: u64,
    schema_cache: HashMap<String, Arc<Schema>>,  // table_name → Arrow schema
}
```

**Testing**:
- `Integration (real PostgreSQL): INSERT into tracked table → CdcEvent with operation=Insert`
- `Integration (real PostgreSQL): UPDATE → CdcEvent with before and after values`
- `Integration (real PostgreSQL): DELETE → CdcEvent with before value and after=None`
- `Integration (real PostgreSQL): snapshot mode=Initial → reads all existing rows first`
- `Integration (real PostgreSQL): checkpoint saves LSN; restore resumes from correct position`
- `Integration (real PostgreSQL): DDL change (ADD COLUMN) → schema_cache updated`
- `Unit (mocked): connection failure → retries with exponential backoff`
- `Unit (mocked): invalid table name → ConnectorError::TableNotFound`

#### 4.2 — MySQL CDC Source

**What**: Implement a MySQL CDC source using the binlog protocol to capture row-level changes.

**Design**:

```rust
// crates/engine-core/src/connectors/cdc_mysql.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlCdcConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    pub tables: Vec<String>,
    pub server_id: u32,              // MySQL replication server ID
    pub snapshot_mode: SnapshotMode,
    pub ssl_mode: SslMode,
    pub gtid_mode: bool,             // use GTID for position tracking
}

pub struct MysqlCdcSource {
    // Uses binlog reader to track position
    binlog_position: BinlogPosition,
    schema_cache: HashMap<String, Arc<Schema>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinlogPosition {
    pub filename: String,
    pub position: u64,
    pub gtid_set: Option<String>,
}
```

**Testing**:
- `Integration (real MySQL): INSERT → CdcEvent with correct field values`
- `Integration (real MySQL): UPDATE → CdcEvent with before/after`
- `Integration (real MySQL): snapshot mode reads existing rows`
- `Integration (real MySQL): checkpoint saves binlog position; restore resumes correctly`
- `Unit (mocked): connection failure → retry with backoff`

#### 4.3 — Distributed Checkpoint Manager

**What**: Implement barrier-based distributed checkpointing that persists operator state to S3-compatible object storage, enabling exactly-once semantics and failure recovery.

**Design**:

```rust
// crates/engine-core/src/runtime/checkpoint.rs

use aws_sdk_s3::Client as S3Client;

/// Manages periodic and manual checkpoints for a pipeline
pub struct CheckpointManager {
    deployment_id: DeploymentId,
    s3_client: S3Client,
    bucket: String,
    prefix: String,                   // e.g. "checkpoints/{tenant_id}/{pipeline_id}/"
    interval_ms: u64,
    /// History of completed checkpoints
    completed: Vec<CompletedCheckpoint>,
    /// Maximum checkpoints to retain
    max_retained: usize,             // default: 10
}

/// A completed checkpoint with all operator states
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedCheckpoint {
    pub checkpoint_id: u64,
    pub deployment_id: DeploymentId,
    pub timestamp: DateTime<Utc>,
    pub duration_ms: u64,
    pub size_bytes: u64,
    pub storage_path: String,
    pub operator_states: HashMap<String, OperatorState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorState {
    pub operator_name: String,
    pub state_path: String,          // S3 key for this operator's state
    pub size_bytes: u64,
    pub checksum: String,            // SHA-256 for integrity verification
}

impl CheckpointManager {
    /// Trigger a checkpoint across all operators in the pipeline
    pub async fn trigger_checkpoint(
        &mut self,
        pipeline: &PipelineRuntime,
    ) -> Result<CompletedCheckpoint, CheckpointError> {
        let checkpoint_id = self.next_checkpoint_id();
        let barrier = BarrierId { checkpoint_id, timestamp_ms: Utc::now().timestamp_millis() as u64 };

        // 1. Inject barrier into source operators
        // 2. Each operator: flush pending output, snapshot state, forward barrier
        // 3. Collect all operator states
        // 4. Upload states to S3
        // 5. Write checkpoint metadata
        // 6. Acknowledge checkpoint to sources (commit Kafka offsets, confirm PostgreSQL LSN)
        todo!()
    }

    /// Restore pipeline state from the latest checkpoint
    pub async fn restore_latest(
        &self,
        pipeline: &mut PipelineRuntime,
    ) -> Result<CompletedCheckpoint, CheckpointError> {
        // 1. Find latest completed checkpoint
        // 2. Download operator states from S3
        // 3. Verify checksums
        // 4. Restore each operator from its state bytes
        // 5. Reset source positions (Kafka offsets, CDC LSN/binlog position)
        todo!()
    }
}
```

**Testing**:
- `Integration (MinIO): checkpoint uploads operator states to S3 bucket`
- `Integration (MinIO): restore downloads states and verifies checksums`
- `Integration: pipeline with 3 operators → checkpoint contains 3 operator states`
- `Integration: corrupt state file → CheckpointError::IntegrityError on restore`
- `Integration: max_retained=3 → oldest checkpoint deleted after 4th completes`
- `Unit: checkpoint_id increments monotonically`
- `Unit: barrier flows through pipeline in correct order (source→operators→sink)`
- `E2E: pipeline crash → restart from checkpoint → no duplicate or missing records`

---

## Phase 5: Control Plane API — Pipelines, Connectors, and Deployments

### Purpose

Build the REST API that users interact with to manage pipelines, connectors, deployments, and schemas. After this phase, users can create pipelines via HTTP, deploy them to the engine, and monitor their status through the API. The OpenAPI 3.1 specification is auto-generated.

### Tasks

#### 5.1 — Authentication and Authorization

**What**: Implement JWT-based authentication, API key authentication, and role-based access control (RBAC) with tenant isolation.

**Design**:

```typescript
// control-plane/src/auth/jwt.ts
import jwt from '@fastify/jwt';

export interface JwtPayload {
  sub: string;          // user ID
  email: string;
  tenantId: string;
  role: 'owner' | 'admin' | 'editor' | 'viewer';
  permissions: string[];
}

// Fastify hook for JWT verification
export async function authenticateJwt(request: FastifyRequest, reply: FastifyReply) {
  try {
    await request.jwtVerify();
  } catch (err) {
    reply.status(401).send({ error: 'Unauthorized', message: 'Invalid or expired token' });
  }
}

// control-plane/src/auth/rbac.ts

export type Permission =
  | 'pipelines:read' | 'pipelines:write' | 'pipelines:deploy' | 'pipelines:delete'
  | 'connectors:read' | 'connectors:write' | 'connectors:delete'
  | 'schemas:read' | 'schemas:write'
  | 'deployments:read' | 'deployments:manage'
  | 'alerts:read' | 'alerts:write'
  | 'tenants:manage' | 'users:manage'
  | 'ai:use';

const ROLE_PERMISSIONS: Record<string, Permission[]> = {
  owner: ['*'],  // all permissions
  admin: [
    'pipelines:read', 'pipelines:write', 'pipelines:deploy', 'pipelines:delete',
    'connectors:read', 'connectors:write', 'connectors:delete',
    'schemas:read', 'schemas:write',
    'deployments:read', 'deployments:manage',
    'alerts:read', 'alerts:write',
    'users:manage', 'ai:use',
  ],
  editor: [
    'pipelines:read', 'pipelines:write', 'pipelines:deploy',
    'connectors:read', 'connectors:write',
    'schemas:read', 'schemas:write',
    'deployments:read', 'alerts:read', 'alerts:write',
    'ai:use',
  ],
  viewer: [
    'pipelines:read', 'connectors:read', 'schemas:read',
    'deployments:read', 'alerts:read',
  ],
};

export function requirePermission(permission: Permission) {
  return async (request: FastifyRequest, reply: FastifyReply) => {
    const { role, permissions, tenantId } = request.user as JwtPayload;
    const rolePerms = ROLE_PERMISSIONS[role] || [];
    if (!rolePerms.includes('*') && !rolePerms.includes(permission) && !permissions.includes(permission)) {
      reply.status(403).send({ error: 'Forbidden', message: `Missing permission: ${permission}` });
    }
  };
}
```

**Testing**:
- `Unit: valid JWT token → user payload extracted correctly`
- `Unit: expired JWT → 401 response`
- `Unit: admin role has pipelines:deploy permission`
- `Unit: viewer role does NOT have pipelines:write permission`
- `Unit: custom permission override grants access regardless of role`
- `Integration: request with valid API key → authenticated`
- `Integration: request with revoked API key → 401`
- `Integration: tenant isolation → user A cannot access tenant B resources`

#### 5.2 — Pipeline CRUD API

**What**: Implement REST endpoints for creating, reading, updating, and deleting streaming pipelines, including SQL validation and version management.

**Design**:

```typescript
// control-plane/src/routes/pipelines.ts

// POST /api/v1/pipelines
interface CreatePipelineRequest {
  name: string;                     // unique within namespace
  namespaceId: string;
  description?: string;
  sqlText: string;                  // streaming SQL definition
  engineTarget?: 'flink' | 'spark' | 'risingwave';  // default: 'flink'
  config?: {
    parallelism?: number;           // default: 1
    checkpointIntervalMs?: number;  // default: 60000
    watermark?: {
      strategy: 'bounded_out_of_orderness' | 'monotonous';
      maxDelayMs?: number;          // default: 5000
    };
  };
  sources?: Array<{ connectorId: string; schemaSubject?: string; alias?: string }>;
  sinks?: Array<{ connectorId: string; schemaSubject?: string; alias?: string }>;
  labels?: Record<string, string>;
}

interface PipelineResponse {
  id: string;
  tenantId: string;
  namespaceId: string;
  name: string;
  description: string | null;
  sqlText: string;
  currentVersion: number;
  engineTarget: string;
  config: Record<string, unknown>;
  sources: Array<{ connectorId: string; schemaSubject?: string; alias?: string }>;
  sinks: Array<{ connectorId: string; schemaSubject?: string; alias?: string }>;
  labels: Record<string, string>;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
}

// GET /api/v1/pipelines                      → list pipelines in tenant
// GET /api/v1/pipelines/:id                  → get pipeline by ID
// PUT /api/v1/pipelines/:id                  → update pipeline (creates new version)
// DELETE /api/v1/pipelines/:id               → soft-delete pipeline
// GET /api/v1/pipelines/:id/versions         → list version history
// GET /api/v1/pipelines/:id/versions/:ver    → get specific version
// POST /api/v1/pipelines/:id/validate        → validate SQL without saving
```

**Testing**:
- `Integration: POST /api/v1/pipelines with valid SQL → 201 with pipeline object`
- `Integration: POST with duplicate name in same namespace → 409 Conflict`
- `Integration: POST with invalid SQL → 400 with parse error details`
- `Integration: GET /api/v1/pipelines → list of tenant's pipelines`
- `Integration: PUT /api/v1/pipelines/:id → currentVersion incremented, version history preserved`
- `Integration: DELETE → pipeline marked deleted, not physically removed`
- `Integration: GET /api/v1/pipelines/:id/versions → all historical versions returned`
- `Integration: POST /api/v1/pipelines/:id/validate with valid SQL → 200`
- `Integration: POST /api/v1/pipelines/:id/validate with invalid SQL → 400 with errors`
- `Integration: viewer role can GET but not POST pipelines`

#### 5.3 — Connector CRUD API

**What**: Implement REST endpoints for managing source and sink connectors with config validation against connector type JSON Schema.

**Design**:

```typescript
// control-plane/src/routes/connectors.ts

// POST /api/v1/connectors
interface CreateConnectorRequest {
  name: string;
  namespaceId: string;
  environmentId: string;
  connectorTypeId: string;          // references connector_types catalog
  config: Record<string, unknown>;  // validated against connector_types.config_schema
}

// GET /api/v1/connectors                     → list connectors
// GET /api/v1/connectors/:id                 → get connector
// PUT /api/v1/connectors/:id                 → update connector config
// DELETE /api/v1/connectors/:id              → delete connector
// POST /api/v1/connectors/:id/test           → test connector connectivity
// GET /api/v1/connector-types                → list available connector types
```

```typescript
// control-plane/src/services/connector.service.ts

import Ajv from 'ajv';

export class ConnectorService {
  private ajv = new Ajv({ allErrors: true });

  async create(tenantId: string, request: CreateConnectorRequest): Promise<ConnectorResponse> {
    // 1. Load connector type and its config_schema
    const connectorType = await this.getConnectorType(request.connectorTypeId);

    // 2. Validate config against JSON Schema
    const validate = this.ajv.compile(connectorType.configSchema);
    if (!validate(request.config)) {
      throw new ValidationError('Invalid connector config', validate.errors);
    }

    // 3. Encrypt secret fields (x-secret: true in schema)
    const encryptedConfig = await this.encryptSecrets(request.config, connectorType.configSchema);

    // 4. Insert into database
    return this.db.insert(connectors).values({ ...request, tenantId, config: encryptedConfig });
  }

  async testConnectivity(connectorId: string): Promise<ConnectivityResult> {
    // Dispatch to engine to test the connection (Kafka broker reachability, DB credentials, etc.)
    return this.engineClient.testConnector(connectorId);
  }
}
```

**Testing**:
- `Integration: POST /api/v1/connectors with valid Kafka config → 201`
- `Integration: POST with config missing required field → 400 with JSON Schema validation errors`
- `Integration: POST with unknown connector type → 404`
- `Integration: POST /api/v1/connectors/:id/test → connectivity result`
- `Integration: secret fields stored encrypted in database`
- `Integration: GET returns config with secret fields masked ("***")`
- `Unit: ConnectorService validates config against JSON Schema correctly`
- `Unit: secret encryption round-trips correctly`

#### 5.4 — Deployment Management API

**What**: Implement endpoints for deploying pipelines to the engine, monitoring status, and managing lifecycle (start, stop, restart).

**Design**:

```typescript
// control-plane/src/routes/deployments.ts

// POST /api/v1/deployments
interface CreateDeploymentRequest {
  pipelineId: string;
  pipelineVersion?: number;        // default: latest
  environmentId: string;
}

// GET /api/v1/deployments                    → list deployments
// GET /api/v1/deployments/:id                → get deployment status
// POST /api/v1/deployments/:id/stop          → stop a running deployment
// POST /api/v1/deployments/:id/restart       → restart from last checkpoint
// GET /api/v1/deployments/:id/checkpoints    → list checkpoint history
// GET /api/v1/deployments/:id/metrics        → get current metrics

interface DeploymentResponse {
  id: string;
  pipelineId: string;
  pipelineName: string;
  pipelineVersion: number;
  environmentId: string;
  environmentName: string;
  status: 'pending' | 'deploying' | 'running' | 'stopping' | 'stopped' | 'failed';
  desiredState: 'running' | 'stopped';
  engineJobId: string | null;
  engineMetadata: Record<string, unknown>;
  deployedBy: string;
  deployedAt: string;
  stoppedAt: string | null;
  failureReason: string | null;
  metrics?: {
    recordsIn: number;
    recordsOut: number;
    consumerLag: number;
    lastCheckpointAt: string | null;
    uptimeMs: number;
  };
}

// Deployment state machine:
// pending → deploying → running → stopping → stopped
//                    ↘ failed
//         stopped → deploying (restart)
```

**Testing**:
- `Integration: POST /api/v1/deployments → 201 with pending status`
- `Integration: deployment transitions to running after engine confirms job started`
- `Integration: POST /api/v1/deployments/:id/stop → status becomes stopping, then stopped`
- `Integration: POST /api/v1/deployments/:id/restart → new deployment from latest checkpoint`
- `Integration: GET /api/v1/deployments/:id/metrics → current pipeline metrics`
- `Integration: deploy non-existent pipeline → 404`
- `Integration: deploy to non-existent environment → 404`
- `Integration: viewer role cannot create deployments`
- `Unit: deployment state machine rejects invalid transitions (e.g. stopped → stopping)`

---

## Phase 6: Web UI — Dashboard, Pipeline Editor, and Observability

### Purpose

Build the React-based web UI that provides a visual interface for pipeline management, a SQL editor with syntax highlighting, real-time metrics dashboards, and connector configuration. After this phase, users have a complete self-service experience without needing to use the API directly.

### Tasks

#### 6.1 — Application Shell and Navigation

**What**: Create the React application with routing, authentication flows, tenant switching, and a navigation sidebar.

**Design**:

```typescript
// web-ui/src/routes/__root.tsx
import { createRootRoute, Outlet } from '@tanstack/react-router';

export const Route = createRootRoute({
  component: () => (
    <AuthProvider>
      <div className="flex h-screen">
        <Sidebar />
        <main className="flex-1 overflow-auto">
          <TopBar />
          <Outlet />
        </main>
      </div>
    </AuthProvider>
  ),
});

// Navigation structure
interface NavItem {
  label: string;
  icon: React.ComponentType;
  path: string;
  permission?: Permission;
}

const NAV_ITEMS: NavItem[] = [
  { label: 'Dashboard', icon: LayoutDashboard, path: '/' },
  { label: 'Pipelines', icon: GitBranch, path: '/pipelines', permission: 'pipelines:read' },
  { label: 'Connectors', icon: Plug, path: '/connectors', permission: 'connectors:read' },
  { label: 'Schemas', icon: FileCode, path: '/schemas', permission: 'schemas:read' },
  { label: 'Deployments', icon: Rocket, path: '/deployments', permission: 'deployments:read' },
  { label: 'Alerts', icon: Bell, path: '/alerts', permission: 'alerts:read' },
  { label: 'Settings', icon: Settings, path: '/settings', permission: 'tenants:manage' },
];
```

**Testing**:
- `E2E (Playwright): login flow → dashboard displayed with correct navigation`
- `E2E (Playwright): tenant switch → navigation reflects new tenant`
- `E2E (Playwright): viewer role → "Settings" nav item hidden`
- `Unit: NavItem filtering respects user permissions`
- `Unit: AuthProvider redirects unauthenticated users to /login`

#### 6.2 — SQL Pipeline Editor

**What**: Build a Monaco-based SQL editor with streaming SQL syntax highlighting, auto-completion for stream names and columns, inline validation, and one-click deployment.

**Design**:

```typescript
// web-ui/src/components/sql-editor/SqlEditor.tsx

interface SqlEditorProps {
  initialValue: string;
  availableStreams: StreamInfo[];    // for auto-completion
  onValidate: (sql: string) => Promise<ValidationResult>;
  onSave: (sql: string) => void;
  onDeploy: (sql: string) => void;
}

interface StreamInfo {
  name: string;
  columns: Array<{ name: string; type: string }>;
}

interface ValidationResult {
  isValid: boolean;
  errors: Array<{
    line: number;
    column: number;
    message: string;
    severity: 'error' | 'warning' | 'info';
  }>;
  plan?: {
    operators: string[];      // for query plan visualization
    estimatedParallelism: number;
  };
}

// Monaco language configuration for streaming SQL
const STREAMING_SQL_KEYWORDS = [
  'TUMBLE', 'HOP', 'SESSION',
  'CREATE STREAM', 'CREATE MATERIALIZED VIEW',
  'EMIT CHANGES', 'WATERMARK',
  ...STANDARD_SQL_KEYWORDS,
];
```

**Testing**:
- `E2E (Playwright): type SQL → syntax highlighting applied for streaming keywords`
- `E2E (Playwright): type stream name → auto-complete suggests matching streams`
- `E2E (Playwright): type column name after stream alias → auto-complete suggests columns`
- `E2E (Playwright): invalid SQL → error markers shown inline with message`
- `E2E (Playwright): click Deploy → deployment created and status shown`
- `Unit: STREAMING_SQL_KEYWORDS includes TUMBLE, HOP, SESSION`
- `Unit: validation result maps to Monaco markers correctly`

#### 6.3 — Real-Time Metrics Dashboard

**What**: Build a dashboard showing real-time pipeline health, throughput, consumer lag, and checkpoint status using Server-Sent Events.

**Design**:

```typescript
// web-ui/src/routes/dashboard.tsx

interface DashboardMetrics {
  totalPipelines: number;
  runningDeployments: number;
  failedDeployments: number;
  totalThroughputEps: number;      // total events per second across all pipelines
  aggregateConsumerLag: number;
}

interface PipelineHealthCard {
  deploymentId: string;
  pipelineName: string;
  environment: string;
  status: DeploymentStatus;
  throughputEps: number;
  consumerLag: number;
  lastCheckpointAge: string;       // human-readable: "2m ago"
  errorCount1h: number;
  healthScore: number;             // 0.0 - 1.0
}

// SSE endpoint for real-time metric updates
// GET /api/v1/deployments/:id/metrics/stream
// Returns: text/event-stream with metric events every 5 seconds

// web-ui/src/hooks/useDeploymentMetrics.ts
function useDeploymentMetrics(deploymentId: string) {
  // Uses EventSource API to subscribe to SSE stream
  // Returns reactive metrics object updated in real-time
}
```

**Testing**:
- `E2E (Playwright): dashboard loads with correct aggregate metrics`
- `E2E (Playwright): pipeline health cards show correct status colors (green=running, red=failed)`
- `E2E (Playwright): metrics update in real-time via SSE (observe value change)`
- `Unit: healthScore calculation: 1.0 for healthy, degrades with lag/errors`
- `Unit: lastCheckpointAge formats correctly ("just now", "2m ago", "1h ago")`
- `Integration: SSE endpoint streams metric updates every 5 seconds`

#### 6.4 — Connector Configuration UI

**What**: Build a dynamic form generator that renders connector configuration forms from the JSON Schema stored in connector_types.config_schema.

**Design**:

```typescript
// web-ui/src/components/connector-form/DynamicForm.tsx

interface DynamicFormProps {
  schema: JSONSchema7;              // JSON Schema from connector_types.config_schema
  initialValues?: Record<string, unknown>;
  onSubmit: (values: Record<string, unknown>) => void;
  onTest: () => void;               // test connectivity
}

// Renders form fields based on JSON Schema property types:
// string → text input (password input if x-secret: true)
// integer/number → number input
// boolean → toggle switch
// enum → dropdown select
// object → nested fieldset
// array → repeatable field group

// Groups properties by category using x-category extension
// Marks required fields with asterisk
// Shows validation errors from AJV inline
```

**Testing**:
- `E2E (Playwright): Kafka connector type → renders bootstrap_servers, topic, group_id fields`
- `E2E (Playwright): required field left empty → validation error shown`
- `E2E (Playwright): secret field (sasl_password) → rendered as password input`
- `E2E (Playwright): submit valid form → connector created, success toast shown`
- `E2E (Playwright): test connectivity → result displayed (success or failure with message)`
- `Unit: JSON Schema with enum → renders dropdown with correct options`
- `Unit: JSON Schema with default → field pre-populated`

---

## Phase 7: Monitoring and Alerting

### Purpose

Implement the observability stack: OpenTelemetry integration for metrics and traces, configurable alerts with notification channels (Slack, email, PagerDuty), and pipeline incident management. After this phase, operations teams can monitor pipeline health and receive automated alerts.

### Tasks

#### 7.1 — OpenTelemetry Metrics Export

**What**: Instrument the Rust engine and TypeScript control plane with OpenTelemetry metrics using standardized OTel semantic conventions for messaging systems.

**Design**:

```rust
// crates/engine-core/src/metrics/otel.rs

use opentelemetry::metrics::{Counter, Histogram, Gauge};

/// Pipeline-level metrics following OTel Messaging Semantic Conventions
pub struct PipelineOtelMetrics {
    /// messaging.consumer.lag — current consumer lag in messages
    pub consumer_lag: Gauge<i64>,
    /// messaging.process.duration — time to process each batch (histogram, ms)
    pub process_duration: Histogram<f64>,
    /// messaging.receive.messages — total messages consumed
    pub messages_received: Counter<u64>,
    /// messaging.publish.messages — total messages produced
    pub messages_published: Counter<u64>,
    /// stream_platform.checkpoint.duration — checkpoint time (histogram, ms)
    pub checkpoint_duration: Histogram<f64>,
    /// stream_platform.checkpoint.size — checkpoint size (histogram, bytes)
    pub checkpoint_size: Histogram<f64>,
    /// stream_platform.watermark.lag — current watermark lag (gauge, ms)
    pub watermark_lag: Gauge<i64>,
    /// stream_platform.window.pending — number of pending (open) windows
    pub pending_windows: Gauge<i64>,
}

/// Standard attribute keys
pub mod attrs {
    pub const DEPLOYMENT_ID: &str = "stream_platform.deployment.id";
    pub const PIPELINE_NAME: &str = "stream_platform.pipeline.name";
    pub const TENANT_ID: &str = "stream_platform.tenant.id";
    pub const ENVIRONMENT: &str = "stream_platform.environment";
    pub const CONNECTOR_TYPE: &str = "stream_platform.connector.type";
    pub const TOPIC: &str = "messaging.destination.name";
    pub const PARTITION: &str = "messaging.destination.partition.id";
}
```

**Testing**:
- `Integration: engine processes events → metrics exported to OTLP endpoint`
- `Integration: consumer_lag gauge updates as lag changes`
- `Integration: process_duration histogram has correct bucket boundaries`
- `Integration: metrics include all required OTel attributes`
- `Unit: PipelineOtelMetrics initializes without error`
- `Unit: attribute keys match OTel Messaging Semantic Conventions`

#### 7.2 — Alert Configuration and Evaluation

**What**: Implement configurable alerts that evaluate metric conditions and fire incidents.

**Design**:

```typescript
// control-plane/src/services/alert.service.ts

interface AlertRule {
  id: string;
  tenantId: string;
  pipelineId?: string;
  name: string;
  config: {
    conditionType: 'threshold' | 'anomaly' | 'absence';
    metricName: string;
    comparison: 'gt' | 'gte' | 'lt' | 'lte' | 'eq';
    thresholdValue: number;
    evaluationWindowMs: number;
    consecutiveBreaches: number;  // fire after N consecutive breaches
  };
  severity: 'info' | 'warning' | 'critical';
  notificationChannels: NotificationChannel[];
}

interface NotificationChannel {
  type: 'slack' | 'email' | 'pagerduty' | 'webhook';
  config: Record<string, string>;  // type-specific: webhook_url, email_addresses, routing_key
}

export class AlertEvaluator {
  /// Run every 30 seconds to evaluate all active alerts
  async evaluateAll(): Promise<void> {
    const activeAlerts = await this.getActiveAlerts();
    for (const alert of activeAlerts) {
      const metrics = await this.getMetricWindow(alert);
      const isFiring = this.evaluateCondition(alert.config, metrics);
      if (isFiring) {
        await this.fireIncident(alert);
      }
    }
  }
}
```

**Testing**:
- `Integration: metric exceeds threshold for consecutiveBreaches → incident created`
- `Integration: metric recovers → incident auto-resolved`
- `Integration: Slack notification sent with correct message format`
- `Integration: PagerDuty incident created with correct severity mapping`
- `Unit: threshold comparison "gt" with value 100 and threshold 90 → fires`
- `Unit: threshold comparison "gt" with value 80 and threshold 90 → does not fire`
- `Unit: absence condition fires when no metrics received in window`
- `Unit: consecutiveBreaches=3 requires 3 consecutive evaluations before firing`

#### 7.3 — Audit Log

**What**: Capture all API mutations as audit log entries with actor, resource, and change details per OWASP API Security guidelines.

**Design**:

```typescript
// control-plane/src/services/audit.service.ts

interface AuditEntry {
  tenantId: string;
  actorUserId?: string;
  actorApiKeyId?: string;
  action: string;             // "pipeline.create", "connector.update", "deployment.start"
  resourceType: string;
  resourceId: string;
  changes?: Record<string, { old: unknown; new: unknown }>;
  requestMetadata: {
    ip: string;
    userAgent: string;
    requestId: string;
  };
}

// Fastify hook to automatically log all mutation requests
export function auditLogHook(app: FastifyInstance) {
  app.addHook('onResponse', async (request, reply) => {
    if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(request.method) && reply.statusCode < 400) {
      await auditService.log({
        tenantId: request.user.tenantId,
        actorUserId: request.user.sub,
        action: deriveAction(request.method, request.url),
        resourceType: deriveResourceType(request.url),
        resourceId: deriveResourceId(request),
        requestMetadata: {
          ip: request.ip,
          userAgent: request.headers['user-agent'] || '',
          requestId: request.id,
        },
      });
    }
  });
}
```

**Testing**:
- `Integration: POST /api/v1/pipelines → audit_log entry with action "pipeline.create"`
- `Integration: PUT /api/v1/connectors/:id → audit_log entry with changes diff`
- `Integration: DELETE request → audit entry with correct resource_id`
- `Integration: GET requests do NOT create audit entries`
- `Integration: failed requests (4xx/5xx) do NOT create audit entries`
- `Unit: deriveAction("POST", "/api/v1/pipelines") → "pipeline.create"`
- `Unit: deriveAction("PUT", "/api/v1/connectors/uuid") → "connector.update"`

---

## Phase 8: AI-Assisted Authoring — Natural Language to SQL

### Purpose

Add the AI-native differentiator: natural language to streaming SQL translation, enabling analysts to describe transformations in plain English. The system generates SQL, explains its reasoning, and learns from user feedback. After this phase, non-SQL users can author streaming pipelines.

### Tasks

#### 8.1 — NL-to-SQL Translation Service

**What**: Implement the LLM-backed service that translates natural language descriptions into validated streaming SQL, with context-aware schema information.

**Design**:

```typescript
// control-plane/src/services/ai.service.ts

import Anthropic from '@anthropic-ai/sdk';

interface NlToSqlRequest {
  naturalLanguage: string;
  context: {
    namespaceId: string;
    availableStreams: Array<{
      name: string;
      columns: Array<{ name: string; type: string; description?: string }>;
    }>;
    availableConnectors: Array<{ id: string; name: string; type: string }>;
    previousQueries?: Array<{ nl: string; sql: string }>;  // conversation history
  };
}

interface NlToSqlResponse {
  generatedSql: string;
  confidence: number;          // 0.0 - 1.0
  explanation: string;         // human-readable explanation of the SQL
  alternatives?: Array<{
    sql: string;
    confidence: number;
    explanation: string;
  }>;
  warnings?: string[];         // e.g. "No watermark strategy specified; using default 5s delay"
}

export class AiService {
  private client: Anthropic;

  async translateNlToSql(
    tenantId: string,
    userId: string,
    request: NlToSqlRequest,
  ): Promise<NlToSqlResponse> {
    const systemPrompt = this.buildSystemPrompt(request.context);
    const userPrompt = request.naturalLanguage;

    const response = await this.client.messages.create({
      model: this.config.model,
      max_tokens: this.config.maxTokens,
      system: systemPrompt,
      messages: [{ role: 'user', content: userPrompt }],
    });

    const result = this.parseResponse(response);

    // Validate generated SQL by sending to the engine parser
    const validation = await this.engineClient.validateSql(result.generatedSql);
    if (!validation.isValid) {
      // Retry with error feedback
      result = await this.retryWithFeedback(request, result.generatedSql, validation.errors);
    }

    // Log interaction for training data
    await this.logInteraction(tenantId, userId, request, result);

    return result;
  }

  private buildSystemPrompt(context: NlToSqlRequest['context']): string {
    return `You are a streaming SQL expert for a stream processing platform.
Your task is to translate natural language descriptions into valid streaming SQL.

Available streams and their schemas:
${context.availableStreams.map(s =>
  `  ${s.name}: ${s.columns.map(c => `${c.name} (${c.type})`).join(', ')}`
).join('\n')}

Supported window functions:
- TUMBLE(time_column, INTERVAL 'N' MINUTE|HOUR|DAY)
- HOP(time_column, INTERVAL 'N' MINUTE, INTERVAL 'M' MINUTE)
- SESSION(time_column, INTERVAL 'N' MINUTE)

Respond with a JSON object:
{
  "sql": "the streaming SQL query",
  "confidence": 0.0-1.0,
  "explanation": "why this SQL achieves the user's goal",
  "alternatives": [{"sql": "...", "confidence": 0.0-1.0, "explanation": "..."}],
  "warnings": ["any warnings about the query"]
}`;
  }
}
```

**Testing**:
- `Integration (mocked LLM): "count orders per user in 5 minute windows" → valid TUMBLE SQL`
- `Integration (mocked LLM): "find users with more than 100 transactions" → HAVING clause`
- `Integration (mocked LLM): ambiguous request → confidence < 0.7, alternatives provided`
- `Integration (mocked LLM): request referencing non-existent stream → warning included`
- `Integration: generated SQL passes engine validation`
- `Integration: interaction logged to ai_interactions table with request/response/model_version`
- `Unit: system prompt includes all available stream schemas`
- `Unit: system prompt includes window function documentation`
- `Unit: confidence score parsed correctly from LLM response`

#### 8.2 — Feedback Loop and Training Data Collection

**What**: Implement the feedback mechanism where users accept, reject, or edit AI-generated SQL, creating a dataset for model improvement.

**Design**:

```typescript
// control-plane/src/routes/ai.ts

// POST /api/v1/ai/nl-to-sql
// → NlToSqlResponse

// POST /api/v1/ai/feedback
interface AiFeedbackRequest {
  interactionId: string;
  wasAccepted: boolean;
  wasEdited: boolean;
  editedSql?: string;          // user's corrected version
  rating?: number;             // 1-5
  comment?: string;
}

// GET /api/v1/ai/interactions
// → paginated list of NL-to-SQL interactions for the tenant
// Used for reviewing AI quality and exporting training data

// GET /api/v1/ai/stats
interface AiStats {
  totalInteractions: number;
  acceptanceRate: number;      // % accepted without editing
  editRate: number;            // % accepted with edits
  rejectionRate: number;       // % rejected
  averageRating: number;
  averageConfidence: number;
  topFailurePatterns: Array<{ pattern: string; count: number }>;
}
```

**Testing**:
- `Integration: POST feedback with wasAccepted=true → stored in ai_interactions`
- `Integration: POST feedback with editedSql → original and edited SQL both stored`
- `Integration: GET /api/v1/ai/stats → correct acceptance/rejection rates`
- `Integration: stats calculation handles division by zero (no interactions)`
- `Unit: acceptance rate = accepted / total * 100`

#### 8.3 — NL Query UI Component

**What**: Build the natural language input component in the web UI with real-time SQL preview, confidence indicator, and feedback buttons.

**Design**:

```typescript
// web-ui/src/components/nl-query/NlQueryInput.tsx

interface NlQueryInputProps {
  namespaceId: string;
  onSqlGenerated: (sql: string) => void;  // sends to SQL editor
}

// UI flow:
// 1. User types natural language in text area
// 2. Debounced request to /api/v1/ai/nl-to-sql (500ms)
// 3. Shows loading spinner during translation
// 4. Displays: generated SQL with syntax highlighting, confidence badge,
//    explanation text, alternative suggestions as tabs
// 5. User clicks "Use this SQL" → inserts into SQL editor
// 6. User clicks "Thumbs up/down" → submits feedback
// 7. User edits SQL in editor → edited version captured on next save
```

**Testing**:
- `E2E (Playwright): type natural language → SQL preview appears with highlighting`
- `E2E (Playwright): confidence badge shows correct color (green >0.8, yellow >0.5, red <0.5)`
- `E2E (Playwright): click "Use this SQL" → SQL inserted into editor`
- `E2E (Playwright): click thumbs up → feedback submitted`
- `E2E (Playwright): alternative tabs show different SQL options`
- `Unit: debounce fires after 500ms of no typing`

---

## Phase 9: AI-Driven Anomaly Detection and Observability

### Purpose

Add automated anomaly detection on streaming pipeline metrics and LLM-powered root-cause analysis. After this phase, the platform proactively detects issues (throughput drops, latency spikes, schema drift) and explains probable causes in natural language.

### Tasks

#### 9.1 — Statistical Anomaly Detection Engine

**What**: Implement a time-series anomaly detector that runs over pipeline metrics to detect throughput anomalies, latency spikes, and data quality degradation without hand-authored rules.

**Design**:

```rust
// crates/engine-core/src/anomaly/detector.rs

/// Anomaly detection using exponentially weighted moving average (EWMA)
/// with adaptive thresholds based on historical variance
pub struct EwmaDetector {
    alpha: f64,                     // smoothing factor (default: 0.1)
    sigma_threshold: f64,           // number of standard deviations for anomaly (default: 3.0)
    ewma: f64,
    ewmvar: f64,                    // exponentially weighted moving variance
    sample_count: u64,
    min_samples: u64,               // minimum samples before detection (default: 100)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyEvent {
    pub deployment_id: DeploymentId,
    pub detection_type: DetectionType,
    pub severity: Severity,
    pub metric_name: String,
    pub expected_value: f64,
    pub actual_value: f64,
    pub z_score: f64,               // how many standard deviations from mean
    pub detected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DetectionType {
    ThroughputDrop,
    ThroughputSpike,
    LatencySpike,
    ConsumerLagSpike,
    CheckpointFailure,
    SchemaEvolution,      // unexpected schema change in source
    DataQuality,          // null rate, duplicate rate anomalies
}

impl EwmaDetector {
    pub fn observe(&mut self, value: f64) -> Option<AnomalyEvent> {
        self.sample_count += 1;
        let delta = value - self.ewma;
        self.ewma += self.alpha * delta;
        self.ewmvar = (1.0 - self.alpha) * (self.ewmvar + self.alpha * delta * delta);

        if self.sample_count < self.min_samples {
            return None;  // not enough data
        }

        let std_dev = self.ewmvar.sqrt();
        let z_score = if std_dev > 0.0 { delta.abs() / std_dev } else { 0.0 };

        if z_score > self.sigma_threshold {
            Some(AnomalyEvent { z_score, expected_value: self.ewma, actual_value: value, .. })
        } else {
            None
        }
    }
}
```

**Testing**:
- `Unit: steady-state metric (100 +-5) → no anomalies detected`
- `Unit: sudden spike (100 → 500) → AnomalyEvent with ThroughputSpike`
- `Unit: gradual drift (100 → 150 over 1000 samples) → no anomaly (EWMA adapts)`
- `Unit: fewer than min_samples → no detection (returns None)`
- `Unit: z_score calculation matches expected value for known input`
- `Proptest: random normal distribution values → anomaly rate < 0.3%`
- `Proptest: random values with injected spikes → spikes always detected`

#### 9.2 — LLM-Powered Root Cause Analysis

**What**: When an anomaly is detected, generate a natural language explanation of the probable root cause by analyzing correlated metrics, recent configuration changes, and upstream system state.

**Design**:

```typescript
// control-plane/src/services/ai.service.ts (continued)

interface RootCauseAnalysisRequest {
  anomaly: {
    deploymentId: string;
    detectionType: string;
    metricName: string;
    expectedValue: number;
    actualValue: number;
    detectedAt: string;
  };
  context: {
    recentMetrics: Array<{ name: string; values: number[]; timestamps: string[] }>;
    recentDeployments: Array<{ pipelineName: string; deployedAt: string; changes: string }>;
    recentConfigChanges: Array<{ resource: string; field: string; oldValue: string; newValue: string; changedAt: string }>;
    upstreamHealth: Array<{ connectorName: string; status: string; lag: number }>;
  };
}

interface RootCauseAnalysisResponse {
  explanation: string;          // plain English explanation
  probableCause: string;        // one-line summary
  confidence: number;
  suggestedActions: string[];   // actionable recommendations
  relatedMetrics: string[];     // which metrics are correlated
}
```

**Testing**:
- `Integration (mocked LLM): consumer lag spike with recent upstream restart → explanation mentions upstream restart`
- `Integration (mocked LLM): latency spike after parallelism change → explanation references config change`
- `Integration: root cause stored in anomaly_detections record`
- `Unit: context assembly includes correct time window of metrics`
- `Unit: context includes all recent config changes within 1 hour of anomaly`

#### 9.3 — AI Watermark Strategy Recommendation

**What**: Analyze historical event-time latency distributions and recommend optimal watermark max_delay_ms settings for each pipeline.

**Design**:

```rust
// crates/engine-core/src/anomaly/watermark_advisor.rs

pub struct WatermarkAdvisor {
    /// Collect event-time delay samples (event_time vs processing_time)
    samples: CircularBuffer<f64>,
    sample_capacity: usize,  // default: 100_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkRecommendation {
    pub pipeline_id: PipelineId,
    pub current_max_delay_ms: u64,
    pub recommended_max_delay_ms: u64,
    pub p50_latency_ms: u64,
    pub p95_latency_ms: u64,
    pub p99_latency_ms: u64,
    pub sample_size: u64,
    pub confidence: f64,
    pub reasoning: String,
}

impl WatermarkAdvisor {
    /// Compute recommendation based on collected samples
    pub fn recommend(&self) -> Option<WatermarkRecommendation> {
        if self.samples.len() < 1000 {
            return None;  // not enough data
        }

        let mut sorted: Vec<f64> = self.samples.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let p99 = sorted[(sorted.len() as f64 * 0.99) as usize];
        // Recommend p99 + 20% buffer, rounded to nearest second
        let recommended = ((p99 * 1.2) / 1000.0).ceil() as u64 * 1000;

        Some(WatermarkRecommendation {
            recommended_max_delay_ms: recommended,
            p99_latency_ms: p99 as u64,
            confidence: if self.samples.len() > 10_000 { 0.95 } else { 0.80 },
            ..
        })
    }
}
```

**Testing**:
- `Unit: uniform delay 1000ms +-100ms → recommends ~1320ms (p99 * 1.2)`
- `Unit: skewed distribution with outliers → recommendation covers p99`
- `Unit: fewer than 1000 samples → returns None`
- `Unit: confidence is 0.95 for >10K samples, 0.80 for 1K-10K`
- `Proptest: any latency distribution → recommended value >= p99`

---

## Phase 10: Declarative Pipeline Testing Framework

### Purpose

Implement snapshot-based regression tests for streaming pipelines, analogous to dbt tests. Users define test fixtures (input events + expected output) and the framework validates that pipeline SQL produces correct results. After this phase, pipeline authors can test their transformations in CI before deployment.

### Tasks

#### 10.1 — Test Runner Engine

**What**: Build a test runner that executes pipeline SQL against fixed input events and compares output to expected results.

**Design**:

```rust
// crates/engine-core/src/testing/runner.rs

/// A pipeline test definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineTestDef {
    pub name: String,
    pub sql: String,
    pub input_events: Vec<TestEvent>,
    pub expected_output: Vec<TestEvent>,
    pub watermark_advance_ms: u64,      // advance watermark by this amount after input
    pub assertions: Vec<Assertion>,
    pub timeout_ms: u64,                 // test timeout (default: 30_000)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestEvent {
    pub stream_name: String,
    pub timestamp_ms: u64,
    pub fields: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Assertion {
    RowCount { expected: usize },
    FieldEquals { row: usize, field: String, value: serde_json::Value },
    FieldRange { row: usize, field: String, min: f64, max: f64 },
    OrderBy { field: String, direction: SortDirection },
    NoNulls { field: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    pub name: String,
    pub status: TestStatus,
    pub actual_output: Vec<TestEvent>,
    pub assertion_results: Vec<AssertionResult>,
    pub diff: Option<String>,           // unified diff between expected and actual
    pub duration_ms: u64,
}

#[derive(Debug, Clone)]
pub enum TestStatus { Passed, Failed, Error(String) }

pub async fn run_pipeline_test(test: &PipelineTestDef) -> TestResult {
    // 1. Parse SQL and build execution plan
    // 2. Create in-memory source from input_events
    // 3. Execute pipeline with bounded source
    // 4. Advance watermark by watermark_advance_ms
    // 5. Collect all output events
    // 6. Evaluate assertions
    // 7. Compute diff if mismatch
    todo!()
}
```

**Testing**:
- `Unit: simple passthrough query → output equals input`
- `Unit: filter query → output contains only matching rows`
- `Unit: tumble window aggregation → correct aggregated results`
- `Unit: assertion RowCount passes when count matches`
- `Unit: assertion RowCount fails with clear error message`
- `Unit: assertion FieldEquals passes for correct value`
- `Unit: timeout exceeded → TestStatus::Error with timeout message`
- `Fixture: 5 predefined pipeline tests from tests/fixtures/ all pass`

#### 10.2 — Test Management API and UI

**What**: REST endpoints for creating, running, and viewing pipeline test results, plus a UI for managing tests and viewing diff output.

**Design**:

```typescript
// control-plane/src/routes/pipeline-tests.ts

// POST /api/v1/pipelines/:id/tests          → create test
// GET /api/v1/pipelines/:id/tests           → list tests
// POST /api/v1/pipelines/:id/tests/:testId/run → run test
// GET /api/v1/pipelines/:id/tests/:testId/runs → list test runs
// POST /api/v1/pipelines/:id/tests/run-all  → run all tests for pipeline
```

**Testing**:
- `Integration: create test → stored in pipeline_tests table`
- `Integration: run test → result stored in pipeline_test_runs with pass/fail`
- `Integration: run-all → all tests executed, summary returned`
- `E2E (Playwright): test results page shows pass/fail with diff viewer`
- `E2E (Playwright): failed test shows unified diff between expected and actual`

---

## Phase 11: Kubernetes Operator and Production Deployment

### Purpose

Build the Kubernetes operator that manages engine instances, auto-scales based on metrics, and provides production-grade deployment. After this phase, the platform can be deployed to any Kubernetes cluster via Helm chart with a custom StreamPipeline CRD.

### Tasks

#### 11.1 — Custom Resource Definitions

**What**: Define Kubernetes CRDs for StreamPipeline, StreamConnector, and StreamCluster resources.

**Design**:

```rust
// k8s/operator/src/crd.rs

use kube::CustomResource;
use schemars::JsonSchema;

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "stream.platform",
    version = "v1alpha1",
    kind = "StreamPipeline",
    namespaced,
    status = "StreamPipelineStatus",
    printcolumn = r#"{"name":"Status", "type":"string", "jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Parallelism", "type":"integer", "jsonPath":".spec.parallelism"}"#,
)]
pub struct StreamPipelineSpec {
    pub sql: String,
    pub engine_target: String,
    pub parallelism: u32,
    pub checkpoint_interval_ms: u64,
    pub checkpoint_storage: String,
    pub sources: Vec<SourceRef>,
    pub sinks: Vec<SinkRef>,
    pub resources: ResourceRequirements,
    pub auto_scaling: Option<AutoScalingSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AutoScalingSpec {
    pub enabled: bool,
    pub min_parallelism: u32,
    pub max_parallelism: u32,
    pub target_consumer_lag: u64,
    pub scale_up_threshold: f64,   // scale up when lag > target * threshold
    pub scale_down_threshold: f64, // scale down when lag < target * threshold
    pub cooldown_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StreamPipelineStatus {
    pub phase: String,              // Pending, Running, Failed, Stopped
    pub engine_job_id: Option<String>,
    pub current_parallelism: u32,
    pub consumer_lag: Option<u64>,
    pub last_checkpoint_at: Option<String>,
    pub conditions: Vec<Condition>,
}
```

**Testing**:
- `Unit: CRD YAML generated from Rust struct matches expected schema`
- `Integration (kind cluster): apply CRD → kubectl get streampipelines works`
- `Integration (kind cluster): create StreamPipeline resource → operator reconciles`
- `Integration (kind cluster): delete StreamPipeline → engine job stopped and cleaned up`

#### 11.2 — Operator Reconciliation Loop

**What**: Implement the operator controller that watches StreamPipeline resources and reconciles desired state with actual engine state.

**Design**:

```rust
// k8s/operator/src/controller.rs

use kube::runtime::controller::{Controller, Action};

pub async fn reconcile(
    pipeline: Arc<StreamPipeline>,
    ctx: Arc<Context>,
) -> Result<Action, Error> {
    let status = pipeline.status.as_ref();
    let desired = &pipeline.spec;

    match status.map(|s| s.phase.as_str()) {
        None | Some("Pending") => {
            // Deploy pipeline to engine
            let job_id = ctx.engine_client.deploy(desired).await?;
            // Update status to Running
            ctx.update_status(&pipeline, "Running", Some(job_id)).await?;
            Ok(Action::requeue(Duration::from_secs(30)))
        }
        Some("Running") => {
            // Check if auto-scaling needed
            if let Some(auto_scaling) = &desired.auto_scaling {
                if auto_scaling.enabled {
                    let lag = ctx.engine_client.get_consumer_lag(&pipeline).await?;
                    let new_parallelism = compute_desired_parallelism(
                        status.unwrap().current_parallelism,
                        lag,
                        auto_scaling,
                    );
                    if new_parallelism != status.unwrap().current_parallelism {
                        ctx.engine_client.rescale(&pipeline, new_parallelism).await?;
                    }
                }
            }
            Ok(Action::requeue(Duration::from_secs(30)))
        }
        Some("Failed") => {
            // Attempt restart from last checkpoint
            ctx.engine_client.restart_from_checkpoint(&pipeline).await?;
            Ok(Action::requeue(Duration::from_secs(60)))
        }
        _ => Ok(Action::requeue(Duration::from_secs(60))),
    }
}
```

**Testing**:
- `Integration (kind cluster): create StreamPipeline → status transitions to Running`
- `Integration (kind cluster): delete spec → engine job stopped`
- `Integration (kind cluster): update SQL → rolling restart with savepoint`
- `Integration (kind cluster): auto-scaling increases parallelism when lag exceeds threshold`
- `Integration (kind cluster): auto-scaling respects cooldown period`
- `Unit: compute_desired_parallelism scales up by 1 when lag > target * scale_up_threshold`
- `Unit: compute_desired_parallelism never exceeds max_parallelism`

#### 11.3 — Helm Chart

**What**: Create a Helm chart for installing the complete platform (engine, control plane, web UI, PostgreSQL, Kafka) on Kubernetes.

**Design**:

```
k8s/manifests/helm/stream-platform/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── _helpers.tpl
│   ├── engine-deployment.yaml
│   ├── engine-service.yaml
│   ├── control-plane-deployment.yaml
│   ├── control-plane-service.yaml
│   ├── web-ui-deployment.yaml
│   ├── web-ui-service.yaml
│   ├── ingress.yaml
│   ├── operator-deployment.yaml
│   ├── crds/
│   │   └── streampipeline-crd.yaml
│   ├── configmap.yaml
│   ├── secret.yaml
│   └── serviceaccount.yaml
└── README.md
```

**Testing**:
- `Unit: helm template renders valid YAML`
- `Unit: helm lint passes with no errors`
- `Integration (kind cluster): helm install creates all resources`
- `Integration (kind cluster): health endpoints respond on all services`
- `Integration (kind cluster): helm upgrade applies changes without downtime`
- `Integration (kind cluster): helm uninstall removes all resources`

---

## Phase 12: Advanced Features — Disaggregated State, Multi-Engine, and Vector Storage

### Purpose

Implement the advanced features from the backlog: disaggregated state backend (compute-storage separation), multi-engine SQL targeting (Flink/Spark runners), and native vector storage for real-time AI/RAG workloads. These are the features that position the platform beyond commodity streaming tools.

### Tasks

#### 12.1 — Disaggregated State Backend

**What**: Implement a state backend where compute nodes access state from object storage (S3) with a local cache layer, enabling independent scaling of compute and storage.

**Design**:

```rust
// crates/engine-core/src/state/s3_backend.rs

/// Disaggregated state backend: local RocksDB cache + S3 persistent store
pub struct DisaggregatedStateBackend {
    /// Local cache for hot state
    local_cache: RocksDB,
    /// Remote persistent store
    remote: S3Client,
    bucket: String,
    /// Cache eviction policy
    max_cache_size_bytes: u64,
    /// Async state prefetch
    prefetch_queue: mpsc::Sender<PrefetchRequest>,
}

pub trait StateBackend: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError>;
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StateError>;
    async fn delete(&self, key: &[u8]) -> Result<(), StateError>;
    async fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StateError>;
    async fn checkpoint(&self, checkpoint_path: &str) -> Result<u64, StateError>;
    async fn restore(&self, checkpoint_path: &str) -> Result<(), StateError>;
}
```

**Testing**:
- `Integration (MinIO): put/get round-trips for 1MB values`
- `Integration (MinIO): cache miss → fetched from S3, cached locally`
- `Integration (MinIO): cache hit → returned from local RocksDB (no S3 call)`
- `Integration (MinIO): cache eviction when max_cache_size_bytes exceeded`
- `Integration (MinIO): checkpoint uploads entire state to S3`
- `Benchmark (criterion): cached get <1ms, uncached get <50ms`

#### 12.2 — Multi-Engine SQL Targeting

**What**: Implement a SQL compilation layer that can target different execution engines (native Rust engine, Apache Flink via REST API, Apache Spark via REST API) from the same SQL surface.

**Design**:

```rust
// crates/engine-core/src/sql/compiler.rs

pub trait EngineCompiler: Send + Sync {
    /// Compile a StreamNode plan into engine-specific job definition
    async fn compile(&self, plan: &StreamNode) -> Result<EngineJob, CompileError>;
    /// Submit a compiled job to the engine
    async fn submit(&self, job: &EngineJob) -> Result<String, SubmitError>;  // returns job ID
    /// Get job status
    async fn status(&self, job_id: &str) -> Result<JobStatus, EngineError>;
    /// Cancel a running job
    async fn cancel(&self, job_id: &str) -> Result<(), EngineError>;
}

pub struct NativeCompiler;     // compiles to native Rust PipelineRuntime
pub struct FlinkCompiler;      // compiles to Flink SQL and submits via Flink REST API
pub struct SparkCompiler;      // compiles to Spark SQL and submits via Spark REST API

pub enum EngineJob {
    Native(PipelineConfig),
    Flink { sql: String, flink_config: HashMap<String, String> },
    Spark { sql: String, spark_config: HashMap<String, String> },
}
```

**Testing**:
- `Unit: NativeCompiler produces valid PipelineConfig from StreamNode`
- `Unit: FlinkCompiler translates TUMBLE to Flink SQL TUMBLE syntax`
- `Unit: SparkCompiler translates TUMBLE to Spark window expression`
- `Integration (mocked Flink REST): FlinkCompiler submits job successfully`
- `Integration: same SQL produces equivalent results on native and Flink engines`

#### 12.3 — Native Vector Storage for Real-Time RAG

**What**: Add vector(n) column type support to streams and materialized views, with real-time similarity search for AI/RAG workloads.

**Design**:

```rust
// crates/engine-core/src/execution/vector.rs

use arrow::array::FixedSizeListArray;

/// Vector similarity search operator
pub struct VectorSearchOperator {
    /// Dimension of vectors
    dimension: usize,
    /// Distance metric
    metric: DistanceMetric,
    /// HNSW index for approximate nearest neighbor
    index: HnswIndex,
}

#[derive(Debug, Clone)]
pub enum DistanceMetric {
    Cosine,
    Euclidean,
    DotProduct,
}

/// SQL extension: SELECT * FROM embeddings ORDER BY vector_distance(embedding, ?, 'cosine') LIMIT 10
pub struct VectorDistanceFunction {
    pub column: String,
    pub query_vector: Vec<f32>,
    pub metric: DistanceMetric,
}
```

**Testing**:
- `Unit: cosine distance between identical vectors → 0.0`
- `Unit: cosine distance between orthogonal vectors → 1.0`
- `Unit: HNSW index returns correct top-K nearest neighbors`
- `Unit: vector(128) column stores and retrieves 128-dimensional vectors`
- `Integration: materialized view with vector column → searchable via SQL`
- `Benchmark (criterion): 1M vector search with HNSW <10ms for top-10`

---

## Phase Summary & Dependencies

```
Phase 1: Foundation                    ─── required by everything
    │
Phase 2: Stream Engine Core           ─── requires Phase 1
    │
Phase 3: Kafka Connectivity           ─── requires Phase 2
    │
    ├── Phase 4: CDC & Checkpointing   ─── requires Phase 3
    │       │
    │       └── Phase 5: Control Plane API ─── requires Phase 4
    │               │
    │               ├── Phase 6: Web UI              ─── requires Phase 5
    │               │       │
    │               │       └── Phase 8: AI NL-to-SQL ─── requires Phase 5 + Phase 6
    │               │
    │               ├── Phase 7: Monitoring & Alerting ─── requires Phase 5
    │               │       │
    │               │       └── Phase 9: AI Anomaly Detection ─── requires Phase 7
    │               │
    │               └── Phase 10: Pipeline Testing     ─── requires Phase 2 + Phase 5
    │
    └── Phase 11: Kubernetes Operator  ─── requires Phase 5
            │
            └── Phase 12: Advanced Features ─── requires Phase 2 + Phase 4 + Phase 11

Parallelism opportunities:
  - Phases 6, 7, and 10 can be developed concurrently after Phase 5
  - Phases 8 and 9 can be developed concurrently after their respective prerequisites
  - Phase 11 can begin as soon as Phase 5 is complete (parallel with Phases 6-10)
  - Phase 12 can begin partially (12.1 state backend) after Phase 4
```

---

## Definition of Done (per phase)

Every phase must satisfy ALL of the following before it is considered complete:

- [ ] All tasks in the phase are implemented
- [ ] All unit tests pass (`cargo test` and `pnpm test`)
- [ ] All integration tests pass (with Docker Compose dependencies running)
- [ ] Rust: `cargo clippy --workspace -- -D warnings` passes with no warnings
- [ ] Rust: `cargo fmt --check` passes
- [ ] TypeScript: `pnpm lint` passes (ESLint + oxlint)
- [ ] TypeScript: `pnpm typecheck` passes (tsc --noEmit)
- [ ] Docker build succeeds (`docker compose build`)
- [ ] No new `TODO` or `FIXME` comments without a linked issue
- [ ] New REST endpoints appear in the auto-generated OpenAPI 3.1 spec
- [ ] New database tables have corresponding Drizzle ORM migrations
- [ ] New metrics use OpenTelemetry semantic conventions
- [ ] Configuration changes documented in example config
- [ ] Breaking API changes increment the API version
