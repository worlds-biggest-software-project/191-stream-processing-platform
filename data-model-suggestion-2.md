# Data Model Suggestion 2: Event-Sourced / Audit-First

> Project: Stream Processing Platform · Created: 2026-05-20

## Philosophy

This model treats every state change in the platform as an immutable event appended to an event store. The current state of any entity — a pipeline, connector, deployment, or schema — is derived by replaying its event stream. A CQRS (Command Query Responsibility Segregation) layer maintains denormalized read models (materialized projections) optimized for the queries the UI and API need.

This is a natural fit for a stream processing platform: the platform itself is built on the same event-driven principles it offers to its users. Every pipeline creation, configuration change, deployment, checkpoint, and alert is an event in the platform's own event stream. This provides a complete, immutable audit trail by construction — there is no separate audit log because the event store *is* the audit log. Temporal queries ("what was the pipeline configuration when it failed at 3am?") are answered by replaying events up to that timestamp.

The pattern is inspired by EventStoreDB, Axon Framework, and the academic work on the Dataflow Model. Confluent's own internal architecture treats Kafka topics as the source of truth for cluster metadata (KRaft log), and this model applies the same principle to platform metadata.

**Best for:** Teams that need full audit trails, temporal queries, and the ability to reconstruct any past state — particularly in regulated environments or where root-cause analysis of pipeline failures requires understanding exact configuration at any point in time.

**Trade-offs:**
- Pro: Complete, immutable audit trail by construction — every change is recorded
- Pro: Temporal queries are natural: "what was true at time T?" is a replay to T
- Pro: Adding new event types requires no schema migration of existing tables
- Pro: Event stream can feed the platform's own analytics (dogfooding the product)
- Con: Higher read latency unless projections are well-maintained
- Con: Projection rebuild can be slow for entities with long event histories
- Con: More complex application code: every write is an event append, every read queries a projection
- Con: JSONB event payloads lose column-level type safety
- Con: Querying across entities requires well-designed projections; ad-hoc cross-entity queries are harder

---

## Standards Alignment

| Standard | How It's Used |
|----------|---------------|
| CloudEvents v1.0 | Every event in the `events` table uses CloudEvents context attributes (specversion, type, source, id, time) as the envelope format |
| Apache Kafka Protocol | The event store can optionally be backed by a Kafka topic for durability; event types align with Kafka record key/value semantics |
| OpenTelemetry | Observability events use OTel semantic conventions for metric and span event types |
| JSON Schema | Event payload schemas are versioned and validated against JSON Schema definitions stored in the schema catalog |
| AsyncAPI 3.0 | Platform event channels are documented as AsyncAPI operations for external consumers of the platform event stream |
| OWASP API Security | All mutation events capture actor, IP, and user agent for security audit |

---

## Event Store (Source of Truth)

```sql
-- The single source of truth for all platform state changes
CREATE TABLE events (
    -- CloudEvents context attributes
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- CloudEvents 'id'
    specversion     VARCHAR(10) NOT NULL DEFAULT '1.0',          -- CloudEvents specversion
    type            VARCHAR(255) NOT NULL,                       -- CloudEvents 'type'
        -- e.g. com.streamplatform.pipeline.created
        --      com.streamplatform.connector.config_updated
        --      com.streamplatform.deployment.started
        --      com.streamplatform.checkpoint.completed
    source          VARCHAR(500) NOT NULL,                       -- CloudEvents 'source'
        -- e.g. /tenants/{tenant_id}/pipelines/{pipeline_id}
    subject         VARCHAR(500),                                -- CloudEvents 'subject' (optional)

    -- Aggregate identity
    aggregate_type  VARCHAR(100) NOT NULL,                       -- pipeline, connector, deployment, schema, tenant, user
    aggregate_id    UUID NOT NULL,                               -- the entity this event belongs to
    sequence_num    BIGINT NOT NULL,                             -- monotonic sequence within the aggregate

    -- Tenant scoping
    tenant_id       UUID NOT NULL,

    -- Actor information (who caused this event)
    actor_user_id   UUID,
    actor_api_key_id UUID,
    actor_ip        INET,
    actor_user_agent VARCHAR(500),

    -- Payload
    data            JSONB NOT NULL,                              -- event-specific payload
    datacontenttype VARCHAR(100) NOT NULL DEFAULT 'application/json',

    -- Metadata
    schema_version  INTEGER NOT NULL DEFAULT 1,                  -- payload schema version for evolution
    correlation_id  UUID,                                        -- links related events across aggregates
    causation_id    UUID REFERENCES events(event_id),            -- the event that caused this event

    -- Timestamp
    time            TIMESTAMPTZ NOT NULL DEFAULT now(),           -- CloudEvents 'time'

    UNIQUE (aggregate_type, aggregate_id, sequence_num)
);

-- Primary query pattern: replay events for an aggregate
CREATE INDEX idx_events_aggregate ON events(aggregate_type, aggregate_id, sequence_num);

-- Query by tenant (for projections and access control)
CREATE INDEX idx_events_tenant ON events(tenant_id, time DESC);

-- Query by type (for building specific projections)
CREATE INDEX idx_events_type ON events(type, time);

-- Correlation tracking
CREATE INDEX idx_events_correlation ON events(correlation_id) WHERE correlation_id IS NOT NULL;

-- Time-based partition key for archival
CREATE INDEX idx_events_time ON events(time);
```

### Example Event Payloads

```sql
-- Pipeline created event
-- type: com.streamplatform.pipeline.created
-- data:
-- {
--   "name": "fraud-detection-v2",
--   "namespace_id": "550e8400-e29b-41d4-a716-446655440000",
--   "sql_text": "SELECT user_id, COUNT(*) as tx_count FROM transactions GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE), user_id HAVING COUNT(*) > 100",
--   "engine_target": "flink",
--   "parallelism": 4,
--   "sources": [{"connector_id": "...", "schema_subject": "transactions-value"}],
--   "sinks": [{"connector_id": "...", "schema_subject": "fraud-alerts-value"}]
-- }

-- Pipeline SQL updated event
-- type: com.streamplatform.pipeline.sql_updated
-- data:
-- {
--   "previous_sql": "SELECT ... HAVING COUNT(*) > 100",
--   "new_sql": "SELECT ... HAVING COUNT(*) > 50",
--   "version": 3,
--   "changelog": "Lowered threshold from 100 to 50 per fraud team request"
-- }

-- Deployment started event
-- type: com.streamplatform.deployment.started
-- data:
-- {
--   "pipeline_id": "...",
--   "pipeline_version": 3,
--   "environment": "production",
--   "engine_job_id": "flink-job-abc123",
--   "parallelism": 4,
--   "checkpoint_interval_ms": 60000
-- }

-- Checkpoint completed event
-- type: com.streamplatform.checkpoint.completed
-- data:
-- {
--   "deployment_id": "...",
--   "checkpoint_id": 42,
--   "storage_path": "s3://checkpoints/tenant-x/pipeline-y/cp-42",
--   "size_bytes": 1048576,
--   "duration_ms": 1230
-- }

-- Anomaly detected event
-- type: com.streamplatform.anomaly.detected
-- data:
-- {
--   "deployment_id": "...",
--   "detection_type": "throughput",
--   "severity": "warning",
--   "metric_name": "messaging.consumer.lag",
--   "expected_value": 150.0,
--   "actual_value": 4500.0,
--   "root_cause_explanation": "Consumer lag spiked 30x following upstream producer restart at 14:32 UTC. Lag is recovering at ~200 events/sec."
-- }
```

---

## Event Type Registry

```sql
-- Catalog of all known event types and their payload schemas
CREATE TABLE event_type_registry (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            VARCHAR(255) NOT NULL UNIQUE,         -- e.g. com.streamplatform.pipeline.created
    aggregate_type  VARCHAR(100) NOT NULL,
    description     TEXT NOT NULL,
    payload_schema  JSONB NOT NULL,                       -- JSON Schema for the data field
    schema_version  INTEGER NOT NULL DEFAULT 1,
    is_deprecated   BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## Read Model Projections

These tables are *derived* from the event store. They can be rebuilt from scratch by replaying events. They exist purely for query performance.

### Projection: Current Pipeline State

```sql
CREATE TABLE proj_pipelines (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    namespace_id    UUID NOT NULL,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    sql_text        TEXT NOT NULL,
    engine_target   VARCHAR(50) NOT NULL,
    parallelism     INTEGER NOT NULL,
    current_version INTEGER NOT NULL,
    status          VARCHAR(30) NOT NULL,                 -- draft, deployed, stopped, deleted
    created_by      UUID NOT NULL,
    last_modified_by UUID,
    event_sequence  BIGINT NOT NULL,                      -- last applied event sequence
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX idx_proj_pipelines_tenant_ns_name ON proj_pipelines(tenant_id, namespace_id, name);
CREATE INDEX idx_proj_pipelines_status ON proj_pipelines(status);
```

### Projection: Current Connector State

```sql
CREATE TABLE proj_connectors (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    namespace_id    UUID NOT NULL,
    environment_id  UUID NOT NULL,
    connector_type  VARCHAR(255) NOT NULL,
    name            VARCHAR(255) NOT NULL,
    config          JSONB NOT NULL,                       -- current merged configuration
    status          VARCHAR(30) NOT NULL,
    created_by      UUID NOT NULL,
    event_sequence  BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX idx_proj_connectors_name ON proj_connectors(tenant_id, namespace_id, environment_id, name);
```

### Projection: Current Deployment State

```sql
CREATE TABLE proj_deployments (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    pipeline_id     UUID NOT NULL,
    pipeline_name   VARCHAR(255) NOT NULL,
    pipeline_version INTEGER NOT NULL,
    environment_id  UUID NOT NULL,
    environment_name VARCHAR(100) NOT NULL,
    status          VARCHAR(30) NOT NULL,
    engine_job_id   VARCHAR(500),
    deployed_by     UUID NOT NULL,
    last_checkpoint_id BIGINT,
    last_checkpoint_at TIMESTAMPTZ,
    records_processed BIGINT NOT NULL DEFAULT 0,
    event_sequence  BIGINT NOT NULL,
    deployed_at     TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_proj_deployments_pipeline ON proj_deployments(pipeline_id);
CREATE INDEX idx_proj_deployments_status ON proj_deployments(status);
CREATE INDEX idx_proj_deployments_tenant ON proj_deployments(tenant_id);
```

### Projection: Schema Registry

```sql
CREATE TABLE proj_schema_subjects (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    namespace_id    UUID NOT NULL,
    name            VARCHAR(500) NOT NULL,
    format          VARCHAR(20) NOT NULL,
    compatibility   VARCHAR(30) NOT NULL,
    latest_version  INTEGER NOT NULL,
    version_count   INTEGER NOT NULL,
    event_sequence  BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE proj_schema_versions (
    id              UUID PRIMARY KEY,
    subject_id      UUID NOT NULL REFERENCES proj_schema_subjects(id),
    version         INTEGER NOT NULL,
    schema_text     TEXT NOT NULL,
    fingerprint     VARCHAR(64) NOT NULL,
    is_active       BOOLEAN NOT NULL,
    registered_by   UUID,
    created_at      TIMESTAMPTZ NOT NULL
);
```

### Projection: Tenant & User State

```sql
CREATE TABLE proj_tenants (
    id              UUID PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    slug            VARCHAR(100) NOT NULL UNIQUE,
    plan            VARCHAR(50) NOT NULL,
    data_residency  VARCHAR(10) NOT NULL,
    member_count    INTEGER NOT NULL DEFAULT 0,
    pipeline_count  INTEGER NOT NULL DEFAULT 0,
    connector_count INTEGER NOT NULL DEFAULT 0,
    event_sequence  BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE proj_users (
    id              UUID PRIMARY KEY,
    email           VARCHAR(320) NOT NULL UNIQUE,
    display_name    VARCHAR(255) NOT NULL,
    auth_provider   VARCHAR(50) NOT NULL,
    is_active       BOOLEAN NOT NULL,
    tenant_roles    JSONB NOT NULL DEFAULT '{}',          -- {tenant_id: role, ...}
    event_sequence  BIGINT NOT NULL,
    last_login_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);
```

### Projection: Observability Dashboard

```sql
CREATE TABLE proj_pipeline_health (
    deployment_id   UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    pipeline_id     UUID NOT NULL,
    pipeline_name   VARCHAR(255) NOT NULL,
    environment     VARCHAR(100) NOT NULL,
    status          VARCHAR(30) NOT NULL,
    throughput_eps  DOUBLE PRECISION,                     -- events per second
    consumer_lag    BIGINT,
    last_checkpoint_age_ms BIGINT,
    error_count_1h  INTEGER NOT NULL DEFAULT 0,
    anomaly_count_1h INTEGER NOT NULL DEFAULT 0,
    health_score    FLOAT,                               -- 0.0 to 1.0, AI-computed
    event_sequence  BIGINT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_proj_pipeline_health_tenant ON proj_pipeline_health(tenant_id);
CREATE INDEX idx_proj_pipeline_health_status ON proj_pipeline_health(status);
```

### Projection: AI Feature Activity

```sql
CREATE TABLE proj_nl_translations (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    user_id         UUID NOT NULL,
    natural_language TEXT NOT NULL,
    generated_sql   TEXT NOT NULL,
    confidence      FLOAT NOT NULL,
    was_accepted    BOOLEAN,
    was_edited      BOOLEAN,
    created_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE proj_anomaly_timeline (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    deployment_id   UUID NOT NULL,
    pipeline_name   VARCHAR(255) NOT NULL,
    detection_type  VARCHAR(50) NOT NULL,
    severity        VARCHAR(20) NOT NULL,
    description     TEXT NOT NULL,
    root_cause      TEXT,
    is_acknowledged BOOLEAN NOT NULL DEFAULT false,
    detected_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_proj_anomaly_timeline_tenant ON proj_anomaly_timeline(tenant_id, detected_at DESC);
```

---

## Projection Tracking

```sql
-- Tracks the last event processed by each projection rebuilder
CREATE TABLE projection_cursors (
    projection_name VARCHAR(255) PRIMARY KEY,             -- e.g. proj_pipelines, proj_deployments
    last_event_id   UUID NOT NULL REFERENCES events(event_id),
    last_event_time TIMESTAMPTZ NOT NULL,
    last_sequence   BIGINT NOT NULL,
    is_rebuilding   BOOLEAN NOT NULL DEFAULT false,
    rebuild_started_at TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## Temporal Query Examples

```sql
-- What was the pipeline configuration when it failed at 3am?
SELECT data
FROM events
WHERE aggregate_type = 'pipeline'
  AND aggregate_id = '550e8400-e29b-41d4-a716-446655440000'
  AND type IN (
      'com.streamplatform.pipeline.created',
      'com.streamplatform.pipeline.sql_updated',
      'com.streamplatform.pipeline.config_updated'
  )
  AND time <= '2026-05-20 03:00:00+00'
ORDER BY sequence_num;

-- All events related to a deployment incident (via correlation_id)
SELECT type, data, time, actor_user_id
FROM events
WHERE correlation_id = 'incident-correlation-uuid'
ORDER BY time;

-- Event frequency by type in the last 24 hours (platform analytics)
SELECT type, COUNT(*) as event_count,
       MIN(time) as first_seen, MAX(time) as last_seen
FROM events
WHERE time > now() - INTERVAL '24 hours'
GROUP BY type
ORDER BY event_count DESC;
```

---

## Table Count Summary

| Category | Tables | Notes |
|----------|--------|-------|
| Event Store | 1 | events — single immutable append-only table |
| Event Registry | 1 | event_type_registry — schema catalog for event payloads |
| Projections: Core | 4 | proj_pipelines, proj_connectors, proj_deployments, proj_schema_subjects |
| Projections: Schema | 1 | proj_schema_versions |
| Projections: Identity | 2 | proj_tenants, proj_users |
| Projections: Observability | 1 | proj_pipeline_health |
| Projections: AI | 2 | proj_nl_translations, proj_anomaly_timeline |
| Infrastructure | 1 | projection_cursors |
| **Total** | **13** | Plus the events table which replaces ~17 normalized tables |

---

## Key Design Decisions

1. **Single `events` table as the sole source of truth.** All state is derived from event replay. This eliminates the need for a separate audit log — the event store IS the audit trail. The `events` table uses CloudEvents context attributes as its envelope schema for standards compliance.

2. **Aggregate-scoped sequences** (`aggregate_type` + `aggregate_id` + `sequence_num`) enable optimistic concurrency control. A writer appends with `sequence_num = last_known + 1`; a unique constraint rejects concurrent conflicting writes.

3. **Correlation and causation tracking** (`correlation_id`, `causation_id`) link related events across aggregates. When a deployment fails and triggers an alert, both events share a correlation ID, enabling incident timeline reconstruction.

4. **Projections are disposable and rebuildable.** The `projection_cursors` table tracks replay progress. If a projection is corrupted or a new projection is needed, truncate and replay from the event store.

5. **Event payload schemas are versioned** via the `event_type_registry` and the `schema_version` field on each event. This allows payload evolution (adding fields, deprecating fields) without breaking existing projection code.

6. **Projections are denormalized for specific query patterns.** `proj_pipeline_health` combines data from pipeline, deployment, checkpoint, metric, and anomaly events into a single row per deployment — optimized for the dashboard view.

7. **The platform dogfoods its own product.** The event store can be exposed as a Kafka topic, allowing the platform's own stream processing engine to build projections, detect anomalies, and power AI features using the same technology it offers to users.

8. **Fewer tables overall** (13 vs ~30 in the normalized model). The complexity moves from the schema to the application layer (event handlers, projection builders), which is a trade-off that favours teams comfortable with event-driven architectures.
