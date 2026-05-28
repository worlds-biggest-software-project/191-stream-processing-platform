# Data Model Suggestion 3: Hybrid Relational + JSONB

> Project: Stream Processing Platform · Created: 2026-05-20

## Philosophy

This model uses relational tables with typed columns for core identity and structural relationships, but leverages PostgreSQL JSONB columns for configurations, metadata, and any domain area where the schema varies by type. Connector configurations, pipeline settings, window strategies, and AI model parameters are stored as validated JSONB rather than as separate relational tables or EAV patterns.

This is the approach used by most modern SaaS platforms that need to ship fast while maintaining data integrity for core relationships. GitLab, Supabase, and Temporal all use this hybrid pattern. The key insight is that not all data deserves its own table: a Kafka connector has different configuration fields than a PostgreSQL CDC connector, and forcing both into the same relational columns (or splitting into EAV tables) creates more complexity than a single JSONB column validated at the application layer.

The JSONB columns are not unstructured blobs — they follow documented schemas validated by the application (and optionally by PostgreSQL CHECK constraints using `jsonb_typeof` or custom functions). GIN indexes on JSONB columns enable fast containment queries (`@>` operator) for filtering by nested properties.

**Best for:** Teams that want rapid iteration, multi-engine/multi-connector flexibility, and a pragmatic balance between type safety and extensibility — particularly for an MVP or early-stage product.

**Trade-offs:**
- Pro: Fewest tables (~20); fastest to build and iterate on
- Pro: Adding a new connector type or configuration field requires no DDL migration
- Pro: JSONB containment queries (`@>`) are fast with GIN indexes
- Pro: Connector/pipeline configs are self-contained documents — easy to export, import, and version
- Con: JSONB columns lose column-level database constraints (NOT NULL, CHECK, FK)
- Con: Schema validation moves to the application layer — bugs in validation can allow invalid data
- Con: JSONB queries are slower than indexed relational columns for large-scale analytical queries
- Con: ORMs have weaker JSONB support than typed columns; custom query builders may be needed

---

## Standards Alignment

| Standard | How It's Used |
|----------|---------------|
| CloudEvents v1.0 | `pipeline_events` JSONB payloads follow CloudEvents envelope structure; context attributes stored as typed columns |
| Apache Kafka Protocol | `connectors.config` JSONB stores Kafka-compatible connector configuration matching the Kafka Connect REST API format |
| Avro / Protobuf / JSON Schema | `schema_subjects` stores schema definitions as text; `config` JSONB holds compatibility settings matching the Confluent Schema Registry API |
| JSON Schema (Draft 2020-12) | `connector_types.config_schema` stores a JSON Schema definition that validates the `connectors.config` JSONB field |
| OpenTelemetry | `metrics` JSONB labels use OTel semantic convention attribute names |
| AsyncAPI 3.0 | Connector definitions include AsyncAPI metadata in their JSONB config for documentation generation |
| OAuth 2.0 / OIDC | `users.auth_metadata` JSONB stores provider-specific OIDC claims and tokens |
| OWASP API Security | `audit_log` captures mutations with JSONB diff payloads |

---

## Identity & Multi-Tenancy

```sql
CREATE TABLE tenants (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL,
    slug            VARCHAR(100) NOT NULL UNIQUE,
    plan            VARCHAR(50) NOT NULL DEFAULT 'free',
    data_residency  VARCHAR(10) NOT NULL DEFAULT 'us',
    settings        JSONB NOT NULL DEFAULT '{}',
    -- settings example:
    -- {
    --   "max_pipelines": 50,
    --   "max_connectors": 100,
    --   "retention_days": 90,
    --   "features": {"ai_authoring": true, "anomaly_detection": true}
    -- }
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           VARCHAR(320) NOT NULL UNIQUE,
    display_name    VARCHAR(255) NOT NULL,
    password_hash   VARCHAR(255),
    auth_provider   VARCHAR(50) NOT NULL DEFAULT 'local',
    auth_metadata   JSONB NOT NULL DEFAULT '{}',
    -- auth_metadata example (Google OIDC):
    -- {
    --   "sub": "1234567890",
    --   "picture": "https://...",
    --   "locale": "en",
    --   "hd": "company.com"
    -- }
    is_active       BOOLEAN NOT NULL DEFAULT true,
    last_login_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE tenant_memberships (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role            VARCHAR(50) NOT NULL DEFAULT 'viewer',
    permissions     JSONB NOT NULL DEFAULT '[]',
    -- permissions override example:
    -- ["pipelines:deploy:production", "connectors:create"]
    invited_by      UUID REFERENCES users(id),
    joined_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, user_id)
);

CREATE INDEX idx_tenant_memberships_tenant ON tenant_memberships(tenant_id);
CREATE INDEX idx_tenant_memberships_user ON tenant_memberships(user_id);

CREATE TABLE api_keys (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    key_prefix      VARCHAR(10) NOT NULL,
    key_hash        VARCHAR(255) NOT NULL,
    scopes          JSONB NOT NULL DEFAULT '[]',
    expires_at      TIMESTAMPTZ,
    last_used_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_api_keys_prefix ON api_keys(key_prefix);
```

---

## Namespaces & Environments

```sql
CREATE TABLE namespaces (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    slug            VARCHAR(100) NOT NULL,
    description     TEXT,
    labels          JSONB NOT NULL DEFAULT '{}',
    -- labels example: {"team": "fraud", "cost_center": "eng-42"}
    is_default      BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, slug)
);

CREATE TABLE environments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name            VARCHAR(100) NOT NULL,
    slug            VARCHAR(50) NOT NULL,
    is_production   BOOLEAN NOT NULL DEFAULT false,
    config          JSONB NOT NULL DEFAULT '{}',
    -- config example:
    -- {
    --   "checkpoint_storage": "s3://my-bucket/checkpoints",
    --   "default_parallelism": 2,
    --   "auto_scaling": {"enabled": true, "min": 1, "max": 8}
    -- }
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, slug)
);
```

---

## Schema Registry

```sql
CREATE TABLE schema_subjects (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    namespace_id    UUID NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    name            VARCHAR(500) NOT NULL,
    format          VARCHAR(20) NOT NULL,                 -- avro, protobuf, json_schema
    config          JSONB NOT NULL DEFAULT '{}',
    -- config example:
    -- {
    --   "compatibility": "BACKWARD",
    --   "normalize_schemas": true,
    --   "validation_level": "full"
    -- }
    latest_version  INTEGER NOT NULL DEFAULT 0,
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, namespace_id, name)
);

CREATE TABLE schema_versions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_id      UUID NOT NULL REFERENCES schema_subjects(id) ON DELETE CASCADE,
    version         INTEGER NOT NULL,
    schema_text     TEXT NOT NULL,
    fingerprint     VARCHAR(64) NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}',
    -- metadata example:
    -- {
    --   "properties": {"owner": "fraud-team", "sensitivity": "pii"},
    --   "rule_set": {"domain_rules": [...]}
    -- }
    is_active       BOOLEAN NOT NULL DEFAULT true,
    registered_by   UUID REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (subject_id, version),
    UNIQUE (subject_id, fingerprint)
);
```

---

## Connectors

```sql
-- Connector type definitions (system-managed catalog)
CREATE TABLE connector_types (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL UNIQUE,
    category        VARCHAR(50) NOT NULL,                 -- source, sink, both
    icon_url        VARCHAR(500),
    documentation_url VARCHAR(500),
    config_schema   JSONB NOT NULL,
    -- config_schema is a JSON Schema document that validates connector.config
    -- Example for kafka_source:
    -- {
    --   "type": "object",
    --   "required": ["bootstrap_servers", "topic", "group_id"],
    --   "properties": {
    --     "bootstrap_servers": {"type": "string", "description": "Comma-separated broker addresses"},
    --     "topic": {"type": "string"},
    --     "group_id": {"type": "string"},
    --     "security_protocol": {"type": "string", "enum": ["PLAINTEXT", "SASL_SSL", "SSL"]},
    --     "sasl_mechanism": {"type": "string", "enum": ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]},
    --     "sasl_username": {"type": "string"},
    --     "sasl_password": {"type": "string", "x-secret": true},
    --     "auto_offset_reset": {"type": "string", "enum": ["earliest", "latest"], "default": "latest"}
    --   }
    -- }
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Connector instances
CREATE TABLE connectors (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    namespace_id    UUID NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    environment_id  UUID NOT NULL REFERENCES environments(id),
    connector_type_id UUID NOT NULL REFERENCES connector_types(id),
    name            VARCHAR(255) NOT NULL,
    config          JSONB NOT NULL,
    -- config validated against connector_types.config_schema
    -- Example for a Kafka source:
    -- {
    --   "bootstrap_servers": "kafka-1:9092,kafka-2:9092",
    --   "topic": "orders",
    --   "group_id": "stream-platform-orders",
    --   "security_protocol": "SASL_SSL",
    --   "sasl_mechanism": "SCRAM-SHA-256",
    --   "sasl_username": "stream-user",
    --   "sasl_password": "vault:secret/kafka/password",
    --   "auto_offset_reset": "earliest"
    -- }
    status          VARCHAR(30) NOT NULL DEFAULT 'created',
    health          JSONB NOT NULL DEFAULT '{}',
    -- health example:
    -- {
    --   "last_check_at": "2026-05-20T10:00:00Z",
    --   "is_reachable": true,
    --   "latency_ms": 12,
    --   "error": null
    -- }
    created_by      UUID NOT NULL REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, namespace_id, environment_id, name)
);

CREATE INDEX idx_connectors_tenant_ns ON connectors(tenant_id, namespace_id);
CREATE INDEX idx_connectors_status ON connectors(status);
CREATE INDEX idx_connectors_config ON connectors USING GIN (config jsonb_path_ops);
```

---

## Pipelines

```sql
CREATE TABLE pipelines (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    namespace_id    UUID NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    sql_text        TEXT NOT NULL,
    current_version INTEGER NOT NULL DEFAULT 1,
    engine_target   VARCHAR(50) NOT NULL DEFAULT 'flink',
    config          JSONB NOT NULL DEFAULT '{}',
    -- config example:
    -- {
    --   "parallelism": 4,
    --   "checkpoint_interval_ms": 60000,
    --   "checkpoint_storage": "s3://checkpoints/",
    --   "restart_strategy": {"type": "fixed-delay", "attempts": 3, "delay_ms": 10000},
    --   "watermark": {
    --     "strategy": "bounded_out_of_orderness",
    --     "max_delay_ms": 5000,
    --     "ai_recommended": true,
    --     "ai_confidence": 0.92
    --   },
    --   "windows": [
    --     {"type": "tumbling", "size_ms": 300000, "time_field": "event_time"},
    --     {"type": "session", "gap_ms": 600000, "time_field": "event_time"}
    --   ]
    -- }
    sources         JSONB NOT NULL DEFAULT '[]',
    -- sources example:
    -- [
    --   {"connector_id": "uuid-1", "schema_subject": "orders-value", "alias": "orders"},
    --   {"connector_id": "uuid-2", "schema_subject": "users-value", "alias": "users"}
    -- ]
    sinks           JSONB NOT NULL DEFAULT '[]',
    -- sinks example:
    -- [
    --   {"connector_id": "uuid-3", "schema_subject": "fraud-alerts-value", "alias": "alerts"}
    -- ]
    labels          JSONB NOT NULL DEFAULT '{}',
    -- labels example: {"team": "fraud", "priority": "p0", "domain": "payments"}
    created_by      UUID NOT NULL REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, namespace_id, name)
);

CREATE INDEX idx_pipelines_tenant_ns ON pipelines(tenant_id, namespace_id);
CREATE INDEX idx_pipelines_labels ON pipelines USING GIN (labels jsonb_path_ops);
CREATE INDEX idx_pipelines_sources ON pipelines USING GIN (sources jsonb_path_ops);

-- Version history (lightweight — just the SQL and config diffs)
CREATE TABLE pipeline_versions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    version         INTEGER NOT NULL,
    sql_text        TEXT NOT NULL,
    config          JSONB NOT NULL,
    sources         JSONB NOT NULL,
    sinks           JSONB NOT NULL,
    changelog       TEXT,
    created_by      UUID NOT NULL REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (pipeline_id, version)
);
```

---

## Deployments & Checkpoints

```sql
CREATE TABLE deployments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    pipeline_version INTEGER NOT NULL,
    environment_id  UUID NOT NULL REFERENCES environments(id),
    status          VARCHAR(30) NOT NULL DEFAULT 'pending',
    desired_state   VARCHAR(30) NOT NULL DEFAULT 'running',
    engine_job_id   VARCHAR(500),
    engine_metadata JSONB NOT NULL DEFAULT '{}',
    -- engine_metadata example:
    -- {
    --   "flink_job_id": "abc123",
    --   "flink_cluster": "prod-cluster-1",
    --   "task_managers": 4,
    --   "total_slots": 16,
    --   "savepoint_path": "s3://..."
    -- }
    deployed_by     UUID NOT NULL REFERENCES users(id),
    deployed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    stopped_at      TIMESTAMPTZ,
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_deployments_pipeline ON deployments(pipeline_id);
CREATE INDEX idx_deployments_status ON deployments(status);
CREATE INDEX idx_deployments_tenant_env ON deployments(tenant_id, environment_id);

CREATE TABLE checkpoints (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id   UUID NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    checkpoint_id   BIGINT NOT NULL,
    status          VARCHAR(30) NOT NULL,
    trigger_type    VARCHAR(30) NOT NULL DEFAULT 'periodic',
    storage_path    VARCHAR(1000),
    size_bytes      BIGINT,
    duration_ms     BIGINT,
    metadata        JSONB NOT NULL DEFAULT '{}',
    -- metadata example:
    -- {
    --   "state_size_bytes": 524288,
    --   "alignment_duration_ms": 50,
    --   "subtask_stats": {"source": {"size": 100000}, "aggregate": {"size": 424288}}
    -- }
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_checkpoints_deployment ON checkpoints(deployment_id, checkpoint_id DESC);
```

---

## Materialized Views

```sql
CREATE TABLE materialized_views (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    namespace_id    UUID NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    definition_sql  TEXT NOT NULL,
    config          JSONB NOT NULL DEFAULT '{}',
    -- config example:
    -- {
    --   "refresh_mode": "continuous",
    --   "refresh_interval_ms": null,
    --   "retention_ms": 86400000,
    --   "indexes": ["user_id", "event_time"]
    -- }
    stats           JSONB NOT NULL DEFAULT '{}',
    -- stats example:
    -- {"row_count": 150000, "size_bytes": 12582912, "last_refreshed_at": "2026-05-20T10:00:00Z"}
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, namespace_id, name)
);
```

---

## Observability

```sql
CREATE TABLE pipeline_metrics (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id   UUID NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    metric_name     VARCHAR(255) NOT NULL,
    metric_value    DOUBLE PRECISION NOT NULL,
    labels          JSONB NOT NULL DEFAULT '{}',
    recorded_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_pipeline_metrics_deployment_time ON pipeline_metrics(deployment_id, recorded_at DESC);
CREATE INDEX idx_pipeline_metrics_name_time ON pipeline_metrics(metric_name, recorded_at DESC);
CREATE INDEX idx_pipeline_metrics_labels ON pipeline_metrics USING GIN (labels jsonb_path_ops);

CREATE TABLE alerts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    pipeline_id     UUID REFERENCES pipelines(id) ON DELETE SET NULL,
    name            VARCHAR(255) NOT NULL,
    config          JSONB NOT NULL,
    -- config example:
    -- {
    --   "condition_type": "threshold",
    --   "metric_name": "messaging.consumer.lag",
    --   "comparison": "gt",
    --   "threshold_value": 10000,
    --   "evaluation_window_ms": 300000,
    --   "notification_channels": [
    --     {"type": "slack", "webhook_url": "https://hooks.slack.com/..."},
    --     {"type": "email", "addresses": ["oncall@company.com"]},
    --     {"type": "pagerduty", "routing_key": "..."}
    --   ]
    -- }
    severity        VARCHAR(20) NOT NULL DEFAULT 'warning',
    is_enabled      BOOLEAN NOT NULL DEFAULT true,
    created_by      UUID NOT NULL REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE alert_incidents (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id        UUID NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    deployment_id   UUID REFERENCES deployments(id),
    status          VARCHAR(30) NOT NULL DEFAULT 'firing',
    details         JSONB NOT NULL DEFAULT '{}',
    -- details example:
    -- {
    --   "metric_value": 45000,
    --   "threshold_value": 10000,
    --   "evaluation_samples": [1000, 5000, 15000, 30000, 45000],
    --   "notification_results": [
    --     {"channel": "slack", "sent": true},
    --     {"channel": "pagerduty", "sent": true, "incident_key": "pd-123"}
    --   ]
    -- }
    fired_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by UUID REFERENCES users(id),
    resolved_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## AI Features

```sql
CREATE TABLE ai_interactions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id),
    interaction_type VARCHAR(50) NOT NULL,
    -- nl_to_sql, anomaly_detection, watermark_recommendation,
    -- root_cause_analysis, connector_suggestion
    request         JSONB NOT NULL,
    -- request example (nl_to_sql):
    -- {
    --   "natural_language": "Count transactions per user in 5-minute windows where count exceeds 100",
    --   "context": {"namespace": "fraud", "available_sources": ["transactions", "users"]}
    -- }
    response        JSONB NOT NULL,
    -- response example (nl_to_sql):
    -- {
    --   "generated_sql": "SELECT user_id, COUNT(*) ...",
    --   "confidence": 0.94,
    --   "explanation": "I created a tumbling window aggregation...",
    --   "alternatives": [{"sql": "...", "confidence": 0.87}]
    -- }
    feedback        JSONB,
    -- feedback example:
    -- {
    --   "was_accepted": true,
    --   "was_edited": true,
    --   "edited_sql": "SELECT ...",
    --   "rating": 4,
    --   "comment": "Good but needed to add a WHERE clause"
    -- }
    model_version   VARCHAR(100) NOT NULL,
    latency_ms      BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_ai_interactions_tenant ON ai_interactions(tenant_id, created_at DESC);
CREATE INDEX idx_ai_interactions_type ON ai_interactions(interaction_type, created_at DESC);
```

---

## Audit Log

```sql
CREATE TABLE audit_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    actor_user_id   UUID,
    actor_api_key_id UUID,
    action          VARCHAR(100) NOT NULL,
    resource_type   VARCHAR(100) NOT NULL,
    resource_id     UUID NOT NULL,
    changes         JSONB,
    -- changes example:
    -- {
    --   "sql_text": {"old": "SELECT ...", "new": "SELECT ..."},
    --   "config.parallelism": {"old": 2, "new": 4}
    -- }
    request_metadata JSONB NOT NULL DEFAULT '{}',
    -- {"ip": "1.2.3.4", "user_agent": "...", "request_id": "..."}
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_log_tenant_time ON audit_log(tenant_id, created_at DESC);
CREATE INDEX idx_audit_log_resource ON audit_log(resource_type, resource_id);
```

---

## Pipeline Testing

```sql
CREATE TABLE pipeline_tests (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    test_type       VARCHAR(50) NOT NULL,
    test_data       JSONB NOT NULL,
    -- test_data example:
    -- {
    --   "input_events": [
    --     {"user_id": "u1", "amount": 100, "event_time": "2026-05-20T10:00:00Z"},
    --     {"user_id": "u1", "amount": 200, "event_time": "2026-05-20T10:01:00Z"}
    --   ],
    --   "expected_output": [
    --     {"user_id": "u1", "total_amount": 300, "window_start": "2026-05-20T10:00:00Z"}
    --   ],
    --   "watermark_advance_ms": 300000,
    --   "assertions": [
    --     {"type": "row_count", "expected": 1},
    --     {"type": "field_value", "field": "total_amount", "operator": "eq", "value": 300}
    --   ]
    -- }
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_by      UUID NOT NULL REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pipeline_test_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_id         UUID NOT NULL REFERENCES pipeline_tests(id) ON DELETE CASCADE,
    pipeline_version INTEGER NOT NULL,
    status          VARCHAR(30) NOT NULL,
    results         JSONB NOT NULL DEFAULT '{}',
    -- results example:
    -- {
    --   "actual_output": [...],
    --   "assertion_results": [
    --     {"type": "row_count", "passed": true, "actual": 1},
    --     {"type": "field_value", "passed": true, "field": "total_amount", "actual": 300}
    --   ],
    --   "diff_summary": null
    -- }
    duration_ms     BIGINT,
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_pipeline_test_runs_test ON pipeline_test_runs(test_id, created_at DESC);
```

---

## Example JSONB Queries

```sql
-- Find all connectors using SASL_SSL security
SELECT id, name, config->>'bootstrap_servers' as brokers
FROM connectors
WHERE config @> '{"security_protocol": "SASL_SSL"}'
  AND tenant_id = 'tenant-uuid';

-- Find all pipelines with AI-recommended watermarks
SELECT id, name, config->'watermark'->>'ai_confidence' as confidence
FROM pipelines
WHERE config->'watermark'->>'ai_recommended' = 'true'
  AND tenant_id = 'tenant-uuid';

-- Find pipelines that read from a specific connector
SELECT id, name
FROM pipelines
WHERE sources @> '[{"connector_id": "connector-uuid"}]'::jsonb
  AND tenant_id = 'tenant-uuid';

-- Filter metrics by JSONB labels
SELECT metric_name, metric_value, recorded_at
FROM pipeline_metrics
WHERE labels @> '{"topic": "orders", "partition": "0"}'
  AND deployment_id = 'deployment-uuid'
  AND recorded_at > now() - INTERVAL '1 hour';

-- Aggregate AI interaction feedback for model improvement
SELECT
    interaction_type,
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE feedback->>'was_accepted' = 'true') as accepted,
    AVG((feedback->>'rating')::float) as avg_rating
FROM ai_interactions
WHERE tenant_id = 'tenant-uuid'
  AND created_at > now() - INTERVAL '30 days'
GROUP BY interaction_type;
```

---

## Table Count Summary

| Category | Tables | Notes |
|----------|--------|-------|
| Identity & Multi-Tenancy | 4 | tenants, users, tenant_memberships, api_keys |
| Namespaces & Environments | 2 | namespaces, environments |
| Schema Registry | 2 | schema_subjects, schema_versions |
| Connectors | 2 | connector_types, connectors (config in JSONB replaces properties tables) |
| Pipelines | 2 | pipelines, pipeline_versions (sources/sinks/windows in JSONB) |
| Deployments & Checkpoints | 2 | deployments, checkpoints |
| Materialized Views | 1 | materialized_views |
| Observability | 3 | pipeline_metrics, alerts, alert_incidents |
| AI Features | 1 | ai_interactions (single table for all AI interaction types) |
| Audit | 1 | audit_log |
| Pipeline Testing | 2 | pipeline_tests, pipeline_test_runs |
| **Total** | **22** | ~27% fewer tables than the normalized model |

---

## Key Design Decisions

1. **Connector configuration is a single JSONB column** validated against a JSON Schema stored in `connector_types.config_schema`. This eliminates the EAV pattern (4 tables in the normalized model reduced to 2) while still providing type-safe validation at the application layer. Adding a new connector type is a single INSERT into `connector_types` with no DDL changes.

2. **Pipeline sources, sinks, windows, and watermark config are embedded JSONB** rather than separate junction and configuration tables. This reduces the pipeline domain from 7 tables to 2 while keeping the full configuration self-contained and versionable as a single document.

3. **All AI interactions share one table** with `interaction_type` discriminator and JSONB `request`/`response`/`feedback` columns. This accommodates new AI features (new interaction types) without schema changes, and keeps the training data pipeline simple.

4. **GIN indexes on JSONB columns** (`jsonb_path_ops`) enable fast containment queries for filtering by nested properties. The `@>` operator with a GIN index is typically within 2-3x of a B-tree index on a typed column for point lookups.

5. **Alert configuration is JSONB** rather than typed columns. This allows notification channel configuration (Slack webhooks, PagerDuty routing keys, email lists) to vary without a separate table per channel type.

6. **Typed columns for identity and relationships, JSONB for configuration and metadata.** The boundary is deliberate: anything that serves as a foreign key, participates in a UNIQUE constraint, or is used for access control is a typed column. Everything else is JSONB.

7. **JSONB fields include inline documentation** via code comments showing example payloads. This compensates for the loss of column-level self-documentation that the normalized model provides.

8. **Engine-specific metadata lives in JSONB** (`deployments.engine_metadata`, `checkpoints.metadata`). This allows the platform to support multiple engines (Flink, Spark, RisingWave) without engine-specific columns or tables.
