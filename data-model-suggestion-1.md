# Data Model Suggestion 1: Entity-Centric Normalized Relational

> Project: Stream Processing Platform · Created: 2026-05-20

## Philosophy

This model follows classical relational database normalization (3NF+), giving every domain concept its own table with explicit foreign key relationships. Every connector type, window strategy, schema version, and permission assignment has a dedicated table with typed columns. This mirrors how mature platforms like Confluent Cloud and Apache Flink's SQL Gateway expose their metadata: each resource type (cluster, topic, connector, schema, job) is a first-class entity with its own lifecycle and relationships.

The normalized approach maximizes data integrity through foreign key constraints, CHECK constraints, and UNIQUE indexes. It enables complex cross-entity queries without JSONB parsing — for example, "find all pipelines in tenant X that read from connector Y using schema version Z and have failed checkpoints in the last hour." Reporting and analytics queries are straightforward SQL JOINs.

This is the most traditional approach and will feel familiar to teams with relational database experience. It trades flexibility (adding a new connector property requires a schema migration) for safety (invalid states are structurally impossible).

**Best for:** Teams building a production-grade platform where data integrity, complex cross-entity queries, and regulatory compliance are paramount.

**Trade-offs:**
- Pro: Maximum data integrity via foreign keys and constraints
- Pro: Complex analytical queries are simple SQL JOINs
- Pro: Schema is self-documenting; every field has a type and constraint
- Pro: Standard tooling (ORMs, migration frameworks) works out of the box
- Con: High table count (~40+) increases migration complexity
- Con: Adding new connector types or configuration fields requires DDL changes
- Con: Connector configurations vary widely; normalized tables may have many nullable columns
- Con: Schema evolution for event payloads is awkward in rigid relational columns

---

## Standards Alignment

| Standard | How It's Used |
|----------|---------------|
| CloudEvents v1.0 | `events` table stores context attributes (id, source, type, specversion, time) as typed columns matching the CloudEvents spec |
| Apache Kafka Protocol | `connectors` table models Kafka-compatible source/sink configurations; `topics` table mirrors Kafka topic metadata |
| Avro / Protobuf / JSON Schema | `schema_versions` table stores serialized schema definitions with compatibility mode per the Confluent Schema Registry API |
| OpenTelemetry | `pipeline_metrics` and `pipeline_spans` tables use OTel semantic conventions for metric names and span attributes |
| AsyncAPI 3.0 | `connector_definitions` table includes AsyncAPI channel/operation metadata for documenting event-driven interfaces |
| ISO 3166 | `jurisdictions` reference table for data residency and compliance region tracking |
| OAuth 2.0 / OIDC | `oauth_clients` and `api_keys` tables model authentication credentials per RFC 6749 |
| OWASP API Security | `audit_logs` table captures all API mutations for security audit compliance |

---

## Identity & Multi-Tenancy

```sql
CREATE TABLE tenants (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL,
    slug            VARCHAR(100) NOT NULL UNIQUE,
    plan            VARCHAR(50) NOT NULL DEFAULT 'free',  -- free, pro, enterprise
    data_residency  VARCHAR(10) NOT NULL DEFAULT 'us',    -- ISO 3166-1 alpha-2
    settings        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           VARCHAR(320) NOT NULL UNIQUE,
    display_name    VARCHAR(255) NOT NULL,
    password_hash   VARCHAR(255),                         -- NULL if SSO-only
    auth_provider   VARCHAR(50) NOT NULL DEFAULT 'local', -- local, google, github, saml
    auth_subject    VARCHAR(500),                         -- external IdP subject ID
    is_active       BOOLEAN NOT NULL DEFAULT true,
    last_login_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE tenant_memberships (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role            VARCHAR(50) NOT NULL DEFAULT 'viewer', -- owner, admin, editor, viewer
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
    key_prefix      VARCHAR(10) NOT NULL,                 -- first 8 chars for identification
    key_hash        VARCHAR(255) NOT NULL,                -- bcrypt hash of full key
    scopes          TEXT[] NOT NULL DEFAULT '{}',          -- e.g. {pipelines:read, pipelines:write}
    expires_at      TIMESTAMPTZ,
    last_used_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_api_keys_tenant ON api_keys(tenant_id);
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
    is_default      BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, slug)
);

CREATE TABLE environments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name            VARCHAR(100) NOT NULL,                -- development, staging, production
    slug            VARCHAR(50) NOT NULL,
    is_production   BOOLEAN NOT NULL DEFAULT false,
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
    name            VARCHAR(500) NOT NULL,                -- e.g. orders-value, users-key
    format          VARCHAR(20) NOT NULL,                 -- avro, protobuf, json_schema
    compatibility   VARCHAR(30) NOT NULL DEFAULT 'BACKWARD', -- BACKWARD, FORWARD, FULL, NONE
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, namespace_id, name)
);

CREATE TABLE schema_versions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_id      UUID NOT NULL REFERENCES schema_subjects(id) ON DELETE CASCADE,
    version         INTEGER NOT NULL,
    schema_text     TEXT NOT NULL,                        -- raw schema definition
    fingerprint     VARCHAR(64) NOT NULL,                 -- SHA-256 of canonical schema
    is_active       BOOLEAN NOT NULL DEFAULT true,
    registered_by   UUID REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (subject_id, version),
    UNIQUE (subject_id, fingerprint)
);

CREATE INDEX idx_schema_versions_subject ON schema_versions(subject_id, version DESC);
```

---

## Connectors

```sql
CREATE TABLE connector_types (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL UNIQUE,         -- kafka, postgresql_cdc, mysql_cdc, s3, kinesis
    category        VARCHAR(50) NOT NULL,                 -- source, sink, both
    icon_url        VARCHAR(500),
    documentation_url VARCHAR(500),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE connector_type_properties (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connector_type_id UUID NOT NULL REFERENCES connector_types(id) ON DELETE CASCADE,
    property_name   VARCHAR(255) NOT NULL,
    display_name    VARCHAR(255) NOT NULL,
    description     TEXT,
    data_type       VARCHAR(50) NOT NULL,                 -- string, integer, boolean, secret, enum
    is_required     BOOLEAN NOT NULL DEFAULT false,
    default_value   TEXT,
    enum_values     TEXT[],                               -- valid values if data_type = enum
    validation_regex VARCHAR(500),
    display_order   INTEGER NOT NULL DEFAULT 0,
    UNIQUE (connector_type_id, property_name)
);

CREATE TABLE connectors (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    namespace_id    UUID NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    environment_id  UUID NOT NULL REFERENCES environments(id),
    connector_type_id UUID NOT NULL REFERENCES connector_types(id),
    name            VARCHAR(255) NOT NULL,
    status          VARCHAR(30) NOT NULL DEFAULT 'created', -- created, running, paused, failed, deleted
    created_by      UUID NOT NULL REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, namespace_id, environment_id, name)
);

CREATE TABLE connector_properties (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connector_id    UUID NOT NULL REFERENCES connectors(id) ON DELETE CASCADE,
    property_name   VARCHAR(255) NOT NULL,
    property_value  TEXT NOT NULL,                        -- encrypted if property is a secret
    is_encrypted    BOOLEAN NOT NULL DEFAULT false,
    UNIQUE (connector_id, property_name)
);

CREATE INDEX idx_connectors_tenant_ns ON connectors(tenant_id, namespace_id);
CREATE INDEX idx_connectors_status ON connectors(status);
```

---

## Pipelines & Jobs

```sql
CREATE TABLE pipelines (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    namespace_id    UUID NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    sql_text        TEXT NOT NULL,                        -- the streaming SQL definition
    engine_target   VARCHAR(50) NOT NULL DEFAULT 'flink', -- flink, spark, risingwave
    parallelism     INTEGER NOT NULL DEFAULT 1,
    created_by      UUID NOT NULL REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, namespace_id, name)
);

CREATE TABLE pipeline_versions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    version         INTEGER NOT NULL,
    sql_text        TEXT NOT NULL,
    engine_target   VARCHAR(50) NOT NULL,
    parallelism     INTEGER NOT NULL,
    changelog       TEXT,
    created_by      UUID NOT NULL REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (pipeline_id, version)
);

CREATE TABLE pipeline_sources (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    connector_id    UUID NOT NULL REFERENCES connectors(id),
    schema_subject_id UUID REFERENCES schema_subjects(id),
    alias           VARCHAR(100),                        -- table alias in the SQL
    UNIQUE (pipeline_id, connector_id)
);

CREATE TABLE pipeline_sinks (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    connector_id    UUID NOT NULL REFERENCES connectors(id),
    schema_subject_id UUID REFERENCES schema_subjects(id),
    alias           VARCHAR(100),
    UNIQUE (pipeline_id, connector_id)
);

CREATE TABLE pipeline_deployments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    version_id      UUID NOT NULL REFERENCES pipeline_versions(id),
    environment_id  UUID NOT NULL REFERENCES environments(id),
    status          VARCHAR(30) NOT NULL DEFAULT 'pending',
        -- pending, deploying, running, stopping, stopped, failed, cancelled
    desired_state   VARCHAR(30) NOT NULL DEFAULT 'running', -- running, stopped
    deployed_by     UUID NOT NULL REFERENCES users(id),
    deployed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    stopped_at      TIMESTAMPTZ,
    failure_reason  TEXT,
    engine_job_id   VARCHAR(500),                        -- external Flink/Spark job ID
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_pipeline_deployments_pipeline ON pipeline_deployments(pipeline_id);
CREATE INDEX idx_pipeline_deployments_status ON pipeline_deployments(status);
CREATE INDEX idx_pipeline_deployments_env ON pipeline_deployments(environment_id);
```

---

## Windowing & Watermark Configuration

```sql
CREATE TABLE window_strategies (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    window_type     VARCHAR(30) NOT NULL,                -- tumbling, sliding, session
    time_field      VARCHAR(255) NOT NULL,               -- the event-time field name
    window_size_ms  BIGINT NOT NULL,                     -- window duration in milliseconds
    slide_size_ms   BIGINT,                              -- slide interval (sliding windows only)
    session_gap_ms  BIGINT,                              -- gap timeout (session windows only)
    late_arrival_ms BIGINT NOT NULL DEFAULT 0,           -- allowed lateness
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE watermark_configs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    strategy        VARCHAR(50) NOT NULL,                -- bounded_out_of_orderness, monotonous, ai_recommended
    max_delay_ms    BIGINT NOT NULL DEFAULT 5000,
    ai_confidence   FLOAT,                               -- confidence score if AI-recommended
    recommended_at  TIMESTAMPTZ,                         -- when AI last recalculated
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## Checkpoints & State

```sql
CREATE TABLE checkpoints (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id   UUID NOT NULL REFERENCES pipeline_deployments(id) ON DELETE CASCADE,
    checkpoint_id   BIGINT NOT NULL,                     -- sequential checkpoint number
    status          VARCHAR(30) NOT NULL,                -- in_progress, completed, failed, expired
    trigger_type    VARCHAR(30) NOT NULL DEFAULT 'periodic', -- periodic, manual, savepoint
    storage_path    VARCHAR(1000),                       -- object storage path (s3://...)
    size_bytes      BIGINT,
    duration_ms     BIGINT,
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_checkpoints_deployment ON checkpoints(deployment_id, checkpoint_id DESC);
CREATE INDEX idx_checkpoints_status ON checkpoints(status);
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
    refresh_mode    VARCHAR(30) NOT NULL DEFAULT 'continuous', -- continuous, periodic, manual
    refresh_interval_ms BIGINT,                          -- for periodic mode
    row_count       BIGINT,
    last_refreshed_at TIMESTAMPTZ,
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
    deployment_id   UUID NOT NULL REFERENCES pipeline_deployments(id) ON DELETE CASCADE,
    metric_name     VARCHAR(255) NOT NULL,               -- OTel convention: e.g. messaging.consumer.lag
    metric_value    DOUBLE PRECISION NOT NULL,
    labels          JSONB NOT NULL DEFAULT '{}',          -- {partition: "0", topic: "orders"}
    recorded_at     TIMESTAMPTZ NOT NULL
);

-- Partitioned by time for efficient range queries
CREATE INDEX idx_pipeline_metrics_deployment_time ON pipeline_metrics(deployment_id, recorded_at DESC);
CREATE INDEX idx_pipeline_metrics_name ON pipeline_metrics(metric_name, recorded_at DESC);

CREATE TABLE alerts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    pipeline_id     UUID REFERENCES pipelines(id) ON DELETE SET NULL,
    name            VARCHAR(255) NOT NULL,
    condition_type  VARCHAR(50) NOT NULL,                -- threshold, anomaly, absence
    metric_name     VARCHAR(255),
    threshold_value DOUBLE PRECISION,
    comparison      VARCHAR(10),                         -- gt, gte, lt, lte, eq
    evaluation_window_ms BIGINT NOT NULL DEFAULT 300000, -- 5 minutes
    severity        VARCHAR(20) NOT NULL DEFAULT 'warning', -- info, warning, critical
    is_enabled      BOOLEAN NOT NULL DEFAULT true,
    notification_channels TEXT[] NOT NULL DEFAULT '{}',   -- {slack, email, pagerduty}
    created_by      UUID NOT NULL REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE alert_incidents (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id        UUID NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    deployment_id   UUID REFERENCES pipeline_deployments(id),
    status          VARCHAR(30) NOT NULL DEFAULT 'firing', -- firing, acknowledged, resolved
    metric_value    DOUBLE PRECISION,
    fired_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by UUID REFERENCES users(id),
    resolved_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_alert_incidents_alert ON alert_incidents(alert_id, fired_at DESC);
CREATE INDEX idx_alert_incidents_status ON alert_incidents(status);
```

---

## AI Features

```sql
CREATE TABLE nl_query_translations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id),
    natural_language TEXT NOT NULL,
    generated_sql   TEXT NOT NULL,
    model_version   VARCHAR(100) NOT NULL,
    confidence      FLOAT NOT NULL,
    was_accepted    BOOLEAN,                             -- did the user accept the translation?
    was_edited      BOOLEAN,                             -- did the user modify before accepting?
    edited_sql      TEXT,                                -- user's corrected version (training data)
    execution_time_ms BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE anomaly_detections (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id   UUID NOT NULL REFERENCES pipeline_deployments(id) ON DELETE CASCADE,
    detection_type  VARCHAR(50) NOT NULL,                -- throughput, latency, schema_drift, data_quality
    severity        VARCHAR(20) NOT NULL,                -- info, warning, critical
    description     TEXT NOT NULL,
    metric_name     VARCHAR(255),
    expected_value  DOUBLE PRECISION,
    actual_value    DOUBLE PRECISION,
    root_cause_explanation TEXT,                          -- LLM-generated explanation
    is_acknowledged BOOLEAN NOT NULL DEFAULT false,
    detected_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE watermark_recommendations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    current_max_delay_ms BIGINT NOT NULL,
    recommended_max_delay_ms BIGINT NOT NULL,
    p99_latency_ms  BIGINT NOT NULL,                    -- observed p99 event-time lag
    sample_size     BIGINT NOT NULL,                    -- number of events analysed
    confidence      FLOAT NOT NULL,
    was_applied     BOOLEAN NOT NULL DEFAULT false,
    recommended_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_anomaly_detections_deployment ON anomaly_detections(deployment_id, detected_at DESC);
CREATE INDEX idx_nl_query_translations_tenant ON nl_query_translations(tenant_id, created_at DESC);
```

---

## Audit Log

```sql
CREATE TABLE audit_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id         UUID REFERENCES users(id),
    api_key_id      UUID REFERENCES api_keys(id),
    action          VARCHAR(100) NOT NULL,               -- pipeline.create, connector.update, deployment.start
    resource_type   VARCHAR(100) NOT NULL,               -- pipeline, connector, schema, deployment
    resource_id     UUID NOT NULL,
    changes         JSONB,                               -- {field: {old: x, new: y}}
    ip_address      INET,
    user_agent      VARCHAR(500),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_logs_tenant_time ON audit_logs(tenant_id, created_at DESC);
CREATE INDEX idx_audit_logs_resource ON audit_logs(resource_type, resource_id);
CREATE INDEX idx_audit_logs_user ON audit_logs(user_id, created_at DESC);
```

---

## Pipeline Testing

```sql
CREATE TABLE pipeline_tests (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    test_type       VARCHAR(50) NOT NULL,                -- snapshot, assertion, schema_validation
    input_snapshot  JSONB NOT NULL,                      -- sample input events
    expected_output JSONB NOT NULL,                      -- expected output events
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_by      UUID NOT NULL REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pipeline_test_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    test_id         UUID NOT NULL REFERENCES pipeline_tests(id) ON DELETE CASCADE,
    pipeline_version_id UUID NOT NULL REFERENCES pipeline_versions(id),
    status          VARCHAR(30) NOT NULL,                -- pending, running, passed, failed, error
    actual_output   JSONB,
    diff_summary    TEXT,
    duration_ms     BIGINT,
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_pipeline_test_runs_test ON pipeline_test_runs(test_id, created_at DESC);
```

---

## Table Count Summary

| Category | Tables | Notes |
|----------|--------|-------|
| Identity & Multi-Tenancy | 4 | tenants, users, tenant_memberships, api_keys |
| Namespaces & Environments | 2 | namespaces, environments |
| Schema Registry | 2 | schema_subjects, schema_versions |
| Connectors | 4 | connector_types, connector_type_properties, connectors, connector_properties |
| Pipelines & Jobs | 5 | pipelines, pipeline_versions, pipeline_sources, pipeline_sinks, pipeline_deployments |
| Windowing & Watermarks | 2 | window_strategies, watermark_configs |
| Checkpoints & State | 1 | checkpoints |
| Materialized Views | 1 | materialized_views |
| Observability | 3 | pipeline_metrics, alerts, alert_incidents |
| AI Features | 3 | nl_query_translations, anomaly_detections, watermark_recommendations |
| Audit | 1 | audit_logs |
| Pipeline Testing | 2 | pipeline_tests, pipeline_test_runs |
| **Total** | **30** | |

---

## Key Design Decisions

1. **Row-level multi-tenancy with `tenant_id` foreign keys** rather than schema-per-tenant. Simpler operationally and supports cross-tenant admin queries. Row-level security (RLS) in PostgreSQL enforces isolation at the database level.

2. **Separate `connector_types` and `connector_type_properties` tables** define the metadata schema for each connector type (Kafka, PostgreSQL CDC, S3, etc.), while `connectors` and `connector_properties` store instance-specific values. This EAV-like pattern is the normalized way to handle varying connector configurations without JSONB.

3. **Pipeline versioning is explicit** with a `pipeline_versions` table. Every SQL change creates a new version, and deployments reference a specific version. This enables rollback and audit without event sourcing.

4. **Schema registry mirrors the Confluent Schema Registry API** with subjects and versions. The `fingerprint` column enables content-addressed deduplication.

5. **Observability tables use OpenTelemetry naming conventions** (`messaging.consumer.lag`, etc.) so metrics are portable to any OTel-compatible backend.

6. **AI feature tables capture training signal**: the `nl_query_translations` table records whether users accepted or edited AI-generated SQL, creating a feedback loop for model improvement.

7. **Audit log uses a single table with `resource_type`/`resource_id` polymorphism** rather than per-entity audit tables. The `changes` JSONB column is the one place where JSONB is used — it captures field-level diffs without requiring a separate table per auditable entity.

8. **Checkpoint tracking is relational** rather than relying on the engine's internal checkpoint metadata. This enables cross-engine checkpoint comparison and platform-level SLA monitoring.
