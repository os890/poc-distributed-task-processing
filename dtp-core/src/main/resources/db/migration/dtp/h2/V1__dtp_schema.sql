-- Copyright 2026 os890
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--     http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- dtp schema, slice 1 of the PoC (H2). See docs/design/03-persistence.md.
-- Deviations from the design are marked with "slice 1".

-- definitions ---------------------------------------------------------------------------------------------------------

CREATE TABLE dtp_job (
    id                 UUID         NOT NULL PRIMARY KEY,
    namespace          VARCHAR(100) NOT NULL,
    name               VARCHAR(200) NOT NULL,
    current_revision   INT          NOT NULL,
    enabled            BOOLEAN      DEFAULT TRUE NOT NULL,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_job_name_uk UNIQUE (namespace, name)
);

CREATE TABLE dtp_job_revision (
    job_id                  UUID          NOT NULL,
    revision                INT           NOT NULL,
    handler_class           VARCHAR(500)  NOT NULL,
    trigger_asap            BOOLEAN       DEFAULT FALSE NOT NULL,
    cron_expression         VARCHAR(100),
    cron_zone               VARCHAR(60),
    trigger_external        BOOLEAN       DEFAULT FALSE NOT NULL,
    inbox_min_items         INT,
    concurrency_policy      VARCHAR(20)   DEFAULT 'SKIP' NOT NULL,
    takeover_policy         VARCHAR(20)   DEFAULT 'RERUN' NOT NULL,
    retry_max               INT           DEFAULT 3 NOT NULL,
    retry_backoff           VARCHAR(40)   DEFAULT 'PT10S' NOT NULL,
    max_item_visits         INT,
    lease_duration          VARCHAR(40),
    default_parameters      CHARACTER LARGE OBJECT,
    changed_by              VARCHAR(100),
    changed_at              TIMESTAMP WITH TIME ZONE,
    CONSTRAINT dtp_job_revision_pk PRIMARY KEY (job_id, revision),
    CONSTRAINT dtp_job_revision_job_fk FOREIGN KEY (job_id) REFERENCES dtp_job (id)
);

CREATE TABLE dtp_transition (
    id                UUID        NOT NULL PRIMARY KEY,
    source_job_id     UUID        NOT NULL,
    source_revision   INT         NOT NULL,
    on_outcome        VARCHAR(40),
    on_result_status  VARCHAR(100),
    target_job_id     UUID        NOT NULL,
    CONSTRAINT dtp_transition_source_fk FOREIGN KEY (source_job_id, source_revision)
        REFERENCES dtp_job_revision (job_id, revision),
    CONSTRAINT dtp_transition_target_fk FOREIGN KEY (target_job_id) REFERENCES dtp_job (id),
    CONSTRAINT dtp_transition_on_ck CHECK ((on_outcome IS NULL AND on_result_status IS NOT NULL)
        OR (on_outcome IS NOT NULL AND on_result_status IS NULL))
);

-- execution -----------------------------------------------------------------------------------------------------------

-- one row per job (DD-1), created by the housekeeping leader
CREATE TABLE dtp_job_lease (
    job_id          UUID         NOT NULL PRIMARY KEY,
    namespace       VARCHAR(100) NOT NULL,
    run_id          UUID,
    owner_node_id   UUID,
    lease_until     TIMESTAMP WITH TIME ZONE,
    fencing_token   BIGINT       DEFAULT 0 NOT NULL,
    CONSTRAINT dtp_job_lease_job_fk FOREIGN KEY (job_id) REFERENCES dtp_job (id)
);

CREATE TABLE dtp_run (
    id                UUID          NOT NULL PRIMARY KEY,
    namespace         VARCHAR(100)  NOT NULL,
    run_key           VARCHAR(300)  NOT NULL,
    job_id            UUID          NOT NULL,
    job_revision      INT           NOT NULL,
    handler_class     VARCHAR(500)  NOT NULL,
    state             VARCHAR(30)   NOT NULL,
    stop_reason       VARCHAR(30),
    outcome           VARCHAR(30),
    result_status     VARCHAR(100),
    trigger_type      VARCHAR(30)   NOT NULL,
    triggered_by      VARCHAR(300),
    -- slice 1: typed run parameters of the trigger (missing in the design)
    parameters        CHARACTER LARGE OBJECT,
    not_before        TIMESTAMP WITH TIME ZONE,
    priority          INT           DEFAULT 0 NOT NULL,
    attempt           INT           DEFAULT 0 NOT NULL,
    owner_node_id     UUID,
    fencing_token     BIGINT,
    info              CHARACTER LARGE OBJECT,
    error             CHARACTER LARGE OBJECT,
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    started_at        TIMESTAMP WITH TIME ZONE,
    finished_at       TIMESTAMP WITH TIME ZONE,
    CONSTRAINT dtp_run_key_uk UNIQUE (namespace, run_key),
    CONSTRAINT dtp_run_job_fk FOREIGN KEY (job_id) REFERENCES dtp_job (id)
);
CREATE INDEX dtp_run_state_ix ON dtp_run (namespace, state);
CREATE INDEX dtp_run_job_ix ON dtp_run (job_id, state);

CREATE TABLE dtp_run_attempt (
    run_id          UUID        NOT NULL,
    attempt         INT         NOT NULL,
    node_id         UUID        NOT NULL,
    fencing_token   BIGINT      NOT NULL,
    started_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    ended_at        TIMESTAMP WITH TIME ZONE,
    end_reason      VARCHAR(30),
    CONSTRAINT dtp_run_attempt_pk PRIMARY KEY (run_id, attempt),
    CONSTRAINT dtp_run_attempt_run_fk FOREIGN KEY (run_id) REFERENCES dtp_run (id)
);

CREATE TABLE dtp_node (
    id              UUID          NOT NULL PRIMARY KEY,
    namespace       VARCHAR(100)  NOT NULL,
    -- slice 1: human-readable node name (missing in the design)
    name            VARCHAR(200)  NOT NULL,
    host            VARCHAR(200),
    color           VARCHAR(20),
    app_version     VARCHAR(100),
    state           VARCHAR(20)   NOT NULL,
    capabilities    CHARACTER LARGE OBJECT,
    started_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    heartbeat_at    TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE dtp_leader (
    namespace        VARCHAR(100) NOT NULL,
    role             VARCHAR(50)  NOT NULL,
    holder_node_id   UUID,
    lease_until      TIMESTAMP WITH TIME ZONE,
    term             BIGINT       DEFAULT 0 NOT NULL,
    acquired_at      TIMESTAMP WITH TIME ZONE,
    CONSTRAINT dtp_leader_pk PRIMARY KEY (namespace, role)
);

-- tracking ------------------------------------------------------------------------------------------------------------

CREATE SEQUENCE dtp_inbox_seq;
CREATE SEQUENCE dtp_event_seq;

CREATE TABLE dtp_item (
    id                UUID          NOT NULL PRIMARY KEY,
    namespace         VARCHAR(100)  NOT NULL,
    item_type         VARCHAR(100)  NOT NULL,
    ref_id            VARCHAR(200)  NOT NULL,
    ref_version       BIGINT        DEFAULT 0 NOT NULL,
    business_key      VARCHAR(200),
    lane              VARCHAR(50),
    lane_priority     INT           DEFAULT 0 NOT NULL,
    current_job_id    UUID,
    inbox_seq         BIGINT,
    state             VARCHAR(30)   NOT NULL,
    -- slice 1: reason of the last routing, read by the next step (the design reads it from the journal)
    arrival_reason    VARCHAR(1000),
    attempts          INT           DEFAULT 0 NOT NULL,
    next_attempt_at   TIMESTAMP WITH TIME ZONE,
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_item_ref_uk UNIQUE (namespace, item_type, ref_id, ref_version)
);
CREATE INDEX dtp_item_inbox_ix ON dtp_item (current_job_id, state, inbox_seq);
CREATE INDEX dtp_item_business_key_ix ON dtp_item (namespace, item_type, business_key);

CREATE TABLE dtp_item_visit (
    item_id   UUID NOT NULL,
    job_id    UUID NOT NULL,
    visits    INT  NOT NULL,
    CONSTRAINT dtp_item_visit_pk PRIMARY KEY (item_id, job_id),
    CONSTRAINT dtp_item_visit_item_fk FOREIGN KEY (item_id) REFERENCES dtp_item (id)
);

CREATE TABLE dtp_item_event (
    id               UUID          NOT NULL PRIMARY KEY,
    -- slice 1: stable order of the journal entries (occurred_at can be equal)
    seq              BIGINT        NOT NULL,
    namespace        VARCHAR(100)  NOT NULL,
    item_id          UUID          NOT NULL,
    event_type       VARCHAR(40)   NOT NULL,
    from_job_id      UUID,
    to_job_id        UUID,
    run_id           UUID,
    run_attempt      INT,
    node_id          UUID,
    reason           VARCHAR(1000),
    failure_kind     VARCHAR(20),
    error_type       VARCHAR(300),
    error_hash       VARCHAR(40),
    occurred_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_item_event_item_fk FOREIGN KEY (item_id) REFERENCES dtp_item (id)
);
CREATE INDEX dtp_item_event_item_ix ON dtp_item_event (item_id, seq);
