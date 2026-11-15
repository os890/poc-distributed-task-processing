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

-- monitoring read model (slice 11, H2, docs/design/09-monitoring-heatmap.md §9.3): small denormalized views that the
-- leader of the role "projection" derives from the journal and the current item states. Clients only read them.
-- Deviations: no hourly rollups (minute buckets are deleted after the retention), the step status keeps the counters
-- of the last 5 minutes and the pending alert transition, edges are keyed by back_route as well.

-- the projector reads the journal by time
CREATE INDEX dtp_item_event_time_ix ON dtp_item_event (namespace, occurred_at);

-- jam threshold per job: the age of the oldest queued item that counts as a jam (default dtp.monitoring.*)
ALTER TABLE dtp_job_revision ADD COLUMN expected_max_queue_age VARCHAR(40);

CREATE TABLE dtp_view_step_status (
    namespace             VARCHAR(100)  NOT NULL,
    job_id                UUID          NOT NULL,
    step_name             VARCHAR(200)  NOT NULL,
    -- PROCESSING, WAITING_FOR_RUN, WAITING_FOR_TRIGGER, IDLE, SLOT_CLOSED, PAUSED_BY_WINDOW, NO_CAPABLE_NODE, DISABLED
    status_reason         VARCHAR(30)   NOT NULL,
    queued                INT           NOT NULL,
    -- JSON object lane -> queued items ("" = no lane)
    queued_by_lane        VARCHAR(2000),
    oldest_queued_ms      BIGINT,
    waiting_for_decision  INT           NOT NULL,
    deferred              INT           NOT NULL,
    forked                INT           NOT NULL,
    packaged              INT           NOT NULL,
    active_run_id         UUID,
    active_node_id        UUID,
    arrived_5m            INT           NOT NULL,
    departed_5m           INT           NOT NULL,
    failed_technical_5m   INT           NOT NULL,
    jam_score             INT           NOT NULL,
    -- OK, CONGESTED or FAILING
    alert_state           VARCHAR(20)   NOT NULL,
    alert_since           TIMESTAMP WITH TIME ZONE,
    -- the alert state the step is moving to, and since when (hysteresis)
    pending_state         VARCHAR(20),
    pending_cycles        INT           DEFAULT 0 NOT NULL,
    pending_since         TIMESTAMP WITH TIME ZONE,
    -- version of the projection cycle that changed the row last (the live feed sends changed rows only)
    projection_version    BIGINT        NOT NULL,
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_view_step_status_pk PRIMARY KEY (namespace, job_id)
);

CREATE TABLE dtp_view_step_minute (
    namespace         VARCHAR(100)  NOT NULL,
    job_id            UUID          NOT NULL,
    minute_at            TIMESTAMP WITH TIME ZONE NOT NULL,
    arrived           INT           NOT NULL,
    departed          INT           NOT NULL,
    routed_back       INT           NOT NULL,
    completed         INT           NOT NULL,
    discarded         INT           NOT NULL,
    escalated         INT           NOT NULL,
    failed_business   INT           NOT NULL,
    failed_technical  INT           NOT NULL,
    -- time from the arrival at the step until the departure, of the items that departed in this minute
    max_wait_ms       BIGINT        NOT NULL,
    sum_wait_ms       BIGINT        NOT NULL,
    CONSTRAINT dtp_view_step_minute_pk PRIMARY KEY (namespace, job_id, minute_at)
);
CREATE INDEX dtp_view_step_minute_ix ON dtp_view_step_minute (namespace, minute_at);

CREATE TABLE dtp_view_edge_minute (
    namespace    VARCHAR(100)  NOT NULL,
    from_job_id  UUID          NOT NULL,
    to_job_id    UUID          NOT NULL,
    back_route   BOOLEAN       NOT NULL,
    minute_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    items        INT           NOT NULL,
    CONSTRAINT dtp_view_edge_minute_pk PRIMARY KEY (namespace, from_job_id, to_job_id, back_route, minute_at)
);
CREATE INDEX dtp_view_edge_minute_ix ON dtp_view_edge_minute (namespace, minute_at);

CREATE TABLE dtp_view_failure_minute (
    namespace       VARCHAR(100)  NOT NULL,
    job_id          UUID          NOT NULL,
    error_type      VARCHAR(300)  NOT NULL,
    error_hash      VARCHAR(40)   NOT NULL,
    minute_at          TIMESTAMP WITH TIME ZONE NOT NULL,
    occurrences     INT           NOT NULL,
    sample_message  VARCHAR(1000),
    sample_item_id  UUID,
    CONSTRAINT dtp_view_failure_minute_pk PRIMARY KEY (namespace, job_id, error_type, error_hash, minute_at)
);
CREATE INDEX dtp_view_failure_minute_ix ON dtp_view_failure_minute (namespace, minute_at);

CREATE TABLE dtp_projection_state (
    namespace        VARCHAR(100)  NOT NULL,
    projection       VARCHAR(50)   NOT NULL,
    version          BIGINT        NOT NULL,
    projected_until  TIMESTAMP WITH TIME ZONE,
    -- leader term of the last cycle
    term             BIGINT,
    CONSTRAINT dtp_projection_state_pk PRIMARY KEY (namespace, projection)
);
