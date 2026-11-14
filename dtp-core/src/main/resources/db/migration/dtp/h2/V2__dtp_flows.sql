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

-- flows (slice 2, H2): immutable versions, activations and route overrides

CREATE TABLE dtp_flow (
    id                 UUID          NOT NULL PRIMARY KEY,
    namespace          VARCHAR(100)  NOT NULL,
    name               VARCHAR(200)  NOT NULL,
    definition_class   VARCHAR(500),
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_flow_name_uk UNIQUE (namespace, name)
);

-- immutable: a changed definition is a new version (hash of its content)
CREATE TABLE dtp_flow_version (
    id             UUID          NOT NULL PRIMARY KEY,
    flow_id        UUID          NOT NULL,
    hash           VARCHAR(64)   NOT NULL,
    origin         VARCHAR(20)   NOT NULL,
    entry_job_id   UUID          NOT NULL,
    created_by     VARCHAR(200)  NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_flow_version_uk UNIQUE (flow_id, hash),
    CONSTRAINT dtp_flow_version_flow_fk FOREIGN KEY (flow_id) REFERENCES dtp_flow (id),
    CONSTRAINT dtp_flow_version_entry_fk FOREIGN KEY (entry_job_id) REFERENCES dtp_job (id)
);

CREATE TABLE dtp_flow_step (
    version_id   UUID NOT NULL,
    job_id       UUID NOT NULL,
    CONSTRAINT dtp_flow_step_pk PRIMARY KEY (version_id, job_id),
    CONSTRAINT dtp_flow_step_version_fk FOREIGN KEY (version_id) REFERENCES dtp_flow_version (id),
    CONSTRAINT dtp_flow_step_job_fk FOREIGN KEY (job_id) REFERENCES dtp_job (id)
);

-- kind: TO, BACK, FLOW, COMPLETE, DISCARD (item signals) or RUN (successor of a typed run result)
CREATE TABLE dtp_flow_route (
    id               UUID          NOT NULL PRIMARY KEY,
    version_id       UUID          NOT NULL,
    from_job_id      UUID          NOT NULL,
    signal_type      VARCHAR(500)  NOT NULL,
    signal           VARCHAR(100)  NOT NULL,
    kind             VARCHAR(20)   NOT NULL,
    target_job_id    UUID,
    target_flow_id   UUID,
    CONSTRAINT dtp_flow_route_uk UNIQUE (version_id, from_job_id, signal_type, signal, kind),
    CONSTRAINT dtp_flow_route_version_fk FOREIGN KEY (version_id) REFERENCES dtp_flow_version (id)
);

-- append-only: the active version is the latest activation with active_from <= now
CREATE TABLE dtp_flow_activation (
    id             UUID          NOT NULL PRIMARY KEY,
    flow_id        UUID          NOT NULL,
    version_id     UUID          NOT NULL,
    active_from    TIMESTAMP WITH TIME ZONE NOT NULL,
    generated      BOOLEAN       NOT NULL,
    activated_by   VARCHAR(200)  NOT NULL,
    reason         VARCHAR(1000),
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_flow_activation_flow_fk FOREIGN KEY (flow_id) REFERENCES dtp_flow (id),
    CONSTRAINT dtp_flow_activation_version_fk FOREIGN KEY (version_id) REFERENCES dtp_flow_version (id)
);

-- redirects one hop of a flow (any version) without a new version, optionally time-boxed
CREATE TABLE dtp_route_override (
    id               UUID          NOT NULL PRIMARY KEY,
    flow_id          UUID          NOT NULL,
    from_job_id      UUID          NOT NULL,
    signal           VARCHAR(100)  NOT NULL,
    kind             VARCHAR(20)   NOT NULL,
    target_job_id    UUID,
    target_flow_id   UUID,
    active_from      TIMESTAMP WITH TIME ZONE NOT NULL,
    active_until     TIMESTAMP WITH TIME ZONE,
    created_by       VARCHAR(200)  NOT NULL,
    reason           VARCHAR(1000),
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_route_override_flow_fk FOREIGN KEY (flow_id) REFERENCES dtp_flow (id)
);

-- the flow version an item is pinned to (null for manually routed items)
ALTER TABLE dtp_item ADD COLUMN flow_version_id UUID;
