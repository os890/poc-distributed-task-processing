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

-- red/green deployments (slice 21, PostgreSQL, mission §6.4 and Q-1): the nodes of a release share a color
-- (dtp.node.color); only nodes of the active color claim runs and lead, the others are STANDBY. A switch activates
-- the flow versions that the nodes of the new color run. Without an active color (no row, or NULL) all nodes claim.
CREATE TABLE dtp_namespace_control (
    namespace     VARCHAR(100)  NOT NULL PRIMARY KEY,
    active_color  VARCHAR(20),
    switched_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    switched_by   VARCHAR(200)  NOT NULL,
    reason        VARCHAR(1000)
);

-- every switch, append-only (who, when, why, which flow versions it activated)
CREATE TABLE dtp_color_switch (
    id               UUID          NOT NULL PRIMARY KEY,
    namespace        VARCHAR(100)  NOT NULL,
    previous_color   VARCHAR(20),
    active_color     VARCHAR(20),
    switched_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    switched_by      VARCHAR(200)  NOT NULL,
    reason           VARCHAR(1000),
    activated_flows  TEXT
);

CREATE INDEX dtp_color_switch_ns_idx ON dtp_color_switch (namespace, switched_at);

-- the flow versions a node runs (JSON object: flow name -> version hash), activated by a switch to its color
ALTER TABLE dtp_node ADD COLUMN flow_versions TEXT;
