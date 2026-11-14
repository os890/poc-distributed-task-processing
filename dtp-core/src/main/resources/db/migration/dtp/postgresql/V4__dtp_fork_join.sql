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

-- fork/join on item level (slice 4, DD-22, PostgreSQL)

-- the branch steps of a FORK route, its target_job_id is the join step
CREATE TABLE dtp_flow_route_branch (
    route_id   UUID  NOT NULL,
    position   INT   NOT NULL,
    job_id     UUID  NOT NULL,
    CONSTRAINT dtp_flow_route_branch_pk PRIMARY KEY (route_id, position),
    CONSTRAINT dtp_flow_route_branch_route_fk FOREIGN KEY (route_id) REFERENCES dtp_flow_route (id)
);

-- FORKED items only: branches that haven't finished yet
ALTER TABLE dtp_item ADD COLUMN open_branches INT;

-- one row per branch of the last fork of an item; the row is in the inbox of the branch step while QUEUED
CREATE TABLE dtp_item_branch (
    item_id              UUID           NOT NULL,
    branch_job_id        UUID           NOT NULL,
    join_job_id          UUID           NOT NULL,
    position             INT            NOT NULL,
    -- copied from the item, so the inbox query of a branch step reads one table
    namespace            VARCHAR(100)   NOT NULL,
    item_type            VARCHAR(100)   NOT NULL,
    lane_priority        INT            DEFAULT 0 NOT NULL,
    inbox_seq            BIGINT         NOT NULL,
    -- QUEUED, WAITING_FOR_DECISION, FINISHED or CANCELLED
    state                VARCHAR(30)    NOT NULL,
    arrival_reason       VARCHAR(1000),
    attempts             INT            DEFAULT 0 NOT NULL,
    next_attempt_at      TIMESTAMP WITH TIME ZONE,
    decision_request_id  UUID,
    -- the result: the signal the branch step reported
    signal_type          VARCHAR(500),
    signal               VARCHAR(100),
    reason               VARCHAR(1000),
    finished_by_node_id  UUID,
    finished_at          TIMESTAMP WITH TIME ZONE,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at           TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_item_branch_pk PRIMARY KEY (item_id, branch_job_id),
    CONSTRAINT dtp_item_branch_item_fk FOREIGN KEY (item_id) REFERENCES dtp_item (id)
);
CREATE INDEX dtp_item_branch_inbox_ix ON dtp_item_branch (branch_job_id, state, inbox_seq);
