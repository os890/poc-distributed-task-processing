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

-- flow instances and run joins (slice 6, DD-4, DD-9, H2)

-- groups the runs of one end-to-end process, e.g. the EOD of a business date; name = the root job
CREATE TABLE dtp_flow_instance (
    id            UUID          NOT NULL PRIMARY KEY,
    namespace     VARCHAR(100)  NOT NULL,
    flow_name     VARCHAR(200)  NOT NULL,
    business_key  VARCHAR(300)  NOT NULL,
    root_job_id   UUID          NOT NULL,
    -- OPEN or COMPLETED
    state         VARCHAR(20)   NOT NULL,
    started_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at  TIMESTAMP WITH TIME ZONE,
    CONSTRAINT dtp_flow_instance_uk UNIQUE (namespace, flow_name, business_key),
    CONSTRAINT dtp_flow_instance_root_fk FOREIGN KEY (root_job_id) REFERENCES dtp_job (id)
);
CREATE INDEX dtp_flow_instance_state_ix ON dtp_flow_instance (namespace, state, started_at);

-- the runs of an instance; successors (transitions, joins) inherit it
ALTER TABLE dtp_run ADD COLUMN flow_instance_id UUID;
CREATE INDEX dtp_run_flow_instance_ix ON dtp_run (flow_instance_id, state);

-- definition: the target job runs once per flow instance when all members finished with an accepted outcome
CREATE TABLE dtp_join_member (
    target_job_id      UUID          NOT NULL,
    target_revision    INT           NOT NULL,
    member_job_id      UUID          NOT NULL,
    accepted_outcomes  VARCHAR(200)  DEFAULT 'SUCCEEDED,COMPLETED_WITH_ISSUES' NOT NULL,
    CONSTRAINT dtp_join_member_pk PRIMARY KEY (target_job_id, target_revision, member_job_id),
    CONSTRAINT dtp_join_member_target_fk FOREIGN KEY (target_job_id, target_revision)
        REFERENCES dtp_job_revision (job_id, revision),
    CONSTRAINT dtp_join_member_member_fk FOREIGN KEY (member_job_id) REFERENCES dtp_job (id)
);

-- one row per join and flow instance, locked by every arriving member (DD-9)
CREATE TABLE dtp_join_instance (
    target_job_id     UUID      NOT NULL,
    flow_instance_id  UUID      NOT NULL,
    fired             BOOLEAN   DEFAULT FALSE NOT NULL,
    run_id            UUID,
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    fired_at          TIMESTAMP WITH TIME ZONE,
    CONSTRAINT dtp_join_instance_pk PRIMARY KEY (target_job_id, flow_instance_id),
    CONSTRAINT dtp_join_instance_flow_fk FOREIGN KEY (flow_instance_id) REFERENCES dtp_flow_instance (id)
);

-- the latest run of each member counts (a rerun replaces the arrival)
CREATE TABLE dtp_join_arrival (
    target_job_id     UUID          NOT NULL,
    flow_instance_id  UUID          NOT NULL,
    member_job_id     UUID          NOT NULL,
    run_id            UUID          NOT NULL,
    outcome           VARCHAR(30)   NOT NULL,
    arrived_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_join_arrival_pk PRIMARY KEY (target_job_id, flow_instance_id, member_job_id),
    CONSTRAINT dtp_join_arrival_join_fk FOREIGN KEY (target_job_id, flow_instance_id)
        REFERENCES dtp_join_instance (target_job_id, flow_instance_id)
);
