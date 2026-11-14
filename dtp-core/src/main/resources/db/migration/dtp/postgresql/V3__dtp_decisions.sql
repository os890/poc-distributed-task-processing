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

-- decision requests and approvals (slice 3, PostgreSQL, DD-5, DD-16, DD-19, DD-24)

CREATE TABLE dtp_decision_request (
    id                   UUID           NOT NULL PRIMARY KEY,
    namespace            VARCHAR(100)   NOT NULL,
    -- CHOICE or APPROVAL
    kind                 VARCHAR(20)    NOT NULL,
    -- ITEM or RUN (work packages and window occurrences follow with their slices)
    subject_type         VARCHAR(30)    NOT NULL,
    subject_id           UUID           NOT NULL,
    -- the decided item is re-queued in the inbox of this job (DD-19)
    requested_by_job_id  UUID           NOT NULL,
    run_id               UUID,
    reason               VARCHAR(1000),
    -- JSON array of {name, label}; option_type is the enum class of custom options
    options              TEXT NOT NULL,
    option_type          VARCHAR(500),
    required_approvals   INT,
    responsible_group    VARCHAR(200),
    -- OPEN or DECIDED, a request never expires (D38)
    state                VARCHAR(20)    NOT NULL,
    decided_option       VARCHAR(100),
    decided_by           VARCHAR(200),
    decided_at           TIMESTAMP WITH TIME ZONE,
    decision_comment     VARCHAR(1000),
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_decision_request_job_fk FOREIGN KEY (requested_by_job_id) REFERENCES dtp_job (id)
);
CREATE INDEX dtp_decision_request_open_ix ON dtp_decision_request (namespace, state, created_at);
CREATE INDEX dtp_decision_request_subject_ix ON dtp_decision_request (subject_id);

-- one vote per user (DD-16)
CREATE TABLE dtp_decision_vote (
    request_id   UUID           NOT NULL,
    voter        VARCHAR(200)   NOT NULL,
    vote         VARCHAR(20)    NOT NULL,
    vote_comment VARCHAR(1000),
    voted_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_decision_vote_pk PRIMARY KEY (request_id, voter),
    CONSTRAINT dtp_decision_vote_request_fk FOREIGN KEY (request_id) REFERENCES dtp_decision_request (id)
);

-- the decision that brought the item back into its inbox, cleared by the next routing
ALTER TABLE dtp_item ADD COLUMN decision_request_id UUID;
