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

-- work packages, item and time limits (slice 8, H2)
-- Deviation from the design: the number of a package is called seq (like dtp_item_event.seq), and a package keeps
-- its item type and group key (the API is typed per item type, design 06: WorkPackage.groupKey()).

CREATE SEQUENCE dtp_package_seq;

-- items processed as a whole; PENDING packages are carried over to the next run of the job (D33)
CREATE TABLE dtp_work_package (
    id                   UUID          NOT NULL PRIMARY KEY,
    namespace            VARCHAR(100)  NOT NULL,
    job_id               UUID          NOT NULL,
    item_type            VARCHAR(100)  NOT NULL,
    group_key            VARCHAR(300),
    seq                  BIGINT        NOT NULL,
    -- PENDING, DONE, WAITING_FOR_DECISION or DISCARDED (processed in one transaction: IN_PROGRESS isn't stored)
    state                VARCHAR(30)   NOT NULL,
    attempts             INT           DEFAULT 0 NOT NULL,
    item_count           INT           DEFAULT 0 NOT NULL,
    created_by_run_id    UUID          NOT NULL,
    processed_by_run_id  UUID,
    next_attempt_at      TIMESTAMP WITH TIME ZONE,
    last_error           VARCHAR(1000),
    info                 CHARACTER LARGE OBJECT,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at           TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_work_package_job_fk FOREIGN KEY (job_id) REFERENCES dtp_job (id)
);
CREATE INDEX dtp_work_package_pending_ix ON dtp_work_package (job_id, state, seq);
CREATE INDEX dtp_work_package_run_ix ON dtp_work_package (processed_by_run_id);

-- the package an item was assigned to (item state PACKAGED while the package is pending), kept afterwards
ALTER TABLE dtp_item ADD COLUMN work_package_id UUID;
CREATE INDEX dtp_item_work_package_ix ON dtp_item (work_package_id);

-- a run stops softly once it processed item_limit items or ran for time_limit (stop reason LIMIT_REACHED)
ALTER TABLE dtp_job_revision ADD COLUMN item_limit BIGINT;
ALTER TABLE dtp_job_revision ADD COLUMN time_limit VARCHAR(40);
