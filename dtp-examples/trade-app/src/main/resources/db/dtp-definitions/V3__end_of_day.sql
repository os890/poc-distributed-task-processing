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

-- end of day (slice 6, DD-4, DD-9): one flow instance per business date, started with POST /eod/{date}
--   end-of-day (cut-off) --SUCCEEDED--> position-report --+
--                        --SUCCEEDED--> pnl-report      --+--> run join --> eod-sign-off

INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at, updated_at) VALUES
    ('0193a5c0-0000-7000-8000-000000000007', 'trade-app', 'end-of-day', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('0193a5c0-0000-7000-8000-000000000008', 'trade-app', 'position-report', 1, TRUE, CURRENT_TIMESTAMP,
     CURRENT_TIMESTAMP),
    ('0193a5c0-0000-7000-8000-000000000009', 'trade-app', 'pnl-report', 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('0193a5c0-0000-7000-8000-00000000000a', 'trade-app', 'eod-sign-off', 1, TRUE, CURRENT_TIMESTAMP,
     CURRENT_TIMESTAMP);

-- the root is triggered externally; the runs of a flow instance are never skipped, whatever the concurrency policy
INSERT INTO dtp_job_revision (job_id, revision, handler_class, trigger_external, changed_by, changed_at) VALUES
    ('0193a5c0-0000-7000-8000-000000000007', 1, 'com.acme.trade.EndOfDay', TRUE, 'flyway', CURRENT_TIMESTAMP);
INSERT INTO dtp_job_revision (job_id, revision, handler_class, changed_by, changed_at) VALUES
    ('0193a5c0-0000-7000-8000-000000000008', 1, 'com.acme.trade.PositionReport', 'flyway', CURRENT_TIMESTAMP),
    ('0193a5c0-0000-7000-8000-000000000009', 1, 'com.acme.trade.PnlReport', 'flyway', CURRENT_TIMESTAMP),
    ('0193a5c0-0000-7000-8000-00000000000a', 1, 'com.acme.trade.EodSignOff', 'flyway', CURRENT_TIMESTAMP);

-- both reports start when the cut-off succeeded
INSERT INTO dtp_transition (id, source_job_id, source_revision, on_outcome, target_job_id) VALUES
    ('0193a5c0-0000-7000-8000-0000000000e1', '0193a5c0-0000-7000-8000-000000000007', 1, 'SUCCEEDED',
     '0193a5c0-0000-7000-8000-000000000008'),
    ('0193a5c0-0000-7000-8000-0000000000e2', '0193a5c0-0000-7000-8000-000000000007', 1, 'SUCCEEDED',
     '0193a5c0-0000-7000-8000-000000000009');

-- the sign-off runs once per business date when both reports succeeded
INSERT INTO dtp_join_member (target_job_id, target_revision, member_job_id, accepted_outcomes) VALUES
    ('0193a5c0-0000-7000-8000-00000000000a', 1, '0193a5c0-0000-7000-8000-000000000008', 'SUCCEEDED'),
    ('0193a5c0-0000-7000-8000-00000000000a', 1, '0193a5c0-0000-7000-8000-000000000009', 'SUCCEEDED');
