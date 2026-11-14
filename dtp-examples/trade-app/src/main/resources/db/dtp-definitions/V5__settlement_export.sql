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

-- settlement export (slice 8, work packages): after the sign-off of an end of day, one settlement instruction per
-- released trade of the business date, one file (work package) per custodian
--   ... --> run join --> eod-sign-off --SUCCEEDED--> settlement-export

-- the inbox trigger also plans a run for pending packages: packages left over by an earlier run, or due again after
-- a rejection (retry backoff: dtp.definitions.placeholders.settlement-retry-backoff)
INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at, updated_at)
VALUES ('0193a5c0-0000-7000-8000-00000000000b', 'trade-app', 'settlement-export', 1, TRUE, CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP);
INSERT INTO dtp_job_revision (job_id, revision, handler_class, inbox_min_items, retry_max, retry_backoff, changed_by,
                              changed_at)
VALUES ('0193a5c0-0000-7000-8000-00000000000b', 1, 'com.acme.trade.SettlementExport', 1, 1,
        '${settlement-retry-backoff}', 'flyway', CURRENT_TIMESTAMP);

-- revision 2 of the sign-off: the join members are part of the revision (DD-10), so they are copied
INSERT INTO dtp_job_revision (job_id, revision, handler_class, changed_by, changed_at)
SELECT job_id, 2, handler_class, 'flyway', CURRENT_TIMESTAMP FROM dtp_job_revision
WHERE job_id = '0193a5c0-0000-7000-8000-00000000000a' AND revision = 1;
INSERT INTO dtp_join_member (target_job_id, target_revision, member_job_id, accepted_outcomes)
SELECT target_job_id, 2, member_job_id, accepted_outcomes FROM dtp_join_member
WHERE target_job_id = '0193a5c0-0000-7000-8000-00000000000a' AND target_revision = 1;
INSERT INTO dtp_transition (id, source_job_id, source_revision, on_outcome, target_job_id) VALUES
    ('0193a5c0-0000-7000-8000-0000000000e5', '0193a5c0-0000-7000-8000-00000000000a', 2, 'SUCCEEDED',
     '0193a5c0-0000-7000-8000-00000000000b');
UPDATE dtp_job SET current_revision = 2, updated_at = CURRENT_TIMESTAMP
WHERE id = '0193a5c0-0000-7000-8000-00000000000a';
