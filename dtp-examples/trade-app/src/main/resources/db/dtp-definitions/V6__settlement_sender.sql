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

-- settlement sender (slice 9, packages handed over): the export only creates the files (work packages) and hands
-- them over to the sender, so the end of day completes once its files are created
--   ... --> eod-sign-off --SUCCEEDED--> settlement-export ==packages==> settlement-sender

-- the inbox trigger plans a run of the sender for due packages: new ones, or due again after a rejection (retry
-- backoff: dtp.definitions.placeholders.settlement-retry-backoff); a person decides once the retry is used up
INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at, updated_at)
VALUES ('0193a5c0-0000-7000-8000-00000000000c', 'trade-app', 'settlement-sender', 1, TRUE, CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP);
INSERT INTO dtp_job_revision (job_id, revision, handler_class, inbox_min_items, retry_max, retry_backoff, changed_by,
                              changed_at)
VALUES ('0193a5c0-0000-7000-8000-00000000000c', 1, 'com.acme.trade.SettlementSender', 1, 1,
        '${settlement-retry-backoff}', 'flyway', CURRENT_TIMESTAMP);

-- revision 2 of the export: started by the sign-off only, it has no packages of its own to process anymore
INSERT INTO dtp_job_revision (job_id, revision, handler_class, changed_by, changed_at)
VALUES ('0193a5c0-0000-7000-8000-00000000000b', 2, 'com.acme.trade.SettlementExport', 'flyway', CURRENT_TIMESTAMP);
UPDATE dtp_job SET current_revision = 2, updated_at = CURRENT_TIMESTAMP
WHERE id = '0193a5c0-0000-7000-8000-00000000000b';

-- files of the export that weren't sent yet (pending or waiting for a decision) are the sender's from now on
UPDATE dtp_item SET current_job_id = '0193a5c0-0000-7000-8000-00000000000c'
WHERE work_package_id IN (SELECT id FROM dtp_work_package WHERE job_id = '0193a5c0-0000-7000-8000-00000000000b'
                          AND state IN ('PENDING', 'WAITING_FOR_DECISION'));
UPDATE dtp_work_package SET job_id = '0193a5c0-0000-7000-8000-00000000000c', updated_at = CURRENT_TIMESTAMP
WHERE job_id = '0193a5c0-0000-7000-8000-00000000000b' AND state IN ('PENDING', 'WAITING_FOR_DECISION');
