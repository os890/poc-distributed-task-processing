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

-- acknowledgements of the settlement files (slice 14): revision 2 of the sender waits for the custodian's answer;
-- a rejection, or no answer within the timeout (dtp.definitions.placeholders.settlement-ack-timeout), is a failed
-- attempt: the file is sent again after the retry backoff, then a person decides
INSERT INTO dtp_job_revision (job_id, revision, handler_class, inbox_min_items, retry_max, retry_backoff,
                              ack_required, ack_timeout, changed_by, changed_at)
VALUES ('0193a5c0-0000-7000-8000-00000000000c', 2, 'com.acme.trade.SettlementSender', 1, 1,
        '${settlement-retry-backoff}', TRUE, '${settlement-ack-timeout}', 'flyway', CURRENT_TIMESTAMP);
UPDATE dtp_job SET current_revision = 2, updated_at = CURRENT_TIMESTAMP
WHERE id = '0193a5c0-0000-7000-8000-00000000000c';
