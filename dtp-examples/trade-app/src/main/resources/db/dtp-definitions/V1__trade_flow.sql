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

-- job definitions of the trade app (docs/design/07-sample-app.md), applied by the engine after its schema (DD-15)

-- level 1: a handler and one definition row (every minute for the demo, nightly in the design)
INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at, updated_at)
VALUES ('0193a5c0-0000-7000-8000-000000000001', 'trade-app', 'temp-file-cleanup', 1, TRUE,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO dtp_job_revision (job_id, revision, handler_class, cron_expression, cron_zone,
                              concurrency_policy, takeover_policy, changed_by, changed_at)
VALUES ('0193a5c0-0000-7000-8000-000000000001', 1, 'com.acme.trade.TempFileCleanup',
        '0 * * * * ?', 'Europe/Vienna', 'SKIP', 'RERUN', 'flyway', CURRENT_TIMESTAMP);

-- level 2: the entry step of the TradeFlow (com.acme.trade.TradeFlow), the jobs of its other steps are generated
INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at, updated_at)
VALUES ('0193a5c0-0000-7000-8000-000000000004', 'trade-app', 'trade-import', 1, TRUE,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO dtp_job_revision (job_id, revision, handler_class, trigger_external, concurrency_policy,
                              max_item_visits, changed_by, changed_at)
VALUES ('0193a5c0-0000-7000-8000-000000000004', 1, 'com.acme.trade.TradeImport', TRUE, 'QUEUE_ALL', 3,
        'flyway', CURRENT_TIMESTAMP);
