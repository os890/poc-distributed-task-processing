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

-- level 2: items with back-routing
INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at, updated_at)
VALUES ('0193a5c0-0000-7000-8000-000000000003', 'trade-app', 'trade-enrichment', 1, TRUE,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO dtp_job_revision (job_id, revision, handler_class, inbox_min_items, retry_max, retry_backoff,
                              lease_duration, changed_by, changed_at)
VALUES ('0193a5c0-0000-7000-8000-000000000003', 1, 'com.acme.trade.TradeEnrichment', 1, 3, 'PT5S',
        'PT10S', 'flyway', CURRENT_TIMESTAMP);

INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at, updated_at)
VALUES ('0193a5c0-0000-7000-8000-000000000002', 'trade-app', 'trade-validation', 1, TRUE,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO dtp_job_revision (job_id, revision, handler_class, inbox_min_items, changed_by, changed_at)
VALUES ('0193a5c0-0000-7000-8000-000000000002', 1, 'com.acme.trade.TradeValidation', 1, 'flyway',
        CURRENT_TIMESTAMP);

INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at, updated_at)
VALUES ('0193a5c0-0000-7000-8000-000000000004', 'trade-app', 'trade-import', 1, TRUE,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO dtp_job_revision (job_id, revision, handler_class, trigger_external, concurrency_policy,
                              max_item_visits, changed_by, changed_at)
VALUES ('0193a5c0-0000-7000-8000-000000000004', 1, 'com.acme.trade.TradeImport', TRUE, 'QUEUE_ALL', 3,
        'flyway', CURRENT_TIMESTAMP);

-- trade-import --SUCCESS--> trade-validation (the validation also has an inbox trigger for re-imports)
INSERT INTO dtp_transition (id, source_job_id, source_revision, on_result_status, target_job_id)
VALUES ('0193a5c0-0000-7000-8000-000000000101', '0193a5c0-0000-7000-8000-000000000004', 1, 'SUCCESS',
        '0193a5c0-0000-7000-8000-000000000002');
