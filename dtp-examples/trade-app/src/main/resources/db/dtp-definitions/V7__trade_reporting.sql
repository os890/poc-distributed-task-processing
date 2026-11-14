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

-- regulatory trade report (slice 10, collection windows): at each occurrence the window stops the enrichment softly,
-- opens with a cut-off once it is idle and runs the collector alone; the enrichment continues after the report
--   window trade-reporting: pauses trade-enrichment ==> trade-reporting (collector, one file per business date)

-- the collector has no trigger of its own: the window plans its runs
INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at, updated_at)
VALUES ('0193a5c0-0000-7000-8000-00000000000d', 'trade-app', 'trade-reporting', 1, TRUE, CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP);
INSERT INTO dtp_job_revision (job_id, revision, handler_class, changed_by, changed_at)
VALUES ('0193a5c0-0000-7000-8000-00000000000d', 1, 'com.acme.trade.TradeReporting', 'flyway', CURRENT_TIMESTAMP);

-- schedule (placeholders dtp.definitions.placeholders.reporting-*): daily at 17:30 (Vienna) on the business days of
-- AT-BANKING, a missed slot is skipped; NONE = started by operators only (tests) or no calendar (cluster demo).
-- A person decides if the enrichment doesn't stop within the quiesce timeout, e.g. a long approval batch.
INSERT INTO dtp_window (id, namespace, name, schedule_cron, schedule_zone, calendar_id, collector_job_id,
                        quiesce_timeout, enabled, created_at)
VALUES ('0193a5c0-0000-7000-8000-0000000000f1', 'trade-app', 'trade-reporting', NULLIF('${reporting-cron}', 'NONE'),
        'Europe/Vienna',
        (SELECT id FROM dtp_calendar WHERE namespace = 'trade-app' AND name = '${reporting-calendar}'),
        '0193a5c0-0000-7000-8000-00000000000d', '${reporting-quiesce-timeout}', TRUE, CURRENT_TIMESTAMP);
INSERT INTO dtp_window_paused_job (window_id, job_id)
VALUES ('0193a5c0-0000-7000-8000-0000000000f1', '0193a5c0-0000-7000-8000-000000000006');
