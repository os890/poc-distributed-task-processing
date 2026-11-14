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

-- business calendar, scheduled end of day and the processing slot of the market data (slice 7)

-- business days of the Austrian banks: Monday to Friday without the bank holidays (sample data, not authoritative);
-- operators add or remove exceptions at runtime, the engine picks them up with the next refresh of the definitions
INSERT INTO dtp_calendar (id, namespace, name, zone, business_weekdays)
VALUES ('0193a5c0-0000-7000-8000-0000000000c1', 'trade-app', 'AT-BANKING', 'Europe/Vienna', 'MON-FRI');
INSERT INTO dtp_calendar_exception (calendar_id, calendar_day, business_day, description) VALUES
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2026-12-08', FALSE, 'Immaculate Conception'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2026-12-24', FALSE, 'Christmas Eve'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2026-12-25', FALSE, 'Christmas Day'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2026-12-31', FALSE, 'New Year''s Eve'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2027-01-01', FALSE, 'New Year''s Day'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2027-01-06', FALSE, 'Epiphany'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2027-03-26', FALSE, 'Good Friday'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2027-03-29', FALSE, 'Easter Monday'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2027-05-06', FALSE, 'Ascension Day'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2027-05-17', FALSE, 'Whit Monday'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2027-05-27', FALSE, 'Corpus Christi'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2027-10-26', FALSE, 'National Day'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2027-11-01', FALSE, 'All Saints'' Day'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2027-12-08', FALSE, 'Immaculate Conception'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2027-12-24', FALSE, 'Christmas Eve'),
    ('0193a5c0-0000-7000-8000-0000000000c1', DATE '2027-12-31', FALSE, 'New Year''s Eve');

-- revision 2 of the end of day: every cron slot on a business day starts the flow instance of its business date,
-- further slots of the same date find it (dtp.definitions.placeholders.eod-cron, NONE: started via REST only).
-- Revisions are immutable (DD-10): the transitions are part of the revision, so they are copied.
INSERT INTO dtp_job_revision (job_id, revision, handler_class, trigger_external, cron_expression, cron_zone,
                              cron_calendar_id, calendar_shift, cron_flow_instance, default_parameters, changed_by,
                              changed_at)
VALUES ('0193a5c0-0000-7000-8000-000000000007', 2, 'com.acme.trade.EndOfDay', TRUE, NULLIF('${eod-cron}', 'NONE'),
        'Europe/Vienna', '0193a5c0-0000-7000-8000-0000000000c1', 'SKIP',
        CASE WHEN '${eod-cron}' = 'NONE' THEN FALSE ELSE TRUE END, '{"requestedBy":"scheduler"}', 'flyway',
        CURRENT_TIMESTAMP);
INSERT INTO dtp_transition (id, source_job_id, source_revision, on_outcome, target_job_id) VALUES
    ('0193a5c0-0000-7000-8000-0000000000e3', '0193a5c0-0000-7000-8000-000000000007', 2, 'SUCCEEDED',
     '0193a5c0-0000-7000-8000-000000000008'),
    ('0193a5c0-0000-7000-8000-0000000000e4', '0193a5c0-0000-7000-8000-000000000007', 2, 'SUCCEEDED',
     '0193a5c0-0000-7000-8000-000000000009');
UPDATE dtp_job SET current_revision = 2, updated_at = CURRENT_TIMESTAMP
WHERE id = '0193a5c0-0000-7000-8000-000000000007';

-- revision 2 of the enrichment: the market data is available on business days from 07:00 to 22:00 (Vienna) only;
-- trades routed outside the slot wait in the inbox, a run still busy at 22:00 stops after the current batch.
-- Placeholders dtp.definitions.placeholders.market-data-*: tests use the whole day without calendar (NONE).
-- The zone of a slot without calendar is the cron zone of the revision.
INSERT INTO dtp_job_revision (job_id, revision, handler_class, inbox_min_items, fallback_job_id, fallback_delay,
                              cron_zone, changed_by, changed_at)
SELECT job_id, 2, handler_class, inbox_min_items, fallback_job_id, fallback_delay, 'Europe/Vienna', 'flyway',
       CURRENT_TIMESTAMP
FROM dtp_job_revision WHERE job_id = '0193a5c0-0000-7000-8000-000000000006' AND revision = 1;
INSERT INTO dtp_job_slot (id, job_id, revision, calendar_id, from_time, to_time)
VALUES ('0193a5c0-0000-7000-8000-0000000000d1', '0193a5c0-0000-7000-8000-000000000006', 2,
        (SELECT id FROM dtp_calendar WHERE namespace = 'trade-app' AND name = '${market-data-calendar}'),
        TIME '${market-data-from}', TIME '${market-data-to}');
UPDATE dtp_job SET current_revision = 2, updated_at = CURRENT_TIMESTAMP
WHERE id = '0193a5c0-0000-7000-8000-000000000006';
