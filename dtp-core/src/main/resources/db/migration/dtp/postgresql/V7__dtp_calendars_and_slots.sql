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

-- business calendars, calendars of cron triggers and processing slots (slice 7, PostgreSQL)
-- Deviation from the design: the day of an exception is called calendar_day (DAY is a keyword in H2).

-- business days of a namespace: weekdays plus exceptions (holidays, or extra business days)
CREATE TABLE dtp_calendar (
    id                 UUID          NOT NULL PRIMARY KEY,
    namespace          VARCHAR(100)  NOT NULL,
    name               VARCHAR(100)  NOT NULL,
    zone               VARCHAR(60)   NOT NULL,
    -- ranges and lists of weekdays, e.g. MON-FRI or MON,WED,FRI
    business_weekdays  VARCHAR(100)  DEFAULT 'MON-FRI' NOT NULL,
    CONSTRAINT dtp_calendar_name_uk UNIQUE (namespace, name)
);

CREATE TABLE dtp_calendar_exception (
    calendar_id   UUID          NOT NULL,
    calendar_day  DATE          NOT NULL,
    -- FALSE: a holiday on a business weekday, TRUE: a business day on another weekday
    business_day  BOOLEAN       DEFAULT FALSE NOT NULL,
    description   VARCHAR(200),
    CONSTRAINT dtp_calendar_exception_pk PRIMARY KEY (calendar_id, calendar_day),
    CONSTRAINT dtp_calendar_exception_calendar_fk FOREIGN KEY (calendar_id) REFERENCES dtp_calendar (id)
);

-- a cron slot on a day that isn't a business day of the calendar is skipped or moved to the next or previous one;
-- with cron_flow_instance every slot starts a flow instance whose business key is the business date of the slot
ALTER TABLE dtp_job_revision ADD COLUMN cron_calendar_id UUID;
ALTER TABLE dtp_job_revision ADD COLUMN calendar_shift VARCHAR(20) DEFAULT 'SKIP' NOT NULL;
ALTER TABLE dtp_job_revision ADD COLUMN cron_flow_instance BOOLEAN DEFAULT FALSE NOT NULL;
ALTER TABLE dtp_job_revision ADD CONSTRAINT dtp_job_revision_calendar_fk
    FOREIGN KEY (cron_calendar_id) REFERENCES dtp_calendar (id);
ALTER TABLE dtp_job_revision ADD CONSTRAINT dtp_job_revision_flow_instance_ck
    CHECK (cron_flow_instance = FALSE OR cron_expression IS NOT NULL);

-- runs of a job with processing slots start only within a slot and are asked to stop at its end;
-- from_time = to_time is the whole day, to_time < from_time ends on the next day, no calendar means every day
CREATE TABLE dtp_job_slot (
    id           UUID   NOT NULL PRIMARY KEY,
    job_id       UUID   NOT NULL,
    revision     INT    NOT NULL,
    calendar_id  UUID,
    from_time    TIME   NOT NULL,
    to_time      TIME   NOT NULL,
    CONSTRAINT dtp_job_slot_revision_fk FOREIGN KEY (job_id, revision) REFERENCES dtp_job_revision (job_id, revision),
    CONSTRAINT dtp_job_slot_calendar_fk FOREIGN KEY (calendar_id) REFERENCES dtp_calendar (id)
);

-- the cron slot a run was planned for (before a calendar shift), and the end of the processing slot of a run
ALTER TABLE dtp_run ADD COLUMN cron_slot TIMESTAMP WITH TIME ZONE;
ALTER TABLE dtp_run ADD COLUMN slot_end TIMESTAMP WITH TIME ZONE;
UPDATE dtp_run SET cron_slot = not_before WHERE trigger_type = 'CRON';
CREATE INDEX dtp_run_cron_slot_ix ON dtp_run (job_id, cron_slot);
