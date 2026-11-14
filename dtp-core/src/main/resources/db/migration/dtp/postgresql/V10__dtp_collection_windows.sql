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

-- collection windows (slice 10, PostgreSQL, docs/design/03-persistence.md, 04 §4.6, 05 §5.6): at each occurrence the
-- paused jobs are stopped softly; once they are idle the occurrence opens with a cut-off and the collector runs.
-- Deviations: the schedule has its own zone (schedule_zone, the calendar's zone wins), schedule_cron may be NULL
-- (started by operators only), the occurrence number is called seq, SCHEDULED isn't stored.
CREATE TABLE dtp_window (
    id                UUID          NOT NULL PRIMARY KEY,
    namespace         VARCHAR(100)  NOT NULL,
    name              VARCHAR(200)  NOT NULL,
    schedule_cron     VARCHAR(100),
    schedule_zone     VARCHAR(50)   DEFAULT 'UTC' NOT NULL,
    calendar_id       UUID,
    collector_job_id  UUID          NOT NULL,
    quiesce_timeout   VARCHAR(40)   DEFAULT 'PT15M' NOT NULL,
    enabled           BOOLEAN       DEFAULT TRUE NOT NULL,
    created_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT dtp_window_name_uk UNIQUE (namespace, name),
    CONSTRAINT dtp_window_calendar_fk FOREIGN KEY (calendar_id) REFERENCES dtp_calendar (id),
    CONSTRAINT dtp_window_collector_fk FOREIGN KEY (collector_job_id) REFERENCES dtp_job (id)
);

CREATE TABLE dtp_window_paused_job (
    window_id  UUID  NOT NULL,
    job_id     UUID  NOT NULL,
    CONSTRAINT dtp_window_paused_job_pk PRIMARY KEY (window_id, job_id),
    CONSTRAINT dtp_window_paused_job_window_fk FOREIGN KEY (window_id) REFERENCES dtp_window (id),
    CONSTRAINT dtp_window_paused_job_job_fk FOREIGN KEY (job_id) REFERENCES dtp_job (id)
);
CREATE INDEX dtp_window_paused_job_job_ix ON dtp_window_paused_job (job_id);

CREATE TABLE dtp_window_occurrence (
    id                  UUID          NOT NULL PRIMARY KEY,
    window_id           UUID          NOT NULL,
    seq                 BIGINT        NOT NULL,
    scheduled_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    -- QUIESCING, OPEN, WAITING_FOR_DECISION, CLOSED or SKIPPED
    state               VARCHAR(30)   NOT NULL,
    triggered_by        VARCHAR(200),
    force_open          BOOLEAN       DEFAULT FALSE NOT NULL,
    -- the cut-off of the last closed occurrence (NULL for the first), and the own cut-off once open
    from_ts             TIMESTAMP WITH TIME ZONE,
    cut_off             TIMESTAMP WITH TIME ZONE,
    collector_run_id    UUID,
    collector_attempts  INT           DEFAULT 0 NOT NULL,
    state_changed_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dtp_window_occurrence_seq_uk UNIQUE (window_id, seq),
    CONSTRAINT dtp_window_occurrence_slot_uk UNIQUE (window_id, scheduled_at),
    CONSTRAINT dtp_window_occurrence_window_fk FOREIGN KEY (window_id) REFERENCES dtp_window (id)
);
CREATE INDEX dtp_window_occurrence_state_ix ON dtp_window_occurrence (state);

ALTER TABLE dtp_run ADD COLUMN window_occurrence_id UUID;
ALTER TABLE dtp_work_package ADD COLUMN window_occurrence_id UUID;
CREATE INDEX dtp_work_package_window_ix ON dtp_work_package (window_occurrence_id);
