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

-- decision fallbacks (slice 5, D38, H2): one run of the fallback job once a request stays open too long

-- the job whose requests get a fallback names the fallback job and the delay (ISO-8601, DD-21)
ALTER TABLE dtp_job_revision ADD COLUMN fallback_job_id UUID;
ALTER TABLE dtp_job_revision ADD COLUMN fallback_delay VARCHAR(40);
ALTER TABLE dtp_job_revision ADD CONSTRAINT dtp_job_revision_fallback_fk
    FOREIGN KEY (fallback_job_id) REFERENCES dtp_job (id);

-- taken over from the requesting job when the request is created, the run is planned once (run key fallback:<id>)
ALTER TABLE dtp_decision_request ADD COLUMN fallback_job_id UUID;
ALTER TABLE dtp_decision_request ADD COLUMN fallback_due_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE dtp_decision_request ADD COLUMN fallback_run_id UUID;
CREATE INDEX dtp_decision_request_fallback_ix ON dtp_decision_request (state, fallback_due_at);
