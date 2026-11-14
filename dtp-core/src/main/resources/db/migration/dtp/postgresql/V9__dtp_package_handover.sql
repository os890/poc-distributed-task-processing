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

-- packages handed over to other jobs (slice 9, PostgreSQL): job_id is the job that processes a package,
-- created_by_job_id the collector that created it (the same job without a handover)
ALTER TABLE dtp_work_package ADD COLUMN created_by_job_id UUID;
UPDATE dtp_work_package SET created_by_job_id = job_id;
ALTER TABLE dtp_work_package ALTER COLUMN created_by_job_id SET NOT NULL;
ALTER TABLE dtp_work_package ADD CONSTRAINT dtp_work_package_creator_fk FOREIGN KEY (created_by_job_id)
    REFERENCES dtp_job (id);
CREATE INDEX dtp_work_package_creator_ix ON dtp_work_package (created_by_job_id, seq);
