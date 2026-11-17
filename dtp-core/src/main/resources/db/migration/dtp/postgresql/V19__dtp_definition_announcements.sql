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

-- domain events of definitions (slice 26, PostgreSQL): the housekeeping leader announces each job revision
-- and window once (JobDefined, JobRevised, WindowDefined), whatever wrote it: a definition script, the builder, a
-- flow registration
ALTER TABLE dtp_job_revision ADD COLUMN announced_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE dtp_window ADD COLUMN announced_at TIMESTAMP WITH TIME ZONE;

-- what existed before was known already
UPDATE dtp_job_revision SET announced_at = COALESCE(changed_at, CURRENT_TIMESTAMP);
UPDATE dtp_window SET announced_at = created_at;
