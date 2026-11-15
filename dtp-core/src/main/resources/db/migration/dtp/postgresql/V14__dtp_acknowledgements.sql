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

-- acknowledgements of work packages (slice 14, PostgreSQL, mission D37, docs/design/04-lifecycles.md §4.4): a job can
-- require that the receiver acknowledges a package it sent; until then the package is SENT, a rejection or a missing
-- acknowledgement within ack_timeout is a failed attempt (retry policy, then a decision request)
ALTER TABLE dtp_job_revision ADD COLUMN ack_required BOOLEAN DEFAULT FALSE NOT NULL;
ALTER TABLE dtp_job_revision ADD COLUMN ack_timeout VARCHAR(40);

-- the reference the receiver answers with (e.g. the file name), when the package was sent, until when the
-- acknowledgement is expected, and when and by whom it was acknowledged
ALTER TABLE dtp_work_package ADD COLUMN ack_reference VARCHAR(300);
ALTER TABLE dtp_work_package ADD COLUMN sent_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE dtp_work_package ADD COLUMN ack_due_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE dtp_work_package ADD COLUMN acknowledged_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE dtp_work_package ADD COLUMN acknowledged_by VARCHAR(200);
CREATE INDEX dtp_work_package_ack_ix ON dtp_work_package (job_id, ack_reference);
CREATE INDEX dtp_work_package_ack_due_ix ON dtp_work_package (namespace, state, ack_due_at);
