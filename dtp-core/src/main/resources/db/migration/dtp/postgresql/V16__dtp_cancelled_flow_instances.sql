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

-- cancelled flow instances (slice 20, PostgreSQL): an operator cancels an open instance, e.g. the end of day of a business
-- date that was started by mistake; its pending and waiting runs are cancelled, its running ones stop softly, the open
-- decision requests about its runs are withdrawn (state WITHDRAWN). state: OPEN, COMPLETED or CANCELLED
ALTER TABLE dtp_flow_instance ADD COLUMN cancelled_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE dtp_flow_instance ADD COLUMN cancelled_by VARCHAR(200);
ALTER TABLE dtp_flow_instance ADD COLUMN cancel_reason VARCHAR(1000);
