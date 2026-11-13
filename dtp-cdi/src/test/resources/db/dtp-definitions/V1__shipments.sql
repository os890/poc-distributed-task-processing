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

-- job definitions of the test application (DD-15)
INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at, updated_at)
VALUES ('0193a5c0-0000-7000-8000-000000000001', 'cdi-test', 'shipment-planning', 1, TRUE,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO dtp_job_revision (job_id, revision, handler_class, trigger_external, changed_by, changed_at)
VALUES ('0193a5c0-0000-7000-8000-000000000001', 1, 'org.os890.dtp.cdi.ShipmentPlanning', TRUE, 'flyway',
        CURRENT_TIMESTAMP);

INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at, updated_at)
VALUES ('0193a5c0-0000-7000-8000-000000000002', 'cdi-test', 'shipment-dispatch', 1, TRUE,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
INSERT INTO dtp_job_revision (job_id, revision, handler_class, inbox_min_items, changed_by, changed_at)
VALUES ('0193a5c0-0000-7000-8000-000000000002', 1, 'org.os890.dtp.cdi.ShipmentDispatch', 1, 'flyway',
        CURRENT_TIMESTAMP);
