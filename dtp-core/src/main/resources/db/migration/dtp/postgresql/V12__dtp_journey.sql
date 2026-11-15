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

-- journey diagram per item (slice 12, PostgreSQL, docs/design/09-monitoring-heatmap.md §9.9): the journal entries
-- link the decision request and the work package they are about, so the diagram shows decisions and packages with
-- their state.
-- Entries written before slice 12 have no links: their decisions and packages are shown without details.
ALTER TABLE dtp_item_event ADD COLUMN decision_request_id UUID;
ALTER TABLE dtp_item_event ADD COLUMN work_package_id UUID;
