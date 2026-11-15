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

-- telemetry traces (slice 13, H2, docs/design/09-monitoring-heatmap.md §9.8): an item tracked inside a traced unit
-- of work or request keeps its W3C traceparent, so the spans of later steps link to it; a journal entry keeps the
-- trace id of the unit of work that wrote it, so the journey links to the traces.
ALTER TABLE dtp_item ADD COLUMN trace_parent VARCHAR(55);
ALTER TABLE dtp_item_event ADD COLUMN trace_id VARCHAR(32);
