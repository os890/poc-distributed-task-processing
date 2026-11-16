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

-- red/green (slice 24, H2): since when the active color has no live node, set once for the cluster by the first
-- standby node that notices it, cleared by a heartbeat of the active color or the next switch
ALTER TABLE dtp_namespace_control ADD COLUMN unavailable_since TIMESTAMP WITH TIME ZONE;
