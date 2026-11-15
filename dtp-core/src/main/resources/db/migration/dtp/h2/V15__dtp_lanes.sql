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

-- lanes (slice 15, H2, DD-23): the weight of the lane is kept with the item and copied to its branches, so an inbox
-- shares its batches between lanes by weight; a branch keeps its lane for the lane filter of the branch inbox
ALTER TABLE dtp_item ADD COLUMN lane_weight INT DEFAULT 1 NOT NULL;
ALTER TABLE dtp_item_branch ADD COLUMN lane VARCHAR(50);
ALTER TABLE dtp_item_branch ADD COLUMN lane_weight INT DEFAULT 1 NOT NULL;
UPDATE dtp_item_branch SET lane = (SELECT i.lane FROM dtp_item i WHERE i.id = dtp_item_branch.item_id);

-- activations per lane: a version can be active for one lane only (e.g. a canary for the VIP lane); a row without
-- version ends the activation of its lane, which follows the activation for all lanes again
ALTER TABLE dtp_flow_activation ADD COLUMN lane VARCHAR(50);
ALTER TABLE dtp_flow_activation ALTER COLUMN version_id DROP NOT NULL;
