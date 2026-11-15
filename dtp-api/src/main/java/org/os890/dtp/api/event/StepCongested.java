/*
 * Copyright 2026 os890
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.os890.dtp.api.event;

import java.time.Duration;
import java.time.Instant;

/**
 * A step is jammed (docs/design/09-monitoring-heatmap.md §9.4): its jam score stayed above the threshold for two
 * projection cycles, or no active node can run it. Fired once per alert transition, also across a change of the
 * projector leader. A jam doesn't change the health of the nodes.
 *
 * @param step         the job of the step
 * @param jamScore     0 to 100
 * @param statusReason why the step doesn't keep up, e.g. {@code PROCESSING} or {@code NO_CAPABLE_NODE}
 * @param queued       the queued items
 * @param oldestQueued the age of the oldest queued item
 * @param since        when the jam started
 */
public record StepCongested(String step, int jamScore, String statusReason, int queued, Duration oldestQueued,
                            Instant since) {
}
