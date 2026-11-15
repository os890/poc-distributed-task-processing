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

import java.time.Instant;

/**
 * A congested or failing step is back to normal (docs/design/09-monitoring-heatmap.md §9.4): its jam score stayed
 * below the threshold for two projection cycles, or its technical failures stayed below the threshold for the
 * failure window.
 *
 * @param step          the job of the step
 * @param previousState {@code CONGESTED} or {@code FAILING}
 * @param since         when it recovered
 */
public record StepRecovered(String step, String previousState, Instant since) {
}
