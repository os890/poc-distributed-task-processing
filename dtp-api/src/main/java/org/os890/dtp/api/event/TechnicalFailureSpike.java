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
import java.util.List;

/**
 * Technical failures of a step reached the threshold within the failure window (docs/design/09-monitoring-heatmap.md
 * §9.4): exceptions thrown by the step, not business failures. Fired once per alert transition.
 *
 * @param step       the job of the step
 * @param failures   the technical failures within the window
 * @param window     the failure window
 * @param errorTypes the exception classes, most frequent first
 * @param since      when the step started failing
 */
public record TechnicalFailureSpike(String step, int failures, Duration window, List<String> errorTypes,
                                    Instant since) {

    public TechnicalFailureSpike {
        errorTypes = List.copyOf(errorTypes);
    }
}
