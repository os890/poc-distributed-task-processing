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
package org.os890.dtp.core.domain;

import java.util.UUID;

import org.os890.dtp.api.RunOutcome;

/**
 * Successor of a job, triggered when a run finishes with the given framework outcome or typed result status.
 *
 * @param onOutcome      framework outcome to match, or {@code null}
 * @param onResultStatus name of the typed result status to match, or {@code null}
 * @param targetJobId    the job to trigger
 */
public record TransitionDefinition(RunOutcome onOutcome, String onResultStatus, UUID targetJobId) {

    /**
     * Checks whether this transition fires for a finished run.
     *
     * @param outcome      the run's outcome
     * @param resultStatus the run's typed result status (may be {@code null})
     * @return {@code true} if the transition fires
     */
    public boolean matches(RunOutcome outcome, String resultStatus) {
        if (onOutcome != null) {
            return onOutcome == outcome;
        }
        return onResultStatus != null && onResultStatus.equals(resultStatus);
    }
}
