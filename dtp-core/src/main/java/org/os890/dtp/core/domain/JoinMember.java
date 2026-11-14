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

import java.util.Set;
import java.util.UUID;

import org.os890.dtp.api.RunOutcome;

/**
 * A member of a run join (DD-9): the join target runs once per flow instance when every member finished with one of
 * its accepted outcomes.
 *
 * @param jobId            the member job
 * @param acceptedOutcomes the outcomes that count as arrived
 */
public record JoinMember(UUID jobId, Set<RunOutcome> acceptedOutcomes) {

    public JoinMember {
        acceptedOutcomes = Set.copyOf(acceptedOutcomes);
    }

    /**
     * Whether an outcome counts as arrived.
     *
     * @param outcome the outcome of the member's run
     * @return {@code true} if accepted
     */
    public boolean accepts(RunOutcome outcome) {
        return acceptedOutcomes.contains(outcome);
    }
}
