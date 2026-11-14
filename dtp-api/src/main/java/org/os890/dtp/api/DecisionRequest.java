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
package org.os890.dtp.api;

import java.time.Instant;
import java.util.UUID;

/**
 * A decision request as the application sees it: in the domain events of decisions and as the parameters of a
 * decision fallback run ({@code JobHandler<DecisionRequest, ...>}). A snapshot taken when the event or run was created.
 *
 * @param id                the request
 * @param kind              CHOICE or APPROVAL
 * @param subjectType       ITEM or RUN
 * @param subjectId         the item or run that waits
 * @param itemType          the item type, or {@code null} for a run
 * @param businessKey       the business key of the item, or {@code null} for a run
 * @param job               the job that requested the decision
 * @param reason            why
 * @param requiredApprovals the quorum of an approval, or {@code null}
 * @param approvals         the approvals so far
 * @param responsibleGroup  the responsible team, or {@code null} for everyone with the business role
 * @param requestedAt       when it was requested
 */
public record DecisionRequest(UUID id, Kind kind, String subjectType, UUID subjectId, String itemType,
                              String businessKey, String job, String reason, Integer requiredApprovals, int approvals,
                              String responsibleGroup, Instant requestedAt) {

    /** Kinds of requests. */
    public enum Kind {
        /** One user picks an option. */
        CHOICE,
        /** A quorum of distinct users approves, one rejection rejects (DD-16). */
        APPROVAL
    }

    /**
     * Whether it's an approval.
     *
     * @return {@code true} for {@link Kind#APPROVAL}
     */
    public boolean isApproval() {
        return kind == Kind.APPROVAL;
    }
}
