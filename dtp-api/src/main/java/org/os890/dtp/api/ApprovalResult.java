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

import java.util.List;

/**
 * Result of an approval request, read by the requesting step with {@link TrackedItem#approval()}.
 *
 * @param approved  {@code true} if the quorum was reached, {@code false} if a user rejected
 * @param approvers the users who approved
 * @param decidedBy the user whose vote decided the request
 * @param comment   the comment of the deciding vote, or {@code null}
 */
public record ApprovalResult(boolean approved, List<String> approvers, String decidedBy, String comment) {

    public ApprovalResult {
        approvers = List.copyOf(approvers);
    }
}
