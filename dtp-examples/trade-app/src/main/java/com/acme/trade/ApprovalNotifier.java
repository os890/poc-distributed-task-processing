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
package com.acme.trade;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

import org.os890.dtp.api.DecisionRequest;
import org.os890.dtp.api.event.DecisionRequested;

/**
 * The framework never notifies people itself: the app observes the domain event, fired after the commit, and informs
 * the responsible team.
 */
@ApplicationScoped
public class ApprovalNotifier {

    @Inject
    TeamNotifications team;

    void onDecisionRequested(@ObservesAsync DecisionRequested event) {
        DecisionRequest request = event.request();
        String subject = request.isApproval()
                ? "Approval needed (%d approvers): %s".formatted(request.requiredApprovals(), request.businessKey())
                : "Decision needed: " + (request.businessKey() != null ? request.businessKey() : request.job());
        team.send(audience(request), subject, request.reason() + "\n\nOpen requests: /dtp/decisions");
    }

    static String audience(DecisionRequest request) {
        return request.responsibleGroup() != null ? request.responsibleGroup() : "business team";
    }
}
