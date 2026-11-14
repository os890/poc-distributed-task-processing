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
import jakarta.inject.Inject;

import org.os890.dtp.api.DecisionRequest;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;

/**
 * Decision fallback of {@link TradeEnrichment} (job definition {@code V2__approval_reminder.sql}): runs once per
 * request that stays open longer than the configured delay. It only reminds people, the request stays open and is never
 * decided automatically (D38).
 */
@ApplicationScoped
public class ApprovalReminder implements JobHandler<DecisionRequest, DefaultOutcome> {

    @Inject
    TeamNotifications team;

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<DecisionRequest> ctx) {
        DecisionRequest open = ctx.parameters();
        String progress = open.isApproval()
                ? ", approvals %d of %d".formatted(open.approvals(), open.requiredApprovals()) : "";
        team.send(ApprovalNotifier.audience(open), "Reminder: still open for " + open.businessKey(),
                "%s%nOpen since %s%s.".formatted(open.reason(), open.requestedAt(), progress));
        return JobResult.success();
    }
}
