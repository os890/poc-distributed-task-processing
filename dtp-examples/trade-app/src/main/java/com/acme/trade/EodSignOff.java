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

import java.time.LocalDate;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.os890.dtp.api.Approval;
import org.os890.dtp.api.ApprovalResult;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.core.engine.FlowInstances;

/**
 * End of day: the run join of the position and the P&amp;L report (definition {@code V3__end_of_day.sql}). Runs once
 * per business date, after both reports succeeded, and informs operations. An end of day whose reports had to be run
 * again is signed off by two members of product control first (four eyes): the run waits for their approval, then
 * it's executed again and reads the result. A rejection fails the run, an operator runs it again after the
 * correction.
 */
@ApplicationScoped
public class EodSignOff implements JobHandler<NoParams, DefaultOutcome> {

    @Inject
    TeamNotifications team;

    @Inject
    FlowInstances instances;

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
        LocalDate date = EndOfDay.businessDate(ctx);
        Optional<ApprovalResult> approval = ctx.approval();
        if (approval.isEmpty() && corrected(date)) {
            ctx.requestApproval(Approval.quorum(2).assignedTo(Desk.PRODUCT_CONTROL),
                    "Reports of the EOD " + date + " were run again: please check and sign off");
            return JobResult.success();
        }
        if (approval.isPresent() && !approval.get().approved()) {
            ApprovalResult rejected = approval.get();
            team.send("operations", "EOD " + date + " sign-off rejected", "Rejected by " + rejected.decidedBy()
                    + (rejected.comment() == null ? "" : ": " + rejected.comment())
                    + ". Correct the reports, then run the sign-off again.");
            throw new IllegalStateException("sign-off of the EOD " + date + " rejected by " + rejected.decidedBy());
        }
        team.send("operations", "EOD " + date + " signed off", "Positions and P&L are complete."
                + approval.map(a -> " Approved by " + String.join(" and ", a.approvers()) + ".").orElse(""));
        return JobResult.success();
    }

    /** {@code true} if a report of the end of day was run again. */
    private boolean corrected(LocalDate date) {
        return instances.find("end-of-day", date.toString()).stream().flatMap(i -> i.runs().stream())
                        .anyMatch(r -> "RERUN".equals(r.triggerType()));
    }
}
