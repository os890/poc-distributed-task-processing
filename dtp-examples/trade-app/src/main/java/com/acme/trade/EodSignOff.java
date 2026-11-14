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

import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;

/**
 * End of day: the run join of the position and the P&amp;L report (definition {@code V3__end_of_day.sql}). Runs once
 * per business date, after both reports succeeded, and informs operations.
 */
@ApplicationScoped
public class EodSignOff implements JobHandler<NoParams, DefaultOutcome> {

    @Inject
    TeamNotifications team;

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
        team.send("operations", "EOD " + EndOfDay.businessDate(ctx) + " signed off",
                "Positions and P&L are complete.");
        return JobResult.success();
    }
}
