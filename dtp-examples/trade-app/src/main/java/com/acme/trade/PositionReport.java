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

import java.time.Duration;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunInfoKey;

/** End of day: positions per ISIN of the business date. Runs in parallel to the {@link PnlReport}. */
@ApplicationScoped
public class PositionReport implements JobHandler<NoParams, DefaultOutcome> {

    public static final RunInfoKey<Long> POSITIONS = RunInfoKey.of("positions", Long.class);

    @Inject
    EntityManager em;

    @ConfigProperty(name = "trade.eod.report-delay", defaultValue = "PT0S")
    Duration delay;

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
        EndOfDay.simulateWork(delay);
        List<Object[]> positions = em.createQuery("SELECT t.isin, SUM(t.quantity) FROM Trade t"
                + " WHERE t.businessDate = :date AND t.isin IS NOT NULL GROUP BY t.isin", Object[].class)
                .setParameter("date", EndOfDay.businessDate(ctx)).getResultList();
        ctx.runInfo().put(POSITIONS, (long) positions.size());
        return JobResult.success();
    }
}
