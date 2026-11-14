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
import java.time.LocalDate;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.FlowInstance;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.RunInfoKey;

/**
 * Root of the end of day, one flow instance per business date ({@code POST /eod/{date}}): the cut-off. Its successors
 * (position and P&amp;L report in parallel, then the sign-off) belong to the same instance and read the business date
 * from it.
 */
@ApplicationScoped
public class EndOfDay implements JobHandler<EodParams, DefaultOutcome> {

    public static final RunInfoKey<Long> TRADES = RunInfoKey.of("trades", Long.class);

    @Inject
    EntityManager em;

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
        LocalDate date = businessDate(ctx);
        long trades = em.createQuery("SELECT COUNT(t) FROM Trade t WHERE t.businessDate = :date", Long.class)
                        .setParameter("date", date).getSingleResult();
        ctx.runInfo().put(TRADES, trades);
        return JobResult.success();
    }

    /**
     * Makes a report take longer, to see the cluster at work in the demo ({@code trade.eod.report-delay}).
     *
     * @param delay the delay, zero by default
     */
    static void simulateWork(Duration delay) {
        if (!delay.isZero()) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * The business date of the end of day: the business key of its flow instance.
     *
     * @param ctx the context of a run of the end of day
     * @return the business date
     */
    static LocalDate businessDate(JobContext<?> ctx) {
        return ctx.flowInstance().map(FlowInstance::businessKey).map(LocalDate::parse)
                  .orElseThrow(() -> new IllegalStateException("runs only within an end of day"));
    }
}
