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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;

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

/**
 * End of day: the traded volume of the business date. Fails while trades of the date wait for their ISIN (routed back
 * to the import), so the end of day waits at the sign-off until an operator runs the report again.
 */
@ApplicationScoped
public class PnlReport implements JobHandler<NoParams, DefaultOutcome> {

    public static final RunInfoKey<String> VOLUME = RunInfoKey.of("volume", String.class);

    @Inject
    EntityManager em;

    @ConfigProperty(name = "trade.eod.report-delay", defaultValue = "PT0S")
    Duration delay;

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
        EndOfDay.simulateWork(delay);
        LocalDate date = EndOfDay.businessDate(ctx);
        long incomplete = em.createQuery("SELECT COUNT(t) FROM Trade t WHERE t.businessDate = :date"
                + " AND t.isin IS NULL", Long.class).setParameter("date", date).getSingleResult();
        if (incomplete > 0) {
            throw new IllegalStateException(incomplete + " trades of " + date + " have no ISIN yet");
        }
        BigDecimal volume = em.createQuery("SELECT SUM(t.price * t.quantity) FROM Trade t"
                + " WHERE t.businessDate = :date AND t.price IS NOT NULL", BigDecimal.class)
                .setParameter("date", date).getSingleResult();
        ctx.runInfo().put(VOLUME, volume == null ? "0" : volume.toPlainString());
        return JobResult.success();
    }
}
