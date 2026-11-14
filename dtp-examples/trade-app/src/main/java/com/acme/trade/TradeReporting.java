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

import java.util.List;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;

import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.ItemType;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunInfoKey;
import org.os890.dtp.api.WindowOccurrence;

/**
 * The collector of the collection window {@code trade-reporting} (definition {@code V7__trade_reporting.sql}): a
 * regulatory trade report of the trades released since the last report. While it runs, the window pauses the
 * {@link TradeEnrichment}, so no trade is released during the collection, and every released trade is in exactly one
 * report: the one whose window {@code (from, cutOff]} contains its release time. One report file per business date.
 */
@ApplicationScoped
public class TradeReporting implements JobHandler<NoParams, DefaultOutcome> {

    /** A trade in a report: its own journey, next to the journey of the trade. */
    public static final ItemType<Trade, UUID> REPORTED = ItemType.of("trade-report", Trade.class, UUID.class);

    public static final RunInfoKey<Long> REPORTS = RunInfoKey.of("reports", Long.class);
    public static final RunInfoKey<Long> TRADES = RunInfoKey.of("trades", Long.class);

    @Inject
    EntityManager em;

    @Inject
    TradeReports reports;

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
        WindowOccurrence window = ctx.window().orElseThrow(
                () -> new IllegalStateException("the trade report runs within the window trade-reporting only"));
        // the trades released within the window, one package (report file) per business date
        TypedQuery<UUID> released = em.createQuery("SELECT t.id FROM Trade t WHERE t.released = TRUE"
                + " AND t.releasedAt <= :cutOff" + (window.since().isPresent() ? " AND t.releasedAt > :from" : "")
                + " ORDER BY t.tradeNumber", UUID.class).setParameter("cutOff", window.cutOff());
        window.since().ifPresent(from -> released.setParameter("from", from));
        ctx.packages(REPORTED).createFrom(released.getResultStream().map(id -> ItemRef.of(REPORTED, id)),
                ref -> em.find(Trade.class, ref.id()).getBusinessDate().toString(), Integer.MAX_VALUE);
        // writing the files: packages left over (e.g. a takeover) are written by the next run of the collector
        ctx.packages(REPORTED).forEachPending(pkg -> {
            List<String> trades = pkg.items().stream().map(item -> em.find(Trade.class, item.ref().id()))
                                     .map(Trade::getTradeNumber).toList();
            reports.write(fileName(window, pkg.groupKey()), trades);
            ctx.runInfo().increment(REPORTS, 1);
            ctx.runInfo().increment(TRADES, trades.size());
            pkg.done();
        });
        return JobResult.success();
    }

    private static String fileName(WindowOccurrence window, String businessDate) {
        return "trade-report-" + window.sequence() + "-" + businessDate + ".csv";
    }
}
