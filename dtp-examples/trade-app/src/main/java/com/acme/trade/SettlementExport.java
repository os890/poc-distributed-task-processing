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
import java.util.List;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.FlowInstance;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.ItemType;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunInfoKey;
import org.os890.dtp.api.WorkPackages;

/**
 * Settlement export after the sign-off of an end of day (a collector with work packages, definition
 * {@code V5__settlement_export.sql}): one settlement instruction per released trade of the business date, one file per
 * custodian. Packages a custodian rejects are retried, then a person decides ({@code RETRY} or {@code DISCARD}).
 * Packages left over (item limit, slot end, crash) are sent by the next run of the job.
 */
@ApplicationScoped
public class SettlementExport implements JobHandler<NoParams, DefaultOutcome> {

    /** A settlement instruction of a trade: its own journey, next to the journey of the trade. */
    public static final ItemType<Trade, UUID> INSTRUCTION = ItemType.of("settlement-instruction", Trade.class,
            UUID.class);

    public static final RunInfoKey<String> FILE = RunInfoKey.of("file", String.class);
    public static final RunInfoKey<Long> FILES = RunInfoKey.of("files", Long.class);

    @Inject
    EntityManager em;

    @Inject
    SettlementGateway gateway;

    @ConfigProperty(name = "trade.settlement.package-size", defaultValue = "500")
    int packageSize;

    @ConfigProperty(name = "trade.settlement.send-delay", defaultValue = "PT0S")
    Duration sendDelay;

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
        WorkPackages<Trade, UUID> packages = ctx.packages(INSTRUCTION);
        // within an end of day: the released trades of its business date (a trade is packaged once, also on a rerun)
        ctx.flowInstance().map(FlowInstance::businessKey).map(LocalDate::parse).ifPresent(date ->
                packages.createFrom(em.createQuery("SELECT t.id FROM Trade t WHERE t.businessDate = :date"
                        + " AND t.released = TRUE ORDER BY t.tradeNumber", UUID.class).setParameter("date", date)
                        .getResultStream().map(id -> ItemRef.of(INSTRUCTION, id)),
                        ref -> gateway.custodianOf(em.find(Trade.class, ref.id())), packageSize));
        packages.forEachPending(pkg -> {
            if (!gateway.isAvailable(pkg.groupKey())) {
                pkg.fail(pkg.groupKey() + " rejected the file");
                return;
            }
            EndOfDay.simulateWork(sendDelay);
            List<String> trades = pkg.items().stream().map(item -> em.find(Trade.class, item.ref().id()))
                                     .map(Trade::getTradeNumber).toList();
            pkg.info().put(FILE, gateway.send(pkg.groupKey(), pkg.sequence(), trades));
            ctx.runInfo().increment(FILES, 1);
            pkg.done();
        });
        return JobResult.success();
    }
}
