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

/**
 * Settlement export after the sign-off of an end of day (a collector, definitions {@code V5__settlement_export.sql}
 * and {@code V6__settlement_sender.sql}): one settlement instruction per released trade of the business date, grouped
 * into one file (work package) per custodian. The files are handed over to the {@link SettlementSender}, so the end
 * of day completes once its files are created, and sending, retries and decisions are the sender's business.
 */
@ApplicationScoped
public class SettlementExport implements JobHandler<NoParams, DefaultOutcome> {

    /** A settlement instruction of a trade: its own journey, next to the journey of the trade. */
    public static final ItemType<Trade, UUID> INSTRUCTION = ItemType.of("settlement-instruction", Trade.class,
            UUID.class);

    public static final RunInfoKey<Long> FILES = RunInfoKey.of("files", Long.class);

    @Inject
    EntityManager em;

    @Inject
    SettlementGateway gateway;

    @ConfigProperty(name = "trade.settlement.package-size", defaultValue = "500")
    int packageSize;

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
        // within an end of day: the released trades of its business date (a trade is packaged once, also on a rerun)
        ctx.flowInstance().map(FlowInstance::businessKey).map(LocalDate::parse).ifPresent(date ->
                ctx.runInfo().increment(FILES, ctx.packages(INSTRUCTION).forStep(SettlementSender.class).createFrom(
                        em.createQuery("SELECT t.id FROM Trade t WHERE t.businessDate = :date AND t.released = TRUE"
                                + " ORDER BY t.tradeNumber", UUID.class).setParameter("date", date)
                                .getResultStream().map(id -> ItemRef.of(INSTRUCTION, id)),
                        ref -> gateway.custodianOf(em.find(Trade.class, ref.id())), packageSize)));
        return JobResult.success();
    }
}
