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

/**
 * Sends the settlement files the {@link SettlementExport} created for it (definition {@code V6__settlement_sender.sql},
 * started by its inbox trigger when packages are due). A file a custodian rejects is retried with this job's retry
 * policy, then a person decides ({@code RETRY} or {@code DISCARD}). Files left over (item limit, slot end, crash) are
 * sent by the next run.
 */
@ApplicationScoped
public class SettlementSender implements JobHandler<NoParams, DefaultOutcome> {

    public static final RunInfoKey<String> FILE = RunInfoKey.of("file", String.class);
    public static final RunInfoKey<Long> SENT = RunInfoKey.of("sent", Long.class);

    @Inject
    EntityManager em;

    @Inject
    SettlementGateway gateway;

    @ConfigProperty(name = "trade.settlement.send-delay", defaultValue = "PT0S")
    Duration sendDelay;

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
        ctx.packages(SettlementExport.INSTRUCTION).forEachPending(pkg -> {
            if (!gateway.isAvailable(pkg.groupKey())) {
                pkg.fail(pkg.groupKey() + " rejected the file");
                return;
            }
            EndOfDay.simulateWork(sendDelay);
            List<String> trades = pkg.items().stream().map(item -> em.find(Trade.class, item.ref().id()))
                                     .map(Trade::getTradeNumber).toList();
            pkg.info().put(FILE, gateway.send(pkg.groupKey(), pkg.sequence(), trades));
            ctx.runInfo().increment(SENT, 1);
            pkg.done();
        });
        return JobResult.success();
    }
}
