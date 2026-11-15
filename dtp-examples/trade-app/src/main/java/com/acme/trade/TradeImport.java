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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.RunInfoKey;

/** Imports the trades of a business date and hands them to the validation (docs/design/07-sample-app.md). */
@ApplicationScoped
public class TradeImport implements JobHandler<ImportParams, ImportOutcome> {

    public static final RunInfoKey<Long> IMPORTED = RunInfoKey.of("importedTrades", Long.class);

    public static final RunInfoKey<Long> REIMPORTED = RunInfoKey.of("reimportedTrades", Long.class);

    @Inject
    TradeFeed feed;

    @Override
    public JobResult<ImportOutcome> execute(JobContext<ImportParams> ctx) {
        // items routed back by TradeValidation are in this job's inbox, re-import them first
        ctx.inbox(Trade.ITEM).forEach(item -> {
            feed.reimport(item.ref().id());
            item.signal(ImportSignal.REIMPORTED, "re-imported");
            ctx.runInfo().increment(REIMPORTED, 1);
        });

        List<Trade> trades = feed.importFor(ctx.parameters().businessDate(), ctx.parameters().trades());
        for (Trade trade : trades) {
            ctx.items()
                    .track(ItemRef.of(Trade.ITEM, trade.getId(), trade.getVersion()), trade.getTradeNumber(),
                            TradeLane.of(trade))
                    .signal(ImportSignal.IMPORTED);
        }
        ctx.runInfo().put(IMPORTED, (long) trades.size());
        return JobResult.of(trades.isEmpty() ? ImportOutcome.NO_DATA : ImportOutcome.SUCCESS);
    }
}
