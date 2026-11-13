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
package org.os890.dtp.core.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.ResultStatus;
import org.os890.dtp.api.RunInfoKey;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.spi.Transactions;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * The trade flow of docs/design/07-sample-app.md (level 2) on two nodes: import, validation with back-routing,
 * enrichment, and the re-import of the routed-back trades.
 */
class ItemFlowTest {

    record ImportParams(int count) {
    }

    enum ImportOutcome implements ResultStatus {
        SUCCESS, NO_DATA
    }

    static class TradeImport implements JobHandler<ImportParams, ImportOutcome> {

        static final RunInfoKey<Long> IMPORTED = RunInfoKey.of("importedTrades", Long.class);

        private final Transactions tx;

        TradeImport(Transactions tx) {
            this.tx = tx;
        }

        @Override
        public JobResult<ImportOutcome> execute(JobContext<ImportParams> ctx) {
            // trades routed back by the validation are in this job's inbox: re-import them first
            ctx.inbox(TestTrade.ITEM).forEach(item -> {
                tx.withConnection(c -> Sql.update(c, "UPDATE test_trade SET isin = 'AT0000FIXED' WHERE id = ?",
                        item.ref().id()));
                item.routeTo(TradeValidation.class, "re-imported");
            });
            int count = ctx.parameters().count();
            int offset = tx.withConnection(c -> Sql.queryOne(c, "SELECT COUNT(*) FROM test_trade",
                    rs -> rs.getInt(1)).orElse(0));
            for (int i = offset + 1; i <= offset + count; i++) {
                UUID id = UUID.randomUUID();
                String number = "T-" + i;
                String isin = i % 4 == 0 ? null : "AT000000000" + i;
                tx.withConnection(c -> Sql.update(c, "INSERT INTO test_trade (id, number, isin) VALUES (?, ?, ?)",
                        id, number, isin));
                ctx.items().track(ItemRef.of(TestTrade.ITEM, id), number).routeTo(TradeValidation.class);
            }
            ctx.runInfo().put(IMPORTED, (long) count);
            return JobResult.of(count == 0 ? ImportOutcome.NO_DATA : ImportOutcome.SUCCESS);
        }
    }

    @ItemProcessing(batchSize = 5, parallelism = 2)
    static class TradeValidation implements ItemProcessor<TestTrade, UUID> {

        private final Transactions tx;

        TradeValidation(Transactions tx) {
            this.tx = tx;
        }

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            String isin = tx.withConnection(c -> Sql.queryOne(c, "SELECT isin FROM test_trade WHERE id = ?",
                    rs -> rs.getString(1), item.ref().id())).orElse(null);
            if (isin != null) {
                item.routeTo(TradeEnrichment.class);
            } else {
                item.routeBack(TradeImport.class, "missing ISIN");
            }
        }
    }

    static class TradeEnrichment implements ItemProcessor<TestTrade, UUID> {

        private final Transactions tx;

        TradeEnrichment(Transactions tx) {
            this.tx = tx;
        }

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            tx.withConnection(c -> Sql.update(c, "UPDATE test_trade SET enriched = TRUE WHERE id = ?",
                    item.ref().id()));
            item.complete();
        }
    }

    @Test
    void invalidTradesAreRoutedBackAndCompleteAfterTheReimport() {
        try (TestCluster cluster = new TestCluster()) {
            Transactions tx = cluster.tx();
            DtpEngine a = cluster.start("a", new TradeImport(tx), new TradeValidation(tx), new TradeEnrichment(tx));
            cluster.execute(TestTrade.TABLE);
            cluster.definitions().define(TradeEnrichment.class).inbox(1).save();
            cluster.definitions().define(TradeValidation.class).inbox(1).save();
            cluster.definitions().define(TradeImport.class).external().maxItemVisits(3)
                   .onStatus(ImportOutcome.SUCCESS, TradeValidation.class).save();
            cluster.start("b", new TradeImport(tx), new TradeValidation(tx), new TradeEnrichment(tx));

            a.jobEngine().trigger(TradeImport.class, new ImportParams(12));
            cluster.awaitIdle(Duration.ofSeconds(20));

            assertThat(cluster.count("SELECT COUNT(*) FROM test_trade WHERE enriched")).isEqualTo(9);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(9);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item i JOIN dtp_job j ON j.id = i.current_job_id"
                    + " WHERE i.state = 'QUEUED' AND j.name = 'TradeImport' AND i.arrival_reason = 'missing ISIN'"))
                    .isEqualTo(3);
            assertThat(types(a, "T-1")).containsExactly("TRACKED", "ROUTED", "ROUTED", "COMPLETED");
            assertThat(types(a, "T-4")).containsExactly("TRACKED", "ROUTED", "ROUTED_BACK");
            assertThat(a.queries().journey("test-trade", "T-4").getLast())
                    .extracting(JourneyEntry::toJob, JourneyEntry::reason)
                    .containsExactly("TradeImport", "missing ISIN");
            assertThat(a.queries().runs(20)).anySatisfy(run -> {
                assertThat(run.job()).isEqualTo("TradeImport");
                assertThat(run.resultStatus()).isEqualTo("SUCCESS");
                assertThat(run.info()).contains("\"importedTrades\":12");
            });

            // the next import (without new trades) re-imports the routed-back ones
            a.jobEngine().trigger(TradeImport.class, new ImportParams(0));
            cluster.awaitIdle(Duration.ofSeconds(20));

            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(12);
            assertThat(types(a, "T-4")).containsExactly("TRACKED", "ROUTED", "ROUTED_BACK", "ROUTED", "ROUTED",
                    "COMPLETED");
            assertThat(a.queries().journey("test-trade", "T-4").get(3).reason()).isEqualTo("re-imported");
            assertThat(a.queries().itemCounts()).anySatisfy(count -> {
                assertThat(count.state()).isEqualTo("COMPLETED");
                assertThat(count.count()).isEqualTo(12);
            });
            assertThat(a.queries().nodes()).hasSize(2).filteredOn(n -> n.leader()).hasSize(1);
        }
    }

    private static List<String> types(DtpEngine engine, String businessKey) {
        return engine.queries().journey("test-trade", businessKey).stream().map(JourneyEntry::type).toList();
    }
}
