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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.os890.dtp.api.FlowBuilder;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.FlowEvent;
import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.ResultStatus;
import org.os890.dtp.api.Signal;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpConfig;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.engine.FlowRegistrar;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.query.FlowQueries.Issue;
import org.os890.dtp.core.spi.Transactions;
import org.os890.dtp.core.testsupport.MapStepRegistry;
import org.os890.dtp.core.testsupport.TestCluster;

/** Items routed by signals: the flow definition in Java becomes a flow version in the DB. */
class FlowTest {

    record ImportParams(int count) {
    }

    enum ImportOutcome implements ResultStatus {
        SUCCESS, NO_DATA
    }

    enum ImportSignal implements Signal {
        IMPORTED, REIMPORTED
    }

    enum ValidationSignal implements Signal {
        VALID, MISSING_ISIN, REJECTED
    }

    enum EnrichmentSignal implements Signal {
        ENRICHED
    }

    static class TradeImport implements JobHandler<ImportParams, ImportOutcome> {

        private final Transactions tx;

        TradeImport(Transactions tx) {
            this.tx = tx;
        }

        @Override
        public JobResult<ImportOutcome> execute(JobContext<ImportParams> ctx) {
            ctx.inbox(TestTrade.ITEM).forEach(item -> item.signal(ImportSignal.REIMPORTED));
            int count = ctx.parameters().count();
            int offset = tx.withConnection(c -> Sql.queryOne(c, "SELECT COUNT(*) FROM test_trade",
                    rs -> rs.getInt(1)).orElse(0));
            for (int i = offset + 1; i <= offset + count; i++) {
                UUID id = UUID.randomUUID();
                String number = "T-" + i;
                String isin = i % 4 == 0 ? null : "AT000000000" + i;
                tx.withConnection(c -> Sql.update(c, "INSERT INTO test_trade (id, number, isin) VALUES (?, ?, ?)",
                        id, number, isin));
                ctx.items().track(ItemRef.of(TestTrade.ITEM, id), number).signal(ImportSignal.IMPORTED);
            }
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
            if (isin == null) {
                item.signal(ValidationSignal.MISSING_ISIN, "no ISIN");
            } else {
                item.signal(isin.startsWith("XX") ? ValidationSignal.REJECTED : ValidationSignal.VALID);
            }
        }
    }

    static class TradeEnrichment implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.signal(EnrichmentSignal.ENRICHED);
        }
    }

    static class ImportReport implements JobHandler<NoParams, ImportOutcome> {

        private final AtomicInteger runs = new AtomicInteger();

        @Override
        public JobResult<ImportOutcome> execute(JobContext<NoParams> ctx) {
            runs.incrementAndGet();
            return JobResult.of(ImportOutcome.SUCCESS);
        }
    }

    static class TradeFlow implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(TradeImport.class)
                    .on(ImportSignal.IMPORTED).to(TradeValidation.class)
                    .on(ImportSignal.REIMPORTED).to(TradeValidation.class)
                    .onResult(ImportOutcome.SUCCESS, ImportReport.class);
            flow.step(TradeValidation.class)
                    .on(ValidationSignal.VALID).to(TradeEnrichment.class)
                    .on(ValidationSignal.MISSING_ISIN).back(TradeImport.class)
                    .on(ValidationSignal.REJECTED).discard();
            flow.step(TradeEnrichment.class)
                    .on(EnrichmentSignal.ENRICHED).complete();
        }
    }

    @Test
    void signalsAreRoutedByTheFlowVersionInTheDb(@TempDir Path dir) throws Exception {
        try (TestCluster cluster = new TestCluster()) {
            Transactions tx = cluster.tx();
            cluster.migrate();
            cluster.execute(TestTrade.TABLE);
            cluster.definitions().define(TradeImport.class).external().maxItemVisits(3).save();
            ImportReport report = new ImportReport();
            Path export = dir.resolve("flows.md");
            DtpEngine a = cluster.start(TestCluster.config("a").flowExport(DtpConfig.FlowExport.MERMAID)
                            .flowExportPath(export).build(),
                    new MapStepRegistry(new TradeImport(tx), new TradeValidation(tx), new TradeEnrichment(),
                            report).flows(new TradeFlow()));
            cluster.start("b", List.of(new TradeFlow()), new TradeImport(tx), new TradeValidation(tx),
                    new TradeEnrichment(), report);

            a.jobEngine().trigger(TradeImport.class, new ImportParams(8));
            cluster.awaitIdle(Duration.ofSeconds(20));

            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(6);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE flow_version_id IS NULL")).isZero();
            assertThat(types(a, "T-1")).containsExactly("TRACKED", "ROUTED", "ROUTED", "COMPLETED");
            assertThat(types(a, "T-4")).containsExactly("TRACKED", "ROUTED", "ROUTED_BACK");
            assertThat(a.queries().journey("test-trade", "T-4").getLast())
                    .extracting(JourneyEntry::toJob, JourneyEntry::reason)
                    .containsExactly("TradeImport", "no ISIN");
            // onResult: the successor run of the typed result
            assertThat(report.runs).hasValue(1);
            assertThat(a.queries().runs(20)).anySatisfy(run -> {
                assertThat(run.job()).isEqualTo("import-report");
                assertThat(run.triggerType()).isEqualTo("TRANSITION");
            });

            // a flow started for an entity outside of runs: into the inbox of the entry step
            UUID id = UUID.randomUUID();
            cluster.execute("INSERT INTO test_trade (id, number, isin) VALUES (?, 'T-X', 'XX0000000001')", id);
            TrackedItem<?, ?> started = tx.required(() -> a.fire(FlowEvent.start(TradeFlow.class,
                    ItemRef.of(TestTrade.ITEM, id), "T-X")));
            assertThat(started.businessKey()).isEqualTo("T-X");
            cluster.execute("UPDATE test_trade SET isin = 'AT0000FIXED' WHERE isin IS NULL");
            a.jobEngine().trigger(TradeImport.class, new ImportParams(0));
            cluster.awaitIdle(Duration.ofSeconds(20));

            assertThat(types(a, "T-X")).containsExactly("TRACKED", "ROUTED", "ROUTED", "DISCARDED");
            assertThat(a.queries().journey("test-trade", "T-X").getLast().reason()).isEqualTo("REJECTED");
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(8);
            assertThat(report.runs).hasValue(1);

            assertThat(a.flowQueries().flows()).singleElement().satisfies(flow -> {
                assertThat(flow.name()).isEqualTo("trade-flow");
                assertThat(flow.versions()).hasSize(1);
                assertThat(flow.activeVersion()).isEqualTo(flow.versions().getFirst().hash());
            });
            String diagram = a.flowQueries().mermaid("trade-flow", null);
            assertThat(diagram).startsWith("flowchart LR")
                               .contains("start([\"external<br/>FlowEvent.start\"])")
                               .contains("-.->|MISSING_ISIN|", "==>|SUCCESS|", "-->|ENRICHED| done", "discarded");
            assertThat(a.flowQueries().validate()).extracting(Issue::message)
                    .contains("trade-validation is part of a loop but has no max_item_visits")
                    .noneMatch(m -> m.contains("has no route"));
            assertThat(Files.readString(export)).contains("## trade-flow", "```mermaid");
        }
    }

    @Test
    void aFlowWithoutEntryOrWithTwoRoutesIsRejected() {
        FlowDefinition noEntry = flow -> flow.step(TradeEnrichment.class).on(EnrichmentSignal.ENRICHED).complete();
        assertThatThrownBy(() -> FlowRegistrar.compile(Map.of(TradeFlow.class, noEntry)))
                .hasMessageContaining("has no entry step");
        FlowDefinition twice = flow -> flow.entry(TradeEnrichment.class)
                                           .on(EnrichmentSignal.ENRICHED).complete()
                                           .on(EnrichmentSignal.ENRICHED).discard();
        assertThatThrownBy(() -> FlowRegistrar.compile(Map.of(TradeFlow.class, twice)))
                .hasMessageContaining("routes ENRICHED twice");
    }

    private static List<String> types(DtpEngine engine, String businessKey) {
        return engine.queries().journey("test-trade", businessKey).stream().map(JourneyEntry::type).toList();
    }
}
