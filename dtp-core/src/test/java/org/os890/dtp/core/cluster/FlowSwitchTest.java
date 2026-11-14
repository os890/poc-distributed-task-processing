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
import static org.assertj.core.api.Assertions.tuple;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.FlowBuilder;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.ResultStatus;
import org.os890.dtp.api.Signal;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.domain.RouteKind;
import org.os890.dtp.core.engine.FlowAdmin.Migration;
import org.os890.dtp.core.engine.FlowAdmin.Redirect;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.query.FlowQueries.ActivationView;
import org.os890.dtp.core.query.FlowQueries.FlowView;
import org.os890.dtp.core.query.FlowQueries.VersionView;
import org.os890.dtp.core.spi.Transactions;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Two versions of a flow (red/green deployment): the new version is activated when it is deployed, items stay on
 * the version they are pinned to, operators switch back, overrule a hop and migrate queued items.
 */
class FlowSwitchTest {

    private static final String FLOW = "trade-flow";

    record ImportParams(List<String> isins) {
    }

    enum ImportOutcome implements ResultStatus {
        SUCCESS
    }

    enum ImportSignal implements Signal {
        IMPORTED, REIMPORTED
    }

    enum ValidationSignal implements Signal {
        VALID, MISSING_ISIN
    }

    enum RepairSignal implements Signal {
        REPAIRED
    }

    static class TradeImport implements JobHandler<ImportParams, ImportOutcome> {

        private final Transactions tx;

        TradeImport(Transactions tx) {
            this.tx = tx;
        }

        @Override
        public JobResult<ImportOutcome> execute(JobContext<ImportParams> ctx) {
            ctx.inbox(TestTrade.ITEM).forEach(item -> item.signal(ImportSignal.REIMPORTED));
            for (String isin : ctx.parameters().isins()) {
                UUID id = UUID.randomUUID();
                String number = "T-" + tx.withConnection(c -> Sql.queryOne(c, "SELECT COUNT(*) + 1 FROM test_trade",
                        rs -> rs.getInt(1)).orElseThrow());
                String value = isin.isEmpty() ? null : isin;
                tx.withConnection(c -> Sql.update(c, "INSERT INTO test_trade (id, number, isin) VALUES (?, ?, ?)",
                        id, number, value));
                ctx.items().track(ItemRef.of(TestTrade.ITEM, id), number).signal(ImportSignal.IMPORTED);
            }
            return JobResult.of(ImportOutcome.SUCCESS);
        }
    }

    static class TradeValidation implements ItemProcessor<TestTrade, UUID> {

        private final Transactions tx;

        TradeValidation(Transactions tx) {
            this.tx = tx;
        }

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            boolean valid = tx.withConnection(c -> Sql.queryOne(c, "SELECT isin FROM test_trade WHERE id = ?",
                    rs -> rs.getString(1) != null, item.ref().id())).orElse(false);
            item.signal(valid ? ValidationSignal.VALID : ValidationSignal.MISSING_ISIN);
        }
    }

    /** New in version 2. */
    static class ManualRepair implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.signal(RepairSignal.REPAIRED, "repaired manually");
        }
    }

    /** Version 1: trades without ISIN go back to the import. */
    static class TradeFlowV1 implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(TradeImport.class)
                    .on(ImportSignal.IMPORTED).to(TradeValidation.class)
                    .on(ImportSignal.REIMPORTED).to(TradeValidation.class);
            flow.step(TradeValidation.class)
                    .on(ValidationSignal.VALID).complete()
                    .on(ValidationSignal.MISSING_ISIN).back(TradeImport.class);
        }

        @Override
        public String name() {
            return FLOW;
        }
    }

    /** Version 2: trades without ISIN are repaired manually. */
    static class TradeFlowV2 implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(TradeImport.class)
                    .on(ImportSignal.IMPORTED).to(TradeValidation.class)
                    .on(ImportSignal.REIMPORTED).to(TradeValidation.class);
            flow.step(TradeValidation.class)
                    .on(ValidationSignal.VALID).complete()
                    .on(ValidationSignal.MISSING_ISIN).to(ManualRepair.class);
            flow.step(ManualRepair.class)
                    .on(RepairSignal.REPAIRED).complete();
        }

        @Override
        public String name() {
            return FLOW;
        }
    }

    @Test
    void versionsAreActivatedPinnedOverruledAndMigrated() {
        try (TestCluster cluster = new TestCluster()) {
            Transactions tx = cluster.tx();
            cluster.migrate();
            cluster.execute(TestTrade.TABLE);
            cluster.definitions().define(TradeImport.class).external().maxItemVisits(5).save();

            // blue: version 1 is active (first registration)
            DtpEngine blue = cluster.start("blue", List.of(new TradeFlowV1()), new TradeImport(tx),
                    new TradeValidation(tx));
            String v1 = flow(blue).activeVersion();
            assertThat(v1).isNotNull();
            importTrades(cluster, blue, "AT0000000001", "");
            assertThat(state(cluster, "T-2")).isEqualTo("QUEUED");
            assertThat(types(blue, "T-2")).containsExactly("TRACKED", "ROUTED", "ROUTED_BACK");

            // green: version 2 is deployed and activated, blue can't run its new step
            DtpEngine green = cluster.start("green", List.of(new TradeFlowV2()), new TradeImport(tx),
                    new TradeValidation(tx), new ManualRepair());
            String v2 = flow(green).activeVersion();
            assertThat(v2).isNotEqualTo(v1);
            assertThat(flow(blue).versions()).extracting(VersionView::hash).containsExactlyInAnyOrder(v1, v2);

            // T-2 stays on version 1 (back to the import again), T-3 takes the route of version 2
            importTrades(cluster, blue, "");
            assertThat(types(blue, "T-2")).containsExactly("TRACKED", "ROUTED", "ROUTED_BACK", "ROUTED",
                    "ROUTED_BACK");
            assertThat(state(cluster, "T-3")).isEqualTo("COMPLETED");
            assertThat(types(blue, "T-3")).containsExactly("TRACKED", "ROUTED", "ROUTED", "COMPLETED");
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE business_key = 'T-2'"
                    + " AND flow_version_id = (SELECT id FROM dtp_flow_version WHERE hash = ?)", v1)).isEqualTo(1);

            // switch back to version 1: restarts and new nodes with version 2 don't activate it again
            blue.flowAdmin().activate(FLOW, v1, null, "ops", "manual repair team is on holiday");
            assertThat(flow(blue).activeVersion()).isEqualTo(v1);
            DtpEngine green2 = cluster.start("green-2", List.of(new TradeFlowV2()), new TradeImport(tx),
                    new TradeValidation(tx), new ManualRepair());
            assertThat(flow(blue).activeVersion()).isEqualTo(v1);
            assertThat(blue.flowQueries().activations(FLOW))
                    .extracting(ActivationView::version, ActivationView::generated)
                    .containsExactly(tuple(v1, false), tuple(v2, true), tuple(v1, true));

            // override: trades without ISIN are discarded while the ISIN feed is down
            UUID override = blue.flowAdmin().override(new Redirect(FLOW, "trade-validation", "MISSING_ISIN",
                    RouteKind.DISCARD, null), null, null, "ops", "ISIN feed down");
            assertThat(blue.flowQueries().mermaid(FLOW, null)).contains("MISSING_ISIN ⚠", "linkStyle");
            assertThat(blue.flowQueries().overrides(FLOW)).singleElement()
                    .satisfies(o -> assertThat(o.reason()).isEqualTo("ISIN feed down"));
            importTrades(cluster, blue, "");
            assertThat(state(cluster, "T-4")).isEqualTo("DISCARDED");
            assertThat(blue.queries().journey("test-trade", "T-4").getLast().reason()).isEqualTo("MISSING_ISIN");
            // T-2 is re-imported again and discarded as well: overrides apply to all versions
            assertThat(state(cluster, "T-2")).isEqualTo("DISCARDED");
            assertThat(blue.flowAdmin().endOverride(override)).isTrue();
            assertThat(blue.flowQueries().overrides(FLOW)).isEmpty();

            // migration: queued items of version 1 move to version 2 and take its routes
            importTrades(cluster, blue, "");
            assertThat(types(blue, "T-5")).endsWith("ROUTED_BACK");
            blue.flowAdmin().activate(FLOW, v2, null, "ops", "repair team is back");
            Migration migration = blue.flowAdmin().migrateQueued(FLOW, v1, v2, "ops");
            assertThat(migration).isEqualTo(new Migration(1, 0));
            importTrades(cluster, blue);
            assertThat(types(blue, "T-5")).containsSubsequence("ROUTED_BACK", "MIGRATED", "ROUTED", "ROUTED",
                    "COMPLETED");
            assertThat(blue.queries().journey("test-trade", "T-5").getLast().reason()).isNull();

            // no active node can run ManualRepair: version 2 can't be activated
            blue.flowAdmin().activate(FLOW, v1, null, "ops", "back again");
            green.stop();
            green2.stop();
            assertThatThrownBy(() -> blue.flowAdmin().activate(FLOW, v2, null, "ops", "try"))
                    .isInstanceOf(DtpException.class)
                    .hasMessageContaining("no active node can run")
                    .hasMessageContaining(ManualRepair.class.getName());
            assertThat(blue.flowQueries().validate(FLOW, v2)).anySatisfy(issue ->
                    assertThat(issue.message()).startsWith("no active node can run manual-repair"));
        }
    }

    private static void importTrades(TestCluster cluster, DtpEngine node, String... isins) {
        node.jobEngine().trigger(TradeImport.class, new ImportParams(List.of(isins)));
        cluster.awaitIdle(Duration.ofSeconds(20));
    }

    private static FlowView flow(DtpEngine node) {
        return node.flowQueries().flows().stream().filter(f -> f.name().equals(FLOW)).findFirst().orElseThrow();
    }

    private static String state(TestCluster cluster, String number) {
        return cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT state FROM dtp_item WHERE business_key = ?",
                rs -> rs.getString(1), number)).orElseThrow();
    }

    private static List<String> types(DtpEngine engine, String businessKey) {
        return engine.queries().journey("test-trade", businessKey).stream().map(JourneyEntry::type).toList();
    }
}
