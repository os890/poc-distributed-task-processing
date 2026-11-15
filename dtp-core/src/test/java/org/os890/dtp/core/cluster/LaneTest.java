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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.FlowBuilder;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.FlowEvent;
import org.os890.dtp.api.ItemLane;
import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.Signal;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.query.FlowQueries.ActivationView;
import org.os890.dtp.core.query.FlowQueries.FlowView;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Lanes (slice 15, DD-23): inboxes share their batches between lanes by weight, branches keep the lane of their
 * item, and a flow version can be active for one lane only (a canary).
 */
class LaneTest {

    private static final String FLOW = "canary-flow";

    enum Tier implements ItemLane {
        VIP(10, 3), STANDARD(0, 1);

        private final int priority;
        private final int weight;

        Tier(int priority, int weight) {
            this.priority = priority;
            this.weight = weight;
        }

        @Override
        public int priority() {
            return priority;
        }

        @Override
        public int weight() {
            return weight;
        }
    }

    enum CheckSignal implements Signal {
        CHECKED
    }

    enum AuditSignal implements Signal {
        AUDITED
    }

    enum SplitSignal implements Signal {
        SPLIT
    }

    enum PartSignal implements Signal {
        DONE
    }

    /** Remembers the lanes in the order it processed the items, one item per batch. */
    @ItemProcessing(batchSize = 1)
    static class Booking implements ItemProcessor<TestTrade, UUID> {

        final List<String> lanes = new CopyOnWriteArrayList<>();

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            lanes.add(item.lane());
            item.complete();
        }
    }

    /** Four workers with batches of five: the run hands out the batches by weight as well. */
    @ItemProcessing(batchSize = 5, parallelism = 4)
    static class ParallelBooking implements ItemProcessor<TestTrade, UUID> {

        final List<String> lanes = new CopyOnWriteArrayList<>();

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            lanes.add(item.lane());
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            item.complete();
        }
    }

    static class Check implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.signal(CheckSignal.CHECKED);
        }
    }

    /** New in version 2 of the flow. */
    static class Audit implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.signal(AuditSignal.AUDITED);
        }
    }

    static class CanaryFlowV1 implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(Check.class).on(CheckSignal.CHECKED).complete();
        }

        @Override
        public String name() {
            return FLOW;
        }
    }

    static class CanaryFlowV2 implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(Check.class).on(CheckSignal.CHECKED).to(Audit.class);
            flow.step(Audit.class).on(AuditSignal.AUDITED).complete();
        }

        @Override
        public String name() {
            return FLOW;
        }
    }

    static class Split implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.signal(SplitSignal.SPLIT);
        }
    }

    static class PartA implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.signal(PartSignal.DONE);
        }
    }

    static class PartB extends PartA {
    }

    static class Merge implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.complete();
        }
    }

    static class SplitFlow implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(Split.class).on(SplitSignal.SPLIT).fork(PartA.class).and(PartB.class).joinAt(Merge.class);
        }
    }

    @Test
    void lanesShareTheInboxByWeight() {
        try (TestCluster cluster = new TestCluster()) {
            Booking booking = new Booking();
            DtpEngine a = cluster.start("a", booking);
            cluster.definitions().define(Booking.class).inbox(1).save();
            // 20 VIP and 20 standard trades, queued at once
            cluster.tx().required(() -> {
                for (int n = 1; n <= 20; n++) {
                    track(a, "T-V" + n, Tier.VIP);
                    track(a, "T-S" + n, Tier.STANDARD);
                }
                return null;
            });
            cluster.awaitIdle(Duration.ofSeconds(30));

            // 3 of 4 batches VIP while both lanes have trades, VIP first; then standard alone
            assertThat(booking.lanes).hasSize(40);
            assertThat(booking.lanes.getFirst()).isEqualTo("VIP");
            assertThat(booking.lanes.subList(0, 20)).filteredOn("VIP"::equals).hasSize(15);
            assertThat(booking.lanes.subList(0, 8)).containsExactly("VIP", "VIP", "STANDARD", "VIP", "VIP", "VIP",
                    "STANDARD", "VIP");
            assertThat(booking.lanes.subList(30, 40)).containsOnly("STANDARD");
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE lane = 'VIP' AND lane_weight = 3"))
                    .isEqualTo(20);
        }
    }

    @Test
    void parallelWorkersShareTheInboxByWeightToo() {
        try (TestCluster cluster = new TestCluster()) {
            ParallelBooking booking = new ParallelBooking();
            DtpEngine a = cluster.start("a", booking);
            cluster.definitions().define(ParallelBooking.class).inbox(1).save();
            cluster.tx().required(() -> {
                for (int n = 1; n <= 40; n++) {
                    a.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-V" + n, Tier.VIP)
                     .routeTo(ParallelBooking.class);
                    a.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-S" + n, Tier.STANDARD)
                     .routeTo(ParallelBooking.class);
                }
                return null;
            });
            cluster.awaitIdle(Duration.ofSeconds(30));
            // each worker locks without the items the others are processing: no worker ends early on a locked row
            assertThat(booking.lanes).hasSize(80);
            assertThat(booking.lanes.subList(0, 40)).filteredOn("VIP"::equals).hasSizeBetween(26, 34);
        }
    }

    @Test
    void branchesKeepTheLaneOfTheirItem() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            DtpEngine a = cluster.start("a", List.of(new SplitFlow()), new Split(), new PartA(), new PartB(),
                    new Merge());
            cluster.tx().required(() -> {
                a.fire(FlowEvent.start(SplitFlow.class, ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-1",
                        Tier.VIP));
                return null;
            });
            cluster.awaitIdle(Duration.ofSeconds(30));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item_branch WHERE lane = 'VIP' AND lane_weight = 3"
                    + " AND lane_priority = 10 AND state = 'FINISHED'")).isEqualTo(2);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(1);
        }
    }

    @Test
    void aVersionCanBeActiveForOneLaneOnly() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            DtpEngine blue = cluster.start("blue", List.of(new CanaryFlowV1()), new Check());
            String v1 = flow(blue).activeVersion();
            // activated by an operator: a newly deployed version is only a candidate
            blue.flowAdmin().activate(FLOW, v1, null, "ops", "release 1");
            DtpEngine green = cluster.start("green", List.of(new CanaryFlowV2()), new Check(), new Audit());
            String v2 = flow(green).versions().stream().map(v -> v.hash()).filter(h -> !h.equals(v1)).findFirst()
                                   .orElseThrow();
            assertThat(flow(green).activeVersion()).isEqualTo(v1);

            // the canary: version 2 for VIP trades only
            green.flowAdmin().activateForLane(FLOW, "VIP", v2, null, "ops", "canary for VIP");
            start(cluster, green, "T-V1", Tier.VIP);
            start(cluster, green, "T-S1", Tier.STANDARD);
            cluster.awaitIdle(Duration.ofSeconds(30));
            assertThat(steps(green, "T-V1")).containsExactly("Check", "Audit");
            assertThat(steps(green, "T-S1")).containsExactly("Check");
            assertThat(version(cluster, "T-V1")).isEqualTo(v2);
            assertThat(version(cluster, "T-S1")).isEqualTo(v1);
            FlowView flow = flow(green);
            assertThat(flow.activeVersion()).isEqualTo(v1);
            assertThat(flow.laneVersions()).containsExactly(Map.entry("VIP", v2));
            assertThat(green.flowQueries().activations(FLOW).getFirst()).extracting(ActivationView::lane,
                    ActivationView::version).containsExactly("VIP", v2);

            // the canary ends: VIP trades follow version 1 again
            green.flowAdmin().endLaneActivation(FLOW, "VIP", "ops", "canary done");
            start(cluster, green, "T-V2", Tier.VIP);
            cluster.awaitIdle(Duration.ofSeconds(30));
            assertThat(steps(green, "T-V2")).containsExactly("Check");
            assertThat(flow(green).laneVersions()).isEmpty();
            assertThat(green.flowQueries().activations(FLOW).getFirst()).extracting(ActivationView::lane,
                    ActivationView::version).containsExactly("VIP", null);

            // no node can run the audit any more: version 2 can't be activated for a lane
            green.stop();
            assertThatThrownBy(() -> blue.flowAdmin().activateForLane(FLOW, "VIP", v2, null, "ops", "again"))
                    .isInstanceOf(DtpException.class).hasMessageContaining("can't be activated");
        }
    }

    private static void track(DtpEngine node, String key, Tier lane) {
        node.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), key, lane).routeTo(Booking.class);
    }

    private static void start(TestCluster cluster, DtpEngine node, String key, Tier lane) {
        cluster.tx().required(() -> {
            node.fire(FlowEvent.start(CanaryFlowV2.class, ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), key, lane));
            return null;
        });
    }

    private static FlowView flow(DtpEngine node) {
        return node.flowQueries().flows().stream().filter(f -> f.name().equals(FLOW)).findFirst().orElseThrow();
    }

    private static List<String> steps(DtpEngine node, String key) {
        return node.queries().journey(TestTrade.ITEM.name(), key).stream()
                   .filter(e -> e.type().equals("ROUTED")).map(JourneyEntry::toJob)
                   .map(job -> job.substring(0, 1).toUpperCase() + job.substring(1)).toList();
    }

    private static String version(TestCluster cluster, String key) {
        return cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT v.hash FROM dtp_item i"
                + " JOIN dtp_flow_version v ON v.id = i.flow_version_id WHERE i.business_key = ?",
                rs -> rs.getString(1), key)).orElseThrow();
    }
}
