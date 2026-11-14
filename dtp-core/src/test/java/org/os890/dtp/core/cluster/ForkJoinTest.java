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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.BranchResult;
import org.os890.dtp.api.DecisionOption;
import org.os890.dtp.api.FlowBuilder;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.FlowEvent;
import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.Signal;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.engine.Decisions.Decider;
import org.os890.dtp.core.engine.Decisions.DecisionView;
import org.os890.dtp.core.engine.FlowRegistrar;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.query.DtpQueries.ItemCount;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.query.FlowQueries.Issue;
import org.os890.dtp.core.spi.Transactions;
import org.os890.dtp.core.testsupport.MapStepRegistry;
import org.os890.dtp.core.testsupport.TestCluster;

/** Fork/join on item level (DD-22): parallel branches on several nodes, the last one moves the item to the join. */
class ForkJoinTest {

    private static final Decider ALICE = Decider.of("alice");
    private static final Decider OPS = Decider.of("otto", Decisions.OPERATORS);

    enum IntakeSignal implements Signal {
        ACCEPTED
    }

    enum PriceSignal implements Signal {
        PRICED
    }

    enum LimitSignal implements Signal {
        WITHIN_LIMIT, ABOVE_LIMIT
    }

    enum ComplianceSignal implements Signal {
        CLEAR
    }

    enum ComplianceOption implements DecisionOption {
        ALLOW
    }

    enum BookingSignal implements Signal {
        BOOKED
    }

    /** Reads the ISIN of a trade, the steps decide by its prefix. */
    abstract static class TradeStep implements ItemProcessor<TestTrade, UUID> {

        private final Transactions tx;
        private volatile long pauseMillis;

        TradeStep(Transactions tx) {
            this.tx = tx;
        }

        /** Makes the step slower than the poll interval, so the other nodes claim the other branch steps. */
        TradeStep slow() {
            pauseMillis = 3;
            return this;
        }

        String isin(TrackedItem<TestTrade, UUID> item) {
            if (pauseMillis > 0) {
                try {
                    Thread.sleep(pauseMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return tx.withConnection(c -> Sql.queryOne(c, "SELECT isin FROM test_trade WHERE id = ?",
                    rs -> rs.getString(1), item.ref().id())).orElseThrow();
        }
    }

    static class Intake extends TradeStep {

        Intake(Transactions tx) {
            super(tx);
        }

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.signal(IntakeSignal.ACCEPTED);
        }
    }

    @ItemProcessing(batchSize = 3)
    static class PriceCheck extends TradeStep {

        PriceCheck(Transactions tx) {
            super(tx);
        }

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            String isin = isin(item);
            if (isin.startsWith("DEF")) {
                item.defer(Duration.ofHours(1), "no price yet");
            } else if (isin.startsWith("BAD")) {
                item.complete();     // not allowed in a branch
            } else {
                item.signal(PriceSignal.PRICED, "price of " + isin);
            }
        }
    }

    @ItemProcessing(batchSize = 4, parallelism = 2)
    static class LimitCheck extends TradeStep {

        private final AtomicBoolean limitServiceDown = new AtomicBoolean(true);

        LimitCheck(Transactions tx) {
            super(tx);
        }

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            String isin = isin(item);
            if (isin.startsWith("ERR") && limitServiceDown.get()) {
                throw new IllegalStateException("limit service down");
            }
            item.signal(isin.startsWith("BIG") ? LimitSignal.ABOVE_LIMIT : LimitSignal.WITHIN_LIMIT);
        }
    }

    static class ComplianceCheck extends TradeStep {

        ComplianceCheck(Transactions tx) {
            super(tx);
        }

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            if (item.decision(ComplianceOption.class).isPresent()) {
                item.signal(ComplianceSignal.CLEAR, "allowed by compliance");
            } else if (isin(item).contains("RST")) {
                item.requestDecision("restricted instrument", ComplianceOption.class);
            } else {
                item.signal(ComplianceSignal.CLEAR);
            }
        }
    }

    /** The join step: reads the results of the branches. */
    static class Booking implements ItemProcessor<TestTrade, UUID> {

        private final Map<String, AtomicInteger> bookings = new ConcurrentHashMap<>();
        private final Map<String, List<BranchResult>> results = new ConcurrentHashMap<>();
        private final Map<String, LimitSignal> limits = new ConcurrentHashMap<>();

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            bookings.computeIfAbsent(item.businessKey(), k -> new AtomicInteger()).incrementAndGet();
            results.put(item.businessKey(), item.branchResults());
            limits.put(item.businessKey(), item.branchResult(LimitCheck.class, LimitSignal.class).orElseThrow());
            assertThat(item.isBranch()).isFalse();
            item.signal(BookingSignal.BOOKED);
        }
    }

    static class TradeFlow implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(Intake.class)
                    .on(IntakeSignal.ACCEPTED).fork(PriceCheck.class).and(LimitCheck.class).and(ComplianceCheck.class)
                    .joinAt(Booking.class);
            flow.step(Booking.class)
                    .on(BookingSignal.BOOKED).complete();
        }
    }

    @Test
    void branchesRunInParallelOnSeveralNodesAndTheLastOneJoins() {
        try (TestCluster cluster = new TestCluster()) {
            Transactions tx = cluster.tx();
            cluster.migrate();
            cluster.execute(TestTrade.TABLE);
            Booking booking = new Booking();
            DtpEngine a = null;
            for (String node : List.of("a", "b", "c")) {
                // one run per node at a time: the steps spread over the nodes
                DtpEngine engine = cluster.start(TestCluster.config(node).maxConcurrentRuns(1).build(),
                        new MapStepRegistry(new Intake(tx), new PriceCheck(tx).slow(), new LimitCheck(tx).slow(),
                                new ComplianceCheck(tx).slow(), booking).flows(new TradeFlow()));
                a = a == null ? engine : a;
            }
            int trades = 40;
            IntFunction<String> isin = n -> (n % 5 == 0 ? "BIG" : "AT0") + "00000000" + n;
            start(cluster, a, trades, isin);
            cluster.awaitIdle(Duration.ofSeconds(30));

            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(trades);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item_branch WHERE state = 'FINISHED'"))
                    .isEqualTo(3 * trades);
            // the last branch joins, exactly once per trade
            assertThat(booking.bookings).hasSize(trades).allSatisfy((k, v) -> assertThat(v).hasValue(1));
            assertThat(types(a, "T-1")).containsExactly("TRACKED", "ROUTED", "FORKED", "BRANCH_FINISHED",
                    "BRANCH_FINISHED", "BRANCH_FINISHED", "JOINED", "COMPLETED");
            assertThat(a.queries().journey("test-trade", "T-1").get(2).reason())
                    .isEqualTo("fork into price-check, limit-check, compliance-check, join at booking (ACCEPTED)");
            // the results in the order of the fork definition, typed by the branch step
            assertThat(booking.results.get("T-1")).extracting(BranchResult::signal)
                    .containsExactly("PRICED", "WITHIN_LIMIT", "CLEAR");
            assertThat(booking.results.get("T-1").getFirst().reason()).isEqualTo("price of " + isin.apply(1));
            assertThat(booking.limits.get("T-5")).isEqualTo(LimitSignal.ABOVE_LIMIT);
            assertThat(booking.limits.get("T-6")).isEqualTo(LimitSignal.WITHIN_LIMIT);
            // the branches ran on several nodes
            Set<String> nodes = IntStream.rangeClosed(1, trades).boxed()
                    .flatMap(n -> cluster.queries().journey("test-trade", "T-" + n).stream())
                    .filter(e -> "BRANCH_FINISHED".equals(e.type())).map(JourneyEntry::node)
                    .collect(Collectors.toSet());
            assertThat(nodes).hasSizeGreaterThan(1);

            String diagram = a.flowQueries().mermaid("trade-flow", null);
            assertThat(diagram).contains("{{\"fork\"}}", "{{\"join\"}}", "-->|ACCEPTED| fork_");
            assertThat(a.flowQueries().validate()).extracting(Issue::message)
                    .noneMatch(m -> m.contains("unreachable"));
        }
    }

    @Test
    void branchesFailDeferAndAskForDecisionsOnTheirOwn() {
        try (TestCluster cluster = new TestCluster()) {
            Transactions tx = cluster.tx();
            cluster.migrate();
            cluster.execute(TestTrade.TABLE);
            cluster.definitions().define(LimitCheck.class).inbox(1).retry(0, Duration.ofMillis(100)).save();
            cluster.definitions().define(PriceCheck.class).inbox(1).retry(0, Duration.ofMillis(100)).save();
            Booking booking = new Booking();
            LimitCheck limitCheck = new LimitCheck(tx);
            DtpEngine a = cluster.start("a", List.of(new TradeFlow()), new Intake(tx), new PriceCheck(tx),
                    limitCheck, new ComplianceCheck(tx), booking);
            cluster.start("b", List.of(new TradeFlow()), new Intake(tx), new PriceCheck(tx), limitCheck,
                    new ComplianceCheck(tx), booking);
            Map<Integer, String> isins = Map.of(1, "ERR000000001", 2, "AT0-RST-0002", 3, "DEF-RST-0003",
                    4, "BAD000000004");
            start(cluster, a, isins.size(), isins::get);
            cluster.awaitIdle(Duration.ofSeconds(30));
            Decisions decisions = a.decisions();

            // each branch waits on its own, the other branches finish
            assertThat(decisions.open(OPS)).extracting(DecisionView::businessKey)
                    .containsExactlyInAnyOrder("T-1", "T-2", "T-3", "T-4");
            assertThat(decisions.open(ALICE)).extracting(DecisionView::businessKey)
                    .containsExactlyInAnyOrder("T-2", "T-3");
            assertThat(request(decisions, OPS, "T-1").reason()).isEqualTo("retries exhausted (0): limit service down");
            assertThat(request(decisions, OPS, "T-4").reason()).contains("is a branch of a fork: complete isn't"
                    + " allowed");
            assertThat(cluster.queries().itemCounts()).contains(new ItemCount("compliance-check",
                    "BRANCH_WAITING_FOR_DECISION", 2), new ItemCount("PriceCheck", "BRANCH_QUEUED", 1));
            assertThat(state(cluster, "T-1")).isEqualTo("FORKED");

            // RETRY: the branch goes back into the inbox of its step
            limitCheck.limitServiceDown.set(false);
            decisions.decide(request(decisions, OPS, "T-1").id(), "RETRY", OPS, "service is back");
            // a custom option: the branch step reads it and finishes the branch
            decisions.decide(request(decisions, ALICE, "T-2").id(), "ALLOW", ALICE, "known client");
            // DISCARD: the item ends, its open branches are cancelled
            decisions.decide(request(decisions, ALICE, "T-3").id(), "DISCARD", ALICE, "sanctioned");
            decisions.decide(request(decisions, OPS, "T-4").id(), "DISCARD", OPS, null);
            cluster.awaitIdle(Duration.ofSeconds(30));

            assertThat(state(cluster, "T-1")).isEqualTo("COMPLETED");
            assertThat(types(a, "T-1")).containsSubsequence("FORKED", "FAILED", "ESCALATED", "DECIDED",
                    "BRANCH_FINISHED", "JOINED", "COMPLETED");
            assertThat(a.queries().journey("test-trade", "T-1")).filteredOn(e -> "FAILED".equals(e.type()))
                    .singleElement().satisfies(e -> {
                        assertThat(e.fromJob()).isEqualTo("LimitCheck");
                        assertThat(e.errorType()).isEqualTo(IllegalStateException.class.getName());
                    });
            assertThat(state(cluster, "T-2")).isEqualTo("COMPLETED");
            assertThat(booking.results.get("T-2")).filteredOn(r -> r.isOf(ComplianceCheck.class)).singleElement()
                    .satisfies(r -> {
                        assertThat(r.signal(ComplianceSignal.class)).contains(ComplianceSignal.CLEAR);
                        assertThat(r.signal(PriceSignal.class)).isEmpty();
                        assertThat(r.is(ComplianceSignal.CLEAR)).isTrue();
                        assertThat(r.is(PriceSignal.PRICED)).isFalse();
                        assertThat(r.reason()).isEqualTo("allowed by compliance");
                    });
            assertThat(state(cluster, "T-3")).isEqualTo("DISCARDED");
            assertThat(branchStates(cluster, "T-3")).containsExactly("CANCELLED", "FINISHED", "CANCELLED");
            assertThat(types(a, "T-3")).endsWith("DECIDED", "DISCARDED");
            assertThat(state(cluster, "T-4")).isEqualTo("DISCARDED");
            assertThat(booking.bookings).containsOnlyKeys("T-1", "T-2");
            assertThat(decisions.open(Decider.of("all", Decisions.OPERATORS))).isEmpty();
        }
    }

    @Test
    void aForkNeedsTwoDistinctBranchesAndAJoinOutsideOfThem() {
        FlowDefinition single = flow -> flow.entry(Intake.class)
                .on(IntakeSignal.ACCEPTED).fork(PriceCheck.class).joinAt(Booking.class);
        assertThatThrownBy(() -> FlowRegistrar.compile(Map.of(TradeFlow.class, single)))
                .hasMessageContaining("needs at least two branches");
        FlowDefinition twice = flow -> flow.entry(Intake.class)
                .on(IntakeSignal.ACCEPTED).fork(PriceCheck.class).and(PriceCheck.class).joinAt(Booking.class);
        assertThatThrownBy(() -> FlowRegistrar.compile(Map.of(TradeFlow.class, twice)))
                .hasMessageContaining("is twice in the fork");
        FlowDefinition joinInFork = flow -> flow.entry(Intake.class)
                .on(IntakeSignal.ACCEPTED).fork(PriceCheck.class).and(Booking.class).joinAt(Booking.class);
        assertThatThrownBy(() -> FlowRegistrar.compile(Map.of(TradeFlow.class, joinInFork)))
                .hasMessageContaining("can't be a branch of its fork");
    }

    private static void start(TestCluster cluster, DtpEngine node, int count,
                              IntFunction<String> isin) {
        Transactions tx = cluster.tx();
        tx.required(() -> {
            for (int n = 1; n <= count; n++) {
                UUID id = UUID.randomUUID();
                String number = "T-" + n;
                String value = isin.apply(n);
                tx.withConnection(c -> Sql.update(c, "INSERT INTO test_trade (id, number, isin) VALUES (?, ?, ?)",
                        id, number, value));
                node.fire(FlowEvent.start(TradeFlow.class, ItemRef.of(TestTrade.ITEM, id), number));
            }
            return null;
        });
    }

    private static DecisionView request(Decisions decisions, Decider who, String businessKey) {
        return decisions.open(who).stream().filter(d -> businessKey.equals(d.businessKey())).findFirst()
                        .orElseThrow();
    }

    private static String state(TestCluster cluster, String businessKey) {
        return cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT state FROM dtp_item WHERE business_key = ?",
                rs -> rs.getString(1), businessKey)).orElseThrow();
    }

    private static List<String> branchStates(TestCluster cluster, String businessKey) {
        return cluster.tx().withConnection(c -> Sql.query(c, "SELECT b.state FROM dtp_item_branch b"
                + " JOIN dtp_item i ON i.id = b.item_id WHERE i.business_key = ? ORDER BY b.position",
                rs -> rs.getString(1), businessKey));
    }

    private static List<String> types(DtpEngine engine, String businessKey) {
        return engine.queries().journey("test-trade", businessKey).stream().map(JourneyEntry::type)
                     .filter(type -> !"REPROCESSED".equals(type)).toList();
    }
}
