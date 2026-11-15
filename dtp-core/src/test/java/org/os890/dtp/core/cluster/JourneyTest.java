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
import static org.assertj.core.api.Assertions.tuple;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.Approval;
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
import org.os890.dtp.core.engine.Decisions.Vote;
import org.os890.dtp.core.query.JourneyGraph;
import org.os890.dtp.core.query.JourneyGraph.Edge;
import org.os890.dtp.core.query.JourneyGraph.EdgeKind;
import org.os890.dtp.core.query.JourneyGraph.Node;
import org.os890.dtp.core.query.JourneyGraph.NodeKind;
import org.os890.dtp.core.query.JourneyGraph.Style;
import org.os890.dtp.core.query.JourneyQueries;
import org.os890.dtp.core.query.JourneyQueries.Detail;
import org.os890.dtp.core.query.JourneyQueries.Options;
import org.os890.dtp.core.spi.Transactions;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Journey diagrams per item (slice 12, docs/design/09-monitoring-heatmap.md §9.9): a fork with a flaky branch that
 * succeeds on its third attempt, an approval with a quorum of two, a back-route that repeats the fork, and an item that
 * isn't finished yet.
 */
class JourneyTest {

    private static final Decider RITA = Decider.of("rita", "risk");
    private static final Decider ROB = Decider.of("rob", "risk");
    private static final Options FULL = new Options(Detail.FULL, false, ZoneOffset.UTC);

    enum IntakeSignal implements Signal {
        ACCEPTED
    }

    enum PriceSignal implements Signal {
        PRICED
    }

    enum ScreeningSignal implements Signal {
        CLEAR
    }

    enum BookingSignal implements Signal {
        BOOKED, RECHECK
    }

    /** A timeout of the pricing service. */
    static class PricingTimeoutException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        PricingTimeoutException(String message) {
            super(message);
        }
    }

    static class Intake implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.signal(IntakeSignal.ACCEPTED);
        }
    }

    /** The pricing service times out twice for flaky trades (one item per batch: no rolled-back attempts). */
    @ItemProcessing(batchSize = 1)
    static class Pricing implements ItemProcessor<TestTrade, UUID> {

        private final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            if (item.businessKey().contains("FLAKY")
                    && calls.computeIfAbsent(item.businessKey(), k -> new AtomicInteger()).incrementAndGet() <= 2) {
                throw new PricingTimeoutException("read timed out after 2000 ms");
            }
            item.signal(PriceSignal.PRICED, "price 101.5");
        }
    }

    /** Large trades need two approvals of the risk desk. */
    static class Screening implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            if (item.approval().isPresent()) {
                item.signal(ScreeningSignal.CLEAR, "approved");
            } else if (item.businessKey().contains("BIG")) {
                item.requestApproval(Approval.quorum(2).assignedTo(() -> "risk"), "large trade");
            } else {
                item.signal(ScreeningSignal.CLEAR);
            }
        }
    }

    /** Sends a looping trade back once; keeps an open trade waiting for its confirmation. */
    static class Booking implements ItemProcessor<TestTrade, UUID> {

        private final Map<String, AtomicInteger> visits = new ConcurrentHashMap<>();

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            int visit = visits.computeIfAbsent(item.businessKey(), k -> new AtomicInteger()).incrementAndGet();
            if (item.businessKey().contains("LOOP") && visit == 1) {
                item.signal(BookingSignal.RECHECK, "price moved");
            } else if (item.businessKey().contains("OPEN")) {
                item.defer(Duration.ofHours(1), "waiting for the confirmation");
            } else {
                item.signal(BookingSignal.BOOKED);
            }
        }
    }

    static class JourneyFlow implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(Intake.class)
                    .on(IntakeSignal.ACCEPTED).fork(Pricing.class).and(Screening.class).joinAt(Booking.class);
            flow.step(Booking.class)
                    .on(BookingSignal.BOOKED).complete()
                    .on(BookingSignal.RECHECK).back(Intake.class);
        }
    }

    @Test
    void journeysFoldIntoDiagrams() {
        try (TestCluster cluster = new TestCluster()) {
            Transactions tx = cluster.tx();
            cluster.migrate();
            cluster.definitions().define(Pricing.class).inbox(1).retry(3, Duration.ofMillis(50)).save();
            Pricing pricing = new Pricing();
            Booking booking = new Booking();
            DtpEngine a = cluster.start("a", List.of(new JourneyFlow()), new Intake(), pricing, new Screening(),
                    booking);
            cluster.start("b", List.of(new JourneyFlow()), new Intake(), pricing, new Screening(), booking);
            tx.required(() -> {
                for (String key : List.of("T-FLAKY", "T-BIG", "T-LOOP", "T-OPEN")) {
                    a.fire(FlowEvent.start(JourneyFlow.class, ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), key));
                }
                return null;
            });
            cluster.awaitIdle(Duration.ofSeconds(30));
            JourneyQueries journeys = a.journeys();

            // the flaky branch: two failed attempts as a dashed self-loop, succeeded on attempt 3
            JourneyGraph flaky = journeys.graph("test-trade", "T-FLAKY", FULL).orElseThrow();
            assertThat(flaky.state()).isEqualTo("COMPLETED");
            assertThat(flaky.nodes()).extracting(Node::kind).containsExactly(NodeKind.START, NodeKind.STEP,
                    NodeKind.STEP, NodeKind.STEP, NodeKind.STEP, NodeKind.END);
            Node pricingNode = step(flaky, "Pricing");
            assertThat(pricingNode.style()).isEqualTo(Style.RETRIED);
            assertThat(pricingNode.lines()).contains("succeeded on attempt 3");
            assertThat(edges(flaky, EdgeKind.RETRY)).singleElement().satisfies(loop -> {
                assertThat(loop.from()).isEqualTo(pricingNode.id()).isEqualTo(loop.to());
                assertThat(loop.entries()).hasSize(2);
                assertThat(loop.label()).endsWith(" retry 2×\ntechnical: JourneyTest.PricingTimeoutException");
            });
            assertThat(edges(flaky, EdgeKind.FORK)).hasSize(2)
                    .allSatisfy(e -> assertThat(e.label()).isEqualTo("3 fork"));
            assertThat(edges(flaky, EdgeKind.BRANCH)).anySatisfy(e -> assertThat(e.label()).endsWith(" join"));
            assertThat(step(flaky, "Screening").lines()).containsExactly("CLEAR");
            assertThat(flaky.nodes().getLast().style()).isEqualTo(Style.DONE);
            String mermaid = flaky.mermaid();
            assertThat(mermaid).startsWith("flowchart LR\n")
                    .contains("[\"JourneyTest.Pricing<br/>PRICED<br/>succeeded on attempt 3\"]",
                            " retry 2×<br/>technical: JourneyTest.PricingTimeoutException\" .-> " + pricingNode.id(),
                            "classDef retried", "classDef done", "((\"COMPLETED<br/>");
            assertThat(mermaid).doesNotContain("read timed out", "<br/>on ");
            // operators see the messages and the nodes
            JourneyGraph technical = journeys.graph("test-trade", "T-FLAKY",
                    new Options(Detail.FULL, true, ZoneOffset.UTC)).orElseThrow();
            assertThat(edges(technical, EdgeKind.RETRY).getFirst().label()).contains("read timed out after 2000 ms");
            assertThat(step(technical, "Pricing").lines().getFirst()).matches("PRICED, on [ab](, [ab])?");

            // the approval: waiting for the second vote, the decision node is where the trade is now
            Decisions decisions = a.decisions();
            DecisionView approval = decisions.open(RITA).stream().filter(d -> "T-BIG".equals(d.businessKey()))
                                             .findFirst().orElseThrow();
            Node waiting = decision(journeys.graph("test-trade", "T-BIG", FULL).orElseThrow());
            assertThat(waiting.title()).isEqualTo("Approval D-" + approval.id().toString().substring(0, 8)
                    + ", quorum 2");
            assertThat(waiting.lines()).containsExactly("team risk", "large trade", "waiting: 0 of 2 approvals");
            assertThat(waiting.style()).isEqualTo(Style.CURRENT);
            assertThat(waiting.decisionId()).isEqualTo(approval.id());
            decisions.vote(approval.id(), Vote.APPROVE, RITA, "fine");
            assertThat(decision(journeys.graph("test-trade", "T-BIG", FULL).orElseThrow()).lines())
                    .contains("waiting: 1 of 2 approvals").anyMatch(l -> l.startsWith("APPROVE by rita: fine, "));
            decisions.vote(approval.id(), Vote.APPROVE, ROB, null);
            cluster.awaitIdle(Duration.ofSeconds(30));
            JourneyGraph big = journeys.graph("test-trade", "T-BIG", FULL).orElseThrow();
            Node decided = decision(big);
            assertThat(decided.style()).isEqualTo(Style.DECISION);
            assertThat(decided.lines()).anyMatch(l -> l.startsWith("APPROVE by rob, "))
                    .anyMatch(l -> l.startsWith("decided APPROVED by rob, "));
            Node screening = step(big, "Screening");
            assertThat(screening.visits()).isEqualTo(2);
            assertThat(screening.lines()).containsExactly("CLEAR, 2 visits");
            assertThat(edges(big, EdgeKind.DECISION)).extracting(Edge::from, Edge::to)
                    .containsExactly(tuple(screening.id(), decided.id()), tuple(decided.id(), screening.id()));
            assertThat(edges(big, EdgeKind.DECISION).getLast().label()).endsWith(" APPROVED");
            assertThat(big.state()).isEqualTo("COMPLETED");
            assertThat(big.mermaid()).contains("{{\"Approval D-", "classDef decision");
            // steps only: no decision node
            JourneyGraph steps = journeys.graph("test-trade", "T-BIG", new Options(Detail.STEPS, false,
                    ZoneOffset.UTC)).orElseThrow();
            assertThat(steps.nodes()).extracting(Node::kind).doesNotContain(NodeKind.DECISION);
            assertThat(step(steps, "Screening").visits()).isEqualTo(2);

            // the back-route: dashed, with its reason; the fork repeats on the same edges
            JourneyGraph loop = journeys.graph("test-trade", "T-LOOP", FULL).orElseThrow();
            Node intake = step(loop, "Intake");
            assertThat(intake.visits()).isEqualTo(2);
            assertThat(edges(loop, EdgeKind.BACK)).singleElement().satisfies(back -> {
                assertThat(back.to()).isEqualTo(intake.id());
                assertThat(back.label()).endsWith(" back-route: price moved");
            });
            assertThat(edges(loop, EdgeKind.FORK)).hasSize(2).allSatisfy(e -> assertThat(e.entries()).hasSize(2));
            assertThat(loop.mermaid()).contains("-. \"", "back-route: price moved\" .-> " + intake.id());

            // an unfinished journey ends at the current step, with the reason why it waits
            JourneyGraph open = journeys.graph("test-trade", "T-OPEN", FULL).orElseThrow();
            assertThat(open.state()).isEqualTo("QUEUED");
            Node current = step(open, "Booking");
            assertThat(current.style()).isEqualTo(Style.CURRENT);
            assertThat(current.lines()).contains("deferred once").last().asString().startsWith("next attempt ");
            assertThat(open.nodes()).extracting(Node::kind).doesNotContain(NodeKind.END);
            assertThat(open.mermaid()).contains("classDef current", "class " + current.id() + " current");

            // by id, and unknown items
            assertThat(journeys.graph(open.itemId(), FULL)).contains(open);
            assertThat(journeys.graph("test-trade", "T-NONE", FULL)).isEmpty();
            assertThat(journeys.diagram("test-trade", "T-LOOP", FULL)).contains(loop.mermaid());
            assertThat(a.queries().journey(open.itemId())).extracting(e -> e.no()).startsWith(1, 2, 3);
        }
    }

    private static Node step(JourneyGraph graph, String title) {
        return graph.nodes().stream().filter(n -> n.kind() == NodeKind.STEP
                    && ("JourneyTest." + title).equals(n.title())).findFirst()
                    .orElseThrow(() -> new AssertionError("no step " + title + " in " + graph.nodes()));
    }

    private static Node decision(JourneyGraph graph) {
        return graph.nodes().stream().filter(n -> n.kind() == NodeKind.DECISION).findFirst().orElseThrow();
    }

    private static List<Edge> edges(JourneyGraph graph, EdgeKind kind) {
        return graph.edges().stream().filter(e -> e.kind() == kind).toList();
    }
}
