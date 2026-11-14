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
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.Approval;
import org.os890.dtp.api.ApprovalResult;
import org.os890.dtp.api.DecisionOption;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.Responsibility;
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.domain.TakeoverPolicy;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.engine.Decisions.Decider;
import org.os890.dtp.core.engine.Decisions.DecisionView;
import org.os890.dtp.core.engine.Decisions.Vote;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.spi.Transactions;
import org.os890.dtp.core.testsupport.TestCluster;

/** Decision requests and approvals: requested by steps, decided by users, the result goes back to the step. */
class DecisionTest {

    private static final Decider ALICE = Decider.of("alice", "data");
    private static final Decider RISK_1 = Decider.of("rita", "risk");
    private static final Decider RISK_2 = Decider.of("rob", "risk");
    private static final Decider BOB = Decider.of("bob");
    private static final Decider OPS = Decider.of("otto", Decisions.OPERATORS);

    enum Team implements Responsibility {
        DATA, RISK;

        @Override
        public String group() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    enum ReviewOption implements DecisionOption {
        ACCEPT, POSTPONE;

        @Override
        public String label() {
            return name().charAt(0) + name().substring(1).toLowerCase(Locale.ROOT);
        }
    }

    /** Asks for decisions depending on the trade, and routes by the result when the trade comes back. */
    static class TradeReview implements ItemProcessor<TestTrade, UUID> {

        private final Transactions tx;
        private final Map<String, ApprovalResult> approvals = new ConcurrentHashMap<>();
        private final AtomicBoolean marketDataDown = new AtomicBoolean(true);

        TradeReview(Transactions tx) {
            this.tx = tx;
        }

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            var decision = item.decision(ReviewOption.class);
            var approval = item.approval();
            if (decision.isPresent()) {
                if (decision.get() == ReviewOption.ACCEPT) {
                    item.complete();
                } else {
                    item.defer(Duration.ofHours(1), "postponed");
                }
                return;
            }
            if (approval.isPresent()) {
                approvals.put(item.businessKey(), approval.get());
                if (approval.get().approved()) {
                    item.complete();
                } else {
                    item.discard("rejected by " + approval.get().decidedBy());
                }
                return;
            }
            String isin = tx.withConnection(c -> Sql.queryOne(c, "SELECT isin FROM test_trade WHERE id = ?",
                    rs -> rs.getString(1), item.ref().id())).orElse(null);
            if (isin == null) {
                item.requestDecision("missing ISIN", ReviewOption.class, Team.DATA);
            } else if (isin.startsWith("XX")) {
                item.requestApproval(Approval.quorum(2).assignedTo(Team.RISK), "exotic instrument " + isin);
            } else if (isin.startsWith("ESC")) {
                item.escalate("unknown market " + isin);
            } else if (isin.startsWith("ERR") && marketDataDown.get()) {
                throw new IllegalStateException("market data service down");
            } else {
                item.complete();
            }
        }
    }

    @Test
    void stepsRequestDecisionsAndApprovalsAndReadTheResults() {
        try (TestCluster cluster = new TestCluster()) {
            Transactions tx = cluster.tx();
            cluster.migrate();
            cluster.execute(TestTrade.TABLE);
            cluster.definitions().define(TradeReview.class).inbox(1).retry(0, Duration.ofMillis(100)).save();
            TradeReview review = new TradeReview(tx);
            DtpEngine a = cluster.start("a", review);
            cluster.start("b", review);

            track(cluster, a, Map.of("T-1", "", "T-2", "XX0000000002", "T-3", "XX0000000003", "T-4", "ESC000000004",
                    "T-5", "ERR000000005", "T-6", "AT0000000006"));
            cluster.awaitIdle(Duration.ofSeconds(20));
            Decisions decisions = a.decisions();

            // each user sees the requests of the teams they are member of, plus those without team
            assertThat(decisions.open(ALICE)).extracting(DecisionView::businessKey)
                    .containsExactlyInAnyOrder("T-1", "T-4");
            assertThat(decisions.open(RISK_1)).extracting(DecisionView::businessKey)
                    .containsExactlyInAnyOrder("T-2", "T-3", "T-4");
            assertThat(decisions.open(OPS)).extracting(DecisionView::businessKey)
                    .containsExactlyInAnyOrder("T-4", "T-5");
            DecisionView missingIsin = request(decisions, ALICE, "T-1");
            assertThat(missingIsin.options()).extracting(Decisions.OptionView::name)
                    .containsExactly("ACCEPT", "POSTPONE", "DISCARD");
            assertThat(missingIsin.options().getFirst().label()).isEqualTo("Accept");
            assertThat(missingIsin.job()).isEqualTo("TradeReview");
            assertThat(request(decisions, OPS, "T-5").reason())
                    .isEqualTo("retries exhausted (0): market data service down");

            // a custom option: the trade comes back to the step, which reads the choice
            assertThatThrownBy(() -> decisions.decide(missingIsin.id(), "ACCEPT", BOB, null))
                    .isInstanceOf(DtpException.class).hasMessageContaining("assigned to data");
            assertThatThrownBy(() -> decisions.decide(missingIsin.id(), "MAYBE", ALICE, null))
                    .isInstanceOf(DtpException.class).hasMessageContaining("has no option MAYBE");
            assertThatThrownBy(() -> decisions.vote(missingIsin.id(), Vote.APPROVE, ALICE, null))
                    .isInstanceOf(DtpException.class).hasMessageContaining("decide instead");
            decisions.decide(missingIsin.id(), "ACCEPT", ALICE, "ISIN follows tomorrow");
            assertThatThrownBy(() -> decisions.decide(missingIsin.id(), "ACCEPT", ALICE, null))
                    .isInstanceOf(DtpException.class).hasMessageContaining("decided already");

            // approvals: two distinct approvers, one rejection rejects
            DecisionView exotic = request(decisions, RISK_1, "T-2");
            assertThat(exotic.requiredApprovals()).isEqualTo(2);
            assertThat(decisions.vote(exotic.id(), Vote.APPROVE, RISK_1, "ok")).isFalse();
            assertThatThrownBy(() -> decisions.vote(exotic.id(), Vote.APPROVE, RISK_1, null))
                    .isInstanceOf(DtpException.class).hasMessageContaining("voted already");
            assertThat(decisions.get(exotic.id()).votes()).singleElement()
                    .satisfies(v -> assertThat(v.voter()).isEqualTo("rita"));
            assertThat(decisions.vote(exotic.id(), Vote.APPROVE, RISK_2, "fine")).isTrue();
            DecisionView other = request(decisions, RISK_1, "T-3");
            assertThat(decisions.vote(other.id(), Vote.REJECT, RISK_2, "not our business")).isTrue();

            // built-in options: DISCARD ends the journey directly, RETRY puts the trade back into the inbox
            DecisionView escalated = request(decisions, BOB, "T-4");
            assertThat(escalated.options()).extracting(Decisions.OptionView::name).containsExactly("RETRY", "DISCARD");
            decisions.decide(escalated.id(), "DISCARD", BOB, "market closed");
            review.marketDataDown.set(false);
            decisions.decide(request(decisions, OPS, "T-5").id(), "RETRY", OPS, "service is back");
            cluster.awaitIdle(Duration.ofSeconds(20));

            assertThat(state(cluster, "T-1")).isEqualTo("COMPLETED");
            assertThat(types(a, "T-1")).containsExactly("TRACKED", "ROUTED", "ESCALATED", "DECIDED", "COMPLETED");
            assertThat(a.queries().journey("test-trade", "T-1")).anySatisfy(e -> assertThat(e.reason())
                    .isEqualTo("ACCEPT by alice: ISIN follows tomorrow"));
            assertThat(state(cluster, "T-2")).isEqualTo("COMPLETED");
            assertThat(review.approvals.get("T-2")).satisfies(r -> {
                assertThat(r.approved()).isTrue();
                assertThat(r.approvers()).containsExactly("rita", "rob");
                assertThat(r.decidedBy()).isEqualTo("rob");
            });
            assertThat(types(a, "T-2")).containsExactly("TRACKED", "ROUTED", "ESCALATED", "VOTED", "VOTED", "DECIDED",
                    "COMPLETED");
            assertThat(state(cluster, "T-3")).isEqualTo("DISCARDED");
            assertThat(a.queries().journey("test-trade", "T-3").getLast().reason()).isEqualTo("rejected by rob");
            assertThat(state(cluster, "T-4")).isEqualTo("DISCARDED");
            assertThat(types(a, "T-4")).containsExactly("TRACKED", "ROUTED", "ESCALATED", "DECIDED", "DISCARDED");
            assertThat(state(cluster, "T-5")).isEqualTo("COMPLETED");
            assertThat(state(cluster, "T-6")).isEqualTo("COMPLETED");
            assertThat(decisions.open(Decider.of("all", "data", "risk", Decisions.OPERATORS))).isEmpty();
            assertThat(decisions.ofSubject(itemId(cluster, "T-1"))).singleElement()
                    .satisfies(d -> assertThat(d.decidedBy()).isEqualTo("alice"));
            assertThat(a.queries().runs(20)).anySatisfy(run -> assertThat(run.outcome())
                    .isEqualTo("COMPLETED_WITH_ISSUES"));
        }
    }

    static class Settlement implements JobHandler<NoParams, DefaultOutcome> {

        private final AtomicInteger executions = new AtomicInteger();
        private final CountDownLatch firstStarted = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            if (executions.incrementAndGet() == 1) {
                firstStarted.countDown();
                try {
                    release.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return JobResult.success();
        }
    }

    @Test
    void anAbandonedRunWaitsForAnOperator() throws InterruptedException {
        try (TestCluster cluster = new TestCluster()) {
            Settlement job = new Settlement();
            cluster.migrate();
            cluster.definitions().define(Settlement.class).external().takeover(TakeoverPolicy.ABANDON).save();
            DtpEngine a = cluster.start("a", job);
            DtpEngine b = cluster.start("b", job);

            RunHandle first = a.jobEngine().trigger(Settlement.class, new NoParams());
            assertThat(job.firstStarted.await(10, TimeUnit.SECONDS)).isTrue();
            UUID runId = UUID.fromString(first.runId());
            UUID owner = cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT owner_node_id FROM dtp_run"
                    + " WHERE id = ?", rs -> Sql.uuid(rs, "owner_node_id"), runId)).orElseThrow();
            DtpEngine crashed = List.of(a, b).stream().filter(n -> n.nodeId().equals(owner)).findFirst().orElseThrow();
            DtpEngine survivor = crashed == a ? b : a;
            crashed.kill();

            await().atMost(Duration.ofSeconds(15)).until(() -> !survivor.decisions().open(OPS).isEmpty());
            DecisionView abandoned = survivor.decisions().open(OPS).getFirst();
            assertThat(abandoned.subjectType()).isEqualTo("RUN");
            assertThat(abandoned.subjectId()).isEqualTo(runId);
            assertThat(abandoned.options()).extracting(Decisions.OptionView::name)
                    .containsExactly("RETRY", "FAIL", "SKIP");
            assertThat(survivor.decisions().open(BOB)).isEmpty();
            assertThat(state(cluster, runId)).isEqualTo("WAITING_FOR_DECISION");

            survivor.decisions().decide(abandoned.id(), "RETRY", OPS, "checked the bank statement");
            await().atMost(Duration.ofSeconds(15)).until(() -> "FINISHED".equals(state(cluster, runId)));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE id = ? AND outcome = 'SUCCEEDED'"
                    + " AND attempt = 2", runId)).isEqualTo(1);
            job.release.countDown();

            assertThatThrownBy(() -> survivor.decisions().decide(abandoned.id(), "SKIP", OPS, null))
                    .isInstanceOf(DtpException.class);
        }
    }

    /** Polls an external system: routes the trade into its own inbox again until an operator resets the loop. */
    static class StatusPolling implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            if (item.arrivalReason().orElse("").startsWith("RESET_VISITS")) {
                item.complete();
            } else {
                item.routeTo(StatusPolling.class, "still pending");
            }
        }
    }

    @Test
    void anItemOverItsLoopLimitWaitsForADecision() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            cluster.execute(TestTrade.TABLE);
            cluster.definitions().define(StatusPolling.class).inbox(1).maxItemVisits(3).save();
            DtpEngine node = cluster.start("a", new StatusPolling());
            Transactions tx = cluster.tx();
            tx.required(() -> {
                UUID id = UUID.randomUUID();
                tx.withConnection(c -> Sql.update(c, "INSERT INTO test_trade (id, number) VALUES (?, 'T-1')", id));
                node.items().track(ItemRef.of(TestTrade.ITEM, id), "T-1").routeTo(StatusPolling.class);
                return null;
            });
            await().atMost(Duration.ofSeconds(15)).until(() -> !node.decisions().open(BOB).isEmpty());

            DecisionView loop = node.decisions().open(BOB).getFirst();
            assertThat(loop.reason()).isEqualTo("visit 4 of StatusPolling exceeds max_item_visits 3");
            assertThat(loop.options()).extracting(Decisions.OptionView::name)
                    .containsExactly("RESET_VISITS", "RETRY", "DISCARD");
            node.decisions().decide(loop.id(), "RESET_VISITS", BOB, null);
            cluster.awaitIdle(Duration.ofSeconds(15));

            assertThat(state(cluster, "T-1")).isEqualTo("COMPLETED");
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item_visit")).isZero();
        }
    }

    private static void track(TestCluster cluster, DtpEngine node, Map<String, String> trades) {
        Transactions tx = cluster.tx();
        tx.required(() -> {
            trades.forEach((number, isin) -> {
                UUID id = UUID.randomUUID();
                tx.withConnection(c -> Sql.update(c, "INSERT INTO test_trade (id, number, isin) VALUES (?, ?, ?)",
                        id, number, isin.isEmpty() ? null : isin));
                node.items().track(ItemRef.of(TestTrade.ITEM, id), number).routeTo(TradeReview.class);
            });
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

    private static String state(TestCluster cluster, UUID runId) {
        return cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT state FROM dtp_run WHERE id = ?",
                rs -> rs.getString(1), runId)).orElseThrow();
    }

    private static UUID itemId(TestCluster cluster, String businessKey) {
        return cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT id FROM dtp_item WHERE business_key = ?",
                rs -> Sql.uuid(rs, "id"), businessKey)).orElseThrow();
    }

    private static List<String> types(DtpEngine engine, String businessKey) {
        // without REPROCESSED: the technical failure of T-5 rolls back the batch it shares with other trades
        return engine.queries().journey("test-trade", businessKey).stream().map(JourneyEntry::type)
                     .filter(type -> !"REPROCESSED".equals(type)).toList();
    }
}
