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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.Approval;
import org.os890.dtp.api.DecisionRequest;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.api.event.DecisionFallbackDue;
import org.os890.dtp.api.event.DecisionMade;
import org.os890.dtp.api.event.DecisionRequested;
import org.os890.dtp.api.event.VoteCast;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.engine.Decisions.Decider;
import org.os890.dtp.core.engine.Decisions.DecisionView;
import org.os890.dtp.core.engine.Decisions.Vote;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.spi.Transactions;
import org.os890.dtp.core.testsupport.TestCluster;

/** Decision fallbacks (D38) and the domain events of decisions, on two nodes. */
class DecisionFallbackTest {

    private static final Decider RISK_1 = Decider.of("rita", "risk");
    private static final Decider RISK_2 = Decider.of("rob", "risk");
    private static final Duration FALLBACK_DELAY = Duration.ofSeconds(2);

    /** Every trade needs the approval of two risk managers. */
    static class LargeTradeApproval implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            var approval = item.approval();
            if (approval.isEmpty()) {
                item.requestApproval(Approval.quorum(2).assignedTo(() -> "risk"), "large trade");
            } else if (approval.get().approved()) {
                item.complete();
            } else {
                item.discard("rejected");
            }
        }
    }

    /** The fallback: reminds the team, the request stays open. */
    static class ApprovalReminder implements JobHandler<DecisionRequest, DefaultOutcome> {

        private final Map<String, DecisionRequest> reminded = new ConcurrentHashMap<>();
        private final AtomicInteger executions = new AtomicInteger();

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<DecisionRequest> ctx) {
            executions.incrementAndGet();
            reminded.put(ctx.parameters().businessKey(), ctx.parameters());
            return JobResult.success();
        }
    }

    @Test
    void aRequestThatStaysOpenGetsOneFallbackRunAndStaysOpen() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            cluster.execute(TestTrade.TABLE);
            cluster.definitions().define(ApprovalReminder.class).save();
            cluster.definitions().define(LargeTradeApproval.class).inbox(1)
                   .decisionFallback(ApprovalReminder.class, FALLBACK_DELAY).save();
            ApprovalReminder reminder = new ApprovalReminder();
            LargeTradeApproval approval = new LargeTradeApproval();
            DtpEngine a = cluster.start("a", approval, reminder);
            cluster.start("b", approval, reminder);
            Decisions decisions = a.decisions();

            track(cluster, a, "T-1", "T-2", "T-3");
            await().atMost(Duration.ofSeconds(15)).until(() -> decisions.open(RISK_1).size() == 3);
            DecisionView first = request(decisions, "T-1");

            // decided before the fallback is due: no fallback run
            decisions.vote(first.id(), Vote.APPROVE, RISK_1, "ok");
            decisions.vote(first.id(), Vote.APPROVE, RISK_2, null);
            await().atMost(Duration.ofSeconds(15)).until(() -> reminder.reminded.size() == 2);
            cluster.awaitIdle(Duration.ofSeconds(15));

            // one run per open request, with the request as typed parameters; the requests stay open
            assertThat(reminder.reminded).containsOnlyKeys("T-2", "T-3");
            assertThat(reminder.executions).hasValue(2);
            DecisionRequest t2 = reminder.reminded.get("T-2");
            assertThat(t2.kind()).isEqualTo(DecisionRequest.Kind.APPROVAL);
            assertThat(t2.isApproval()).isTrue();
            assertThat(t2.requiredApprovals()).isEqualTo(2);
            assertThat(t2.approvals()).isZero();
            assertThat(t2.responsibleGroup()).isEqualTo("risk");
            assertThat(t2.job()).isEqualTo("LargeTradeApproval");
            assertThat(t2.itemType()).isEqualTo("test-trade");
            assertThat(t2.reason()).isEqualTo("large trade");
            assertThat(decisions.open(RISK_1)).extracting(DecisionView::businessKey)
                    .containsExactlyInAnyOrder("T-2", "T-3");
            UUID t2Request = request(decisions, "T-2").id();
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE run_key = ? AND trigger_type = 'FALLBACK'"
                    + " AND outcome = 'SUCCEEDED'", "fallback:" + t2Request)).isEqualTo(1);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_decision_request WHERE fallback_run_id IS NOT NULL"))
                    .isEqualTo(2);

            // the fallback doesn't decide: the trade still waits for the risk desk
            decisions.vote(t2Request, Vote.APPROVE, RISK_1, null);
            decisions.vote(t2Request, Vote.APPROVE, RISK_2, null);
            decisions.vote(request(decisions, "T-3").id(), Vote.REJECT, RISK_1, "too large");
            cluster.awaitIdle(Duration.ofSeconds(15));
            assertThat(state(cluster, "T-2")).isEqualTo("COMPLETED");
            assertThat(state(cluster, "T-3")).isEqualTo("DISCARDED");
            assertThat(reminder.executions).hasValue(2);

            // the domain events, published after the commits
            assertThat(cluster.events(DecisionRequested.class)).extracting(e -> e.request().businessKey())
                    .containsExactlyInAnyOrder("T-1", "T-2", "T-3");
            assertThat(cluster.events(DecisionRequested.class).getFirst().request().requiredApprovals())
                    .isEqualTo(2);
            List<VoteCast> votes = cluster.events(VoteCast.class).stream()
                    .filter(v -> "T-1".equals(v.request().businessKey())).toList();
            assertThat(votes).extracting(VoteCast::voter).containsExactly("rita", "rob");
            assertThat(votes).extracting(v -> v.request().approvals()).containsExactly(1, 2);
            assertThat(votes.getFirst().comment()).isEqualTo("ok");
            assertThat(cluster.events(DecisionMade.class)).extracting(e -> e.request().businessKey() + " "
                    + e.option() + " by " + e.decidedBy())
                    .containsExactly("T-1 APPROVED by rob", "T-2 APPROVED by rob", "T-3 REJECTED by rita");
            assertThat(cluster.events(DecisionFallbackDue.class)).extracting(e -> e.request().businessKey())
                    .containsExactlyInAnyOrder("T-2", "T-3");
            DecisionFallbackDue due = cluster.events(DecisionFallbackDue.class).stream()
                    .filter(e -> e.request().id().equals(t2Request)).findFirst().orElseThrow();
            assertThat(due.fallbackJob()).isEqualTo("ApprovalReminder");
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE id = ? AND run_key = ?", due.runId(),
                    "fallback:" + t2Request)).isEqualTo(1);
        }
    }

    @Test
    void aRolledBackVotePublishesNothing() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            cluster.execute(TestTrade.TABLE);
            cluster.definitions().define(LargeTradeApproval.class).inbox(1).save();
            DtpEngine a = cluster.start("a", new LargeTradeApproval());
            track(cluster, a, "T-1");
            await().atMost(Duration.ofSeconds(15)).until(() -> a.decisions().open(RISK_1).size() == 1);
            UUID id = request(a.decisions(), "T-1").id();
            Transactions tx = cluster.tx();

            assertThatThrownBy(() -> tx.required(() -> {
                a.decisions().vote(id, Vote.APPROVE, RISK_1, null);
                throw new IllegalStateException("the caller fails");
            })).hasMessage("the caller fails");
            assertThat(cluster.events(VoteCast.class)).isEmpty();
            assertThat(a.decisions().get(id).votes()).isEmpty();

            a.decisions().vote(id, Vote.APPROVE, RISK_1, null);
            assertThat(cluster.events(VoteCast.class)).singleElement()
                    .satisfies(v -> assertThat(v.voter()).isEqualTo("rita"));
            assertThat(cluster.published()).extracting(TestCluster.Published::node).containsOnly("a");
            // without a fallback job, a request has no fallback
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_decision_request WHERE fallback_due_at IS NULL"))
                    .isEqualTo(1);
        }
    }

    private static void track(TestCluster cluster, DtpEngine node, String... numbers) {
        Transactions tx = cluster.tx();
        tx.required(() -> {
            for (String number : numbers) {
                UUID id = UUID.randomUUID();
                tx.withConnection(c -> Sql.update(c, "INSERT INTO test_trade (id, number) VALUES (?, ?)", id, number));
                node.items().track(ItemRef.of(TestTrade.ITEM, id), number).routeTo(LargeTradeApproval.class);
            }
            return null;
        });
    }

    private static DecisionView request(Decisions decisions, String businessKey) {
        return decisions.open(Decider.of("all", "risk")).stream().filter(d -> businessKey.equals(d.businessKey()))
                        .findFirst().orElseThrow();
    }

    private static String state(TestCluster cluster, String businessKey) {
        return cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT state FROM dtp_item WHERE business_key = ?",
                rs -> rs.getString(1), businessKey)).orElseThrow();
    }
}
