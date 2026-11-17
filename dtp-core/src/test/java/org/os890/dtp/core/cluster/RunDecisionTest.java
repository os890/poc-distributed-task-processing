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
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.Approval;
import org.os890.dtp.api.ApprovalResult;
import org.os890.dtp.api.DecisionOption;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.FlowInstance;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.Responsibility;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.event.DecisionMade;
import org.os890.dtp.api.event.DecisionRequested;
import org.os890.dtp.api.event.RunFinished;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.engine.Decisions.Decider;
import org.os890.dtp.core.engine.Decisions.DecisionView;
import org.os890.dtp.core.engine.Decisions.OptionView;
import org.os890.dtp.core.engine.Decisions.Vote;
import org.os890.dtp.core.engine.FlowInstances.InstanceView;
import org.os890.dtp.core.engine.FlowInstances.RunView;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Job runs that ask a person ({@code JobContext#requestDecision}, {@code #requestApproval}): the run waits for the
 * decision, then its handler is executed again and reads it. An end of day per business date: the cut-off, then the
 * sign-off asks product control, then operations are informed.
 */
class RunDecisionTest {

    private static final Responsibility CONTROLLING = () -> "controlling";
    private static final Decider CLARA = Decider.of("clara", "controlling");
    private static final Decider CARL = Decider.of("carl", "controlling");
    private static final Decider OTTO = Decider.of("otto", Decisions.OPERATORS);

    /** Parameters of the root run. */
    public record EodParams(String requestedBy) {
    }

    enum SignOffChoice implements DecisionOption {
        RELEASE, CORRECT;

        @Override
        public String label() {
            return this == RELEASE ? "Release the reports" : "Correct the reports first";
        }
    }

    /** How the sign-off asks: a choice, a four-eyes approval or a request followed by an exception. */
    enum Mode {
        CHOICE, APPROVAL, BROKEN
    }

    static class CutOff implements JobHandler<EodParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            return JobResult.success();
        }
    }

    /** Asks for its decision on the first execution, acts on it on the next one. */
    static class SignOff implements JobHandler<EodParams, DefaultOutcome> {

        final Map<String, AtomicInteger> executions = new ConcurrentHashMap<>();
        final Queue<SignOffChoice> choices = new ConcurrentLinkedQueue<>();
        final Queue<ApprovalResult> approvals = new ConcurrentLinkedQueue<>();
        private final AtomicReference<Mode> mode = new AtomicReference<>(Mode.CHOICE);

        void mode(Mode value) {
            mode.set(value);
        }

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            String day = ctx.flowInstance().map(FlowInstance::businessKey).orElseThrow();
            executions.computeIfAbsent(day, d -> new AtomicInteger()).incrementAndGet();
            var choice = ctx.decision(SignOffChoice.class);
            var approval = ctx.approval();
            if (choice.isPresent()) {
                choices.add(choice.get());
                if (choice.get() == SignOffChoice.CORRECT) {
                    // asks again: the reports were corrected in the meantime
                    ctx.requestDecision("sign off " + day + " after the correction", SignOffChoice.class,
                            CONTROLLING);
                }
                return JobResult.success();
            }
            if (approval.isPresent()) {
                approvals.add(approval.get());
                if (!approval.get().approved()) {
                    throw new IllegalStateException("sign-off of " + day + " rejected by "
                            + approval.get().decidedBy());
                }
                return JobResult.success();
            }
            switch (mode.get()) {
                case APPROVAL -> ctx.requestApproval(Approval.quorum(2).assignedTo(CONTROLLING), "sign off " + day);
                case BROKEN -> {
                    ctx.requestDecision("sign off " + day, SignOffChoice.class, CONTROLLING);
                    throw new IllegalStateException("reports missing for " + day);
                }
                default -> ctx.requestDecision("sign off " + day, SignOffChoice.class, CONTROLLING);
            }
            return JobResult.success();
        }
    }

    /** Runs after the sign-off succeeded. */
    static class Notify implements JobHandler<EodParams, DefaultOutcome> {

        final Queue<String> sent = new ConcurrentLinkedQueue<>();

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            sent.add(ctx.flowInstance().map(FlowInstance::businessKey).orElseThrow());
            return JobResult.success();
        }
    }

    private static void define(TestCluster cluster) {
        cluster.definitions().define(Notify.class).save();
        cluster.definitions().define(SignOff.class).on(RunOutcome.SUCCEEDED, Notify.class).save();
        cluster.definitions().define(CutOff.class).external().on(RunOutcome.SUCCEEDED, SignOff.class).save();
    }

    private static InstanceView day(DtpEngine node, String day) {
        return node.flowInstances().find("CutOff", day).orElseThrow();
    }

    private static RunView signOffRun(InstanceView day) {
        return day.runs().stream().filter(r -> "SignOff".equals(r.job())).findFirst().orElseThrow();
    }

    @Test
    void aRunWaitsForTheChoiceOfAPersonAndContinuesWithIt() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            define(cluster);
            SignOff signOff = new SignOff();
            Notify notify = new Notify();
            DtpEngine a = cluster.start("a", new CutOff(), signOff, notify);
            cluster.start("b", new CutOff(), signOff, notify);

            a.jobEngine().startFlowInstance(CutOff.class, new EodParams("ops"), "2026-11-18");
            await().atMost(Duration.ofSeconds(20)).until(() -> !a.decisions().open(CLARA).isEmpty());
            cluster.awaitIdle(Duration.ofSeconds(10));

            // the sign-off waits: no transition, the instance stays open
            InstanceView day = day(a, "2026-11-18");
            assertThat(day.state()).isEqualTo("OPEN");
            assertThat(signOffRun(day).state()).isEqualTo("WAITING_FOR_DECISION");
            assertThat(signOffRun(day).outcome()).isNull();
            assertThat(notify.sent).isEmpty();
            DecisionView request = a.decisions().open(CLARA).getFirst();
            assertThat(request.subjectType()).isEqualTo("RUN");
            assertThat(request.subjectId()).isEqualTo(signOffRun(day).id());
            assertThat(request.businessKey()).isEqualTo("CutOff 2026-11-18");
            assertThat(request.job()).isEqualTo("SignOff");
            assertThat(request.options()).extracting(OptionView::name).containsExactly("RELEASE", "CORRECT", "FAIL");
            assertThat(a.decisions().open(OTTO)).isEmpty();
            assertThatThrownBy(() -> a.decisions().decide(request.id(), "RELEASE", OTTO, null))
                    .isInstanceOf(DtpException.class).hasMessageContaining("controlling");
            assertThat(cluster.events(DecisionRequested.class)).hasSize(1);

            // CORRECT: the run is executed again and asks again, still the same run
            a.decisions().decide(request.id(), "CORRECT", CLARA, "P&L of desk 7 is off");
            await().atMost(Duration.ofSeconds(20)).until(() -> a.decisions().open(CLARA).size() == 1
                    && !a.decisions().open(CLARA).getFirst().id().equals(request.id()));
            cluster.awaitIdle(Duration.ofSeconds(10));
            assertThat(signOffRun(day(a, "2026-11-18")).state()).isEqualTo("WAITING_FOR_DECISION");
            assertThat(notify.sent).isEmpty();

            // RELEASE: the run finishes, the transition follows, the instance completes
            a.decisions().decide(a.decisions().open(CLARA).getFirst().id(), "RELEASE", CARL, null);
            await().atMost(Duration.ofSeconds(20)).until(() -> "COMPLETED".equals(day(a, "2026-11-18").state()));

            assertThat(signOff.choices).containsExactly(SignOffChoice.CORRECT, SignOffChoice.RELEASE);
            assertThat(signOff.executions.get("2026-11-18")).hasValue(3);
            assertThat(notify.sent).containsExactly("2026-11-18");
            InstanceView completed = day(a, "2026-11-18");
            assertThat(completed.runs()).filteredOn(r -> "SignOff".equals(r.job())).singleElement()
                    .satisfies(r -> assertThat(r.outcome()).isEqualTo("SUCCEEDED"));
            assertThat(cluster.events(DecisionMade.class)).extracting(DecisionMade::option)
                    .containsExactly("CORRECT", "RELEASE");
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run_attempt WHERE run_id = ?"
                    + " AND end_reason = 'DECISION_REQUESTED'", signOffRun(completed).id())).isEqualTo(2);
        }
    }

    @Test
    void aFourEyesApprovalOfARunAndARejectionThatKeepsTheInstanceOpen() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            define(cluster);
            SignOff signOff = new SignOff();
            signOff.mode(Mode.APPROVAL);
            Notify notify = new Notify();
            DtpEngine a = cluster.start("a", new CutOff(), signOff, notify);
            cluster.start("b", new CutOff(), signOff, notify);

            a.jobEngine().startFlowInstance(CutOff.class, new EodParams("ops"), "2026-11-19");
            a.jobEngine().startFlowInstance(CutOff.class, new EodParams("ops"), "2026-11-20");
            await().atMost(Duration.ofSeconds(20)).until(() -> a.decisions().open(CLARA).size() == 2);
            List<DecisionView> open = a.decisions().open(CLARA);
            DecisionView first = open.stream().filter(r -> r.businessKey().endsWith("2026-11-19")).findFirst()
                                     .orElseThrow();
            DecisionView second = open.stream().filter(r -> r.businessKey().endsWith("2026-11-20")).findFirst()
                                      .orElseThrow();
            assertThat(first.kind()).isEqualTo("APPROVAL");
            assertThat(first.requiredApprovals()).isEqualTo(2);

            // one approval isn't enough, the second one plans the run again
            assertThat(a.decisions().vote(first.id(), Vote.APPROVE, CLARA, null)).isFalse();
            cluster.awaitIdle(Duration.ofSeconds(10));
            assertThat(signOffRun(day(a, "2026-11-19")).state()).isEqualTo("WAITING_FOR_DECISION");
            assertThat(a.decisions().vote(first.id(), Vote.APPROVE, CARL, "checked")).isTrue();
            await().atMost(Duration.ofSeconds(20)).until(() -> "COMPLETED".equals(day(a, "2026-11-19").state()));
            assertThat(signOff.approvals).singleElement().satisfies(r -> {
                assertThat(r.approved()).isTrue();
                assertThat(r.approvers()).containsExactlyInAnyOrder("clara", "carl");
            });
            assertThat(notify.sent).containsExactly("2026-11-19");

            // a rejection: the handler fails the run, the instance stays open for a rerun, which asks again
            a.decisions().vote(second.id(), Vote.REJECT, CARL, "positions incomplete");
            await().atMost(Duration.ofSeconds(20)).until(() -> "FAILED".equals(signOffRun(day(a, "2026-11-20"))
                    .outcome()));
            cluster.awaitIdle(Duration.ofSeconds(10));
            assertThat(day(a, "2026-11-20").state()).isEqualTo("OPEN");
            UUID rerun = a.flowInstances().rerun(signOffRun(day(a, "2026-11-20")).id(), "otto");
            await().atMost(Duration.ofSeconds(20)).until(() -> a.decisions().open(CLARA).size() == 1);
            DecisionView again = a.decisions().open(CLARA).getFirst();
            assertThat(again.subjectId()).isEqualTo(rerun);
            a.decisions().vote(again.id(), Vote.APPROVE, CLARA, null);
            a.decisions().vote(again.id(), Vote.APPROVE, CARL, null);
            await().atMost(Duration.ofSeconds(20)).until(() -> "COMPLETED".equals(day(a, "2026-11-20").state()));
            assertThat(notify.sent).containsExactlyInAnyOrder("2026-11-19", "2026-11-20");
        }
    }

    @Test
    void failFinishesTheWaitingRunAndARequestOfAFailedExecutionIsRolledBack() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            define(cluster);
            SignOff signOff = new SignOff();
            Notify notify = new Notify();
            DtpEngine a = cluster.start("a", new CutOff(), signOff, notify);

            // FAIL: the run finishes as failed without being executed again
            a.jobEngine().startFlowInstance(CutOff.class, new EodParams("ops"), "2026-11-23");
            await().atMost(Duration.ofSeconds(20)).until(() -> !a.decisions().open(CLARA).isEmpty());
            a.decisions().decide(a.decisions().open(CLARA).getFirst().id(), "FAIL", CLARA, "wrong day");
            cluster.awaitIdle(Duration.ofSeconds(10));
            assertThat(signOffRun(day(a, "2026-11-23")).outcome()).isEqualTo("FAILED");
            UUID failed = signOffRun(day(a, "2026-11-23")).id();
            assertThat(cluster.events(RunFinished.class)).filteredOn(e -> e.run().id().equals(failed))
                    .singleElement().satisfies(e -> {
                        assertThat(e.outcome()).isEqualTo(RunOutcome.FAILED);
                        assertThat(e.run().attempt()).isEqualTo(1);
                        assertThat(e.elapsed()).isNull();
                    });
            assertThat(signOff.executions.get("2026-11-23")).hasValue(1);
            assertThat(day(a, "2026-11-23").state()).isEqualTo("OPEN");
            assertThat(notify.sent).isEmpty();

            // the handler threw after its request: no request, the run failed
            signOff.mode(Mode.BROKEN);
            a.jobEngine().startFlowInstance(CutOff.class, new EodParams("ops"), "2026-11-24");
            await().atMost(Duration.ofSeconds(20)).until(() -> day(a, "2026-11-24").runs().size() == 2
                    && signOffRun(day(a, "2026-11-24")).outcome() != null);
            assertThat(signOffRun(day(a, "2026-11-24")).outcome()).isEqualTo("FAILED");
            assertThat(a.decisions().ofSubject(signOffRun(day(a, "2026-11-24")).id())).isEmpty();
            assertThat(a.decisions().open(CLARA)).isEmpty();
        }
    }
}
