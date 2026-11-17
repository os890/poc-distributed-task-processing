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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DecisionOption;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.FlowInstance;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.Responsibility;
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.event.DecisionWithdrawn;
import org.os890.dtp.api.event.FlowInstanceCancelled;
import org.os890.dtp.api.event.FlowInstanceCompleted;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.engine.Decisions.Decider;
import org.os890.dtp.core.engine.Decisions.DecisionView;
import org.os890.dtp.core.engine.FlowInstances.Cancelled;
import org.os890.dtp.core.engine.FlowInstances.InstanceView;
import org.os890.dtp.core.engine.FlowInstances.RunView;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * An operator cancels a flow instance (an end of day started by mistake) or a single run: pending and waiting runs
 * are cancelled, running ones stop softly, the open decision requests about them are withdrawn, nothing is planned in
 * the instance any more.
 */
class FlowInstanceCancelTest {

    private static final Responsibility CONTROLLING = () -> "controlling";
    private static final Decider CLARA = Decider.of("clara", "controlling");

    /** Parameters of the root run. */
    public record EodParams(String requestedBy) {
    }

    enum Release implements DecisionOption {
        RELEASE;

        @Override
        public String label() {
            return "Release the report";
        }
    }

    static class CutOff implements JobHandler<EodParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            return JobResult.success();
        }
    }

    /** Works until it is asked to stop. */
    static class Positions implements JobHandler<EodParams, DefaultOutcome> {

        final CountDownLatch started = new CountDownLatch(1);
        final AtomicInteger stopped = new AtomicInteger();

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            started.countDown();
            try {
                while (ctx.shouldContinue()) {
                    Thread.sleep(20);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            stopped.incrementAndGet();
            return JobResult.success();
        }
    }

    /** Asks product control; optionally holds after asking, before its unit of work commits. */
    static class Pnl implements JobHandler<EodParams, DefaultOutcome> {

        final AtomicBoolean holdAfterAsking = new AtomicBoolean();
        final CountDownLatch asked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger decided = new AtomicInteger();

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            if (ctx.decision(Release.class).isPresent()) {
                decided.incrementAndGet();
                return JobResult.success();
            }
            String key = ctx.flowInstance().map(FlowInstance::businessKey).orElse("ad hoc");
            ctx.requestDecision("release the P&L of " + key, Release.class, CONTROLLING);
            asked.countDown();
            if (holdAfterAsking.get()) {
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("not released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return JobResult.success();
        }
    }

    /** No node has its handler: its runs stay pending. */
    static class Archive implements JobHandler<EodParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            return JobResult.success();
        }
    }

    /** Planned when the positions were cancelled, but not in a cancelled instance. */
    static class Cleanup implements JobHandler<EodParams, DefaultOutcome> {

        final AtomicInteger runs = new AtomicInteger();

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            runs.incrementAndGet();
            return JobResult.success();
        }
    }

    /** The join target of positions and P&L. */
    static class SignOff implements JobHandler<EodParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            return JobResult.success();
        }
    }

    /** A flow of its own that completes at once. */
    static class Solo implements JobHandler<EodParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            return JobResult.success();
        }
    }

    private static void define(TestCluster cluster) {
        cluster.definitions().define(Archive.class).save();
        cluster.definitions().define(Cleanup.class).save();
        cluster.definitions().define(Positions.class).on(RunOutcome.CANCELLED, Cleanup.class).save();
        cluster.definitions().define(Pnl.class).external().save();
        cluster.definitions().define(SignOff.class)
               .joinOf(Positions.class, RunOutcome.SUCCEEDED).joinOf(Pnl.class, RunOutcome.SUCCEEDED).save();
        cluster.definitions().define(CutOff.class).external().on(RunOutcome.SUCCEEDED, Positions.class)
               .on(RunOutcome.SUCCEEDED, Pnl.class).on(RunOutcome.SUCCEEDED, Archive.class).save();
        cluster.definitions().define(Solo.class).external().save();
    }

    private static RunView run(InstanceView day, String job) {
        return day.runs().stream().filter(r -> job.equals(r.job())).findFirst().orElseThrow();
    }

    @Test
    void cancellingAnInstanceCancelsItsRunsWithdrawsItsDecisionsAndPlansNothingMore() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            define(cluster);
            Positions positions = new Positions();
            Pnl pnl = new Pnl();
            Cleanup cleanup = new Cleanup();
            DtpEngine a = cluster.start("a", new CutOff(), positions, pnl, cleanup, new SignOff(), new Solo());
            cluster.start("b", new CutOff(), positions, pnl, cleanup, new SignOff(), new Solo());

            a.jobEngine().startFlowInstance(CutOff.class, new EodParams("ops"), "2026-11-18");
            await().atMost(Duration.ofSeconds(20)).until(() -> positions.started.getCount() == 0
                    && !a.decisions().open(CLARA).isEmpty());
            await().atMost(Duration.ofSeconds(10)).until(() -> "WAITING_FOR_DECISION".equals(
                    run(a.flowInstances().find("CutOff", "2026-11-18").orElseThrow(), "Pnl").state()));
            InstanceView day = a.flowInstances().find("CutOff", "2026-11-18").orElseThrow();
            assertThat(run(day, "Positions").state()).isIn("RUNNING", "STOPPING");
            assertThat(run(day, "Archive").state()).isEqualTo("PENDING");
            DecisionView request = a.decisions().open(CLARA).getFirst();

            assertThatThrownBy(() -> a.flowInstances().cancel(day.id(), "otto", " "))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> a.flowInstances().cancel(UUID.randomUUID(), "otto", "wrong date"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown flow instance");
            Cancelled cancelled = a.flowInstances().cancel(day.id(), "otto", "started for the wrong date");
            assertThat(cancelled).isEqualTo(new Cancelled(3, 1));

            // the running positions stop softly and end as cancelled: their transition isn't planned any more
            await().atMost(Duration.ofSeconds(20)).until(() -> a.flowInstances().get(day.id()).runs().stream()
                    .allMatch(r -> "FINISHED".equals(r.state())));
            InstanceView gone = a.flowInstances().get(day.id());
            assertThat(gone.state()).isEqualTo("CANCELLED");
            assertThat(gone.completedAt()).isNull();
            assertThat(gone.cancel().by()).isEqualTo("otto");
            assertThat(gone.cancel().reason()).isEqualTo("started for the wrong date");
            assertThat(gone.cancel().at()).isNotNull();
            assertThat(gone.runs()).extracting(RunView::job)
                    .containsExactlyInAnyOrder("CutOff", "Positions", "Pnl", "Archive");
            assertThat(gone.runs()).filteredOn(r -> !"CutOff".equals(r.job()))
                    .allSatisfy(r -> assertThat(r.outcome()).isEqualTo("CANCELLED"));
            assertThat(positions.stopped).hasValue(1);
            assertThat(gone.joins()).allSatisfy(j -> assertThat(j.fired()).isFalse());
            assertThat(cleanup.runs).hasValue(0);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run r JOIN dtp_job j ON j.id = r.job_id"
                    + " WHERE j.name = 'Cleanup'")).isZero();

            // the request of the P&L was withdrawn: nobody can decide it any more
            assertThat(a.decisions().open(CLARA)).isEmpty();
            DecisionView withdrawn = a.decisions().get(request.id());
            assertThat(withdrawn.state()).isEqualTo("WITHDRAWN");
            assertThat(withdrawn.decidedBy()).isEqualTo("otto");
            assertThat(withdrawn.comment()).isEqualTo("flow instance cancelled: started for the wrong date");
            assertThat(cluster.events(DecisionWithdrawn.class)).singleElement().satisfies(e -> {
                assertThat(e.request().id()).isEqualTo(request.id());
                assertThat(e.request().subjectType()).isEqualTo("RUN");
                assertThat(e.by()).isEqualTo("otto");
                assertThat(e.reason()).isEqualTo("flow instance cancelled: started for the wrong date");
            });
            assertThatThrownBy(() -> a.decisions().decide(request.id(), "RELEASE", CLARA, null))
                    .isInstanceOf(DtpException.class).hasMessageContaining("withdrawn");
            assertThat(pnl.decided).hasValue(0);

            // a cancelled instance stays cancelled: no second cancel, no rerun, starting it finds it
            assertThatThrownBy(() -> a.flowInstances().cancel(day.id(), "otto", "again"))
                    .isInstanceOf(DtpException.class).hasMessageContaining("is cancelled already");
            assertThatThrownBy(() -> a.flowInstances().rerun(run(gone, "Pnl").id(), "otto"))
                    .isInstanceOf(DtpException.class).hasMessageContaining("is cancelled already");
            RunHandle again = a.jobEngine().startFlowInstance(CutOff.class, new EodParams("ops"), "2026-11-18");
            assertThat(again.newlyCreated()).isFalse();
            assertThat(a.flowInstances().get(day.id()).runs()).hasSize(4);
            assertThat(cluster.events(FlowInstanceCancelled.class)).singleElement().satisfies(e -> {
                assertThat(e.instance().businessKey()).isEqualTo("2026-11-18");
                assertThat(e.by()).isEqualTo("otto");
                assertThat(e.reason()).isEqualTo("started for the wrong date");
            });

            // a completed instance can't be cancelled
            a.jobEngine().startFlowInstance(Solo.class, new EodParams("ops"), "2026-11-18");
            await().atMost(Duration.ofSeconds(20)).until(() -> "COMPLETED".equals(
                    a.flowInstances().find("Solo", "2026-11-18").orElseThrow().state()));
            UUID solo = a.flowInstances().find("Solo", "2026-11-18").orElseThrow().id();
            assertThatThrownBy(() -> a.flowInstances().cancel(solo, "otto", "too late"))
                    .isInstanceOf(DtpException.class).hasMessageContaining("is completed already");
            assertThat(cluster.events(FlowInstanceCompleted.class)).singleElement()
                    .satisfies(e -> assertThat(e.instance().name()).isEqualTo("Solo"));
        }
    }

    @Test
    void aRunThatWaitsForADecisionCanBeCancelled() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            define(cluster);
            Pnl pnl = new Pnl();
            DtpEngine a = cluster.start("a", pnl);

            UUID runId = UUID.fromString(a.jobEngine().trigger(Pnl.class, new EodParams("ops")).runId());
            await().atMost(Duration.ofSeconds(20)).until(() -> "WAITING_FOR_DECISION".equals(
                    cluster.queries().run(runId).orElseThrow().state()));
            DecisionView request = a.decisions().open(CLARA).getFirst();

            assertThat(a.jobAdmin().cancel(runId, "otto")).isTrue();
            assertThat(cluster.queries().run(runId).orElseThrow().outcome()).isEqualTo("CANCELLED");
            DecisionView withdrawn = a.decisions().get(request.id());
            assertThat(withdrawn.state()).isEqualTo("WITHDRAWN");
            assertThat(withdrawn.decidedBy()).isEqualTo("otto");
            assertThat(withdrawn.comment()).isEqualTo("run cancelled");
            assertThat(cluster.events(DecisionWithdrawn.class)).singleElement().satisfies(e -> {
                assertThat(e.request().id()).isEqualTo(request.id());
                assertThat(e.by()).isEqualTo("otto");
                assertThat(e.reason()).isEqualTo("run cancelled");
            });
            assertThat(a.decisions().open(CLARA)).isEmpty();
            assertThat(a.jobAdmin().cancel(runId, "otto")).isFalse();
            cluster.awaitIdle(Duration.ofSeconds(10));
            assertThat(pnl.decided).hasValue(0);
        }
    }

    @Test
    void aRunCancelledWhileItAsksDoesntWaitAndItsRequestIsWithdrawn() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            define(cluster);
            Pnl pnl = new Pnl();
            pnl.holdAfterAsking.set(true);
            DtpEngine a = cluster.start("a", pnl);

            UUID runId = UUID.fromString(a.jobEngine().trigger(Pnl.class, new EodParams("ops")).runId());
            await().atMost(Duration.ofSeconds(20)).until(() -> pnl.asked.getCount() == 0);
            // the request isn't committed yet: the cancel asks the run to stop
            assertThat(a.jobAdmin().cancel(runId, "otto")).isTrue();
            assertThat(cluster.queries().run(runId).orElseThrow().state()).isEqualTo("STOPPING");
            pnl.release.countDown();

            await().atMost(Duration.ofSeconds(20)).until(() -> "FINISHED".equals(
                    cluster.queries().run(runId).orElseThrow().state()));
            assertThat(cluster.queries().run(runId).orElseThrow().outcome()).isEqualTo("CANCELLED");
            assertThat(a.decisions().open(CLARA)).isEmpty();
            assertThat(a.decisions().ofSubject(runId)).singleElement().satisfies(r -> {
                assertThat(r.state()).isEqualTo("WITHDRAWN");
                assertThat(r.comment()).isEqualTo("run cancelled");
            });
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run_attempt WHERE run_id = ?"
                    + " AND end_reason = 'DECISION_REQUESTED'", runId)).isZero();
        }
    }
}
