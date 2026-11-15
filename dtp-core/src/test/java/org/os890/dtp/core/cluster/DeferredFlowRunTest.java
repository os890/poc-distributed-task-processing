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
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.FlowInstance;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.engine.FlowInstances.InstanceView;
import org.os890.dtp.core.engine.FlowInstances.RunView;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Runs of a flow instance that stop early with work left ({@code DEFERRED}, here at their time limit) are continued in
 * the same instance: a join waits for the run that finishes the work, the instance completes only after it.
 */
class DeferredFlowRunTest {

    /** Parameters of the root run. */
    public record EodParams(String requestedBy) {
    }

    /**
     * Work in steps per business date, more than fits into the time limit of one run. Its progress survives the runs
     * (a real handler keeps it in the DB), so each run continues where the previous one stopped.
     */
    static class Chunked implements JobHandler<EodParams, DefaultOutcome> {

        static final int STEPS = 8;

        final Map<String, AtomicInteger> done = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> executions = new ConcurrentHashMap<>();
        final Queue<String> requestedBy = new ConcurrentLinkedQueue<>();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch hold;

        Chunked(int holds) {
            hold = new CountDownLatch(holds);
        }

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            String day = ctx.flowInstance().map(FlowInstance::businessKey).orElseThrow();
            if (ctx.parameters() != null) {
                requestedBy.add(ctx.parameters().requestedBy());
            }
            started.countDown();
            try {
                if (!hold.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("not released");
                }
                AtomicInteger progress = done.computeIfAbsent(day, d -> new AtomicInteger());
                while (progress.get() < STEPS) {
                    Thread.sleep(60);
                    executions.computeIfAbsent(day, d -> new AtomicInteger()).incrementAndGet();
                    progress.incrementAndGet();
                    if (!ctx.shouldContinue()) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return JobResult.success();
        }
    }

    /** The root of the end of day. */
    static class CutOff implements JobHandler<EodParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            return JobResult.success();
        }
    }

    /** A member of the join that is done in one run. */
    static class Pnl implements JobHandler<EodParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            return JobResult.success();
        }
    }

    /** A member of the join that needs several runs. */
    static class Positions extends Chunked {

        Positions() {
            super(0);
        }
    }

    /** The root of an instance, chunked too. */
    static class Booking extends Chunked {

        Booking() {
            // the first run waits until the test released it
            super(1);
        }
    }

    /** The join target: once per business date. */
    static class SignOff implements JobHandler<EodParams, DefaultOutcome> {

        private final Map<String, AtomicInteger> signedOff = new ConcurrentHashMap<>();

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            String day = ctx.flowInstance().map(FlowInstance::businessKey).orElseThrow();
            signedOff.computeIfAbsent(day, d -> new AtomicInteger()).incrementAndGet();
            return JobResult.success();
        }
    }

    @Test
    void aDeferredMemberIsContinuedAndTheJoinWaitsForItsLastRun() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            cluster.definitions().define(Positions.class).timeLimit(Duration.ofMillis(200)).save();
            cluster.definitions().define(Pnl.class).save();
            cluster.definitions().define(SignOff.class)
                   .joinOf(Positions.class, RunOutcome.SUCCEEDED).joinOf(Pnl.class, RunOutcome.SUCCEEDED).save();
            cluster.definitions().define(CutOff.class).external()
                   .on(RunOutcome.SUCCEEDED, Positions.class).on(RunOutcome.SUCCEEDED, Pnl.class).save();
            Positions positions = new Positions();
            SignOff signOff = new SignOff();
            DtpEngine a = cluster.start("a", new CutOff(), positions, new Pnl(), signOff);
            cluster.start("b", new CutOff(), positions, new Pnl(), signOff);

            a.jobEngine().startFlowInstance(CutOff.class, new EodParams("ops"), "2026-11-18");
            await().atMost(Duration.ofSeconds(30)).until(() -> "COMPLETED".equals(
                    a.flowInstances().find("CutOff", "2026-11-18").orElseThrow().state()));

            // every step exactly once, in several runs: each run continued the previous one
            assertThat(positions.executions.get("2026-11-18")).hasValue(Chunked.STEPS);
            InstanceView day = a.flowInstances().find("CutOff", "2026-11-18").orElseThrow();
            List<RunView> chunks = day.runs().stream().filter(r -> "Positions".equals(r.job())).toList();
            assertThat(chunks).hasSizeGreaterThan(1);
            assertThat(chunks.getFirst().triggerType()).isEqualTo("TRANSITION");
            for (int i = 1; i < chunks.size(); i++) {
                RunView previous = chunks.get(i - 1);
                assertThat(previous.outcome()).isEqualTo("DEFERRED");
                assertThat(chunks.get(i).triggerType()).isEqualTo("CONTINUATION");
                assertThat(chunks.get(i).triggeredBy())
                        .isEqualTo("continuation of " + previous.id() + " (LIMIT_REACHED)");
            }
            RunView last = chunks.getLast();
            assertThat(last.outcome()).isEqualTo("SUCCEEDED");

            // the join fired once, for the run that finished the work, and the instance completed after it
            assertThat(signOff.signedOff).containsOnlyKeys("2026-11-18");
            assertThat(signOff.signedOff.get("2026-11-18")).hasValue(1);
            assertThat(day.joins()).singleElement().satisfies(join -> {
                assertThat(join.fired()).isTrue();
                assertThat(join.arrivals()).filteredOn(r -> "Positions".equals(r.member())).singleElement()
                        .satisfies(r -> assertThat(r.runId()).isEqualTo(last.id()));
            });
            assertThat(day.runs()).filteredOn(r -> "SignOff".equals(r.job())).singleElement()
                    .satisfies(r -> assertThat(r.createdAt()).isAfterOrEqualTo(last.finishedAt()));
            assertThat(day.completedAt()).isAfterOrEqualTo(last.finishedAt());
        }
    }

    @Test
    void aDeferredRunOfADisabledJobKeepsTheInstanceOpenUntilItIsRunAgain() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            cluster.definitions().define(Booking.class).external().timeLimit(Duration.ofMillis(200)).save();
            Booking booking = new Booking();
            DtpEngine a = cluster.start("a", booking);

            a.jobEngine().startFlowInstance(Booking.class, new EodParams("ops"), "2026-11-20");
            await().atMost(Duration.ofSeconds(10)).until(() -> booking.started.getCount() == 0);
            // disabled while its first run works: the run stops at its limit, but nothing continues it
            assertThat(a.jobAdmin().setEnabled("Booking", false, "otto")).isTrue();
            booking.hold.countDown();
            await().atMost(Duration.ofSeconds(10)).until(() -> a.flowInstances().find("Booking", "2026-11-20")
                    .orElseThrow().runs().getFirst().outcome() != null);
            cluster.awaitIdle(Duration.ofSeconds(10));

            InstanceView day = a.flowInstances().find("Booking", "2026-11-20").orElseThrow();
            assertThat(day.state()).isEqualTo("OPEN");
            RunView deferred = day.runs().getFirst();
            assertThat(day.runs()).singleElement().satisfies(r -> assertThat(r.outcome()).isEqualTo("DEFERRED"));
            assertThat(booking.executions.get("2026-11-20").get()).isLessThan(Chunked.STEPS);

            // the operator enables the job again and runs the deferred run again: it's continued from there on
            a.jobAdmin().setEnabled("Booking", true, "otto");
            a.flowInstances().rerun(deferred.id(), "otto");
            await().atMost(Duration.ofSeconds(30)).until(() -> "COMPLETED".equals(
                    a.flowInstances().find("Booking", "2026-11-20").orElseThrow().state()));

            assertThat(booking.executions.get("2026-11-20")).hasValue(Chunked.STEPS);
            InstanceView completed = a.flowInstances().get(day.id());
            assertThat(completed.runs()).extracting(RunView::triggerType).startsWith("EXTERNAL", "RERUN");
            assertThat(completed.runs().subList(2, completed.runs().size()))
                    .allSatisfy(r -> assertThat(r.triggerType()).isEqualTo("CONTINUATION"));
            assertThat(completed.runs().getLast().outcome()).isEqualTo("SUCCEEDED");
            // every run got the parameters of the root run
            assertThat(booking.requestedBy).hasSize(completed.runs().size()).containsOnly("ops");
        }
    }
}
