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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.FlowInstance;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.event.FlowInstanceCompleted;
import org.os890.dtp.api.event.FlowInstanceStarted;
import org.os890.dtp.api.event.JoinFired;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.engine.FlowInstances.InstanceView;
import org.os890.dtp.core.engine.FlowInstances.JoinView;
import org.os890.dtp.core.engine.FlowInstances.RunView;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Flow instances and run joins (DD-4, DD-9): an end of day per business date, cut-off, then positions and P&amp;L in
 * parallel, signed off when both finished.
 */
class FlowInstanceTest {

    /** Parameters of the root run. */
    public record EodParams(String requestedBy) {
    }

    /** The root: reads its business date from the flow instance. */
    static class CutOff implements JobHandler<EodParams, DefaultOutcome> {

        private final Map<String, String> requestedBy = new ConcurrentHashMap<>();
        private final Set<String> failing = ConcurrentHashMap.newKeySet();

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            String day = ctx.flowInstance().map(FlowInstance::businessKey).orElseThrow();
            if (failing.contains(day)) {
                throw new IllegalStateException("no trades delivered for " + day);
            }
            requestedBy.put(day, ctx.parameters().requestedBy());
            return JobResult.success();
        }
    }

    /**
     * A member of the join. On the rendezvous day both members wait for each other, so they run at the same time on
     * different nodes and arrive at the join at the same time.
     */
    static class Report implements JobHandler<EodParams, DefaultOutcome> {

        static final String RENDEZVOUS_DAY = "2026-11-01";

        final Set<String> failing = ConcurrentHashMap.newKeySet();
        private final CyclicBarrier rendezvous;

        Report(CyclicBarrier rendezvous) {
            this.rendezvous = rendezvous;
        }

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<EodParams> ctx) {
            String day = ctx.flowInstance().map(FlowInstance::businessKey).orElseThrow();
            try {
                if (RENDEZVOUS_DAY.equals(day)) {
                    rendezvous.await(10, TimeUnit.SECONDS);
                } else {
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (BrokenBarrierException | TimeoutException e) {
                throw new IllegalStateException("the other member didn't run at the same time", e);
            }
            if (failing.contains(day)) {
                throw new IllegalStateException("prices missing for " + day);
            }
            return JobResult.success();
        }
    }

    static class Positions extends Report {

        Positions(CyclicBarrier rendezvous) {
            super(rendezvous);
        }
    }

    static class Pnl extends Report {

        Pnl(CyclicBarrier rendezvous) {
            super(rendezvous);
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

    private static void define(TestCluster cluster) {
        cluster.definitions().define(Positions.class).save();
        cluster.definitions().define(Pnl.class).save();
        cluster.definitions().define(SignOff.class)
               .joinOf(Positions.class, RunOutcome.SUCCEEDED, RunOutcome.COMPLETED_WITH_ISSUES)
               .joinOf(Pnl.class, RunOutcome.SUCCEEDED).save();
        cluster.definitions().define(CutOff.class).external()
               .on(RunOutcome.SUCCEEDED, Positions.class).on(RunOutcome.SUCCEEDED, Pnl.class).save();
    }

    @Test
    void eachBusinessDateIsSignedOffExactlyOnce() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            define(cluster);
            CutOff cutOff = new CutOff();
            CyclicBarrier rendezvous = new CyclicBarrier(2);
            Positions positions = new Positions(rendezvous);
            Pnl pnl = new Pnl(rendezvous);
            SignOff signOff = new SignOff();
            DtpEngine a = null;
            for (String node : List.of("a", "b", "c")) {
                DtpEngine engine = cluster.start(TestCluster.config(node).maxConcurrentRuns(1).build(), cutOff,
                        positions, pnl, signOff);
                a = a == null ? engine : a;
            }
            DtpEngine first = a;

            List<String> days = java.util.stream.IntStream.rangeClosed(1, 20)
                    .mapToObj(d -> "2026-11-%02d".formatted(d)).toList();
            for (String day : days) {
                assertThat(first.jobEngine().startFlowInstance(CutOff.class, new EodParams("ops"), day)
                                .newlyCreated()).isTrue();
            }
            RunHandle again = first.jobEngine().startFlowInstance(CutOff.class, new EodParams("ops"), days.getFirst());
            assertThat(again.newlyCreated()).isFalse();
            await().atMost(Duration.ofSeconds(60)).until(() -> first.flowInstances().latest(50).stream()
                    .allMatch(i -> "COMPLETED".equals(i.state())));

            // one sign-off per business date, after both reports
            assertThat(signOff.signedOff).containsOnlyKeys(days);
            assertThat(signOff.signedOff.values()).allSatisfy(count -> assertThat(count).hasValue(1));
            assertThat(cutOff.requestedBy).containsOnlyKeys(days).containsValue("ops");
            InstanceView day1 = first.flowInstances().find("CutOff", days.getFirst()).orElseThrow();
            assertThat(again.runId()).isEqualTo(day1.runs().getFirst().id().toString());
            assertThat(day1.runs()).extracting(RunView::job)
                    .containsExactlyInAnyOrder("CutOff", "Positions", "Pnl", "SignOff");
            assertThat(day1.runs()).extracting(RunView::triggerType)
                    .containsExactlyInAnyOrder("EXTERNAL", "TRANSITION", "TRANSITION", "JOIN");
            assertThat(day1.joins()).singleElement().satisfies(join -> {
                assertThat(join.target()).isEqualTo("SignOff");
                assertThat(join.fired()).isTrue();
                assertThat(join.waitingFor()).isEmpty();
                assertThat(join.arrivals()).hasSize(2).allSatisfy(arrival -> assertThat(arrival.accepted()).isTrue());
            });
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE run_key LIKE 'join:%'")).isEqualTo(20);
            // on the rendezvous day both members ran at the same time on different nodes: the join fired once
            assertThat(cluster.count("SELECT COUNT(DISTINCT r.owner_node_id) FROM dtp_run r"
                    + " JOIN dtp_flow_instance f ON f.id = r.flow_instance_id AND f.business_key = ?"
                    + " JOIN dtp_job j ON j.id = r.job_id AND j.name IN ('Positions', 'Pnl')", Report.RENDEZVOUS_DAY))
                    .isEqualTo(2);
            assertThat(signOff.signedOff.get(Report.RENDEZVOUS_DAY)).hasValue(1);

            assertThat(cluster.events(FlowInstanceStarted.class)).hasSize(20);
            assertThat(cluster.events(JoinFired.class)).hasSize(20)
                    .allSatisfy(e -> assertThat(e.targetJob()).isEqualTo("SignOff"));
            assertThat(cluster.events(FlowInstanceCompleted.class)).extracting(e -> e.instance().businessKey())
                    .containsExactlyInAnyOrderElementsOf(days);
        }
    }

    @Test
    void aFailedMemberKeepsTheInstanceOpenUntilItIsRunAgain() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            define(cluster);
            CutOff cutOff = new CutOff();
            Pnl pnl = new Pnl(new CyclicBarrier(2));
            SignOff signOff = new SignOff();
            Positions positions = new Positions(new CyclicBarrier(2));
            DtpEngine a = cluster.start("a", cutOff, positions, pnl, signOff);
            cluster.start("b", cutOff, positions, pnl, signOff);
            pnl.failing.add("2026-11-16");
            cutOff.failing.add("2026-11-17");

            a.jobEngine().startFlowInstance(CutOff.class, new EodParams("ops"), "2026-11-16");
            a.jobEngine().startFlowInstance(CutOff.class, new EodParams("ops"), "2026-11-17");
            cluster.awaitIdle(Duration.ofSeconds(20));

            // the join waits for the P&L, the instance stays open
            InstanceView day = a.flowInstances().find("CutOff", "2026-11-16").orElseThrow();
            assertThat(day.state()).isEqualTo("OPEN");
            JoinView join = day.joins().getFirst();
            assertThat(join.fired()).isFalse();
            assertThat(join.waitingFor()).containsExactly("Pnl");
            assertThat(join.arrivals()).anySatisfy(arrival -> {
                assertThat(arrival.member()).isEqualTo("Pnl");
                assertThat(arrival.outcome()).isEqualTo("FAILED");
                assertThat(arrival.accepted()).isFalse();
            });
            assertThat(signOff.signedOff).isEmpty();
            // a failed root: nothing waits, but the failure keeps the instance open
            InstanceView failedRoot = a.flowInstances().find("CutOff", "2026-11-17").orElseThrow();
            assertThat(failedRoot.state()).isEqualTo("OPEN");
            assertThat(failedRoot.runs()).singleElement().satisfies(r -> assertThat(r.outcome()).isEqualTo("FAILED"));

            // the operator runs the failed members again
            UUID failedPnl = day.runs().stream().filter(r -> "Pnl".equals(r.job())).findFirst().orElseThrow().id();
            UUID rootRun = day.runs().getFirst().id();
            assertThatThrownBy(() -> a.flowInstances().rerun(UUID.randomUUID(), "otto"))
                    .isInstanceOf(DtpException.class);
            pnl.failing.clear();
            cutOff.failing.clear();
            UUID rerun = a.flowInstances().rerun(failedPnl, "otto");
            assertThat(a.flowInstances().rerun(failedPnl, "otto")).isEqualTo(rerun);
            a.flowInstances().rerun(failedRoot.runs().getFirst().id(), "otto");
            await().atMost(Duration.ofSeconds(20)).until(() -> a.flowInstances().latest(10).stream()
                    .allMatch(i -> "COMPLETED".equals(i.state())));

            assertThat(signOff.signedOff).containsOnlyKeys("2026-11-16", "2026-11-17");
            InstanceView completed = a.flowInstances().get(day.id());
            assertThat(completed.runs()).extracting(RunView::triggerType).contains("RERUN", "JOIN");
            assertThat(completed.runs()).filteredOn(r -> "RERUN".equals(r.triggerType())).singleElement()
                    .satisfies(r -> assertThat(r.triggeredBy()).isEqualTo("rerun of " + failedPnl + " by otto"));
            assertThat(completed.joins().getFirst().arrivals()).filteredOn(r -> "Pnl".equals(r.member()))
                    .singleElement().satisfies(r -> assertThat(r.runId()).isEqualTo(rerun));
            assertThat(completed.completedAt()).isNotNull();
            assertThatThrownBy(() -> a.flowInstances().rerun(rootRun, "otto"))
                    .isInstanceOf(DtpException.class).hasMessageContaining("completed already");
        }
    }
}
