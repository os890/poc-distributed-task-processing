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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DecisionOption;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.FlowBuilder;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.FlowEvent;
import org.os890.dtp.api.ItemLane;
import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.api.Signal;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpConfig;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.engine.Decisions.Decider;
import org.os890.dtp.core.engine.Decisions.DecisionView;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.query.JourneyGraph.Node;
import org.os890.dtp.core.query.JourneyQueries;
import org.os890.dtp.core.spi.ClusterState;
import org.os890.dtp.core.testsupport.MapStepRegistry;
import org.os890.dtp.core.testsupport.RecordingTelemetry;
import org.os890.dtp.core.testsupport.RecordingTelemetry.Decision;
import org.os890.dtp.core.testsupport.RecordingTelemetry.Entry;
import org.os890.dtp.core.testsupport.RecordingTelemetry.Item;
import org.os890.dtp.core.testsupport.RecordingTelemetry.SpanRecord;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Telemetry metrics and traces (slice 13, docs/design/09-monitoring-heatmap.md §9.8): what the engine reports, only
 * for committed work, the spans of the units of work linked to the request that tracked the items, the cluster gauges
 * of the projection leader, and the lease metrics of a takeover.
 */
class TelemetryTest {

    private static final Decider RITA = Decider.of("rita", "risk");

    enum Lane implements ItemLane {
        VIP(10), STANDARD(0);

        private final int priority;

        Lane(int priority) {
            this.priority = priority;
        }

        @Override
        public int priority() {
            return priority;
        }
    }

    enum IntakeSignal implements Signal {
        ACCEPTED
    }

    enum CheckSignal implements Signal {
        OK
    }

    enum CheckOption implements DecisionOption {
        ALLOW
    }

    static class Intake implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.signal(IntakeSignal.ACCEPTED);
        }
    }

    /**
     * The limit service fails once for ERR trades; unusual trades go to the risk desk. One item per batch, so the
     * failure isn't absorbed by the rollback of a batch.
     */
    @ItemProcessing(batchSize = 1)
    static class Check implements ItemProcessor<TestTrade, UUID> {

        private final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            String key = item.businessKey();
            if (key.contains("ERR") && calls.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet() == 1) {
                throw new IllegalStateException("limit service down");
            }
            if (key.contains("ASK") && item.decision(CheckOption.class).isEmpty()) {
                item.requestDecision("unusual trade", CheckOption.class, () -> "risk");
                return;
            }
            item.signal(CheckSignal.OK);
        }
    }

    static class CheckFlow implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(Intake.class).on(IntakeSignal.ACCEPTED).to(Check.class);
            flow.step(Check.class).on(CheckSignal.OK).complete();
        }
    }

    /** The first execution blocks until it is released, like a hanging node. */
    static class SlowJob implements JobHandler<NoParams, DefaultOutcome> {

        private final AtomicInteger executions = new AtomicInteger();
        private final CountDownLatch firstStarted = new CountDownLatch(1);
        private final CountDownLatch releaseFirst = new CountDownLatch(1);
        private final CountDownLatch firstDone = new CountDownLatch(1);

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            if (executions.incrementAndGet() == 1) {
                firstStarted.countDown();
                try {
                    releaseFirst.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    firstDone.countDown();
                }
            }
            return JobResult.success();
        }
    }

    @Test
    void committedWorkIsMeasuredAndTracedAcrossNodes() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            cluster.definitions().define(Check.class).inbox(1).retry(1, Duration.ofMillis(50)).save();
            Check check = new Check();
            RecordingTelemetry ta = new RecordingTelemetry();
            RecordingTelemetry tb = new RecordingTelemetry();
            DtpEngine a = cluster.start(config("a"), registry(check), ta);
            DtpEngine b = cluster.start(config("b"), registry(check), tb);
            List<String> keys = List.of("T-1", "T-2", "T-3", "T-4", "T-5", "T-6", "T-ERR", "T-ASK", "T-VIP-1",
                    "T-VIP-2");
            String request = RecordingTelemetry.newTraceParent();
            cluster.tx().required(() -> ta.inRequest(request, () -> {
                for (String key : keys) {
                    a.fire(FlowEvent.start(CheckFlow.class, ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), key,
                            key.contains("VIP") ? Lane.VIP : Lane.STANDARD));
                }
                return null;
            }));
            cluster.awaitIdle(Duration.ofSeconds(30));
            String checkName = job(cluster, Check.class);
            // the projection leader reports the trade waiting for the risk desk
            await().atMost(Duration.ofSeconds(10)).until(() -> Stream.of(ta, tb).anyMatch(t -> t.cluster()
                    .filter(c -> c.waiting().contains(new ClusterState.Waiting(checkName, "risk", 1))
                            && c.decisions().stream().anyMatch(d -> d.group().equals("risk"))).isPresent()));
            DecisionView ask = a.decisions().open(RITA).getFirst();
            a.decisions().decide(ask.id(), "ALLOW", RITA, null);
            cluster.awaitIdle(Duration.ofSeconds(30));
            String intake = job(cluster, Intake.class);
            String checkJob = job(cluster, Check.class);

            // item results, counted once per committed result: the batch that failed with T-ERR was rolled back
            List<Item> items = both(ta, tb, t -> t.items);
            assertThat(items).filteredOn(i -> i.step().equals(intake)).extracting(Item::result)
                    .hasSize(keys.size()).containsOnly("routed");
            assertThat(items).filteredOn(i -> i.step().equals(checkJob) && i.result().equals("completed"))
                    .hasSize(keys.size());
            assertThat(items).filteredOn(i -> i.step().equals(checkJob) && !i.result().equals("completed"))
                    .containsExactlyInAnyOrder(new Item(checkJob, "STANDARD", "failed", "TECHNICAL"),
                            new Item(checkJob, "STANDARD", "escalated", null));
            assertThat(items).filteredOn(i -> "VIP".equals(i.lane())).hasSize(4);
            assertThat(both(ta, tb, t -> t.technical)).containsExactly(new Entry(checkJob,
                    IllegalStateException.class.getName()));
            assertThat(both(ta, tb, t -> t.rollbacks)).contains(new Entry(checkJob, "exception"))
                    .doesNotContain(new Entry(intake, "exception"));
            // waits per step and lane: every committed processing of an item, i.e. not the rolled-back first attempt
            // of T-ERR, but its retry and the processing of T-ASK after the decision
            List<RecordingTelemetry.Wait> waits = both(ta, tb, t -> t.waits);
            assertThat(waits).filteredOn(w -> w.step().equals(checkJob)).hasSize(keys.size() + 1)
                    .allSatisfy(w -> assertThat(w.duration()).isGreaterThanOrEqualTo(Duration.ZERO));
            assertThat(waits).filteredOn(w -> "VIP".equals(w.lane())).isNotEmpty();
            assertThat(both(ta, tb, t -> t.units)).contains(intake, checkJob);
            assertThat(both(ta, tb, t -> t.runs)).extracting(Entry::step).contains(intake, checkJob);
            assertThat(both(ta, tb, t -> t.claims)).isNotEmpty()
                    .allSatisfy(lag -> assertThat(lag).isGreaterThanOrEqualTo(Duration.ZERO));
            assertThat(both(ta, tb, t -> t.elections)).contains("housekeeping", "projection");
            assertThat(both(ta, tb, t -> t.created)).containsExactly(new Decision(checkJob, "CHOICE", "risk"));
            assertThat(both(ta, tb, t -> t.decided)).containsExactly(new Decision(checkJob, "CHOICE", "risk"));

            // traces: the items keep the trace of the request that tracked them, the spans of later steps link to it,
            // the journal names the trace of the unit of work that wrote each entry
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE trace_parent = ?", request))
                    .isEqualTo(keys.size());
            List<SpanRecord> spans = both(ta, tb, t -> t.spans);
            assertThat(spans).filteredOn(s -> s.step().equals(intake)).isNotEmpty()
                    .allSatisfy(s -> assertThat(s.links()).containsExactly(request));
            assertThat(spans).filteredOn(s -> s.step().equals(checkJob) && s.failure() != null).isNotEmpty()
                    .allSatisfy(s -> assertThat(s.failure()).hasMessage("limit service down"));
            Set<String> traces = spans.stream().map(SpanRecord::traceId).collect(Collectors.toSet());
            List<JourneyEntry> journey = a.queries().journey("test-trade", "T-ERR");
            assertThat(journey).filteredOn(e -> e.runId() != null).isNotEmpty()
                    .allSatisfy(e -> assertThat(e.traceId()).isIn(traces));
            Node checkNode = a.journeys().graph("test-trade", "T-ERR", JourneyQueries.Options.defaults())
                              .orElseThrow().nodes().stream().filter(n -> checkJob.equals(n.job())).findFirst()
                              .orElseThrow();
            assertThat(checkNode.traceIds()).isNotEmpty().allSatisfy(id -> assertThat(id).isIn(traces));

            // cluster gauges: only the projection leader reports them; a stopped leader stops at once
            DtpEngine leader = a.isProjectionLeader() ? a : b;
            DtpEngine other = leader == a ? b : a;
            RecordingTelemetry leading = leader == a ? ta : tb;
            RecordingTelemetry following = leader == a ? tb : ta;
            await().atMost(Duration.ofSeconds(10)).until(() -> leading.cluster().isPresent());
            ClusterState state = leading.cluster().orElseThrow();
            assertThat(state.steps()).extracting(ClusterState.Step::step).contains(intake, checkJob);
            assertThat(state.nodes()).contains(new ClusterState.Nodes("", "ACTIVE", 2));
            assertThat(following.cluster()).isEmpty();
            leader.stop();
            assertThat(leading.cluster()).isEmpty();
            await().atMost(Duration.ofSeconds(15)).until(() -> following.cluster().isPresent());
            assertThat(other.isProjectionLeader()).isTrue();
        }
    }

    @Test
    void clusterGaugesShowQueuesPerLane() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            // the check runs only when it is triggered: the trades wait in its inbox
            cluster.definitions().define(Check.class).external().save();
            RecordingTelemetry telemetry = new RecordingTelemetry();
            DtpEngine a = cluster.start(config("a"), registry(new Check()), telemetry);
            cluster.tx().required(() -> {
                for (int n = 1; n <= 5; n++) {
                    a.fire(FlowEvent.start(CheckFlow.class, ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-" + n,
                            n <= 2 ? Lane.VIP : Lane.STANDARD));
                }
                return null;
            });
            String checkJob = job(cluster, Check.class);
            await().atMost(Duration.ofSeconds(15)).until(() -> telemetry.cluster().map(ClusterState::queues)
                    .filter(q -> q.contains(new ClusterState.Queue(checkJob, "VIP", 2))
                            && q.contains(new ClusterState.Queue(checkJob, "STANDARD", 3))).isPresent());
            ClusterState.Step step = telemetry.cluster().orElseThrow().steps().stream()
                    .filter(s -> s.step().equals(checkJob)).findFirst().orElseThrow();
            assertThat(step.statusReason()).isEqualTo("WAITING_FOR_TRIGGER");
            assertThat(step.alertState()).isZero();
            assertThat(step.oldestQueuedSecs()).isNotNegative();
        }
    }

    @Test
    void aTakeoverIsCountedOnBothSides() throws InterruptedException {
        try (TestCluster cluster = new TestCluster()) {
            SlowJob job = new SlowJob();
            RecordingTelemetry ta = new RecordingTelemetry();
            RecordingTelemetry tb = new RecordingTelemetry();
            DtpEngine a = cluster.start(TestCluster.config("a").build(), new MapStepRegistry(job), ta);
            cluster.definitions().define(SlowJob.class).external().save();
            DtpEngine b = cluster.start(TestCluster.config("b").build(), new MapStepRegistry(job), tb);

            RunHandle run = a.jobEngine().trigger(SlowJob.class, new NoParams());
            assertThat(job.firstStarted.await(10, TimeUnit.SECONDS)).isTrue();
            UUID owner = cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT owner_node_id FROM dtp_run"
                    + " WHERE id = ?", rs -> Sql.uuid(rs, "owner_node_id"), UUID.fromString(run.runId())))
                                .orElseThrow();
            DtpEngine crashed = owner.equals(a.nodeId()) ? a : b;
            RecordingTelemetry zombie = crashed == a ? ta : tb;
            RecordingTelemetry survivor = crashed == a ? tb : ta;
            String name = job(cluster, SlowJob.class);
            await().atMost(Duration.ofSeconds(10)).until(() -> zombie.renewals.contains(name));
            crashed.kill();

            await().atMost(Duration.ofSeconds(15)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_run"
                    + " WHERE id = ? AND outcome = 'SUCCEEDED'", UUID.fromString(run.runId())) == 1);
            assertThat(survivor.takeovers).containsExactly(name);
            assertThat(survivor.runs).containsExactly(new Entry(name, "SUCCEEDED"));
            // the zombie wakes up: its commit is fenced
            job.releaseFirst.countDown();
            assertThat(job.firstDone.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).until(() -> zombie.lost.contains(name));
            assertThat(zombie.rollbacks).containsExactly(new Entry(name, "lease_lost"));
            assertThat(zombie.lost).containsExactly(name);
            assertThat(zombie.runs).isEmpty();
        }
    }

    private static DtpConfig config(String node) {
        return TestCluster.config(node).monitoring(DtpConfig.Monitoring.builder().interval(Duration.ofMillis(200))
                .build()).build();
    }

    private static MapStepRegistry registry(Check check) {
        return new MapStepRegistry(new Intake(), check).flows(new CheckFlow());
    }

    private static String job(TestCluster cluster, Class<?> handler) {
        return cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT j.name FROM dtp_job j JOIN dtp_job_revision r"
                + " ON r.job_id = j.id AND r.revision = j.current_revision WHERE r.handler_class = ?",
                rs -> rs.getString(1), handler.getName())).orElseThrow();
    }

    private static <T> List<T> both(RecordingTelemetry a, RecordingTelemetry b,
                                    java.util.function.Function<RecordingTelemetry, List<T>> list) {
        return Stream.concat(list.apply(a).stream(), list.apply(b).stream()).toList();
    }
}
