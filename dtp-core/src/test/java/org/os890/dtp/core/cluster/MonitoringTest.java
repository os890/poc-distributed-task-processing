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
import static org.assertj.core.groups.Tuple.tuple;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.api.event.StepCongested;
import org.os890.dtp.api.event.StepRecovered;
import org.os890.dtp.api.event.TechnicalFailureSpike;
import org.os890.dtp.core.DtpConfig;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.query.MonitoringFeed;
import org.os890.dtp.core.query.MonitoringQueries.EdgeView;
import org.os890.dtp.core.query.MonitoringQueries.FailureView;
import org.os890.dtp.core.query.MonitoringQueries.HeatMap;
import org.os890.dtp.core.query.MonitoringQueries.StepView;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * The monitoring read model (docs/design/09-monitoring-heatmap.md): minute buckets and edges from the journal, the
 * status of every step, jams and technical failures with their alert transitions, the projector leader and the live
 * feed.
 */
class MonitoringTest {

    private static final Duration RANGE = Duration.ofMinutes(15);

    /** Routes items to {@link Finish}, items ending with 3 back to {@link Fixer} on their first visit. */
    static class Forward implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            if (item.businessKey().endsWith("3") && item.visits(Forward.class) == 1) {
                item.routeBack(Fixer.class, "needs a fix");
            } else {
                item.routeTo(Finish.class);
            }
        }
    }

    static class Fixer implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.routeTo(Forward.class);
        }
    }

    static class Finish implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.complete();
        }
    }

    /** Slower than the inflow. */
    @ItemProcessing(batchSize = 10)
    static class Slow implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            sleep(40);
            item.complete();
        }
    }

    /** Its scoring service times out until it is back. */
    static class Flaky implements ItemProcessor<TestTrade, UUID> {

        final AtomicBoolean failing = new AtomicBoolean(true);

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            if (failing.get()) {
                throw new IllegalStateException("scoring service timed out");
            }
            item.complete();
        }
    }

    /** Defers every item once, e.g. waiting for a delivery note. */
    static class Deferrer implements ItemProcessor<TestTrade, UUID> {

        final Set<UUID> deferred = ConcurrentHashMap.newKeySet();

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            if (deferred.add(item.ref().id())) {
                item.defer(Duration.ofSeconds(3), "delivery note expected");
            } else {
                item.complete();
            }
        }
    }

    /** Runs only when it is triggered, e.g. an import that re-imports what was routed back. */
    static class Manual implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.complete();
        }
    }

    /** A step no node has, e.g. after a red/green mistake. */
    static class Orphan implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.complete();
        }
    }

    private static DtpConfig config(String node) {
        return TestCluster.config(node).monitoring(DtpConfig.Monitoring.builder().interval(Duration.ofMillis(200))
                .failureThreshold(5).failureWindow(Duration.ofSeconds(2)).feedInterval(Duration.ofMillis(100))
                .build()).build();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void track(TestCluster cluster, DtpEngine node, int count, Class<? extends ProcessStep> step) {
        cluster.tx().required(() -> {
            for (int i = 1; i <= count; i++) {
                node.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-" + i).routeTo(step);
            }
            return null;
        });
    }

    private static StepView step(DtpEngine node, String name) {
        return node.monitoring().heatmap(RANGE).steps().stream().filter(s -> s.step().equals(name)).findFirst()
                   .orElseThrow();
    }

    /** Waits for two more projection cycles, so the views show what is committed now. */
    private static void awaitProjection(DtpEngine node) {
        long version = node.monitoring().projection().version();
        await().atMost(Duration.ofSeconds(10)).until(() -> node.monitoring().projection().version() >= version + 2);
    }

    @Test
    void theHeatMapCountsArrivalsDeparturesAndRoutes() {
        try (TestCluster cluster = new TestCluster()) {
            DtpEngine node = cluster.start(config("a"), new Forward(), new Fixer(), new Finish());
            cluster.definitions().define(Forward.class).inbox(1).save();
            cluster.definitions().define(Fixer.class).inbox(1).save();
            cluster.definitions().define(Finish.class).inbox(1).save();
            track(cluster, node, 30, Forward.class);
            cluster.awaitIdle(Duration.ofSeconds(20));
            awaitProjection(node);

            HeatMap heatmap = node.monitoring().heatmap(RANGE);
            assertThat(heatmap.version()).isPositive();
            assertThat(heatmap.projectedUntil()).isNotNull();
            StepView forward = step(node, "Forward");
            // 30 tracked, 3 of them (3, 13, 23) came back from the fixer
            assertThat(forward.totals().arrived()).isEqualTo(33);
            assertThat(forward.totals().departed()).isEqualTo(33);
            assertThat(forward.totals().routedBack()).isEqualTo(3);
            assertThat(forward.totals().maxWaitMs()).isGreaterThanOrEqualTo(forward.totals().avgWaitMs());
            assertThat(step(node, "Fixer").totals().departed()).isEqualTo(3);
            StepView finish = step(node, "Finish");
            assertThat(finish.totals().arrived()).isEqualTo(30);
            assertThat(finish.totals().completed()).isEqualTo(30);
            assertThat(finish.statusReason()).isEqualTo("IDLE");
            assertThat(finish.alertState()).isEqualTo("OK");
            assertThat(heatmap.edges()).extracting(EdgeView::from, EdgeView::to, EdgeView::backRoute,
                    EdgeView::items).contains(
                            tuple("Forward", "Finish", false, 30),
                            tuple("Forward", "Fixer", true, 3),
                            tuple("Fixer", "Forward", false, 3));
            assertThat(node.monitoring().series("Finish", RANGE)).isNotEmpty()
                    .allSatisfy(minute -> assertThat(minute.minute()).isNotNull());

            // recomputing the horizon counts every entry once
            awaitProjection(node);
            assertThat(step(node, "Finish").totals().completed()).isEqualTo(30);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_view_step_status")).isEqualTo(3);
        }
    }

    @Test
    void aSlowStepIsCongestedOnceAndRecovers() {
        try (TestCluster cluster = new TestCluster()) {
            DtpEngine node = cluster.start(config("a"), new Slow());
            cluster.definitions().define(Slow.class).inbox(1).save();
            cluster.execute("UPDATE dtp_job_revision SET expected_max_queue_age = 'PT0.5S'"
                    + " WHERE job_id = (SELECT id FROM dtp_job WHERE name = 'Slow')");
            track(cluster, node, 100, Slow.class);

            await().atMost(Duration.ofSeconds(10)).until(() -> !cluster.events(StepCongested.class).isEmpty());
            StepCongested congested = cluster.events(StepCongested.class).getFirst();
            assertThat(congested.step()).isEqualTo("Slow");
            assertThat(congested.statusReason()).isIn("PROCESSING", "WAITING_FOR_RUN");
            assertThat(congested.jamScore()).isGreaterThanOrEqualTo(70);
            assertThat(congested.queued()).isPositive();
            assertThat(step(node, "Slow").alertState()).isEqualTo("CONGESTED");

            cluster.awaitIdle(Duration.ofSeconds(30));
            await().atMost(Duration.ofSeconds(10)).until(() -> !cluster.events(StepRecovered.class).isEmpty());
            assertThat(cluster.events(StepRecovered.class)).singleElement().satisfies(recovered -> {
                assertThat(recovered.step()).isEqualTo("Slow");
                assertThat(recovered.previousState()).isEqualTo("CONGESTED");
            });
            assertThat(cluster.events(StepCongested.class)).hasSize(1);
            StepView slow = step(node, "Slow");
            assertThat(slow.alertState()).isEqualTo("OK");
            assertThat(slow.queued()).isZero();
            assertThat(slow.totals().completed()).isEqualTo(100);
        }
    }

    @Test
    void technicalFailuresAreGroupedByErrorTypeAndAlertOnce() {
        try (TestCluster cluster = new TestCluster()) {
            Flaky flaky = new Flaky();
            DtpEngine node = cluster.start(config("a"), flaky);
            cluster.definitions().define(Flaky.class).inbox(1).retry(5, Duration.ofMillis(300)).save();
            track(cluster, node, 10, Flaky.class);

            await().atMost(Duration.ofSeconds(10)).until(() -> !cluster.events(TechnicalFailureSpike.class)
                    .isEmpty());
            assertThat(cluster.events(TechnicalFailureSpike.class)).singleElement().satisfies(spike -> {
                assertThat(spike.step()).isEqualTo("Flaky");
                assertThat(spike.failures()).isGreaterThanOrEqualTo(5);
                assertThat(spike.errorTypes()).containsExactly(IllegalStateException.class.getName());
                assertThat(spike.window()).isEqualTo(Duration.ofSeconds(2));
            });
            StepView failing = step(node, "Flaky");
            assertThat(failing.alertState()).isEqualTo("FAILING");
            assertThat(failing.totals().failedTechnical()).isGreaterThanOrEqualTo(5);
            List<FailureView> failures = node.monitoring().failures("Flaky", Duration.ofHours(1));
            assertThat(failures).singleElement().satisfies(f -> {
                assertThat(f.errorType()).isEqualTo(IllegalStateException.class.getName());
                assertThat(f.occurrences()).isGreaterThanOrEqualTo(5);
                assertThat(f.sampleMessage()).contains("scoring service timed out");
                assertThat(f.sampleItemId()).isNotNull();
            });

            // the service is back: the retries complete the items, the step recovers after the calm period
            flaky.failing.set(false);
            cluster.awaitIdle(Duration.ofSeconds(20));
            await().atMost(Duration.ofSeconds(15)).until(() -> !cluster.events(StepRecovered.class).isEmpty());
            assertThat(cluster.events(StepRecovered.class)).singleElement()
                    .satisfies(r -> assertThat(r.previousState()).isEqualTo("FAILING"));
            assertThat(cluster.events(TechnicalFailureSpike.class)).hasSize(1);
            assertThat(step(node, "Flaky").totals().completed()).isEqualTo(10);
        }
    }

    @Test
    void deferredItemsCountFromWhenTheyAreDueAgain() {
        try (TestCluster cluster = new TestCluster()) {
            Deferrer deferrer = new Deferrer();
            DtpEngine node = cluster.start(config("a"), deferrer);
            cluster.definitions().define(Deferrer.class).inbox(1).save();
            track(cluster, node, 5, Deferrer.class);
            await().atMost(Duration.ofSeconds(5)).until(() -> deferrer.deferred.size() == 5);
            awaitProjection(node);
            assertThat(step(node, "Deferrer").deferred()).isEqualTo(5);
            assertThat(step(node, "Deferrer").queued()).isZero();

            // due again after 3 s, longer than the recent window: not a stuck step
            await().atMost(Duration.ofSeconds(15)).until(() -> step(node, "Deferrer").totals().completed() == 5);
            awaitProjection(node);
            assertThat(cluster.events(StepCongested.class)).isEmpty();
            assertThat(step(node, "Deferrer").alertState()).isEqualTo("OK");
        }
    }

    @Test
    void aStepWithoutCapableNodeIsAJamAndADisabledOneIsNot() {
        try (TestCluster cluster = new TestCluster()) {
            DtpEngine node = cluster.start(config("a"), new Finish(), new Manual());
            cluster.definitions().define(Finish.class).inbox(1).save();
            cluster.definitions().define(Orphan.class).inbox(1).save();
            cluster.definitions().define(Manual.class).external().save();
            cluster.execute("UPDATE dtp_job_revision SET expected_max_queue_age = 'PT0.1S'"
                    + " WHERE job_id = (SELECT id FROM dtp_job WHERE name = 'Manual')");
            track(cluster, node, 5, Orphan.class);
            track(cluster, node, 3, Manual.class);

            await().atMost(Duration.ofSeconds(10)).until(() -> !cluster.events(StepCongested.class).isEmpty());
            assertThat(cluster.events(StepCongested.class)).singleElement().satisfies(c -> {
                assertThat(c.step()).isEqualTo("Orphan");
                assertThat(c.statusReason()).isEqualTo("NO_CAPABLE_NODE");
                assertThat(c.jamScore()).isEqualTo(100);
            });
            assertThat(step(node, "Orphan").queued()).isEqualTo(5);
            // queued for its next run: an expected backlog, however old
            StepView manual = step(node, "Manual");
            assertThat(manual.statusReason()).isEqualTo("WAITING_FOR_TRIGGER");
            assertThat(manual.queued()).isEqualTo(3);
            assertThat(manual.jamScore()).isZero();
            assertThat(step(node, "Finish").statusReason()).isEqualTo("IDLE");

            // disabled on purpose: the queue is expected, no jam
            cluster.execute("UPDATE dtp_job SET enabled = FALSE WHERE name = 'Orphan'");
            await().atMost(Duration.ofSeconds(10)).until(() -> "DISABLED".equals(step(node, "Orphan").statusReason())
                    && !cluster.events(StepRecovered.class).isEmpty());
            assertThat(step(node, "Orphan").jamScore()).isZero();
        }
    }

    @Test
    void theProjectionFollowsItsLeaderAndTheFeedSendsChanges() {
        try (TestCluster cluster = new TestCluster()) {
            DtpEngine a = cluster.start(config("a"), new Forward(), new Fixer(), new Finish());
            DtpEngine b = cluster.start(config("b"), new Forward(), new Fixer(), new Finish());
            cluster.definitions().define(Forward.class).inbox(1).save();
            cluster.definitions().define(Fixer.class).inbox(1).save();
            cluster.definitions().define(Finish.class).inbox(1).save();
            await().atMost(Duration.ofSeconds(10)).until(() -> a.isProjectionLeader() || b.isProjectionLeader());
            DtpEngine leader = a.isProjectionLeader() ? a : b;
            DtpEngine other = leader == a ? b : a;
            awaitProjection(other);

            // the feed of the other node sends the changed steps
            List<MonitoringFeed.Update> updates = new CopyOnWriteArrayList<>();
            MonitoringFeed.Subscription subscription = other.monitoringFeed().subscribe(updates::add);
            try {
                track(cluster, other, 20, Forward.class);
                cluster.awaitIdle(Duration.ofSeconds(20));
                await().atMost(Duration.ofSeconds(10)).until(() -> updates.stream()
                        .filter(u -> MonitoringFeed.HEATMAP_UPDATE.equals(u.type()))
                        .flatMap(u -> u.steps().stream()).anyMatch(s -> s.step().equals("Finish")
                                && s.totals().completed() == 20));
                assertThat(other.monitoringFeed().listeners()).isEqualTo(1);
            } finally {
                subscription.close();
            }
            assertThat(other.monitoringFeed().listeners()).isZero();

            // the leader dies: the other node takes over the projection with a new term
            int term = cluster.count("SELECT term FROM dtp_projection_state");
            leader.kill();
            await().atMost(Duration.ofSeconds(10)).until(other::isProjectionLeader);
            awaitProjection(other);
            assertThat(cluster.count("SELECT term FROM dtp_projection_state")).isGreaterThan(term);
            track(cluster, other, 5, Forward.class);
            cluster.awaitIdle(Duration.ofSeconds(20));
            awaitProjection(other);
            assertThat(step(other, "Finish").totals().completed()).isEqualTo(25);
        }
    }
}
