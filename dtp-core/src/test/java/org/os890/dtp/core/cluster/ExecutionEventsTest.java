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
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.StopReason;
import org.os890.dtp.api.event.JobDisabled;
import org.os890.dtp.api.event.JobEnabled;
import org.os890.dtp.api.event.LeaseExpired;
import org.os890.dtp.api.event.LeaseLost;
import org.os890.dtp.api.event.NodeDrained;
import org.os890.dtp.api.event.NodeJoined;
import org.os890.dtp.api.event.NodeLost;
import org.os890.dtp.api.event.RunFinished;
import org.os890.dtp.api.event.RunRef;
import org.os890.dtp.api.event.RunSkipped;
import org.os890.dtp.api.event.RunStarted;
import org.os890.dtp.api.event.RunStopRequested;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.domain.ConcurrencyPolicy;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * The domain events of the execution (design 02 §2.6, slice 23): runs, leases, nodes and jobs. Each event is published
 * by the node that committed what it tells, after the commit.
 */
class ExecutionEventsTest {

    /** The first execution blocks until it is released, the later ones succeed at once. */
    static class Hold implements JobHandler<NoParams, DefaultOutcome> {

        final AtomicInteger executions = new AtomicInteger();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch firstDone = new CountDownLatch(1);

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            if (executions.incrementAndGet() == 1) {
                started.countDown();
                try {
                    while (ctx.shouldContinue() && !release.await(20, TimeUnit.MILLISECONDS)) {
                        // waiting
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    firstDone.countDown();
                }
            }
            return JobResult.success();
        }
    }

    static class Queued extends Hold {
    }

    @Test
    void aTakeoverIsAnnouncedByTheLeaderTheSurvivorAndTheZombie() throws InterruptedException {
        try (TestCluster cluster = new TestCluster()) {
            Hold job = new Hold();
            DtpEngine a = cluster.start("a", job);
            cluster.definitions().define(Hold.class).external().save();
            DtpEngine b = cluster.start("b", job);
            UUID runId = UUID.fromString(a.jobEngine().trigger(Hold.class, new NoParams()).runId());
            assertThat(job.started.await(10, TimeUnit.SECONDS)).isTrue();

            UUID owner = cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT owner_node_id FROM dtp_run"
                    + " WHERE id = ?", rs -> Sql.uuid(rs, "owner_node_id"), runId)).orElseThrow();
            DtpEngine crashed = owner.equals(a.nodeId()) ? a : b;
            DtpEngine survivor = crashed == a ? b : a;
            String crashedName = crashed == a ? "a" : "b";
            String survivorName = crashed == a ? "b" : "a";
            crashed.kill();

            await().atMost(Duration.ofSeconds(15)).until(() -> !cluster.events(RunFinished.class).isEmpty()
                    && !cluster.events(NodeLost.class).isEmpty());
            // the zombie wakes up: its commit is fenced, it reports the lost lease
            job.release.countDown();
            assertThat(job.firstDone.await(10, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(10)).until(() -> !cluster.events(LeaseLost.class).isEmpty());

            assertThat(cluster.events(NodeJoined.class)).extracting(NodeJoined::node, NodeJoined::standby)
                    .containsExactly(tuple("a", false), tuple("b", false));
            assertThat(cluster.events(RunStarted.class)).extracting(RunStarted::run).extracting(RunRef::id,
                    RunRef::job, RunRef::attempt, RunRef::node).containsExactly(tuple(runId, "Hold", 1, crashedName),
                    tuple(runId, "Hold", 2, survivorName));
            assertThat(cluster.events(NodeLost.class)).singleElement().satisfies(e -> {
                assertThat(e.nodeId()).isEqualTo(crashed.nodeId());
                assertThat(e.node()).isEqualTo(crashedName);
                assertThat(e.lastHeartbeat()).isNotNull();
            });
            assertThat(cluster.events(LeaseExpired.class)).singleElement().satisfies(e -> {
                assertThat(e.run().attempt()).isEqualTo(1);
                assertThat(e.run().node()).isEqualTo(crashedName);
                assertThat(e.requeued()).isTrue(); // takeover policy RERUN
            });
            // only the survivor finished the run: the zombie's finish was fenced
            assertThat(cluster.events(RunFinished.class)).singleElement().satisfies(e -> {
                assertThat(e.run().attempt()).isEqualTo(2);
                assertThat(e.run().node()).isEqualTo(survivorName);
                assertThat(e.outcome()).isEqualTo(RunOutcome.SUCCEEDED);
                assertThat(e.stopReason()).isNull();
                assertThat(e.elapsed()).isNotNull();
            });
            assertThat(cluster.published()).filteredOn(p -> p.event() instanceof LeaseLost).singleElement()
                    .satisfies(p -> {
                        assertThat(p.node()).isEqualTo(crashedName);
                        assertThat(((LeaseLost) p.event()).run().attempt()).isEqualTo(1);
                    });

            survivor.stop();
            assertThat(cluster.events(NodeDrained.class)).singleElement().satisfies(e -> {
                assertThat(e.nodeId()).isEqualTo(survivor.nodeId());
                assertThat(e.leftRuns()).isZero();
            });
        }
    }

    @Test
    void operatorsCancelRunsAndSwitchJobsSkippedTriggersAreAnnounced() throws InterruptedException {
        try (TestCluster cluster = new TestCluster()) {
            Hold hold = new Hold();
            Queued queued = new Queued();
            DtpEngine node = cluster.start("a", hold, queued);
            cluster.definitions().define(Hold.class).external().save();
            cluster.definitions().define(Queued.class).external().concurrency(ConcurrencyPolicy.QUEUE_ALL).save();

            // a trigger while a run of a SKIP job is open is skipped
            UUID holding = UUID.fromString(node.jobEngine().trigger(Hold.class, new NoParams()).runId());
            assertThat(hold.started.await(10, TimeUnit.SECONDS)).isTrue();
            UUID skipped = UUID.fromString(node.jobEngine().trigger(Hold.class, new NoParams()).runId());
            assertThat(cluster.events(RunSkipped.class)).singleElement().satisfies(e -> {
                assertThat(e.run().id()).isEqualTo(skipped);
                assertThat(e.run().job()).isEqualTo("Hold");
                assertThat(e.run().attempt()).isZero();
                assertThat(e.run().node()).isNull();
                assertThat(e.triggerType()).isEqualTo("EXTERNAL");
            });

            // a pending run is cancelled at once, a running one is asked to stop
            node.jobEngine().trigger(Queued.class, new NoParams());
            assertThat(queued.started.await(10, TimeUnit.SECONDS)).isTrue();
            UUID pending = UUID.fromString(node.jobEngine().trigger(Queued.class, new NoParams()).runId());
            assertThat(node.jobAdmin().cancel(pending, "otto")).isTrue();
            assertThat(cluster.events(RunFinished.class)).singleElement().satisfies(e -> {
                assertThat(e.run().id()).isEqualTo(pending);
                assertThat(e.run().node()).isNull();
                assertThat(e.outcome()).isEqualTo(RunOutcome.CANCELLED);
                assertThat(e.stopReason()).isEqualTo(StopReason.CANCEL);
                assertThat(e.elapsed()).isNull();
            });
            assertThat(node.jobAdmin().cancel(holding, "otto")).isTrue();
            assertThat(cluster.events(RunStopRequested.class)).singleElement().satisfies(e -> {
                assertThat(e.run().id()).isEqualTo(holding);
                assertThat(e.run().node()).isEqualTo("a");
                assertThat(e.run().attempt()).isEqualTo(1);
                assertThat(e.reason()).isEqualTo(StopReason.CANCEL);
                assertThat(e.by()).isEqualTo("otto");
            });
            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.events(RunFinished.class).size() == 2);
            assertThat(cluster.events(RunFinished.class).getLast()).satisfies(e -> {
                assertThat(e.run().id()).isEqualTo(holding);
                assertThat(e.run().node()).isEqualTo("a");
                assertThat(e.outcome()).isEqualTo(RunOutcome.CANCELLED);
                assertThat(e.stopReason()).isEqualTo(StopReason.CANCEL);
                assertThat(e.elapsed()).isPositive();
            });
            assertThat(node.jobAdmin().cancel(holding, "otto")).isFalse();
            assertThat(cluster.events(RunStopRequested.class)).hasSize(1);

            // switching a job off and on; switching it to its state again changes nothing
            assertThat(node.jobAdmin().setEnabled("Queued", false, "otto")).isTrue();
            assertThat(node.jobAdmin().setEnabled("Queued", false, "otto")).isFalse();
            assertThat(node.jobAdmin().setEnabled("Queued", true, "bea")).isTrue();
            assertThat(cluster.events(JobDisabled.class)).containsExactly(new JobDisabled("Queued", "otto"));
            assertThat(cluster.events(JobEnabled.class)).containsExactly(new JobEnabled("Queued", "bea"));

            queued.release.countDown();
            cluster.awaitIdle(Duration.ofSeconds(10));
            assertThat(cluster.events(RunFinished.class)).extracting(e -> e.run().job(), RunFinished::outcome)
                    .containsExactly(tuple("Queued", RunOutcome.CANCELLED), tuple("Hold", RunOutcome.CANCELLED),
                            tuple("Queued", RunOutcome.SUCCEEDED));
            assertThat(cluster.events(RunStarted.class)).extracting(e -> e.run().job()).containsExactly("Hold",
                    "Queued");
            assertThat(cluster.events(RunStarted.class)).allSatisfy(e -> assertThat(e.run().node()).isEqualTo("a"));
        }
    }
}
