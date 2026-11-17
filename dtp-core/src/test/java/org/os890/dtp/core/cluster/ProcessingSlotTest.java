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
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DecisionOption;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Processing slots (docs/design/05-coordination-flows.md §5.7): runs start only within a slot, are asked to stop at
 * its end, and their remaining work waits for the next slot.
 */
class ProcessingSlotTest {

    @ItemProcessing(batchSize = 1)
    static class Settlement implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            item.complete();
        }
    }

    static class Blocker implements JobHandler<NoParams, DefaultOutcome> {

        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            started.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return JobResult.success();
        }
    }

    static class Statement implements JobHandler<NoParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            return JobResult.success();
        }
    }

    enum Check implements DecisionOption {
        GO
    }

    /** Asks a person on its first execution: the run waits, the decision plans it again. */
    static class Asking implements JobHandler<NoParams, DefaultOutcome> {

        final AtomicInteger executions = new AtomicInteger();

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            executions.incrementAndGet();
            if (ctx.decision(Check.class).isEmpty()) {
                ctx.requestDecision("check the statement", Check.class);
            }
            return JobResult.success();
        }
    }

    @Test
    void aRunThatADecisionPlansAgainStartsInTheNextSlot() {
        try (TestCluster cluster = new TestCluster()) {
            Asking asking = new Asking();
            DtpEngine node = cluster.start("a", asking);
            Instant start = cluster.dbNow().plusSeconds(1).truncatedTo(ChronoUnit.SECONDS);
            Instant end = start.plusSeconds(2);
            UUID job = cluster.definitions().define(Asking.class).external().slot(null, utc(start), utc(end)).save();
            UUID run = UUID.fromString(node.jobEngine().trigger(Asking.class, new NoParams()).runId());
            await().atMost(Duration.ofSeconds(5)).until(() -> "WAITING_FOR_DECISION".equals(
                    cluster.queries().run(run).orElseThrow().state()));

            // decided after the slot ended: the run is planned for the slot of the next day at once (D39)
            await().atMost(Duration.ofSeconds(5)).until(() -> cluster.dbNow().isAfter(end));
            Decisions.Decider bea = Decisions.Decider.of("bea");
            node.decisions().decide(node.decisions().open(bea).getFirst().id(), "GO", bea, null);
            assertThat(instant(cluster, "SELECT not_before AS v FROM dtp_run WHERE id = ?", run))
                    .isEqualTo(start.plus(1, ChronoUnit.DAYS));
            assertThat(instant(cluster, "SELECT slot_end AS v FROM dtp_run WHERE id = ?", run))
                    .isEqualTo(end.plus(1, ChronoUnit.DAYS));
            sleep(Duration.ofMillis(500));
            assertThat(asking.executions).hasValue(1);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE job_id = ? AND state = 'PENDING'", job))
                    .isEqualTo(1);
        }
    }

    private static LocalTime utc(Instant instant) {
        return LocalTime.ofInstant(instant, ZoneOffset.UTC);
    }

    /** Reads one point in time, the query names it {@code v}. */
    private static Instant instant(TestCluster cluster, String sql, Object... params) {
        return cluster.tx().withConnection(c -> Sql.queryOne(c, sql, rs -> Sql.instant(rs, "v"), params).orElse(null));
    }

    private static void sleep(Duration duration) {
        await().pollDelay(duration).atMost(duration.plusSeconds(1)).until(() -> true);
    }

    @Test
    void aRunStartsInItsSlotStopsAtItsEndAndTheRestWaitsForTheNextSlot() {
        try (TestCluster cluster = new TestCluster()) {
            DtpEngine node = cluster.start("a", new Settlement());
            Instant start = cluster.dbNow().plusSeconds(3).truncatedTo(ChronoUnit.SECONDS);
            Instant end = start.plusSeconds(3);
            cluster.definitions().define(Settlement.class).inbox(1).slot(null, utc(start), utc(end)).save();
            cluster.tx().required(() -> {
                for (int i = 1; i <= 40; i++) {
                    node.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "S-" + i)
                        .routeTo(Settlement.class);
                }
                return null;
            });

            // planned right away, but for the start of the slot
            await().atMost(Duration.ofSeconds(2)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_run") == 1);
            assertThat(instant(cluster, "SELECT not_before AS v FROM dtp_run")).isEqualTo(start);
            assertThat(instant(cluster, "SELECT slot_end AS v FROM dtp_run")).isEqualTo(end);
            await().atMost(Duration.ofSeconds(5)).until(() -> cluster.dbNow().isAfter(start.minusMillis(300)));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isZero();

            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_run"
                    + " WHERE outcome = 'DEFERRED' AND stop_reason = 'SLOT_END'") == 1);
            int completed = cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'");
            assertThat(completed).isBetween(5, 30);
            assertThat(instant(cluster, "SELECT MIN(occurred_at) AS v FROM dtp_item_event"
                    + " WHERE event_type = 'COMPLETED'"))
                    .isAfterOrEqualTo(start);
            // the stop is noticed within a heartbeat, the running item is finished
            assertThat(instant(cluster, "SELECT MAX(occurred_at) AS v FROM dtp_item_event"
                    + " WHERE event_type = 'COMPLETED'"))
                    .isBefore(end.plusSeconds(1));

            // the rest waits for the slot of the next day
            await().atMost(Duration.ofSeconds(3)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_run"
                    + " WHERE state = 'PENDING'") == 1);
            assertThat(instant(cluster, "SELECT not_before AS v FROM dtp_run WHERE state = 'PENDING'"))
                    .isEqualTo(start.plus(1, ChronoUnit.DAYS));
            assertThat(instant(cluster, "SELECT slot_end AS v FROM dtp_run WHERE state = 'PENDING'"))
                    .isEqualTo(end.plus(1, ChronoUnit.DAYS));
            sleep(Duration.ofSeconds(1));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(completed);
        }
    }

    @Test
    void aRunThatMissedItsSlotMovesToTheNextOne() throws InterruptedException {
        try (TestCluster cluster = new TestCluster()) {
            Blocker blocker = new Blocker();
            DtpEngine node = cluster.start(TestCluster.config("a").maxConcurrentRuns(1).build(), blocker,
                    new Statement());
            Instant start = cluster.dbNow().plusSeconds(2).truncatedTo(ChronoUnit.SECONDS);
            Instant end = start.plusSeconds(1);
            cluster.definitions().define(Blocker.class).external().save();
            UUID statement = cluster.definitions().define(Statement.class).external()
                                    .slot(null, utc(start), utc(end)).save();
            node.jobEngine().trigger(Blocker.class, new NoParams());
            assertThat(blocker.started.await(5, TimeUnit.SECONDS)).isTrue();
            // the only run slot of the node is taken during the whole processing slot
            node.jobEngine().trigger(Statement.class, new NoParams());

            await().atMost(Duration.ofSeconds(6)).until(() -> start.plus(1, ChronoUnit.DAYS).equals(instant(cluster,
                    "SELECT not_before AS v FROM dtp_run WHERE job_id = ?", statement)));
            assertThat(instant(cluster, "SELECT slot_end AS v FROM dtp_run WHERE job_id = ?", statement))
                    .isEqualTo(end.plus(1, ChronoUnit.DAYS));
            blocker.release.countDown();
            sleep(Duration.ofSeconds(1));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE job_id = ? AND state = 'PENDING'"
                    + " AND attempt = 0", statement)).isEqualTo(1);
        }
    }
}
