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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.api.WindowOccurrence;
import org.os890.dtp.api.event.WindowClosed;
import org.os890.dtp.api.event.WindowOpened;
import org.os890.dtp.api.event.WindowQuiescing;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.engine.CollectionWindows.OccurrenceView;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.engine.Decisions.DecisionView;
import org.os890.dtp.core.engine.Decisions.OptionView;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Collection windows (docs/design/04-lifecycles.md §4.6, 05 §5.6): the paused jobs stop softly, the collector runs
 * alone with a cut-off, a quiesce timeout and a failed collector are decided by operations.
 */
class WindowTest {

    /** A writer, e.g. the step that changes what the collector exports. */
    static class Writer implements ItemProcessor<TestTrade, UUID> {

        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger written = new AtomicInteger();
        private final long delayMillis;

        Writer(long delayMillis) {
            this.delayMillis = delayMillis;
        }

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            inFlight.incrementAndGet();
            try {
                sleep(delayMillis);
                written.incrementAndGet();
                item.complete();
            } finally {
                inFlight.decrementAndGet();
            }
        }
    }

    /** The collector: records its occurrence and whether a writer was active while it collected. */
    static class Collector implements JobHandler<NoParams, DefaultOutcome> {

        final List<WindowOccurrence> seen = new CopyOnWriteArrayList<>();
        final AtomicInteger maxWritersInFlight = new AtomicInteger();
        final AtomicBoolean failNext = new AtomicBoolean();
        private volatile Writer writer;

        Collector watching(Writer watched) {
            writer = watched;
            return this;
        }

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            WindowOccurrence window = ctx.window().orElseThrow();
            seen.add(window);
            if (failNext.getAndSet(false)) {
                throw new IllegalStateException("export target unreachable");
            }
            for (int i = 0; i < 15; i++) {
                maxWritersInFlight.accumulateAndGet(writer.inFlight.get(), Math::max);
                sleep(20);
            }
            ctx.packages(TestTrade.ITEM).createFrom(Stream.of(ItemRef.of(TestTrade.ITEM, UUID.randomUUID())),
                    ref -> window.window() + "-" + window.sequence(), 10);
            ctx.packages(TestTrade.ITEM).forEachPending(pkg -> pkg.done());
            return JobResult.success();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void track(TestCluster cluster, DtpEngine node, int count) {
        cluster.tx().required(() -> {
            for (int i = 1; i <= count; i++) {
                node.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-" + i).routeTo(Writer.class);
            }
            return null;
        });
    }

    private static int occurrences(TestCluster cluster, String state) {
        return cluster.count("SELECT COUNT(*) FROM dtp_window_occurrence WHERE state = ?", state);
    }

    @Test
    void theCollectorRunsAloneWithACutOffAndTheNextOccurrenceStartsThere() {
        try (TestCluster cluster = new TestCluster()) {
            Writer writer = new Writer(20);
            Collector collector = new Collector().watching(writer);
            DtpEngine node = cluster.start("a", writer, collector);
            cluster.definitions().define(Writer.class).inbox(1).save();
            cluster.definitions().define(Collector.class).external().save();
            cluster.definitions().window("export", null, Duration.ofSeconds(10), Collector.class,
                    List.of(Writer.class));
            track(cluster, node, 150);
            await().atMost(Duration.ofSeconds(10)).until(() -> writer.written.get() >= 10);

            OccurrenceView started = node.windows().startNow("export", "otto");
            assertThat(started.state()).isEqualTo("QUIESCING");
            assertThat(started.triggeredBy()).isEqualTo("otto");
            assertThat(node.windows().startNow("export", "otto").id()).isEqualTo(started.id());
            await().atMost(Duration.ofSeconds(15)).until(() -> occurrences(cluster, "CLOSED") == 1);

            // the writer stopped softly (DEFERRED, WINDOW) and didn't run while the collector collected
            assertThat(collector.maxWritersInFlight.get()).isZero();
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run r JOIN dtp_job j ON j.id = r.job_id"
                    + " WHERE j.name = 'Writer' AND r.outcome = 'DEFERRED' AND r.stop_reason = 'WINDOW'"))
                    .isEqualTo(1);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE trigger_type = 'WINDOW'"
                    + " AND window_occurrence_id = ? AND outcome = 'SUCCEEDED'", started.id())).isEqualTo(1);
            assertThat(collector.seen).singleElement().satisfies(w -> {
                assertThat(w.window()).isEqualTo("export");
                assertThat(w.sequence()).isEqualTo(1);
                assertThat(w.since()).isEmpty();
                assertThat(w.cutOff()).isNotNull();
            });
            // the collector's packages belong to the occurrence
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_work_package WHERE window_occurrence_id = ?",
                    started.id())).isEqualTo(1);

            // afterwards the writer continues, the next occurrence collects from the cut-off of the first
            await().atMost(Duration.ofSeconds(20)).until(() -> writer.written.get() == 150);
            node.windows().startNow("export", "otto");
            await().atMost(Duration.ofSeconds(15)).until(() -> occurrences(cluster, "CLOSED") == 2);
            assertThat(collector.seen).hasSize(2);
            assertThat(collector.seen.get(1).sequence()).isEqualTo(2);
            assertThat(collector.seen.get(1).from()).isEqualTo(collector.seen.get(0).cutOff());
            assertThat(node.windows().latest(5)).extracting(OccurrenceView::sequence).containsExactly(2L, 1L);
            // domain events: per occurrence quiescing, opened with the collector run, closed
            assertThat(cluster.events(WindowQuiescing.class)).extracting(e -> e.occurrence().sequence(),
                    WindowQuiescing::triggeredBy).containsExactly(tuple(1L, "otto"), tuple(2L, "otto"));
            assertThat(cluster.events(WindowQuiescing.class)).allSatisfy(e -> assertThat(e.occurrence().cutOff())
                    .isNull());
            assertThat(cluster.events(WindowOpened.class)).hasSize(2).allSatisfy(e -> {
                assertThat(e.forced()).isFalse();
                assertThat(e.collectorRunId()).isNotNull();
            }).extracting(e -> e.occurrence().cutOff()).containsExactly(collector.seen.get(0).cutOff(),
                    collector.seen.get(1).cutOff());
            assertThat(cluster.events(WindowClosed.class)).extracting(e -> e.occurrence().sequence(),
                    WindowClosed::skipped, WindowClosed::collectorOutcome).containsExactly(
                    tuple(1L, false, RunOutcome.SUCCEEDED), tuple(2L, false, RunOutcome.SUCCEEDED));
            cluster.awaitIdle(Duration.ofSeconds(10));
            // the writer's items, besides the one item the collector packaged per occurrence
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'"
                    + " AND work_package_id IS NULL")).isEqualTo(150);
        }
    }

    @Test
    void occurrencesFollowTheSchedule() {
        try (TestCluster cluster = new TestCluster()) {
            Writer writer = new Writer(0);
            Collector collector = new Collector().watching(writer);
            DtpEngine node = cluster.start("a", writer, collector);
            cluster.definitions().define(Writer.class).inbox(1).save();
            cluster.definitions().define(Collector.class).external().save();
            cluster.definitions().window("tick", "*/2 * * ? * *", Duration.ofSeconds(10), Collector.class,
                    List.of(Writer.class));

            await().atMost(Duration.ofSeconds(15)).until(() -> occurrences(cluster, "CLOSED") >= 2);
            List<OccurrenceView> latest = node.windows().latest(10).stream()
                    .filter(o -> "CLOSED".equals(o.state())).toList().reversed();
            assertThat(latest.getFirst().triggeredBy()).isNull();
            assertThat(latest.get(0).scheduledAt().getEpochSecond() % 2).isZero();
            // a chain of cut-offs: each occurrence starts where the previous one ended
            assertThat(latest.get(1).from()).isEqualTo(latest.get(0).cutOff());
            assertThat(latest.get(1).sequence()).isEqualTo(latest.get(0).sequence() + 1);
        }
    }

    @Test
    void aQuiesceTimeoutIsDecidedByOperations() {
        try (TestCluster cluster = new TestCluster()) {
            Writer writer = new Writer(4000);
            Collector collector = new Collector().watching(writer);
            DtpEngine node = cluster.start("a", writer, collector);
            cluster.definitions().define(Writer.class).inbox(1).save();
            cluster.definitions().define(Collector.class).external().save();
            cluster.definitions().window("export", null, Duration.ofMillis(500), Collector.class,
                    List.of(Writer.class));
            track(cluster, node, 1);
            await().atMost(Duration.ofSeconds(10)).until(() -> writer.inFlight.get() == 1);

            node.windows().startNow("export", "otto");
            await().atMost(Duration.ofSeconds(10)).until(() -> occurrences(cluster, "WAITING_FOR_DECISION") == 1);
            Decisions decisions = node.decisions();
            List<DecisionView> open = decisions.open(Decisions.Decider.of("otto", Decisions.OPERATORS));
            assertThat(open).singleElement().satisfies(d -> {
                assertThat(d.subjectType()).isEqualTo("WINDOW_OCCURRENCE");
                assertThat(d.businessKey()).isEqualTo("window export #1");
                assertThat(d.reason()).contains("didn't stop within PT0.5S");
                assertThat(d.options()).extracting(OptionView::name)
                        .containsExactly("KEEP_WAITING", "FORCE_OPEN", "SKIP_OCCURRENCE");
            });
            assertThat(decisions.open(Decisions.Decider.of("bea"))).isEmpty();

            // FORCE_OPEN: the collector runs although the writer is still busy
            decisions.decide(open.getFirst().id(), "FORCE_OPEN", Decisions.Decider.of("otto", Decisions.OPERATORS),
                    "export can't wait");
            await().atMost(Duration.ofSeconds(10)).until(() -> occurrences(cluster, "CLOSED") == 1);
            assertThat(collector.maxWritersInFlight.get()).isEqualTo(1);
            assertThat(cluster.events(WindowOpened.class)).singleElement().satisfies(e -> assertThat(e.forced())
                    .isTrue());
        }
    }

    @Test
    void aFailedCollectorIsRetriedOrItsOccurrenceSkipped() {
        try (TestCluster cluster = new TestCluster()) {
            Writer writer = new Writer(0);
            Collector collector = new Collector().watching(writer);
            DtpEngine node = cluster.start("a", writer, collector);
            cluster.definitions().define(Writer.class).inbox(1).save();
            cluster.definitions().define(Collector.class).external().save();
            cluster.definitions().window("export", null, Duration.ofSeconds(10), Collector.class,
                    List.of(Writer.class));
            Decisions decisions = node.decisions();
            Decisions.Decider otto = Decisions.Decider.of("otto", Decisions.OPERATORS);

            // occurrence 1: the collector fails, RETRY plans a new collector run within the same occurrence
            collector.failNext.set(true);
            OccurrenceView first = node.windows().startNow("export", "otto");
            await().atMost(Duration.ofSeconds(10)).until(() -> occurrences(cluster, "WAITING_FOR_DECISION") == 1);
            DecisionView failed = decisions.open(otto).getFirst();
            assertThat(failed.reason()).contains("collector run ended FAILED");
            assertThat(failed.options()).extracting(OptionView::name).containsExactly("RETRY", "SKIP_OCCURRENCE");
            decisions.decide(failed.id(), "RETRY", otto, "target is back");
            await().atMost(Duration.ofSeconds(10)).until(() -> occurrences(cluster, "CLOSED") == 1);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE window_occurrence_id = ?", first.id()))
                    .isEqualTo(2);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE run_key LIKE ?", "window:" + first.id()
                    + ":2")).isEqualTo(1);
            assertThat(collector.seen).hasSize(2).allSatisfy(w -> assertThat(w.sequence()).isEqualTo(1));

            // occurrence 2: fails again and is skipped; occurrence 3 collects from the cut-off of occurrence 1
            collector.failNext.set(true);
            node.windows().startNow("export", "otto");
            await().atMost(Duration.ofSeconds(10)).until(() -> occurrences(cluster, "WAITING_FOR_DECISION") == 1);
            decisions.decide(decisions.open(otto).getFirst().id(), "SKIP_OCCURRENCE", otto, "next one covers it");
            await().atMost(Duration.ofSeconds(5)).until(() -> occurrences(cluster, "SKIPPED") == 1);
            await().atMost(Duration.ofSeconds(5)).until(() -> cluster.events(WindowClosed.class).size() == 2);
            assertThat(cluster.events(WindowClosed.class).getLast()).satisfies(e -> {
                assertThat(e.occurrence().sequence()).isEqualTo(2);
                assertThat(e.skipped()).isTrue();
                assertThat(e.collectorOutcome()).isNull();
            });
            node.windows().startNow("export", "otto");
            await().atMost(Duration.ofSeconds(10)).until(() -> occurrences(cluster, "CLOSED") == 2);
            WindowOccurrence third = collector.seen.getLast();
            assertThat(third.sequence()).isEqualTo(3);
            assertThat(third.from()).isEqualTo(collector.seen.get(1).cutOff());
        }
    }
}
