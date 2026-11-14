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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunInfoKey;
import org.os890.dtp.api.WorkPackages;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.engine.Decisions.DecisionView;
import org.os890.dtp.core.query.DtpQueries.PackageView;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Work packages (docs/design/04-lifecycles.md §4.4, 05 §5.7): grouped items processed as a whole, carried over at the
 * item limit, retried, decided, processed once after a takeover.
 */
class WorkPackageTest {

    static final RunInfoKey<String> FILE = RunInfoKey.of("file", String.class);

    /** A collector: packages its inbox per group (the business key's middle part), then sends each package. */
    static class Export implements JobHandler<NoParams, DefaultOutcome> {

        final Queue<String> sent = new ConcurrentLinkedQueue<>();
        final Set<String> rejected = ConcurrentHashMap.newKeySet();
        final Set<String> crashing = ConcurrentHashMap.newKeySet();
        final Set<String> forgotten = ConcurrentHashMap.newKeySet();
        private volatile long delayMillis;

        Export slow(long millis) {
            delayMillis = millis;
            return this;
        }

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            WorkPackages<TestTrade, UUID> packages = ctx.packages(TestTrade.ITEM);
            packages.createFromInbox(item -> item.businessKey().split("-")[1], 3);
            packages.forEachPending(pkg -> {
                sleep(delayMillis);
                if (rejected.contains(pkg.groupKey())) {
                    pkg.fail("receiver rejected " + pkg.groupKey());
                } else if (crashing.contains(pkg.groupKey())) {
                    throw new IllegalStateException("receiver unreachable");
                } else if (!forgotten.contains(pkg.groupKey())) {
                    String file = "export-" + pkg.groupKey() + "-" + pkg.sequence() + ".xml";
                    pkg.info().put(FILE, file);
                    sent.add(file + ":" + pkg.items().size());
                    pkg.done();
                }
            });
            return JobResult.success();
        }
    }

    /** Packages a selection of references (e.g. the changes since the last export) without an inbox. */
    static class Delivery implements JobHandler<NoParams, DefaultOutcome> {

        final List<ItemRef<TestTrade, UUID>> selection = new ArrayList<>();
        final Map<String, Integer> created = new ConcurrentHashMap<>();

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            WorkPackages<TestTrade, UUID> packages = ctx.packages(TestTrade.ITEM);
            int count = packages.createFrom(selection.stream(), ref -> "all", 2);
            created.merge("packages", count, Integer::sum);
            packages.forEachPending(pkg -> pkg.done());
            return JobResult.success();
        }
    }

    private static void sleep(long millis) {
        if (millis > 0) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void track(TestCluster cluster, DtpEngine node, Map<String, Integer> perGroup) {
        cluster.tx().required(() -> {
            perGroup.forEach((group, count) -> {
                for (int i = 1; i <= count; i++) {
                    node.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-" + group + "-" + i)
                        .routeTo(Export.class);
                }
            });
            return null;
        });
    }

    /** Groups and their number of items, tracked in this order. */
    private static Map<String, Integer> groups(Object... groupsAndCounts) {
        Map<String, Integer> groups = new LinkedHashMap<>();
        for (int i = 0; i < groupsAndCounts.length; i += 2) {
            groups.put((String) groupsAndCounts[i], (Integer) groupsAndCounts[i + 1]);
        }
        return groups;
    }

    private static int count(TestCluster cluster, String sql, Object... params) {
        return cluster.count(sql, params);
    }

    @Test
    void packagesPerGroupAreCarriedOverToTheNextRunAtTheItemLimit() {
        try (TestCluster cluster = new TestCluster()) {
            Export export = new Export();
            DtpEngine node = cluster.start("a", export);
            cluster.definitions().define(Export.class).inbox(1).itemLimit(5).save();
            track(cluster, node, groups("A", 4, "B", 4, "C", 2));

            await().atMost(Duration.ofSeconds(10)).until(() -> count(cluster, "SELECT COUNT(*) FROM dtp_work_package"
                    + " WHERE state = 'DONE'") == 5);
            cluster.awaitIdle(Duration.ofSeconds(5));
            // 4 + 4 + 2 items, at most 3 per package: 5 packages, created by the first run
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_work_package")).isEqualTo(5);
            assertThat(count(cluster, "SELECT COUNT(DISTINCT created_by_run_id) FROM dtp_work_package")).isEqualTo(1);
            // the first run stopped at the item limit (3 + 1 + 3 items), the next run processed the rest
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_run WHERE outcome = 'DEFERRED'"
                    + " AND stop_reason = 'LIMIT_REACHED'")).isEqualTo(1);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_run WHERE outcome = 'SUCCEEDED'")).isEqualTo(1);
            assertThat(count(cluster, "SELECT COUNT(DISTINCT processed_by_run_id) FROM dtp_work_package"))
                    .isEqualTo(2);
            // oldest package first
            assertThat(export.sent).hasSize(5);
            List<Long> order = export.sent.stream()
                    .map(file -> Long.parseLong(file.replaceAll("export-\\w-(\\d+)\\.xml:\\d+", "$1"))).toList();
            assertThat(order).isSorted();
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'"
                    + " AND work_package_id IS NOT NULL")).isEqualTo(10);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_item_event WHERE event_type = 'PACKAGED'"))
                    .isEqualTo(10);
            List<PackageView> views = node.queries().packages(10);
            assertThat(views).hasSize(5).allSatisfy(p -> {
                assertThat(p.job()).isEqualTo("Export");
                assertThat(p.info()).contains("export-" + p.groupKey() + "-" + p.seq() + ".xml");
            });
            assertThat(views).extracting(PackageView::itemCount).containsExactlyInAnyOrder(3, 1, 3, 1, 2);
        }
    }

    @Test
    void failedPackagesAreRetriedThenDecided() {
        try (TestCluster cluster = new TestCluster()) {
            Export export = new Export();
            export.rejected.add("B");
            export.rejected.add("C");
            export.crashing.add("X");
            export.forgotten.add("Y");
            DtpEngine node = cluster.start("a", export);
            cluster.definitions().define(Export.class).inbox(1).retry(1, Duration.ofMillis(200)).save();
            track(cluster, node, groups("A", 2, "B", 2, "C", 1, "X", 1, "Y", 1));

            // one retry each, then a decision: business failures for the business, technical ones for operations
            await().atMost(Duration.ofSeconds(10)).until(() -> count(cluster, "SELECT COUNT(*) FROM dtp_work_package"
                    + " WHERE state = 'WAITING_FOR_DECISION'") == 4);
            cluster.awaitIdle(Duration.ofSeconds(5));
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_work_package WHERE state = 'WAITING_FOR_DECISION'"
                    + " AND attempts = 2")).isEqualTo(4);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_work_package WHERE group_key = 'Y'"
                    + " AND last_error LIKE '%ended without done() or fail()%'")).isEqualTo(1);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_run WHERE outcome = 'COMPLETED_WITH_ISSUES'"))
                    .isPositive();
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_item WHERE state = 'PACKAGED'")).isEqualTo(5);

            Decisions decisions = node.decisions();
            List<DecisionView> business = decisions.open(Decisions.Decider.of("bea"));
            assertThat(business).extracting(DecisionView::businessKey).allSatisfy(key -> assertThat(key)
                    .matches("work package \\d+ \\([BC]\\)"));
            assertThat(business).hasSize(2).allSatisfy(d -> assertThat(d.subjectType()).isEqualTo("WORK_PACKAGE"));
            List<DecisionView> operators = decisions.open(Decisions.Decider.of("otto", Decisions.OPERATORS));
            assertThat(operators).hasSize(4);

            // B: the receiver accepts it again, RETRY sends it; C is discarded with its items
            export.rejected.remove("B");
            decisions.decide(find(business, "B").id(), "RETRY", Decisions.Decider.of("bea"), "receiver fixed");
            decisions.decide(find(business, "C").id(), "DISCARD", Decisions.Decider.of("bea"), "cancelled");
            await().atMost(Duration.ofSeconds(10)).until(() -> count(cluster, "SELECT COUNT(*) FROM dtp_work_package"
                    + " WHERE group_key = 'B' AND state = 'DONE'") == 1);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_work_package WHERE group_key = 'C'"
                    + " AND state = 'DISCARDED'")).isEqualTo(1);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_item WHERE state = 'DISCARDED'")).isEqualTo(1);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(4);
            assertThat(export.sent).anySatisfy(file -> assertThat(file).startsWith("export-B-").endsWith(":2"));
        }
    }

    private static DecisionView find(List<DecisionView> decisions, String group) {
        return decisions.stream().filter(d -> d.businessKey().endsWith("(" + group + ")")).findFirst().orElseThrow();
    }

    @Test
    void aSelectionIsPackagedOnceEvenIfItIsSelectedAgain() {
        try (TestCluster cluster = new TestCluster()) {
            Delivery delivery = new Delivery();
            DtpEngine node = cluster.start("a", delivery);
            cluster.definitions().define(Delivery.class).external().save();
            for (int i = 0; i < 5; i++) {
                delivery.selection.add(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()));
            }
            node.jobEngine().trigger(Delivery.class, new NoParams(), "first");
            await().atMost(Duration.ofSeconds(10)).until(() -> count(cluster, "SELECT COUNT(*) FROM dtp_run"
                    + " WHERE state = 'FINISHED'") == 1);

            // the next selection contains the first one plus two changes
            delivery.selection.add(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()));
            delivery.selection.add(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()));
            node.jobEngine().trigger(Delivery.class, new NoParams(), "second");
            await().atMost(Duration.ofSeconds(10)).until(() -> count(cluster, "SELECT COUNT(*) FROM dtp_run"
                    + " WHERE state = 'FINISHED'") == 2);

            assertThat(delivery.created).containsEntry("packages", 3 + 1);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_item")).isEqualTo(7);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(7);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_item_event WHERE event_type = 'TRACKED'"))
                    .isEqualTo(7);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_work_package WHERE state = 'DONE'")).isEqualTo(4);
        }
    }

    @Test
    void theRunOfACrashedNodeContinuesWithThePendingPackages() {
        try (TestCluster cluster = new TestCluster()) {
            Export first = new Export().slow(500);
            DtpEngine a = cluster.start("a", first);
            cluster.definitions().define(Export.class).inbox(1).save();
            track(cluster, a, groups("A", 3, "B", 3, "C", 3, "D", 3, "E", 3, "F", 3));
            await().atMost(Duration.ofSeconds(10)).until(() -> count(cluster, "SELECT COUNT(*) FROM dtp_work_package"
                    + " WHERE state = 'DONE'") >= 2);

            Export second = new Export();
            cluster.start("b", second);
            a.kill();
            await().atMost(Duration.ofSeconds(15)).until(() -> count(cluster, "SELECT COUNT(*) FROM dtp_work_package"
                    + " WHERE state = 'DONE'") == 6);
            cluster.awaitIdle(Duration.ofSeconds(5));

            // the takeover (RERUN) processed only the pending packages: each package and item exactly once. A package
            // the zombie of node a still holds locked is skipped and processed by a later run of the job.
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_work_package")).isEqualTo(6);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_run_attempt WHERE end_reason = 'LEASE_EXPIRED'"))
                    .isEqualTo(1);
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_item_event WHERE event_type = 'COMPLETED'"))
                    .isEqualTo(18);
            assertThat(count(cluster, "SELECT COUNT(DISTINCT item_id) FROM dtp_item_event"
                    + " WHERE event_type = 'COMPLETED'")).isEqualTo(18);
            assertThat(second.sent).isNotEmpty();
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_run WHERE outcome = 'SUCCEEDED' AND attempt = 2"))
                    .isEqualTo(1);
        }
    }

    @Test
    void aRunStopsAtItsTimeLimitAndTheNextOneContinues() {
        try (TestCluster cluster = new TestCluster()) {
            Export export = new Export().slow(200);
            DtpEngine node = cluster.start("a", export);
            cluster.definitions().define(Export.class).inbox(1).timeLimit(Duration.ofMillis(700)).save();
            track(cluster, node, groups("A", 3, "B", 3, "C", 3, "D", 3, "E", 3, "F", 3, "G", 3, "H", 3));

            await().atMost(Duration.ofSeconds(15)).until(() -> count(cluster, "SELECT COUNT(*) FROM dtp_work_package"
                    + " WHERE state = 'DONE'") == 8);
            cluster.awaitIdle(Duration.ofSeconds(5));
            assertThat(count(cluster, "SELECT COUNT(*) FROM dtp_run WHERE outcome = 'DEFERRED'"
                    + " AND stop_reason = 'LIMIT_REACHED'")).isPositive();
            assertThat(export.sent).hasSize(8);
        }
    }
}
