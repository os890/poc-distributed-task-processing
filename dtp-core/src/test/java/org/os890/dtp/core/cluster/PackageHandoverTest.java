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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.engine.Decisions.DecisionView;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.query.DtpQueries.PackageView;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Packages handed over (slice 9): a collector groups what is pending in its inbox, other jobs process the packages
 * with their own runs and retry policies.
 */
class PackageHandoverTest {

    /** Groups its inbox per counterparty (the business key's middle part) for the job of the group. */
    static class Collector implements JobHandler<NoParams, DefaultOutcome> {

        private volatile Class<? extends JobHandler<?, ?>> domestic = Sender.class;
        private volatile Class<? extends JobHandler<?, ?>> international = Sender.class;

        Collector split(Class<? extends JobHandler<?, ?>> domesticStep,
                        Class<? extends JobHandler<?, ?>> internationalStep) {
            domestic = domesticStep;
            international = internationalStep;
            return this;
        }

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            ctx.packages(TestTrade.ITEM).forSteps(group -> group.startsWith("AT") ? domestic : international)
                    .createFromInbox(item -> item.businessKey().split("-")[1], 2);
            return JobResult.success();
        }
    }

    /** Sends the packages created for it. */
    static class Sender implements JobHandler<NoParams, DefaultOutcome> {

        final Queue<String> sent = new ConcurrentLinkedQueue<>();
        final Set<String> rejected = ConcurrentHashMap.newKeySet();
        final Map<String, String> runs = new ConcurrentHashMap<>();
        private volatile long delayMillis;

        Sender slow(long millis) {
            delayMillis = millis;
            return this;
        }

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            ctx.packages(TestTrade.ITEM).forEachPending(pkg -> {
                sleep(delayMillis);
                if (rejected.contains(pkg.groupKey())) {
                    pkg.fail("receiver rejected " + pkg.groupKey());
                    return;
                }
                sent.add(pkg.groupKey() + ":" + pkg.items().size());
                pkg.done();
            });
            return JobResult.success();
        }
    }

    /** A second sender, e.g. for international counterparties. */
    static class InternationalSender extends Sender {
    }

    /** Not a target for packages: an ItemProcessor has no packages to process. */
    static class Printer implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.complete();
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

    private static void track(TestCluster cluster, DtpEngine node, String... groups) {
        cluster.tx().required(() -> {
            int i = 0;
            for (String group : groups) {
                node.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-" + group + "-" + ++i)
                        .routeTo(Collector.class);
            }
            return null;
        });
    }

    @Test
    void theCollectorHandsItsPackagesOverToTheSender() {
        try (TestCluster cluster = new TestCluster()) {
            Collector collector = new Collector();
            Sender sender = new Sender();
            sender.rejected.add("DE");
            DtpEngine node = cluster.start("a", collector, sender);
            cluster.definitions().define(Collector.class).inbox(1).save();
            // the retry policy of the sender applies to the packages, not the one of the collector
            cluster.definitions().define(Sender.class).inbox(1).retry(1, Duration.ofMillis(200)).save();
            track(cluster, node, "AT", "AT", "AT", "DE", "CH");

            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_work_package"
                    + " WHERE state = 'DONE'") == 3 && cluster.count("SELECT COUNT(*) FROM dtp_work_package"
                    + " WHERE state = 'WAITING_FOR_DECISION'") == 1);
            cluster.awaitIdle(Duration.ofSeconds(5));

            // AT (2 + 1), DE, CH: created by the collector, processed by the sender
            List<PackageView> packages = node.queries().packages(10);
            assertThat(packages).hasSize(4).allSatisfy(p -> {
                assertThat(p.collector()).isEqualTo("Collector");
                assertThat(p.job()).isEqualTo("Sender");
            });
            assertThat(sender.sent).containsExactlyInAnyOrder("AT:2", "AT:1", "CH:1");
            assertThat(cluster.count("SELECT COUNT(DISTINCT p.processed_by_run_id) FROM dtp_work_package p"
                    + " JOIN dtp_run r ON r.id = p.processed_by_run_id JOIN dtp_job j ON j.id = r.job_id"
                    + " WHERE j.name = 'Sender'")).isPositive();
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_work_package p JOIN dtp_run r"
                    + " ON r.id = p.processed_by_run_id JOIN dtp_job j ON j.id = r.job_id"
                    + " WHERE j.name = 'Collector'")).isZero();

            // the items moved to the sender with their packages
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item i JOIN dtp_job j ON j.id = i.current_job_id"
                    + " WHERE j.name = 'Sender'")).isEqualTo(5);
            List<JourneyEntry> journey = node.queries().journey(TestTrade.ITEM.name(), "T-CH-5");
            assertThat(journey).anySatisfy(e -> {
                assertThat(e.type()).isEqualTo("PACKAGED");
                assertThat(e.fromJob()).isEqualTo("Collector");
                assertThat(e.toJob()).isEqualTo("Sender");
            });
            assertThat(journey.getLast().type()).isEqualTo("COMPLETED");

            // DE: two attempts with the sender's policy, then a decision of the sender's job; RETRY sends it
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_work_package WHERE group_key = 'DE'"
                    + " AND attempts = 2")).isEqualTo(1);
            List<DecisionView> open = node.decisions().open(Decisions.Decider.of("bea"));
            assertThat(open).singleElement().satisfies(d -> assertThat(d.businessKey()).endsWith("(DE)"));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_decision_request d JOIN dtp_job j"
                    + " ON j.id = d.requested_by_job_id"
                    + " WHERE j.name = 'Sender'")).isEqualTo(1);
            sender.rejected.clear();
            node.decisions().decide(open.getFirst().id(), "RETRY", Decisions.Decider.of("bea"), "receiver fixed");
            await().atMost(Duration.ofSeconds(10)).until(() -> sender.sent.contains("DE:1"));
            cluster.awaitIdle(Duration.ofSeconds(5));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(5);
        }
    }

    @Test
    void groupsAreSplitBetweenSendersWhichWorkInParallel() {
        try (TestCluster cluster = new TestCluster()) {
            Collector collector = new Collector().split(Sender.class, InternationalSender.class);
            Sender domestic = new Sender().slow(300);
            InternationalSender international = new InternationalSender();
            international.slow(300);
            DtpEngine a = cluster.start("a", collector, domestic, international);
            cluster.start("b", collector, domestic, international);
            cluster.definitions().define(Collector.class).inbox(1).save();
            cluster.definitions().define(Sender.class).inbox(1).save();
            cluster.definitions().define(InternationalSender.class).inbox(1).save();
            track(cluster, a, "AT1", "AT1", "AT1", "AT2", "AT2", "DE", "DE", "CH", "CH", "CH");

            await().atMost(Duration.ofSeconds(15)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_item"
                    + " WHERE state = 'COMPLETED'") == 10);
            cluster.awaitIdle(Duration.ofSeconds(5));
            assertThat(domestic.sent).containsExactlyInAnyOrder("AT1:2", "AT1:1", "AT2:2");
            assertThat(international.sent).containsExactlyInAnyOrder("DE:2", "CH:2", "CH:1");
            // one job lease per sender: the two senders overlapped, each package was processed once
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run r1 JOIN dtp_job j1 ON j1.id = r1.job_id,"
                    + " dtp_run r2 JOIN dtp_job j2 ON j2.id = r2.job_id WHERE j1.name = 'Sender'"
                    + " AND j2.name = 'InternationalSender' AND r1.started_at < r2.finished_at"
                    + " AND r2.started_at < r1.finished_at")).isPositive();
            assertThat(cluster.count("SELECT COUNT(DISTINCT item_id) FROM dtp_item_event"
                    + " WHERE event_type = 'COMPLETED'")).isEqualTo(10);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item_event WHERE event_type = 'COMPLETED'"))
                    .isEqualTo(10);
        }
    }

    @Test
    void packagesCanOnlyBeCreatedForAJobHandler() {
        try (TestCluster cluster = new TestCluster()) {
            Collector collector = new Collector();
            collector.split(Sender.class, rawHandler(Printer.class));
            DtpEngine node = cluster.start("a", collector, new Sender(), new Printer());
            cluster.definitions().define(Collector.class).inbox(1).retry(0, Duration.ofMillis(100)).save();
            cluster.definitions().define(Sender.class).inbox(1).save();
            cluster.definitions().define(Printer.class).inbox(1).save();
            track(cluster, node, "AT", "DE");

            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_run r"
                    + " JOIN dtp_job j ON j.id = r.job_id WHERE j.name = 'Collector' AND r.state = 'FINISHED'") > 0);
            // the chunk is rolled back: no package, the items stay in the collector's inbox
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_work_package")).isZero();
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item i JOIN dtp_job j ON j.id = i.current_job_id"
                    + " WHERE j.name = 'Collector' AND i.state = 'QUEUED'")).isEqualTo(2);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run r JOIN dtp_job j ON j.id = r.job_id"
                    + " WHERE j.name = 'Collector' AND r.outcome = 'FAILED'")).isPositive();
        }
    }

    /** Bypasses the compile-time check, like a raw type or a class name from the configuration would. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Class<? extends JobHandler<?, ?>> rawHandler(Class<?> type) {
        return (Class) type;
    }
}
