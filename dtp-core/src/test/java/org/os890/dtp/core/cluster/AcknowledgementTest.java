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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.event.PackageCompleted;
import org.os890.dtp.api.event.PackageRejected;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.engine.Acknowledgements;
import org.os890.dtp.core.engine.Acknowledgements.Outcome;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.engine.Decisions.Decider;
import org.os890.dtp.core.engine.Decisions.DecisionView;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.query.DtpQueries.PackageView;
import org.os890.dtp.core.query.JourneyGraph.Node;
import org.os890.dtp.core.query.JourneyGraph.NodeKind;
import org.os890.dtp.core.query.JourneyQueries;
import org.os890.dtp.core.testsupport.MapStepRegistry;
import org.os890.dtp.core.testsupport.RecordingTelemetry;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Acknowledgements of sent work packages (slice 14, mission D37): the receiver answers later, on any node; a
 * rejection or a missing answer is a failed attempt with the retry policy of the job, then a decision.
 */
class AcknowledgementTest {

    private static final Decider OTTO = Decider.of("otto", Decisions.OPERATORS);

    /** Packages its inbox per group and sends each one as a file. */
    static class Sender implements JobHandler<NoParams, DefaultOutcome> {

        final Map<String, Integer> sends = new ConcurrentHashMap<>();

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            ctx.packages(TestTrade.ITEM).createFromInbox(item -> item.businessKey().split("-")[1], 10);
            ctx.packages(TestTrade.ITEM).forEachPending(pkg -> {
                String file = file(pkg.groupKey(), pkg.sequence());
                sends.merge(file, 1, Integer::sum);
                pkg.sent(file);
            });
            return JobResult.success();
        }
    }

    /** Routes the items before it sends the package. */
    static class HastySender implements JobHandler<NoParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            ctx.packages(TestTrade.ITEM).createFromInbox(item -> "ALL", 10);
            ctx.packages(TestTrade.ITEM).forEachPending(pkg -> {
                pkg.items().forEach(item -> item.complete());
                pkg.sent("hasty-" + pkg.sequence());
            });
            return JobResult.success();
        }
    }

    @Test
    void theReceiverAcknowledgesOrRejectsOnAnyNode() {
        try (TestCluster cluster = new TestCluster()) {
            Sender sender = new Sender();
            RecordingTelemetry ta = new RecordingTelemetry();
            RecordingTelemetry tb = new RecordingTelemetry();
            DtpEngine a = cluster.start(TestCluster.config("a").build(), new MapStepRegistry(sender), ta);
            cluster.definitions().define(Sender.class).inbox(1).retry(1, Duration.ofMillis(200)).acknowledged(null)
                   .save();
            DtpEngine b = cluster.start(TestCluster.config("b").build(), new MapStepRegistry(sender), tb);
            track(cluster, a, "AT", "AT", "DE");

            await().atMost(Duration.ofSeconds(10)).until(() -> state(cluster, "SENT") == 2);
            cluster.awaitIdle(Duration.ofSeconds(5)); // nothing to do while the packages wait for their answers
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'PACKAGED'")).isEqualTo(3);
            String at = fileOf(a, "AT");
            String de = fileOf(a, "DE");
            assertThat(a.queries().packages(10)).filteredOn(p -> p.reference().equals(at)).singleElement()
                    .satisfies(p -> {
                        assertThat(p.sentAt()).isNotNull();
                        assertThat(p.ackDueAt()).isNull();
                    });

            // the answers arrive on the other node
            Acknowledgements answers = b.acknowledgements();
            assertThat(answers.acknowledge(Sender.class, at, "custodian AT")).isEqualTo(Outcome.ACKNOWLEDGED);
            assertThat(answers.acknowledge(Sender.class, at, "custodian AT")).isEqualTo(Outcome.NOT_WAITING);
            assertThat(answers.acknowledge(Sender.class, "unknown.xml", null)).isEqualTo(Outcome.UNKNOWN);
            assertThat(answers.acknowledge(UUID.randomUUID(), null)).isEqualTo(Outcome.UNKNOWN);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(2);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_work_package WHERE acknowledged_by = 'custodian AT'"))
                    .isEqualTo(1);

            // a rejection is a failed attempt: sent again after the backoff with the same file name
            assertThat(answers.reject(Sender.class, de, "DE rejected the file")).isEqualTo(Outcome.REJECTED);
            await().atMost(Duration.ofSeconds(10))
                    .until(() -> sender.sends.get(de) == 2 && state(cluster, "SENT") == 1);
            // rejected again: the retries are exhausted, a person decides (business failure: no team)
            assertThat(answers.reject(Sender.class, de, "DE rejected the file again")).isEqualTo(Outcome.REJECTED);
            assertThat(state(cluster, "WAITING_FOR_DECISION")).isEqualTo(1);
            DecisionView decision = a.decisions().open(OTTO).getFirst();
            assertThat(decision.reason()).contains("retries exhausted (1): DE rejected the file again");
            assertThat(decision.responsibleGroup()).isNull();
            a.decisions().decide(decision.id(), "RETRY", OTTO, "fixed the instructions");
            await().atMost(Duration.ofSeconds(10))
                    .until(() -> sender.sends.get(de) == 3 && state(cluster, "SENT") == 1);
            UUID dePackage = a.queries().packages(10).stream().filter(p -> p.reference().equals(de)).findFirst()
                              .orElseThrow().id();
            assertThat(answers.acknowledge(dePackage, "custodian DE")).isEqualTo(Outcome.ACKNOWLEDGED);
            assertThat(answers.reject(dePackage, "too late")).isEqualTo(Outcome.NOT_WAITING);
            assertThat(state(cluster, "DONE")).isEqualTo(2);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(3);

            // the journal and the journey show every send and answer
            List<JourneyEntry> journey = a.queries().journey(TestTrade.ITEM.name(), "T-DE-3");
            assertThat(journey).extracting(JourneyEntry::type).containsSubsequence("PACKAGED", "SENT", "REJECTED",
                    "SENT", "REJECTED", "SENT", "ACKNOWLEDGED", "COMPLETED");
            assertThat(journey).filteredOn(e -> e.type().equals("REJECTED")).extracting(JourneyEntry::reason)
                    .containsExactly("DE rejected the file", "DE rejected the file again");
            assertThat(journey).filteredOn(e -> e.type().equals("SENT"))
                    .allSatisfy(e -> assertThat(e.reason()).isEqualTo(de));
            Node pkg = a.journeys().graph(TestTrade.ITEM.name(), "T-DE-3", JourneyQueries.Options.defaults())
                        .orElseThrow().nodes().stream().filter(n -> n.kind() == NodeKind.PACKAGE).findFirst()
                        .orElseThrow();
            assertThat(pkg.lines()).contains("sent 3×").anyMatch(l -> l.startsWith("attempt 2 rejected: DE"))
                    .anyMatch(l -> l.startsWith("acknowledged "));

            // telemetry: sent by the sending nodes, answered on node b
            assertThat(Stream.concat(ta.items.stream(), tb.items.stream()).filter(i -> i.result().equals("sent")))
                    .hasSize(2 + 1 + 1 + 1);
            assertThat(tb.acknowledged).containsExactly("Sender", "Sender");
            assertThat(tb.rejected).containsExactly("Sender:rejected", "Sender:rejected");

            // domain events: one per answer, the second rejection exhausted the retries
            assertThat(cluster.events(PackageCompleted.class)).extracting(e -> e.pkg().reference(),
                    PackageCompleted::acknowledgedBy).containsExactly(tuple(at, "custodian AT"),
                    tuple(de, "custodian DE"));
            assertThat(cluster.events(PackageRejected.class)).allSatisfy(e -> {
                assertThat(e.pkg().job()).isEqualTo("Sender");
                assertThat(e.pkg().groupKey()).isEqualTo("DE");
                assertThat(e.pkg().items()).isEqualTo(1);
                assertThat(e.timeout()).isFalse();
            }).extracting(PackageRejected::reason, PackageRejected::retriesExhausted).containsExactly(
                    tuple("DE rejected the file", false), tuple("DE rejected the file again", true));
        }
    }

    @Test
    void aMissingAcknowledgementIsATechnicalRejection() {
        try (TestCluster cluster = new TestCluster()) {
            Sender sender = new Sender();
            RecordingTelemetry telemetry = new RecordingTelemetry();
            DtpEngine a = cluster.start(TestCluster.config("a").build(), new MapStepRegistry(sender), telemetry);
            cluster.definitions().define(Sender.class).inbox(1).retry(1, Duration.ofMillis(100))
                   .acknowledged(Duration.ofMillis(300)).save();
            track(cluster, a, "CH");

            // sent, no answer in time, sent again, no answer again: operations decide (technical failure)
            await().atMost(Duration.ofSeconds(10)).until(() -> state(cluster, "WAITING_FOR_DECISION") == 1);
            String file = fileOf(a, "CH");
            assertThat(sender.sends).containsEntry(file, 2);
            assertThat(a.queries().journey(TestTrade.ITEM.name(), "T-CH-1"))
                    .filteredOn(e -> e.type().equals("REJECTED")).extracting(JourneyEntry::reason)
                    .containsExactly("no acknowledgement within PT0.3S", "no acknowledgement within PT0.3S");
            DecisionView decision = a.decisions().open(OTTO).getFirst();
            assertThat(decision.responsibleGroup()).isEqualTo(Decisions.OPERATORS);
            assertThat(telemetry.rejected).containsExactly("Sender:timeout", "Sender:timeout");
            assertThat(cluster.events(PackageRejected.class)).allSatisfy(e -> assertThat(e.timeout()).isTrue())
                    .extracting(PackageRejected::retriesExhausted).containsExactly(false, true);
            // a late answer finds nothing to acknowledge
            assertThat(a.acknowledgements().acknowledge(Sender.class, file, null)).isEqualTo(Outcome.NOT_WAITING);
        }
    }

    @Test
    void withoutAcknowledgementASentPackageIsDone() {
        try (TestCluster cluster = new TestCluster()) {
            Sender sender = new Sender();
            HastySender hasty = new HastySender();
            DtpEngine a = cluster.start("a", sender, hasty);
            cluster.definitions().define(Sender.class).inbox(1).save();
            cluster.definitions().define(HastySender.class).inbox(1).retry(0, Duration.ofMillis(100))
                   .acknowledged(null).save();
            track(cluster, a, "AT");
            cluster.tx().required(() -> {
                a.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-HASTY-9").routeTo(HastySender.class);
                return null;
            });

            await().atMost(Duration.ofSeconds(10)).until(() -> state(cluster, "DONE") == 1
                    && state(cluster, "WAITING_FOR_DECISION") == 1);
            PackageView done = a.queries().packages(10).stream().filter(p -> p.state().equals("DONE")).findFirst()
                                .orElseThrow();
            assertThat(done.reference()).isEqualTo(fileOf(a, "AT"));
            assertThat(cluster.events(PackageCompleted.class)).singleElement().satisfies(e -> {
                assertThat(e.pkg().id()).isEqualTo(done.id());
                assertThat(e.pkg().reference()).isEqualTo(done.reference());
                assertThat(e.pkg().items()).isEqualTo(1);
                assertThat(e.acknowledgedBy()).isNull();
            });
            assertThat(done.sentAt()).isNotNull();
            assertThat(a.queries().journey(TestTrade.ITEM.name(), "T-AT-1")).extracting(JourneyEntry::type)
                    .doesNotContain("SENT").endsWith("COMPLETED");
            // items routed before the package waits for its acknowledgement: a technical failure, nothing committed
            PackageView hastyPackage = a.queries().packages(10).stream()
                                        .filter(p -> p.state().equals("WAITING_FOR_DECISION")).findFirst()
                                        .orElseThrow();
            assertThat(hastyPackage.lastError()).contains("items were routed already, it can't wait for an"
                    + " acknowledgement");
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE business_key = 'T-HASTY-9'"
                    + " AND state = 'PACKAGED'")).isEqualTo(1);
        }
    }

    private static String file(String group, long seq) {
        return "file-" + group + "-" + seq + ".xml";
    }

    private static String fileOf(DtpEngine node, String group) {
        PackageView pkg = node.queries().packages(10).stream().filter(p -> group.equals(p.groupKey())).findFirst()
                              .orElseThrow();
        return file(group, pkg.seq());
    }

    private static int state(TestCluster cluster, String state) {
        return cluster.count("SELECT COUNT(*) FROM dtp_work_package WHERE state = ?", state);
    }

    private static void track(TestCluster cluster, DtpEngine node, String... groups) {
        cluster.tx().required(() -> {
            int i = 0;
            for (String group : groups) {
                node.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-" + group + "-" + ++i)
                        .routeTo(Sender.class);
            }
            return null;
        });
    }
}
