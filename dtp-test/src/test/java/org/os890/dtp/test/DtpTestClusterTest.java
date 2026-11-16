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
package org.os890.dtp.test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.os890.dtp.test.DtpAssertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.os890.dtp.api.event.NodeJoined;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.query.DtpQueries.NodeView;
import org.os890.dtp.core.query.DtpQueries.RunView;
import org.os890.dtp.test.Orders.CreditCheck;
import org.os890.dtp.test.Orders.DailyReport;
import org.os890.dtp.test.Orders.IntakeParams;
import org.os890.dtp.test.Orders.OrderCheck;
import org.os890.dtp.test.Orders.OrderFlow;
import org.os890.dtp.test.Orders.OrderIntake;
import org.os890.dtp.test.Orders.Shipping;
import org.os890.dtp.test.Orders.StockCheck;

/** The test support as an application uses it: {@code @DtpTest}, a cluster of simulated nodes and journeys. */
@DtpTest
class DtpTestClusterTest {

    private static Object[] orderSteps(DtpTestCluster cluster, Shipping shipping) {
        return new Object[] {new OrderFlow(), new OrderIntake(cluster.tx()), new OrderCheck(cluster.tx()),
                new CreditCheck(), new StockCheck(), shipping};
    }

    private static void defineOrderJobs(DtpTestCluster cluster) {
        cluster.execute(Orders.TABLE);
        cluster.definitions().define(OrderIntake.class).external().maxItemVisits(3).save();
        // the shipping retries a failed shipment after an hour
        cluster.definitions().define(Shipping.class).inbox(1).retry(3, Duration.ofHours(1)).save();
    }

    @Test
    void aValidOrderIsShippedAndAnOrderWithoutCustomerGoesBack(DtpTestCluster cluster) {
        defineOrderJobs(cluster);
        Shipping shipping = new Shipping();
        List<DtpEngine> nodes = cluster.start(2, orderSteps(cluster, shipping));
        Assertions.assertThat(nodes).extracting(node -> node.config().nodeName()).containsExactly("node-1", "node-2");

        cluster.jobEngine().trigger(OrderIntake.class, new IntakeParams(3));
        cluster.awaitIdle();

        Journey shipped = cluster.journey(Orders.ITEM, "O-1");
        assertThat(shipped)
                .hasVisited(OrderIntake.class, OrderCheck.class, CreditCheck.class, StockCheck.class, Shipping.class)
                .hasEntries("TRACKED", "ROUTED", "FORKED", "BRANCH_FINISHED", "BRANCH_FINISHED", "JOINED",
                        "COMPLETED")
                .isCompleted();
        // the branches finish in any order, the join step follows both
        Assertions.assertThat(shipped.path()).startsWith("OrderIntake", "order-check").endsWith("Shipping")
                  .hasSize(5);
        assertThat(cluster.journey(Orders.ITEM, "O-3"))
                .hasPath(OrderIntake.class, OrderCheck.class, OrderIntake.class)
                .hasNotVisited(Shipping.class)
                .isQueuedIn(OrderIntake.class);
        Assertions.assertThat(shipping.shipped).containsExactlyInAnyOrder("O-1", "O-2");
        Assertions.assertThat(cluster.runningNodes()).hasSize(2);
    }

    @Test
    void theClockMovesPastTheBackoffOfARetry(DtpTestCluster cluster) {
        defineOrderJobs(cluster);
        Shipping shipping = new Shipping();
        shipping.carrierDownFor.add("O-1");
        cluster.start(2, orderSteps(cluster, shipping));

        cluster.jobEngine().trigger(OrderIntake.class, new IntakeParams(2));
        cluster.awaitIdle();

        // the retry is due in an hour: the cluster is idle meanwhile
        assertThat(cluster.journey(Orders.ITEM, "O-1")).hasRetries(Shipping.class, 1).isQueuedIn(Shipping.class);
        assertThat(cluster.journey(Orders.ITEM, "O-2")).isCompleted();
        shipping.carrierDownFor.clear();

        Instant before = cluster.now();
        cluster.advance(Duration.ofMinutes(59));
        cluster.awaitIdle();
        assertThat(cluster.journey(Orders.ITEM, "O-1")).isQueuedIn(Shipping.class);

        cluster.advance(Duration.ofMinutes(1));
        cluster.awaitIdle();
        assertThat(cluster.journey(Orders.ITEM, "O-1")).hasRetries(Shipping.class, 1).isCompleted();
        Assertions.assertThat(cluster.now()).isAfterOrEqualTo(before.plus(Duration.ofHours(1)));
        // the jump didn't look like a crash: both nodes are alive, nothing was taken over
        Assertions.assertThat(cluster.queries().nodes()).extracting(NodeView::state)
                  .containsOnly("ACTIVE");
        Assertions.assertThat(cluster.queries().runs(50)).allSatisfy(run ->
                Assertions.assertThat(run.attempt()).isEqualTo(1));
        Assertions.assertThat(shipping.shipments).hasValue(2);
    }

    @Test
    void aCronJobRunsWhenTheClockReachesItsSlot(DtpTestCluster cluster) {
        cluster.definitions().define(DailyReport.class).cron("0 0 6 * * ?").save();
        DailyReport report = new DailyReport();
        cluster.start("node-a", report);
        cluster.start("node-b", report);

        cluster.awaitIdle();
        Assertions.assertThat(report.runs).hasValue(0);

        LocalDate today = LocalDate.ofInstant(cluster.now(), ZoneOffset.UTC);
        Instant sixToday = today.atTime(6, 0).toInstant(ZoneOffset.UTC);
        Instant nextSix = sixToday.isAfter(cluster.now()) ? sixToday : sixToday.plus(Duration.ofDays(1));
        cluster.advanceTo(nextSix.plusMillis(300));
        cluster.awaitIdle();
        Assertions.assertThat(report.runs).hasValue(1);

        cluster.advance(Duration.ofDays(1));
        cluster.awaitIdle();
        Assertions.assertThat(report.runs).hasValue(2);
        Assertions.assertThat(cluster.queries().runs(10)).extracting(RunView::triggerType)
                  .containsOnly("CRON");
    }

    @Test
    void aKilledNodeIsTakenOverByTheOthers(DtpTestCluster cluster) {
        defineOrderJobs(cluster);
        Shipping shipping = new Shipping();
        cluster.start("node-a", orderSteps(cluster, shipping));
        cluster.start("node-b", orderSteps(cluster, shipping));

        cluster.kill("node-a");
        Assertions.assertThat(cluster.runningNodes()).extracting(node -> node.config().nodeName())
                  .containsExactly("node-b");
        cluster.jobEngine().trigger(OrderIntake.class, new IntakeParams(6));
        cluster.awaitIdle();

        for (String order : List.of("O-1", "O-2", "O-4", "O-5")) {
            assertThat(cluster.journey(Orders.ITEM, order)).isCompleted();
        }
        Assertions.assertThat(shipping.shipments).hasValue(4);
        Assertions.assertThat(cluster.queries().journey(Orders.ITEM.name(), "O-1"))
                  .allSatisfy(entry -> Assertions.assertThat(entry.node()).isIn("node-b", null));

        cluster.stop("node-b");
        Assertions.assertThat(cluster.runningNodes()).isEmpty();
        assertThatThrownBy(cluster::jobEngine).hasMessage("no node runs");
    }

    @Test
    void aFailedAssertionShowsTheJourney(DtpTestCluster cluster) {
        defineOrderJobs(cluster);
        cluster.start(1, orderSteps(cluster, new Shipping()));
        cluster.jobEngine().trigger(OrderIntake.class, new IntakeParams(3));
        cluster.awaitIdle();

        Journey back = cluster.journey(Orders.ITEM, "O-3");
        assertThatThrownBy(() -> assertThat(back).hasPath(OrderIntake.class, OrderCheck.class))
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("expected the path [OrderIntake, order-check], but it was"
                        + " [OrderIntake, order-check, OrderIntake]")
                .hasMessageContaining("journey of order O-3 (QUEUED in OrderIntake)")
                .hasMessageContaining("ROUTED_BACK order-check -> OrderIntake: no customer [node-1]");
        assertThatThrownBy(() -> assertThat(back).isCompleted())
                .hasMessageStartingWith("expected the state COMPLETED, but it is QUEUED");
        assertThatThrownBy(() -> assertThat(back).isQueuedIn(Shipping.class))
                .hasMessageStartingWith("expected the item in the inbox of Shipping, but it is in OrderIntake");
        assertThatThrownBy(() -> assertThat(back).hasVisited(Shipping.class))
                .hasMessageStartingWith("expected a visit of [Shipping]");
        assertThatThrownBy(() -> assertThat(back).hasNotVisited(OrderCheck.class))
                .hasMessageStartingWith("expected no visit of [order-check]");
        assertThatThrownBy(() -> assertThat(back).hasEntries("TRACKED"))
                .hasMessageStartingWith("expected the entries [TRACKED], but they were [TRACKED, ROUTED, ROUTED_BACK]");
        assertThatThrownBy(() -> assertThat(back).hasRetries(OrderCheck.class, 1))
                .hasMessageStartingWith("expected 1 retries in order-check, but there were 0");
        assertThatThrownBy(() -> assertThat(back).isDiscarded()).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> assertThat(back).isWaitingForDecision()).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> back.jobName(String.class)).hasMessage("no job runs java.lang.String");
        Assertions.assertThat(back.toString()).isEqualTo(back.describe());

        Journey none = cluster.journey(Orders.ITEM, "O-99");
        Assertions.assertThat(none.item()).isEmpty();
        Assertions.assertThat(none.path()).isEmpty();
        assertThatThrownBy(() -> assertThat(none)).hasMessage("expected an item, but there is none\n"
                + "journey of order O-99: no such item");
    }

    @Test
    void eachTestHasItsOwnCluster(DtpTestCluster cluster) {
        cluster.placeholder("unused", "x");
        DailyReport report = new DailyReport();
        DtpEngine node = cluster.start(cluster.config("own").maxConcurrentRuns(2).build(), report);
        Assertions.assertThat(cluster.node("own")).isSameAs(node);
        Assertions.assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item")).isZero();
        Assertions.assertThat(cluster.dataSource()).isNotNull();
        // only the events of this cluster: its node joined, nothing else happened
        Assertions.assertThat(cluster.events(Object.class)).singleElement().isInstanceOfSatisfying(NodeJoined.class,
                e -> Assertions.assertThat(e.node()).isEqualTo("own"));

        assertThatThrownBy(() -> cluster.placeholder("late", "x"))
                .hasMessage("the definition scripts were applied already");
        assertThatThrownBy(() -> cluster.start("own", report)).hasMessage("node own exists already");
        assertThatThrownBy(() -> cluster.start(cluster.config("other").namespace("prod").build(), report))
                .hasMessage("the nodes of a test cluster use the namespace dtp-test");
        assertThatThrownBy(() -> cluster.node("missing")).hasMessage("no node missing");
        assertThatThrownBy(() -> cluster.start("x", "no step")).hasMessageStartingWith("neither a process step");
        assertThatThrownBy(() -> cluster.advance(Duration.ZERO)).hasMessageStartingWith("the clock only moves");
        assertThatThrownBy(() -> cluster.tx().required(() -> {
            cluster.advance(Duration.ofSeconds(1));
            return null;
        })).hasMessage("the clock can't be moved inside a transaction");
        assertThatThrownBy(() -> cluster.awaitIdle(Duration.ofMillis(1))).isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("the cluster isn't idle after");
    }
}
