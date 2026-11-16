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
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.api.event.ActiveColorSwitched;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.domain.ConcurrencyPolicy;
import org.os890.dtp.core.engine.ColorAdmin.ColorSwitch;
import org.os890.dtp.core.engine.ColorAdmin.SwitchView;
import org.os890.dtp.core.query.FlowQueries.ActivationView;
import org.os890.dtp.core.query.FlowQueries.FlowView;
import org.os890.dtp.core.testsupport.MapStepRegistry;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Red/green deployments (slice 21): only the nodes of the active color claim runs and lead; a switch lets the old
 * color finish the current unit of work of its runs, the new color continues; the switch activates the flow versions
 * of the new color, switching back is the rollback; a crashed active color is switched back and taken over.
 */
class RedGreenTest {

    private static final String FLOW = "trade-flow";

    @ItemProcessing(batchSize = 1)
    static class SlowStep implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            item.complete();
        }
    }

    static class Ping implements JobHandler<NoParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            return JobResult.success();
        }
    }

    @Test
    void theNewColorTakesOverWhileTheOldOneFinishesItsUnitOfWork() {
        try (TestCluster cluster = new TestCluster()) {
            DtpEngine red1 = node(cluster, "red-1", "red");
            DtpEngine red2 = node(cluster, "red-2", "red");
            cluster.definitions().define(SlowStep.class).inbox(1).save();
            cluster.definitions().define(Ping.class).external().concurrency(ConcurrencyPolicy.QUEUE_ALL).save();
            assertThat(red1.colors().state(10).activeColor()).isNull();

            // the pipeline activates red: both red nodes are live
            ColorSwitch toRed = red1.colors().activate("red", "pipeline", "release 1.0");
            assertThat(toRed).isEqualTo(new ColorSwitch(true, null, "red", List.of("red-1", "red-2"), List.of(),
                    List.of()));

            // green starts on standby: it doesn't claim, even the runs it triggers itself
            DtpEngine green = node(cluster, "green-1", "green");
            assertThat(green.isStandby()).isTrue();
            assertThat(states(red1)).containsExactly(tuple("red-1", "ACTIVE", "red"), tuple("red-2", "ACTIVE", "red"),
                    tuple("green-1", "STANDBY", "green"));
            for (int i = 0; i < 4; i++) {
                green.jobEngine().trigger(Ping.class, new NoParams());
            }
            cluster.awaitIdle(Duration.ofSeconds(10));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run r JOIN dtp_node n ON n.id = r.owner_node_id"
                    + " WHERE n.color = 'red' AND r.outcome = 'SUCCEEDED'")).isEqualTo(4);

            // a red run works through 60 items when green is activated
            cluster.tx().required(() -> {
                for (int i = 1; i <= 60; i++) {
                    red1.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-" + i)
                            .routeTo(SlowStep.class);
                }
                return null;
            });
            await().atMost(Duration.ofSeconds(10)).until(() -> red1.activeRuns() + red2.activeRuns() == 1
                    && cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'") > 5);
            ColorSwitch toGreen = green.colors().activate("green", "pipeline", "release 1.1");
            assertThat(toGreen.changed()).isTrue();
            assertThat(toGreen.previousColor()).isEqualTo("red");
            assertThat(toGreen.nodes()).containsExactly("green-1");

            // red finishes the item at hand and stops (DEFERRED, DRAIN), green continues and takes the lead
            await().atMost(Duration.ofSeconds(10)).until(() -> red1.activeRuns() + red2.activeRuns() == 0
                    && red1.isStandby() && red2.isStandby());
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run r JOIN dtp_node n ON n.id = r.owner_node_id"
                    + " WHERE n.color = 'red' AND r.outcome = 'DEFERRED' AND r.stop_reason = 'DRAIN'")).isEqualTo(1);
            cluster.awaitIdle(Duration.ofSeconds(20));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(60);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item_event WHERE event_type = 'COMPLETED'"))
                    .isEqualTo(60);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE owner_node_id = ? AND outcome = 'SUCCEEDED'",
                    green.nodeId())).isPositive();
            await().atMost(Duration.ofSeconds(10)).until(() -> green.isLeader() && green.isProjectionLeader());
            assertThat(red1.isLeader() || red2.isLeader() || red1.isProjectionLeader() || red2.isProjectionLeader())
                    .isFalse();
            assertThat(states(red1)).containsExactly(tuple("red-1", "STANDBY", "red"),
                    tuple("red-2", "STANDBY", "red"), tuple("green-1", "ACTIVE", "green"));
            red1.jobEngine().trigger(Ping.class, new NoParams());
            cluster.awaitIdle(Duration.ofSeconds(10));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE owner_node_id = ? AND handler_class = ?",
                    green.nodeId(), Ping.class.getName())).isEqualTo(1);

            // a color without nodes, an invalid color, the active color again
            assertThatThrownBy(() -> red1.colors().activate("blue", "pipeline", null))
                    .isInstanceOf(DtpException.class).hasMessageContaining("color blue has no live node");
            assertThatThrownBy(() -> red1.colors().activate("Blue!", "pipeline", null))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(red1.colors().activate("green", "pipeline", "again").changed()).isFalse();
            assertThat(red1.colors().state(10).switches()).extracting(SwitchView::previousColor,
                    SwitchView::activeColor, SwitchView::switchedBy, SwitchView::reason)
                    .containsExactly(tuple("red", "green", "pipeline", "release 1.1"),
                            tuple(null, "red", "pipeline", "release 1.0"));
            assertThat(cluster.events(ActiveColorSwitched.class)).extracting(ActiveColorSwitched::activeColor)
                    .containsExactly("red", "green");
        }
    }

    @Test
    void theSwitchActivatesTheFlowVersionsOfTheNewColorAndSwitchingBackIsTheRollback() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            cluster.execute(TestTrade.TABLE);
            cluster.definitions().define(FlowSwitchTest.TradeImport.class).external().maxItemVisits(5).save();
            var tx = cluster.tx();

            DtpEngine red = node(cluster, "red-1", "red", new FlowSwitchTest.TradeFlowV1(),
                    new FlowSwitchTest.TradeImport(tx), new FlowSwitchTest.TradeValidation(tx));
            red.colors().activate("red", "pipeline", "release 1");
            String v1 = flow(red).activeVersion();

            // green deploys version 2 on standby: stored as a candidate, red keeps version 1
            DtpEngine green = node(cluster, "green-1", "green", new FlowSwitchTest.TradeFlowV2(),
                    new FlowSwitchTest.TradeImport(tx), new FlowSwitchTest.TradeValidation(tx),
                    new FlowSwitchTest.ManualRepair());
            assertThat(flow(red).activeVersion()).isEqualTo(v1);
            String v2 = flow(red).versions().stream().map(v -> v.hash()).filter(h -> !h.equals(v1)).findFirst()
                    .orElseThrow();

            // the switch activates version 2: trades without ISIN are repaired instead of re-imported
            ColorSwitch toGreen = red.colors().activate("green", "pipeline", "release 2");
            assertThat(toGreen.activatedFlows()).containsExactly(FLOW + " " + v2);
            assertThat(toGreen.warnings()).isEmpty();
            assertThat(flow(red).activeVersion()).isEqualTo(v2);
            assertThat(red.flowQueries().activations(FLOW).getFirst()).extracting(ActivationView::version,
                    ActivationView::generated, ActivationView::activatedBy, ActivationView::reason)
                    .containsExactly(v2, false, "pipeline", "color green activated: release 2");
            await().atMost(Duration.ofSeconds(5)).until(red::isStandby);
            green.jobEngine().trigger(FlowSwitchTest.TradeImport.class,
                    new FlowSwitchTest.ImportParams(List.of("")));
            cluster.awaitIdle(Duration.ofSeconds(20));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE business_key = 'T-1'"
                    + " AND state = 'COMPLETED'")).isEqualTo(1);

            // rollback: back to red, which can't run the new manual repair step
            ColorSwitch back = green.colors().activate("red", "ops", "repair bug in release 2");
            assertThat(back.activatedFlows()).containsExactly(FLOW + " " + v1);
            assertThat(back.warnings()).singleElement().asString()
                    .contains("no node of color red can run job manual-repair");
            assertThat(flow(red).activeVersion()).isEqualTo(v1);

            // a green node with the old version: the nodes of green disagree, no version is activated
            node(cluster, "green-2", "green", new FlowSwitchTest.TradeFlowV1(), new FlowSwitchTest.TradeImport(tx),
                    new FlowSwitchTest.TradeValidation(tx));
            ColorSwitch mixed = red.colors().activate("green", "pipeline", "release 2 again");
            assertThat(mixed.activatedFlows()).isEmpty();
            assertThat(mixed.warnings()).anySatisfy(w -> assertThat(w)
                    .contains("the nodes of color green run different versions of flow " + FLOW));
            assertThat(flow(red).activeVersion()).isEqualTo(v1);
        }
    }

    @Test
    void aCrashedActiveColorIsSwitchedBackAndTheOldColorTakesOver() {
        try (TestCluster cluster = new TestCluster()) {
            DtpEngine red = node(cluster, "red-1", "red");
            cluster.definitions().define(SlowStep.class).inbox(1).save();
            cluster.definitions().define(Ping.class).external().save();
            DtpEngine green = node(cluster, "green-1", "green");
            red.colors().activate("green", "pipeline", "release 2");
            await().atMost(Duration.ofSeconds(5)).until(() -> red.isStandby() && green.isLeader());

            cluster.tx().required(() -> {
                for (int i = 1; i <= 40; i++) {
                    green.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-" + i)
                            .routeTo(SlowStep.class);
                }
                return null;
            });
            await().atMost(Duration.ofSeconds(10)).until(() -> green.activeRuns() == 1
                    && cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'") > 3);
            assertThat(red.activeRuns()).isZero();

            // green crashes: red is still live on standby, so the operator switches back; red leads, its reaper takes
            // the expired lease of green over and reruns the run (RERUN)
            green.kill();
            ColorSwitch back = red.colors().activate("red", "ops", "green crashed");
            assertThat(back.changed()).isTrue();
            await().atMost(Duration.ofSeconds(10)).until(red::isLeader);
            cluster.awaitIdle(Duration.ofSeconds(30));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(40);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE owner_node_id = ? AND outcome = 'SUCCEEDED'",
                    red.nodeId())).isPositive();

            // without an active color, all colors claim again
            assertThat(red.colors().activate(null, "ops", "only red is left").activeColor()).isNull();
            assertThat(red.isStandby()).isFalse();
        }
    }

    private static DtpEngine node(TestCluster cluster, String name, String color, ProcessStep... steps) {
        ProcessStep[] all = steps.length > 0 ? steps : new ProcessStep[] {new SlowStep(), new Ping()};
        return cluster.start(TestCluster.config(name).color(color).build(), new MapStepRegistry(all));
    }

    private static DtpEngine node(TestCluster cluster, String name, String color, FlowDefinition flow,
                                  ProcessStep... steps) {
        return cluster.start(TestCluster.config(name).color(color).build(), new MapStepRegistry(steps).flows(flow));
    }

    private static List<Tuple> states(DtpEngine node) {
        return node.queries().nodes().stream()
                .map(n -> tuple(n.name(), n.state(), n.color()))
                .toList();
    }

    private static FlowView flow(DtpEngine node) {
        return node.flowQueries().flows().stream().filter(f -> f.name().equals(FLOW)).findFirst().orElseThrow();
    }
}
