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
package org.os890.dtp.test.cdi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;

import jakarta.enterprise.inject.spi.BeanManager;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.Test;
import org.os890.dtp.api.FlowEvent;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.test.DtpAssertions;
import org.os890.dtp.test.DtpTest;
import org.os890.dtp.test.DtpTestCluster;
import org.os890.dtp.test.cdi.Parcels.HeavyHandling;
import org.os890.dtp.test.cdi.Parcels.NodeLog;
import org.os890.dtp.test.cdi.Parcels.ParcelCheck;
import org.os890.dtp.test.cdi.Parcels.ParcelFlow;
import org.os890.dtp.test.cdi.Parcels.ParcelShipping;
import org.os890.dtp.test.cdi.Parcels.Scale;

/** The process steps of a test cluster as CDI beans (slice 27): Weld SE, two nodes. */
@DtpTest
class DtpTestCdiTest {

    private static WeldContainer container(Class<?>... beans) {
        return new Weld().disableDiscovery().addBeanClasses(beans).initialize();
    }

    @Test
    void stepsAreBeansFlowEventsAreRoutedOnTheNodeOfTheRunAndDomainEventsAreCdiEvents(DtpTestCluster cluster) {
        try (WeldContainer container = container(FlowEventBridge.class, ParcelFlow.class, ParcelCheck.class,
                HeavyHandling.class, ParcelShipping.class, Scale.class, NodeLog.class)) {
            BeanManager beans = container.getBeanManager();
            cluster.start(2, DtpTestCdi.connect(cluster, beans));

            // started like in production: a FlowEvent, here outside of a run (the first node routes it)
            for (long id = 1; id <= 10; id++) {
                beans.getEvent().select(FlowEvent.class)
                     .fire(FlowEvent.start(ParcelFlow.class, ItemRef.of(Parcels.ITEM, id), "P-" + id));
            }
            cluster.awaitIdle();

            DtpAssertions.assertThat(cluster.journey(Parcels.ITEM, "P-2"))
                         .hasPath(ParcelCheck.class, ParcelShipping.class).isCompleted();
            DtpAssertions.assertThat(cluster.journey(Parcels.ITEM, "P-8"))
                         .hasPath(ParcelCheck.class, HeavyHandling.class, ParcelShipping.class).isCompleted();
            assertThat(cluster.queries().itemCounts()).isNotEmpty();
            // a step's FlowEvent joins the unit of work of its run: the entry names the node of the run
            DtpQueries queries = cluster.queries();
            for (long id = 1; id <= 10; id++) {
                List<JourneyEntry> journey = queries.journey("parcel", "P-" + id);
                assertThat(journey).extracting(JourneyEntry::type).last().isEqualTo("COMPLETED");
                journey.stream().filter(e -> e.runId() != null).forEach(e -> {
                    UUID run = e.runId();
                    assertThat(e.node()).isEqualTo(queries.run(run).orElseThrow().node());
                });
            }
            // the observer of the application heard the nodes join
            assertThat(container.select(NodeLog.class).get().joined()).containsExactly("node-1", "node-2");
        }
    }

    @Test
    void aContainerWithoutTheBridgeIsRefused(DtpTestCluster cluster) {
        try (WeldContainer container = container(ParcelFlow.class, ParcelCheck.class, Scale.class)) {
            assertThatThrownBy(() -> DtpTestCdi.connect(cluster, container.getBeanManager()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining(FlowEventBridge.class.getName());
        }
        try (WeldContainer container = container(FlowEventBridge.class)) {
            assertThatThrownBy(() -> container.getBeanManager().getEvent().select(FlowEvent.class)
                    .fire(FlowEvent.start(ParcelFlow.class, ItemRef.of(Parcels.ITEM, 1L), "P-1")))
                    .hasMessageContaining("isn't connected to a test cluster");
        }
    }
}
