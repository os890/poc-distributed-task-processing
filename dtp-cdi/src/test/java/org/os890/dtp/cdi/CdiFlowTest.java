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
package org.os890.dtp.cdi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.Test;
import org.os890.dtp.api.FlowEvent;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.FlowQueries;

class CdiFlowTest {

    @Test
    void flowsAreBeansAndFlowEventsAreCdiEvents() throws Exception {
        try (WeldContainer container = new Weld().disableDiscovery()
                .addBeanClasses(DtpBootstrap.class, TestDataSource.class, ShipmentFlow.class, ShipmentSorting.class,
                        ShipmentDelivery.class)
                .initialize()) {
            container.select(DtpBootstrap.class).get().engine();
            for (long id = 101; id <= 103; id++) {
                container.getBeanManager().getEvent().select(FlowEvent.class)
                         .fire(FlowEvent.start(ShipmentFlow.class, ItemRef.of(Shipment.ITEM, id), "F-" + id));
            }
            DtpQueries queries = container.select(DtpQueries.class).get();
            await().atMost(Duration.ofSeconds(20)).until(() -> queries.journey("shipment", "F-103").stream()
                    .anyMatch(e -> "COMPLETED".equals(e.type())));

            assertThat(queries.journey("shipment", "F-101")).extracting(DtpQueries.JourneyEntry::type,
                    DtpQueries.JourneyEntry::toJob).containsExactly(
                            tuple("TRACKED", null),
                            tuple("ROUTED", "shipment-sorting"),
                            tuple("ROUTED", "shipment-delivery"),
                            tuple("COMPLETED", null));
            assertThat(queries.journey("shipment", "F-101").get(2).reason()).isEqualTo("hub 1");

            FlowQueries flows = container.select(FlowQueries.class).get();
            assertThat(flows.flows()).extracting(FlowQueries.FlowView::name).contains("shipment-flow");
            assertThat(flows.validate("shipment-flow", null)).extracting(FlowQueries.Issue::message)
                    .containsExactly("shipment-delivery: ShipmentDelivery.DeliverySignal.REFUSED has no route");
            assertThat(Files.readString(Path.of("target", "dtp-flows-cdi.md")))
                    .contains("## shipment-flow", "-->|SORTED|");
        }
    }
}
