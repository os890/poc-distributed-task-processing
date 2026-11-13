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
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.stream.LongStream;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.Test;
import org.os890.dtp.api.JobEngine;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.query.DtpQueries;

/**
 * The engine starts with the CDI container, discovers the process step beans and applies the definitions of
 * {@code db/dtp-definitions}. Steps are CDI beans with injection.
 */
class CdiIntegrationTest {

    @Test
    void engineRunsCdiBeansAsProcessSteps() {
        try (WeldContainer container = new Weld().disableDiscovery()
                .addBeanClasses(DtpBootstrap.class, TestDataSource.class, Carrier.class, ShipmentPlanning.class,
                        ShipmentDispatch.class)
                .initialize()) {
            DtpEngine engine = container.select(DtpBootstrap.class).get().engine();
            assertThat(engine.isRunning()).isTrue();
            assertThat(engine.config().namespace()).isEqualTo("cdi-test");

            container.select(JobEngine.class).get().trigger(ShipmentPlanning.class, new PlanningParams(5));
            DtpQueries queries = container.select(DtpQueries.class).get();
            await().atMost(Duration.ofSeconds(20)).until(() -> queries.itemCounts().stream()
                    .anyMatch(c -> "COMPLETED".equals(c.state()) && c.count() == 5));

            assertThat(container.select(Carrier.class).get().dispatched())
                    .containsExactlyInAnyOrderElementsOf(LongStream.rangeClosed(1, 5).boxed().toList());
            assertThat(queries.journey("shipment", "S-3")).extracting(DtpQueries.JourneyEntry::type)
                    .containsExactly("TRACKED", "ROUTED", "COMPLETED");
            assertThat(queries.nodes()).singleElement().satisfies(node -> assertThat(node.leader()).isTrue());
        }
    }

    @Test
    void durationsAcceptIsoAndShortFormat() {
        assertThat(DtpSettings.parse("PT2S")).isEqualTo(Duration.ofSeconds(2));
        assertThat(DtpSettings.parse("250ms")).isEqualTo(Duration.ofMillis(250));
        assertThat(DtpSettings.parse("3s")).isEqualTo(Duration.ofSeconds(3));
        assertThat(DtpSettings.parse("1m")).isEqualTo(Duration.ofMinutes(1));
        assertThat(DtpSettings.parse("2h")).isEqualTo(Duration.ofHours(2));
    }
}
