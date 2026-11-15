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
package org.os890.dtp.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.FlowBuilder;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.FlowEvent;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.Signal;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpConfig;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.cluster.TestTrade;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.testsupport.MapStepRegistry;
import org.os890.dtp.core.testsupport.TestCluster;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.trace.data.SpanData;

/**
 * The engine with OpenTelemetry: items tracked inside a request span, two steps, the metrics, the spans of the units
 * of work linked to the request, the journal naming their traces, and the cluster gauges of the projection leader.
 */
class EngineTelemetryTest {

    enum IntakeSignal implements Signal {
        ACCEPTED
    }

    static class Intake implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.signal(IntakeSignal.ACCEPTED);
        }
    }

    static class Booking implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.complete();
        }
    }

    static class BookingFlow implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(Intake.class).on(IntakeSignal.ACCEPTED).to(Booking.class);
        }
    }

    @Test
    void theEngineReportsMetricsAndLinkedSpans() {
        try (TestCluster cluster = new TestCluster(); InMemoryOpenTelemetry otel = new InMemoryOpenTelemetry()) {
            cluster.migrate();
            DtpConfig config = TestCluster.config("a").monitoring(DtpConfig.Monitoring.builder()
                    .interval(Duration.ofMillis(200)).build()).build();
            DtpEngine node = cluster.start(config, new MapStepRegistry(new Intake(), new Booking())
                    .flows(new BookingFlow()), new DtpOpenTelemetry(otel.api(), config.namespace()));
            Span request = otel.api().getTracer("test").spanBuilder("POST /bookings").startSpan();
            Scope scope = request.makeCurrent();
            try {
                cluster.tx().required(() -> {
                    for (int n = 1; n <= 6; n++) {
                        node.fire(FlowEvent.start(BookingFlow.class, ItemRef.of(TestTrade.ITEM, UUID.randomUUID()),
                                "B-" + n));
                    }
                    return null;
                });
            } finally {
                scope.close();
                request.end();
            }
            cluster.awaitIdle(Duration.ofSeconds(30));

            Attributes ns = Attributes.of(DtpOpenTelemetry.NAMESPACE, config.namespace());
            await().atMost(Duration.ofSeconds(10)).until(() -> {
                otel.collect();
                return !otel.points("dtp.cluster.step.jam_score", ns).isEmpty();
            });
            assertThat(otel.points("dtp.items.processed", ns.toBuilder().put(DtpOpenTelemetry.RESULT, "completed")
                    .build())).singleElement().satisfies(p -> assertThat(((LongPointData) p).getValue())
                            .isEqualTo(6));
            assertThat(otel.points("dtp.items.processed", ns.toBuilder().put(DtpOpenTelemetry.RESULT, "routed")
                    .build())).singleElement().satisfies(p -> assertThat(((LongPointData) p).getValue())
                            .isEqualTo(6));
            assertThat(otel.points("dtp.item.wait_time", ns)).hasSize(2);
            assertThat(otel.points("dtp.runs.finished", ns)).isNotEmpty();
            assertThat(otel.points("dtp.leader.elections", ns)).hasSize(2);
            assertThat(otel.points("dtp.cluster.nodes", ns)).hasSize(1);

            // the spans of both steps link to the request; the journal names the traces of the units of work
            List<SpanData> units = otel.spans().stream().filter(s -> s.getName().startsWith("dtp ")).toList();
            assertThat(units).extracting(s -> s.getAttributes().get(DtpOpenTelemetry.STEP)).hasSizeGreaterThan(1)
                    .doesNotHaveDuplicates().isNotEmpty();
            assertThat(units).allSatisfy(s -> assertThat(s.getLinks()).extracting(l -> l.getSpanContext()
                    .getTraceId()).containsExactly(request.getSpanContext().getTraceId()));
            Set<String> traces = units.stream().map(SpanData::getTraceId).collect(Collectors.toSet());
            List<JourneyEntry> journey = node.queries().journey("test-trade", "B-1");
            assertThat(journey).extracting(JourneyEntry::type).containsExactly("TRACKED", "ROUTED", "ROUTED",
                    "COMPLETED");
            // tracked in the request: the first entries carry its trace, the steps their own
            assertThat(journey.getFirst().traceId()).isEqualTo(request.getSpanContext().getTraceId());
            assertThat(journey.subList(2, 4)).allSatisfy(e -> assertThat(e.traceId()).isIn(traces));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE trace_parent LIKE ?",
                    "00-" + request.getSpanContext().getTraceId() + "-%")).isEqualTo(6);
        }
    }
}
