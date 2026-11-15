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

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.os890.dtp.core.spi.ClusterState;
import org.os890.dtp.core.spi.Telemetry;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.trace.data.SpanData;

/** The instruments, attributes and spans of {@link DtpOpenTelemetry}. */
class DtpOpenTelemetryTest {

    private static final Attributes CHECK = Attributes.of(DtpOpenTelemetry.NAMESPACE, "trades",
            DtpOpenTelemetry.STEP, "trade-check");

    @Test
    void perNodeMetricsCarryBoundedAttributes() {
        try (InMemoryOpenTelemetry otel = new InMemoryOpenTelemetry()) {
            DtpOpenTelemetry telemetry = new DtpOpenTelemetry(otel.api(), "trades");
            telemetry.itemProcessed("trade-check", "VIP", "routed", null);
            telemetry.itemProcessed("trade-check", "VIP", "routed", null);
            telemetry.itemProcessed("trade-check", null, "failed", "TECHNICAL");
            telemetry.itemWaited("trade-check", "VIP", Duration.ofMillis(1500));
            telemetry.technicalFailure("trade-check", "java.lang.IllegalStateException");
            telemetry.unitOfWork("trade-check", Duration.ofMillis(20));
            telemetry.rollback("trade-check", "lease_lost");
            telemetry.runFinished("trade-check", "SUCCEEDED", Duration.ofSeconds(2));
            telemetry.claimed(Duration.ofMillis(300));
            telemetry.claimConflict();
            telemetry.leaseRenewed("trade-check");
            telemetry.leaseLost("trade-check");
            telemetry.leaseTakenOver("trade-check");
            telemetry.leaderElected("projection");
            telemetry.decisionCreated("trade-check", "APPROVAL", "risk");
            telemetry.decisionDecided("trade-check", "CHOICE", null);
            otel.collect();

            assertThat(sum(otel, "dtp.items.processed", CHECK.toBuilder().put(DtpOpenTelemetry.LANE, "VIP")
                    .put(DtpOpenTelemetry.RESULT, "routed").build())).isEqualTo(2);
            assertThat(sum(otel, "dtp.items.processed", CHECK.toBuilder().put(DtpOpenTelemetry.LANE, "")
                    .put(DtpOpenTelemetry.FAILURE_KIND, "TECHNICAL").build())).isEqualTo(1);
            HistogramPointData wait = (HistogramPointData) otel.points("dtp.item.wait_time", CHECK).getFirst();
            assertThat(wait.getCount()).isEqualTo(1);
            assertThat(wait.getSum()).isEqualTo(1.5);
            assertThat(otel.metric("dtp.item.wait_time").map(MetricData::getUnit)).contains("s");
            assertThat(sum(otel, "dtp.failures.technical", CHECK.toBuilder()
                    .put(DtpOpenTelemetry.ERROR_TYPE, "java.lang.IllegalStateException").build())).isEqualTo(1);
            assertThat(sum(otel, "dtp.unit_of_work.rollbacks", CHECK.toBuilder()
                    .put(DtpOpenTelemetry.ROLLBACK_REASON, "lease_lost").build())).isEqualTo(1);
            assertThat(sum(otel, "dtp.runs.finished", CHECK.toBuilder()
                    .put(DtpOpenTelemetry.RUN_OUTCOME, "SUCCEEDED").build())).isEqualTo(1);
            assertThat(((HistogramPointData) otel.points("dtp.run.duration", CHECK).getFirst()).getSum())
                    .isEqualTo(2.0);
            assertThat(otel.points("dtp.unit_of_work.duration", CHECK)).hasSize(1);
            assertThat(otel.points("dtp.claim.duration", Attributes.empty())).hasSize(1);
            for (String counter : List.of("dtp.claim.conflicts", "dtp.lease.renewals", "dtp.lease.lost",
                    "dtp.lease.takeovers", "dtp.leader.elections", "dtp.decisions.created",
                    "dtp.decisions.decided")) {
                assertThat(sum(otel, counter, Attributes.of(DtpOpenTelemetry.NAMESPACE, "trades")))
                        .as(counter).isEqualTo(1);
            }
            assertThat(sum(otel, "dtp.decisions.decided", CHECK.toBuilder().put(DtpOpenTelemetry.GROUP, "")
                    .put(DtpOpenTelemetry.DECISION_KIND, "CHOICE").build())).isEqualTo(1);
            assertThat(sum(otel, "dtp.leader.elections", Attributes.of(DtpOpenTelemetry.LEADER_ROLE, "projection")))
                    .isEqualTo(1);
        }
    }

    @Test
    void clusterGaugesAreReportedOnlyWhileLeading() {
        try (InMemoryOpenTelemetry otel = new InMemoryOpenTelemetry()) {
            DtpOpenTelemetry telemetry = new DtpOpenTelemetry(otel.api(), "trades");
            otel.collect();
            assertThat(otel.metric("dtp.cluster.step.jam_score")).isEmpty();

            telemetry.clusterState(new ClusterState(
                    List.of(new ClusterState.Step("trade-check", "PROCESSING", 42.5, 76, 1)),
                    List.of(new ClusterState.Queue("trade-check", "VIP", 3), new ClusterState.Queue("trade-check",
                            "", 12)),
                    List.of(new ClusterState.Waiting("trade-check", "risk", 2)),
                    List.of(new ClusterState.OpenDecisions("risk", 90)),
                    List.of(new ClusterState.Nodes("", "ACTIVE", 3))));
            otel.collect();
            assertThat(longValue(otel, "dtp.cluster.step.jam_score", CHECK.toBuilder()
                    .put(DtpOpenTelemetry.STATUS_REASON, "PROCESSING").build())).isEqualTo(76);
            assertThat(longValue(otel, "dtp.cluster.step.alert_state", CHECK)).isEqualTo(1);
            assertThat(longValue(otel, "dtp.cluster.queue.depth", CHECK.toBuilder().put(DtpOpenTelemetry.LANE,
                    "VIP").build())).isEqualTo(3);
            assertThat(otel.points("dtp.cluster.queue.depth", CHECK)).hasSize(2);
            assertThat(otel.points("dtp.cluster.queue.oldest_age", CHECK)).singleElement()
                    .satisfies(p -> assertThat(((io.opentelemetry.sdk.metrics.data.DoublePointData) p).getValue())
                            .isEqualTo(42.5));
            assertThat(longValue(otel, "dtp.cluster.items.waiting_for_decision", CHECK.toBuilder()
                    .put(DtpOpenTelemetry.GROUP, "risk").build())).isEqualTo(2);
            assertThat(otel.points("dtp.cluster.decisions.oldest_open_age", Attributes.of(DtpOpenTelemetry.GROUP,
                    "risk"))).hasSize(1);
            assertThat(longValue(otel, "dtp.cluster.nodes", Attributes.of(DtpOpenTelemetry.NODE_STATE, "ACTIVE")))
                    .isEqualTo(3);

            // revoked: another node reports the gauges now
            telemetry.clusterState(null);
            otel.collect();
            assertThat(otel.points("dtp.cluster.step.jam_score", CHECK)).isEmpty();
            assertThat(otel.points("dtp.cluster.nodes", Attributes.empty())).isEmpty();
        }
    }

    @Test
    void unitsOfWorkAreSpansLinkedToTheTracesOfTheirItems() {
        try (InMemoryOpenTelemetry otel = new InMemoryOpenTelemetry()) {
            DtpOpenTelemetry telemetry = new DtpOpenTelemetry(otel.api(), "trades");
            assertThat(telemetry.tracing()).isTrue();
            assertThat(telemetry.currentTraceParent()).isNull();
            assertThat(telemetry.currentTraceId()).isNull();
            Span request = otel.api().getTracer("test").spanBuilder("POST /trades").startSpan();
            String parent;
            Scope scope = request.makeCurrent();
            try {
                parent = telemetry.currentTraceParent();
                assertThat(telemetry.currentTraceId()).isEqualTo(request.getSpanContext().getTraceId());
            } finally {
                scope.close();
                request.end();
            }
            assertThat(parent).isEqualTo("00-" + request.getSpanContext().getTraceId() + "-"
                    + request.getSpanContext().getSpanId() + "-01");

            UUID runId = UUID.randomUUID();
            String inside;
            try (Telemetry.Span span = telemetry.startUnitOfWork("trade-check", runId, 2,
                    List.of(parent, "not a traceparent"))) {
                inside = telemetry.currentTraceId();
                span.items(3);
                span.failed(new IllegalStateException("limit service down"));
            }
            assertThat(telemetry.currentTraceId()).isNull();

            SpanData unit = otel.spans().stream().filter(s -> s.getName().equals("dtp trade-check")).findFirst()
                                .orElseThrow();
            assertThat(unit.getTraceId()).isEqualTo(inside).isNotEqualTo(request.getSpanContext().getTraceId());
            assertThat(unit.getKind()).isEqualTo(SpanKind.CONSUMER);
            assertThat(unit.getParentSpanContext().isValid()).isFalse();
            assertThat(unit.getAttributes().get(DtpOpenTelemetry.RUN_ID)).isEqualTo(runId.toString());
            assertThat(unit.getAttributes().get(DtpOpenTelemetry.RUN_ATTEMPT)).isEqualTo(2L);
            assertThat(unit.getAttributes().get(DtpOpenTelemetry.BATCH_SIZE)).isEqualTo(3L);
            assertThat(unit.getAttributes().get(DtpOpenTelemetry.STEP)).isEqualTo("trade-check");
            assertThat(unit.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
            assertThat(unit.getEvents()).extracting(e -> e.getName()).containsExactly("exception");
            assertThat(unit.getLinks()).singleElement()
                    .satisfies(link -> assertThat(link.getSpanContext().getSpanId())
                            .isEqualTo(request.getSpanContext().getSpanId()));
            assertThat(DtpOpenTelemetry.spanContext("00-zz-yy-01").isValid()).isFalse();
        }
    }

    private static long sum(InMemoryOpenTelemetry otel, String name, Attributes attributes) {
        return otel.points(name, attributes).stream().mapToLong(p -> ((LongPointData) p).getValue()).sum();
    }

    private static long longValue(InMemoryOpenTelemetry otel, String name, Attributes attributes) {
        List<PointData> points = otel.points(name, attributes);
        assertThat(points).as(name).hasSize(1);
        return ((LongPointData) points.getFirst()).getValue();
    }
}
