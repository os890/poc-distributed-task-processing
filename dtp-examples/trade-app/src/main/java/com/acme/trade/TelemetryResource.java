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
package com.acme.trade;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.PointData;

/**
 * What the example's OpenTelemetry SDK collected on this node (in production a collector receives it): the dtp
 * metrics and the recent spans.
 */
@Path("/telemetry")
@Produces(MediaType.APPLICATION_JSON)
public class TelemetryResource {

    @Inject
    TelemetrySetup setup;

    /**
     * A metric.
     *
     * @param name   the name
     * @param unit   the unit
     * @param type   e.g. {@code LONG_SUM}, {@code HISTOGRAM}, {@code LONG_GAUGE}
     * @param points the points
     */
    public record MetricView(String name, String unit, String type, List<PointView> points) {
    }

    /**
     * A point of a metric.
     *
     * @param attributes the attributes
     * @param value      the value of a counter or gauge, the sum of a histogram
     * @param count      the count of a histogram, else {@code null}
     * @param max        the maximum of a histogram, else {@code null}
     */
    public record PointView(Map<String, String> attributes, double value, Long count, Double max) {
    }

    /**
     * A span.
     *
     * @param name       the name
     * @param traceId    the trace
     * @param spanId     the span
     * @param status     {@code OK}, {@code ERROR} or {@code UNSET}
     * @param durationMs the duration
     * @param attributes the attributes
     * @param links      the linked spans as {@code traceId/spanId}
     */
    public record SpanView(String name, String traceId, String spanId, String status, double durationMs,
                           Map<String, String> attributes, List<String> links) {
    }

    /**
     * The metrics, collected now.
     *
     * @param prefix only metrics whose name starts with it
     * @return the metrics, by name
     */
    @GET
    @Path("/metrics")
    public List<MetricView> metrics(@QueryParam("prefix") @DefaultValue("dtp.") String prefix) {
        return setup.collect().stream().filter(m -> m.getName().startsWith(prefix))
                    .sorted((a, b) -> a.getName().compareTo(b.getName()))
                    .map(m -> new MetricView(m.getName(), m.getUnit(), m.getType().name(),
                            m.getData().getPoints().stream().map(TelemetryResource::point).toList()))
                    .toList();
    }

    /**
     * The recent spans, newest first.
     *
     * @param traceId only the spans of this trace, or the spans linked to it
     * @param limit   the maximum number of spans
     * @return the spans
     */
    @GET
    @Path("/spans")
    public List<SpanView> spans(@QueryParam("traceId") String traceId,
                                @QueryParam("limit") @DefaultValue("50") int limit) {
        return setup.spans().stream()
                    .filter(s -> traceId == null || s.getTraceId().equals(traceId) || s.getLinks().stream()
                            .anyMatch(l -> l.getSpanContext().getTraceId().equals(traceId)))
                    .limit(limit)
                    .map(s -> new SpanView(s.getName(), s.getTraceId(), s.getSpanId(),
                            s.getStatus().getStatusCode().name(), (s.getEndEpochNanos() - s.getStartEpochNanos()) / 1e6,
                            attributes(s.getAttributes()), s.getLinks().stream()
                                    .map(l -> l.getSpanContext().getTraceId() + "/" + l.getSpanContext().getSpanId())
                                    .toList()))
                    .toList();
    }

    private static PointView point(PointData point) {
        Map<String, String> attributes = attributes(point.getAttributes());
        return switch (point) {
            case LongPointData p -> new PointView(attributes, p.getValue(), null, null);
            case DoublePointData p -> new PointView(attributes, p.getValue(), null, null);
            case HistogramPointData p -> new PointView(attributes, p.getSum(), p.getCount(),
                    p.hasMax() ? p.getMax() : null);
            default -> new PointView(attributes, Double.NaN, null, null);
        };
    }

    private static Map<String, String> attributes(Attributes attributes) {
        Map<String, String> map = new TreeMap<>();
        attributes.forEach((key, value) -> map.put(key.getKey(), String.valueOf(value)));
        return map;
    }
}
