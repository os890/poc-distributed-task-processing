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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;


import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;

/** An OpenTelemetry SDK that keeps the finished spans and the metrics of the last collection in memory. */
class InMemoryOpenTelemetry implements AutoCloseable {

    private final List<SpanData> spans = new CopyOnWriteArrayList<>();
    private final SdkMeterProvider meters;
    private final OpenTelemetrySdk sdk;
    private volatile Collection<MetricData> metrics = List.of();

    InMemoryOpenTelemetry() {
        SpanExporter spanExporter = new SpanExporter() {
            @Override
            public CompletableResultCode export(Collection<SpanData> data) {
                spans.addAll(data);
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode flush() {
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode shutdown() {
                return CompletableResultCode.ofSuccess();
            }
        };
        MetricExporter metricExporter = new MetricExporter() {
            @Override
            public AggregationTemporality getAggregationTemporality(InstrumentType type) {
                return AggregationTemporality.CUMULATIVE;
            }

            @Override
            public CompletableResultCode export(Collection<MetricData> data) {
                metrics = List.copyOf(data);
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode flush() {
                return CompletableResultCode.ofSuccess();
            }

            @Override
            public CompletableResultCode shutdown() {
                return CompletableResultCode.ofSuccess();
            }
        };
        meters = SdkMeterProvider.builder().registerMetricReader(PeriodicMetricReader.builder(metricExporter)
                .setInterval(Duration.ofHours(1)).build()).build();
        sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(spanExporter)).build())
                .setMeterProvider(meters).build();
    }

    OpenTelemetry api() {
        return sdk;
    }

    List<SpanData> spans() {
        return List.copyOf(spans);
    }

    /** Collects the metrics now. */
    Collection<MetricData> collect() {
        metrics = List.of();
        meters.forceFlush().join(10, TimeUnit.SECONDS);
        return metrics;
    }

    Optional<MetricData> metric(String name) {
        return metrics.stream().filter(m -> m.getName().equals(name)).findFirst();
    }

    /** The points of a metric whose attributes contain the given ones. */
    List<PointData> points(String name, Attributes contained) {
        return metric(name).map(m -> m.getData().getPoints().stream()
                .filter(p -> p.getAttributes().asMap().entrySet().containsAll(contained.asMap().entrySet()))
                .map(PointData.class::cast).toList()).orElse(List.of());
    }

    @Override
    public void close() {
        sdk.getSdkTracerProvider().close();
        meters.close();
    }
}
