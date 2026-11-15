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

import java.time.Duration;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import org.eclipse.microprofile.config.ConfigProvider;
import org.os890.dtp.cdi.DtpSettings;
import org.os890.dtp.core.spi.Telemetry;
import org.os890.dtp.telemetry.DtpOpenTelemetry;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;

/**
 * The OpenTelemetry SDK of the example. A MicroProfile Telemetry runtime (e.g. {@code quarkus-opentelemetry}) provides
 * one and exports to a collector; this one keeps the metrics of the last collection and the recent spans in memory,
 * shown by {@link TelemetryResource}. dtp reports to it through {@link DtpOpenTelemetry}.
 */
@ApplicationScoped
public class TelemetrySetup {

    private static final int MAX_SPANS = 2000;

    private final Deque<SpanData> spans = new ConcurrentLinkedDeque<>();
    private final SdkMeterProvider meters;
    private final OpenTelemetrySdk sdk;
    private volatile Collection<MetricData> metrics = List.of();

    public TelemetrySetup() {
        meters = SdkMeterProvider.builder().registerMetricReader(PeriodicMetricReader.builder(new MetricExporter() {
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
        }).setInterval(Duration.ofMinutes(1)).build()).build();
        SpanExporter exporter = new SpanExporter() {
            @Override
            public CompletableResultCode export(Collection<SpanData> data) {
                data.forEach(spans::addFirst);
                while (spans.size() > MAX_SPANS) {
                    spans.pollLast();
                }
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
        sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter))
                        .build())
                .setMeterProvider(meters).build();
    }

    /**
     * The telemetry of dtp on this node.
     *
     * @return the telemetry
     */
    @Produces
    @Singleton
    Telemetry telemetry() {
        return new DtpOpenTelemetry(sdk, DtpSettings.read(ConfigProvider.getConfig()).namespace());
    }

    /**
     * Collects the metrics now.
     *
     * @return the metrics
     */
    public Collection<MetricData> collect() {
        meters.forceFlush().join(10, TimeUnit.SECONDS);
        return metrics;
    }

    /**
     * The recent spans, newest first.
     *
     * @return the spans
     */
    public List<SpanData> spans() {
        return List.copyOf(spans);
    }

    @PreDestroy
    void close() {
        sdk.getSdkTracerProvider().close();
        meters.close();
    }
}
