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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.os890.dtp.core.spi.ClusterState;
import org.os890.dtp.core.spi.Telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;

/**
 * Reports the telemetry of the engine with the OpenTelemetry API (docs/design/09-monitoring-heatmap.md §9.8): the
 * per-node metrics, the cluster gauges (only while this node leads the projection) and a span per unit of work,
 * linked to the traces that tracked its items. On MicroProfile Telemetry runtimes, pass the {@link OpenTelemetry}
 * instance of the runtime:
 *
 * <pre>{@code
 * @Produces
 * @ApplicationScoped
 * Telemetry telemetry(OpenTelemetry openTelemetry) {
 *     return new DtpOpenTelemetry(openTelemetry, "trades");
 * }
 * }</pre>
 */
public class DtpOpenTelemetry implements Telemetry {

    /** The instrumentation scope of the meter and the tracer. */
    public static final String SCOPE = "org.os890.dtp";

    static final AttributeKey<String> NAMESPACE = AttributeKey.stringKey("dtp.namespace");
    static final AttributeKey<String> STEP = AttributeKey.stringKey("dtp.step");
    static final AttributeKey<String> LANE = AttributeKey.stringKey("dtp.lane");
    static final AttributeKey<String> RESULT = AttributeKey.stringKey("dtp.result");
    static final AttributeKey<String> FAILURE_KIND = AttributeKey.stringKey("dtp.failure.kind");
    static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error.type");
    static final AttributeKey<String> ROLLBACK_REASON = AttributeKey.stringKey("dtp.rollback.reason");
    static final AttributeKey<String> RUN_OUTCOME = AttributeKey.stringKey("dtp.run.outcome");
    static final AttributeKey<String> LEADER_ROLE = AttributeKey.stringKey("dtp.leader.role");
    static final AttributeKey<String> DECISION_KIND = AttributeKey.stringKey("dtp.decision.kind");
    static final AttributeKey<String> GROUP = AttributeKey.stringKey("dtp.responsible.group");
    static final AttributeKey<String> STATUS_REASON = AttributeKey.stringKey("dtp.step.status_reason");
    static final AttributeKey<String> NODE_COLOR = AttributeKey.stringKey("dtp.node.color");
    static final AttributeKey<String> NODE_STATE = AttributeKey.stringKey("dtp.node.state");
    static final AttributeKey<String> REJECTION = AttributeKey.stringKey("dtp.rejection.reason");
    static final AttributeKey<String> RUN_ID = AttributeKey.stringKey("dtp.run.id");
    static final AttributeKey<Long> RUN_ATTEMPT = AttributeKey.longKey("dtp.run.attempt");
    static final AttributeKey<Long> BATCH_SIZE = AttributeKey.longKey("dtp.batch.size");

    private static final String TRACEPARENT = "traceparent";
    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };

    private final String namespace;
    private final Tracer tracer;
    private final LongCounter itemsProcessed;
    private final DoubleHistogram itemWaitTime;
    private final DoubleHistogram unitDuration;
    private final LongCounter rollbacks;
    private final LongCounter technicalFailures;
    private final LongCounter runsFinished;
    private final DoubleHistogram runDuration;
    private final DoubleHistogram claimDuration;
    private final LongCounter claimConflicts;
    private final LongCounter leaseRenewals;
    private final LongCounter leasesLost;
    private final LongCounter leaseTakeovers;
    private final LongCounter leaderElections;
    private final LongCounter packagesAcknowledged;
    private final LongCounter packagesRejected;
    private final DoubleHistogram acknowledgementTime;
    private final LongCounter decisionsCreated;
    private final LongCounter decisionsDecided;
    private volatile ClusterState cluster;

    /**
     * Creates the telemetry of one node.
     *
     * @param openTelemetry the OpenTelemetry instance, e.g. of the MicroProfile Telemetry runtime
     * @param namespace     the dtp namespace, an attribute of every metric
     */
    public DtpOpenTelemetry(OpenTelemetry openTelemetry, String namespace) {
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.tracer = openTelemetry.getTracer(SCOPE);
        Meter meter = openTelemetry.getMeter(SCOPE);
        itemsProcessed = counter(meter, "dtp.items.processed", "Items that left a step, by result");
        itemWaitTime = histogram(meter, "dtp.item.wait_time", "Time in the inbox until the processing started");
        unitDuration = histogram(meter, "dtp.unit_of_work.duration", "Duration of committed units of work");
        rollbacks = counter(meter, "dtp.unit_of_work.rollbacks", "Rolled back units of work");
        technicalFailures = counter(meter, "dtp.failures.technical", "Technical failures of items");
        runsFinished = counter(meter, "dtp.runs.finished", "Run attempts that ended on this node");
        runDuration = histogram(meter, "dtp.run.duration", "Duration of run attempts");
        claimDuration = histogram(meter, "dtp.claim.duration", "Time from due to claimed (scheduling lag)");
        claimConflicts = counter(meter, "dtp.claim.conflicts", "Lost claim races");
        leaseRenewals = counter(meter, "dtp.lease.renewals", "Lease renewals");
        leasesLost = counter(meter, "dtp.lease.lost", "Runs of this node that lost their lease");
        leaseTakeovers = counter(meter, "dtp.lease.takeovers", "Expired leases taken over by the leader");
        leaderElections = counter(meter, "dtp.leader.elections", "Times this node was elected leader");
        packagesAcknowledged = counter(meter, "dtp.packages.acknowledged", "Sent work packages acknowledged");
        packagesRejected = counter(meter, "dtp.packages.rejected", "Sent work packages rejected or not acknowledged");
        acknowledgementTime = histogram(meter, "dtp.package.ack_time", "Time from sending to the acknowledgement");
        decisionsCreated = counter(meter, "dtp.decisions.created", "Decision requests created");
        decisionsDecided = counter(meter, "dtp.decisions.decided", "Decision requests decided");
        registerClusterGauges(meter);
    }

    @Override
    public boolean tracing() {
        return true;
    }

    @Override
    public void itemProcessed(String step, String lane, String result, String failureKind) {
        AttributesBuilder attributes = step(step).put(LANE, lane(lane)).put(RESULT, result);
        if (failureKind != null) {
            attributes.put(FAILURE_KIND, failureKind);
        }
        itemsProcessed.add(1, attributes.build());
    }

    @Override
    public void itemWaited(String step, String lane, Duration wait) {
        itemWaitTime.record(seconds(wait), step(step).put(LANE, lane(lane)).build());
    }

    @Override
    public void technicalFailure(String step, String errorType) {
        technicalFailures.add(1, step(step).put(ERROR_TYPE, errorType).build());
    }

    @Override
    public void unitOfWork(String step, Duration duration) {
        unitDuration.record(seconds(duration), step(step).build());
    }

    @Override
    public void rollback(String step, String reason) {
        rollbacks.add(1, step(step).put(ROLLBACK_REASON, reason).build());
    }

    @Override
    public void runFinished(String step, String outcome, Duration duration) {
        runsFinished.add(1, step(step).put(RUN_OUTCOME, outcome).build());
        runDuration.record(seconds(duration), step(step).build());
    }

    @Override
    public void claimed(Duration lag) {
        claimDuration.record(seconds(lag), base().build());
    }

    @Override
    public void claimConflict() {
        claimConflicts.add(1, base().build());
    }

    @Override
    public void leaseRenewed(String step) {
        leaseRenewals.add(1, step(step).build());
    }

    @Override
    public void leaseLost(String step) {
        leasesLost.add(1, step(step).build());
    }

    @Override
    public void leaseTakenOver(String step) {
        leaseTakeovers.add(1, step(step).build());
    }

    @Override
    public void leaderElected(String role) {
        leaderElections.add(1, base().put(LEADER_ROLE, role).build());
    }

    @Override
    public void packageAcknowledged(String step, Duration sinceSent) {
        packagesAcknowledged.add(1, step(step).build());
        acknowledgementTime.record(seconds(sinceSent), step(step).build());
    }

    @Override
    public void packageRejected(String step, String reason) {
        packagesRejected.add(1, step(step).put(REJECTION, reason).build());
    }

    @Override
    public void decisionCreated(String step, String kind, String group) {
        decisionsCreated.add(1, decision(step, kind, group));
    }

    @Override
    public void decisionDecided(String step, String kind, String group) {
        decisionsDecided.add(1, decision(step, kind, group));
    }

    @Override
    public void clusterState(ClusterState state) {
        cluster = state;
    }

    @Override
    public Span startUnitOfWork(String step, UUID runId, int attempt, List<String> links) {
        SpanBuilder builder = tracer.spanBuilder("dtp " + step).setSpanKind(SpanKind.CONSUMER).setNoParent()
                .setAttribute(NAMESPACE, namespace).setAttribute(STEP, step)
                .setAttribute(RUN_ID, runId.toString()).setAttribute(RUN_ATTEMPT, (long) attempt);
        for (String link : links) {
            SpanContext context = spanContext(link);
            if (context.isValid()) {
                builder.addLink(context);
            }
        }
        io.opentelemetry.api.trace.Span span = builder.startSpan();
        Scope scope = span.makeCurrent();
        return new Span() {
            @Override
            public void items(int count) {
                span.setAttribute(BATCH_SIZE, (long) count);
            }

            @Override
            public void failed(Throwable error) {
                span.recordException(error);
                span.setStatus(StatusCode.ERROR, error.getClass().getName());
            }

            @Override
            public void close() {
                scope.close();
                span.end();
            }
        };
    }

    @Override
    public String currentTraceParent() {
        if (!io.opentelemetry.api.trace.Span.current().getSpanContext().isValid()) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        W3CTraceContextPropagator.getInstance().inject(Context.current(), carrier, Map::put);
        return carrier.get(TRACEPARENT);
    }

    @Override
    public String currentTraceId() {
        SpanContext context = io.opentelemetry.api.trace.Span.current().getSpanContext();
        return context.isValid() ? context.getTraceId() : null;
    }

    /**
     * The span context of a W3C traceparent.
     *
     * @param traceParent the traceparent
     * @return the context, invalid if the traceparent is
     */
    static SpanContext spanContext(String traceParent) {
        Context context = W3CTraceContextPropagator.getInstance().extract(Context.root(),
                Map.of(TRACEPARENT, traceParent), GETTER);
        return io.opentelemetry.api.trace.Span.fromContext(context).getSpanContext();
    }

    /** The gauges read the latest cluster state; without one (not the projection leader) they report nothing. */
    private void registerClusterGauges(Meter meter) {
        meter.gaugeBuilder("dtp.cluster.queue.depth").ofLongs().setDescription("Queued items per step and lane")
                .buildWithCallback(m -> ifLeading(state -> state.queues().forEach(q -> m.record(q.depth(),
                     step(q.step()).put(LANE, lane(q.lane())).build()))));
        meter.gaugeBuilder("dtp.cluster.queue.oldest_age").setUnit("s")
                .setDescription("Age of the oldest due item per step")
                .buildWithCallback(m -> ifLeading(state -> state.steps().forEach(s -> m.record(s.oldestQueuedSecs(),
                     step(s.step()).build()))));
        meter.gaugeBuilder("dtp.cluster.step.jam_score").ofLongs().setDescription("Jam score 0 to 100 per step")
                .buildWithCallback(m -> ifLeading(state -> state.steps().forEach(s -> m.record(s.jamScore(),
                     step(s.step()).put(STATUS_REASON, s.statusReason()).build()))));
        meter.gaugeBuilder("dtp.cluster.step.alert_state").ofLongs()
                .setDescription("Alert state per step: 0 = OK, 1 = CONGESTED, 2 = FAILING")
                .buildWithCallback(m -> ifLeading(state -> state.steps().forEach(s -> m.record(s.alertState(),
                     step(s.step()).build()))));
        meter.gaugeBuilder("dtp.cluster.items.waiting_for_decision").ofLongs()
                .setDescription("Items waiting for people per step and team")
                .buildWithCallback(m -> ifLeading(state -> state.waiting().forEach(w -> m.record(w.items(),
                     step(w.step()).put(GROUP, w.group()).build()))));
        meter.gaugeBuilder("dtp.cluster.decisions.oldest_open_age").setUnit("s")
                .setDescription("Age of the oldest open decision request per team")
                .buildWithCallback(m -> ifLeading(state -> state.decisions().forEach(d -> m.record(d.oldestOpenSecs(),
                     base().put(GROUP, d.group()).build()))));
        meter.gaugeBuilder("dtp.cluster.nodes").ofLongs().setDescription("Nodes per color and state")
                .buildWithCallback(m -> ifLeading(state -> state.nodes().forEach(n -> m.record(n.count(),
                     base().put(NODE_COLOR, n.color()).put(NODE_STATE, n.state()).build()))));
    }

    private void ifLeading(java.util.function.Consumer<ClusterState> report) {
        ClusterState state = cluster;
        if (state != null) {
            report.accept(state);
        }
    }

    private AttributesBuilder base() {
        return Attributes.builder().put(NAMESPACE, namespace);
    }

    private AttributesBuilder step(String step) {
        return base().put(STEP, step);
    }

    private Attributes decision(String step, String kind, String group) {
        return step(step).put(DECISION_KIND, kind).put(GROUP, group == null ? "" : group).build();
    }

    private static String lane(String lane) {
        return lane == null ? "" : lane;
    }

    private static double seconds(Duration duration) {
        return duration.toNanos() / 1e9;
    }

    private static LongCounter counter(Meter meter, String name, String description) {
        return meter.counterBuilder(name).setDescription(description).build();
    }

    private static DoubleHistogram histogram(Meter meter, String name, String description) {
        return meter.histogramBuilder(name).setUnit("s").setDescription(description).build();
    }
}
