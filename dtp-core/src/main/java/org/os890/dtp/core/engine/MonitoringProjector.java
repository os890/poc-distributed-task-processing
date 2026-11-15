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
package org.os890.dtp.core.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.os890.dtp.api.event.StepCongested;
import org.os890.dtp.api.event.StepRecovered;
import org.os890.dtp.api.event.TechnicalFailureSpike;
import org.os890.dtp.core.DtpConfig;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.ProcessingSlots;
import org.os890.dtp.core.domain.StepHealth;
import org.os890.dtp.core.domain.StepHealth.Alert;
import org.os890.dtp.core.domain.StepHealth.AlertState;
import org.os890.dtp.core.domain.StepHealth.StatusReason;
import org.os890.dtp.core.jdbc.MonitoringStore;
import org.os890.dtp.core.jdbc.MonitoringStore.ActiveLease;
import org.os890.dtp.core.jdbc.MonitoringStore.EdgeMinute;
import org.os890.dtp.core.jdbc.MonitoringStore.EventRow;
import org.os890.dtp.core.jdbc.MonitoringStore.FailureMinute;
import org.os890.dtp.core.jdbc.MonitoringStore.Parked;
import org.os890.dtp.core.jdbc.MonitoringStore.QueueRow;
import org.os890.dtp.core.jdbc.MonitoringStore.Recent;
import org.os890.dtp.core.jdbc.MonitoringStore.StatusRow;
import org.os890.dtp.core.jdbc.MonitoringStore.StepMinute;
import org.os890.dtp.core.spi.Telemetry;

/**
 * The monitoring projector (docs/design/09-monitoring-heatmap.md §9.2), run by the leader of the role
 * {@code projection}: every cycle recomputes the minute buckets of the recent horizon from the journal, the status of
 * every step from the current item states, leases, windows, slots and nodes, and its alert state. One transaction per
 * cycle, serialized by a lock on the projection state, so a briefly duplicated projector does no harm.
 *
 * <p>The horizon is two lease durations: a journal entry is written with the DB time of its unit of work, which
 * can't commit later than one lease duration after that (fence check), so the buckets before the horizon are final.
 */
public class MonitoringProjector {

    private static final System.Logger LOG = System.getLogger(MonitoringProjector.class.getName());

    private static final int ARRIVED = 0;
    private static final int DEPARTED = 1;
    private static final int ROUTED_BACK = 2;
    private static final int COMPLETED = 3;
    private static final int DISCARDED = 4;
    private static final int ESCALATED = 5;
    private static final int FAILED_BUSINESS = 6;
    private static final int FAILED_TECHNICAL = 7;
    private static final int MAX_WAIT = 8;
    private static final int SUM_WAIT = 9;

    private final EngineContext ctx;
    private final LeaderElector leader;
    private final MonitoringStore store = new MonitoringStore();
    private long nextCycleNanos = System.nanoTime();
    private boolean reporting;

    public MonitoringProjector(EngineContext ctx, LeaderElector leader) {
        this.ctx = ctx;
        this.leader = leader;
    }

    /** One cycle, if this node leads the projection and the projection interval has passed since the last one. */
    public void cycle() {
        if (!leader.isLeader()) {
            // a revoked leader stops reporting the cluster gauges at once, the new leader reports them
            if (reporting) {
                reporting = false;
                ctx.telemetry().clusterState(null);
            }
            return;
        }
        if (System.nanoTime() - nextCycleNanos < 0) {
            return;
        }
        nextCycleNanos = System.nanoTime() + ctx.config().monitoring().interval().toNanos();
        try {
            project(leader.term());
            if (ctx.telemetry() != Telemetry.NONE) {
                ctx.telemetry().clusterState(ctx.tx().withConnection(c -> store.clusterState(c, ctx.namespace(),
                        ctx.clock().now(c))));
                reporting = true;
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "monitoring projection failed", e);
        }
    }

    /**
     * Projects the views once.
     *
     * @param term the leader term
     * @return the version of the cycle
     */
    public long project(long term) {
        DtpConfig.Monitoring settings = ctx.config().monitoring();
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            Instant now = ctx.clock().now(c);
            long version = store.nextVersion(c, ctx.namespace());
            Instant horizon = minute(now.minus(maxLease().multipliedBy(2)));
            projectMinutes(c, horizon);
            store.deleteBefore(c, ctx.namespace(), minute(now.minus(settings.retention())));
            projectStatus(c, now, version, settings);
            store.finishCycle(c, ctx.namespace(), version, now, term);
            return version;
        }));
    }

    private Duration maxLease() {
        Duration max = ctx.config().leaseDuration();
        for (JobDefinition job : ctx.catalog().all()) {
            if (job.leaseDuration() != null && job.leaseDuration().compareTo(max) > 0) {
                max = job.leaseDuration();
            }
        }
        return max;
    }

    private static Instant minute(Instant at) {
        return at.truncatedTo(ChronoUnit.MINUTES);
    }

    // ------------------------------------------------------------------------------------------------ minute buckets

    private record StepKey(UUID jobId, Instant minute) {
    }

    private record EdgeKey(UUID fromJobId, UUID toJobId, boolean backRoute, Instant minute) {
    }

    private record FailureKey(UUID jobId, String errorType, String errorHash, Instant minute) {
    }

    private static class FailureCount {
        private int occurrences;
        private String sampleMessage;
        private UUID sampleItemId;
    }

    private void projectMinutes(Connection c, Instant horizon) throws SQLException {
        Map<StepKey, long[]> steps = new HashMap<>();
        Map<EdgeKey, Integer> edges = new HashMap<>();
        Map<FailureKey, FailureCount> failures = new HashMap<>();
        Map<UUID, List<UUID>> branches = store.branchSteps(c, ctx.namespace(), horizon);
        for (EventRow e : store.events(c, ctx.namespace(), horizon)) {
            Instant minute = minute(e.occurredAt());
            UUID from = e.fromJobId();
            UUID to = e.toJobId();
            switch (e.type()) {
                case "ROUTED", "ROUTED_BACK" -> {
                    boolean back = "ROUTED_BACK".equals(e.type());
                    if (from != null) {
                        depart(steps, from, minute, e);
                        if (back) {
                            count(steps, from, minute, ROUTED_BACK);
                        }
                        if (to != null) {
                            edges.merge(new EdgeKey(from, to, back, minute), 1, Integer::sum);
                        }
                    }
                    if (to != null) {
                        count(steps, to, minute, ARRIVED);
                    }
                }
                case "FORKED" -> {
                    depart(steps, from, minute, e);
                    for (UUID branch : branches.getOrDefault(e.itemId(), List.of())) {
                        count(steps, branch, minute, ARRIVED);
                        edges.merge(new EdgeKey(from, branch, false, minute), 1, Integer::sum);
                    }
                }
                case "BRANCH_FINISHED" -> {
                    depart(steps, from, minute, e);
                    edges.merge(new EdgeKey(from, to, false, minute), 1, Integer::sum);
                }
                case "JOINED" -> count(steps, to, minute, ARRIVED);
                case "PACKAGED" -> {
                    // handed over to another job (slice 9); a package of the collector's own stays at the step
                    if (from != null && to != null && !from.equals(to)) {
                        depart(steps, from, minute, e);
                        count(steps, to, minute, ARRIVED);
                        edges.merge(new EdgeKey(from, to, false, minute), 1, Integer::sum);
                    }
                }
                case "COMPLETED" -> {
                    depart(steps, from, minute, e);
                    count(steps, from, minute, COMPLETED);
                }
                case "DISCARDED" -> {
                    depart(steps, from, minute, e);
                    count(steps, from, minute, DISCARDED);
                }
                case "ESCALATED" -> count(steps, from, minute, ESCALATED);
                case "FAILED" -> {
                    if ("TECHNICAL".equals(e.failureKind())) {
                        count(steps, from, minute, FAILED_TECHNICAL);
                        if (from != null) {
                            FailureCount f = failures.computeIfAbsent(new FailureKey(from,
                                    Objects.requireNonNullElse(e.errorType(), "unknown"),
                                    Objects.requireNonNullElse(e.errorHash(), "-"), minute), k -> new FailureCount());
                            f.occurrences++;
                            f.sampleMessage = truncate(e.reason());
                            f.sampleItemId = e.itemId();
                        }
                    } else {
                        count(steps, from, minute, FAILED_BUSINESS);
                    }
                }
                default -> {
                    // TRACKED, DEFERRED, RETRY_SCHEDULED, REPROCESSED, VOTED, DECIDED, MIGRATED, LOOP_LIMIT_EXCEEDED
                }
            }
        }
        List<StepMinute> stepRows = new ArrayList<>();
        steps.forEach((k, v) -> stepRows.add(new StepMinute(k.jobId(), k.minute(), (int) v[ARRIVED],
                (int) v[DEPARTED], (int) v[ROUTED_BACK], (int) v[COMPLETED], (int) v[DISCARDED], (int) v[ESCALATED],
                (int) v[FAILED_BUSINESS], (int) v[FAILED_TECHNICAL], v[MAX_WAIT], v[SUM_WAIT])));
        List<EdgeMinute> edgeRows = new ArrayList<>();
        edges.forEach((k, v) -> edgeRows.add(new EdgeMinute(k.fromJobId(), k.toJobId(), k.backRoute(), k.minute(),
                v)));
        List<FailureMinute> failureRows = new ArrayList<>();
        failures.forEach((k, v) -> failureRows.add(new FailureMinute(k.jobId(), k.errorType(), k.errorHash(),
                k.minute(), v.occurrences, v.sampleMessage, v.sampleItemId)));
        store.replaceMinutes(c, ctx.namespace(), horizon, stepRows, edgeRows, failureRows);
    }

    private static void count(Map<StepKey, long[]> steps, UUID jobId, Instant minute, int index) {
        if (jobId != null) {
            steps.computeIfAbsent(new StepKey(jobId, minute), k -> new long[SUM_WAIT + 1])[index]++;
        }
    }

    private static void depart(Map<StepKey, long[]> steps, UUID jobId, Instant minute, EventRow e) {
        if (jobId == null) {
            return;
        }
        long[] counters = steps.computeIfAbsent(new StepKey(jobId, minute), k -> new long[SUM_WAIT + 1]);
        counters[DEPARTED]++;
        if (e.arrivedAt() != null) {
            long wait = Math.max(0, Duration.between(e.arrivedAt(), e.occurredAt()).toMillis());
            counters[MAX_WAIT] = Math.max(counters[MAX_WAIT], wait);
            counters[SUM_WAIT] += wait;
        }
    }

    private static String truncate(String message) {
        return message == null || message.length() <= 1000 ? message : message.substring(0, 997) + "...";
    }

    // --------------------------------------------------------------------------------------------------- step status

    private record Queue(int queued, int deferred, Instant oldestSince, Map<String, Integer> byLane) {
    }

    private void projectStatus(Connection c, Instant now, long version, DtpConfig.Monitoring settings)
            throws SQLException {
        String ns = ctx.namespace();
        Map<UUID, StatusRow> stored = store.statuses(c, ns);
        Map<UUID, Queue> queues = queues(store.queues(c, ns, now));
        Map<UUID, Parked> parked = store.parked(c, ns);
        Map<UUID, ActiveLease> leases = store.activeLeases(c, ns);
        Set<UUID> paused = store.pausedJobs(c, ns);
        Map<UUID, Duration> ages = store.expectedMaxQueueAges(c, ns);
        Map<UUID, Recent> recent = store.recent(c, ns, now.minus(settings.failureWindow()));
        Set<String> capabilities = capabilities(c);
        Set<UUID> seen = new HashSet<>();
        for (JobDefinition job : ctx.catalog().all()) {
            seen.add(job.id());
            Queue queue = queues.getOrDefault(job.id(), new Queue(0, 0, null, Map.of()));
            Parked park = parked.getOrDefault(job.id(), new Parked(0, 0, 0));
            ActiveLease lease = leases.get(job.id());
            Recent counts = recent.getOrDefault(job.id(), new Recent(0, 0, 0));
            Long oldestMs = queue.oldestSince() == null ? null
                    : Math.max(0, Duration.between(queue.oldestSince(), now).toMillis());
            StatusReason reason = StepHealth.reason(job.enabled(), paused.contains(job.id()),
                    capabilities.contains(job.handlerClass()), lease != null, slotClosed(job, now),
                    job.asap() || job.inboxMinItems() != null && queue.queued() >= job.inboxMinItems(),
                    queue.queued());
            int jam = StepHealth.jamScore(reason, queue.queued(), oldestMs, counts.arrived(), counts.departed(),
                    ages.getOrDefault(job.id(), settings.expectedMaxQueueAge()), settings.failureWindow());
            StatusRow old = stored.get(job.id());
            // a planned pause by a collection window keeps the alert: the jam is still there after the window
            Alert alert = reason == StatusReason.PAUSED_BY_WINDOW ? alert(old, now).hold()
                    : alert(old, now).next(StepHealth.target(jam, counts.failedTechnical(), settings.jamThreshold(),
                            settings.failureThreshold()), settings.failureWindow(), now);
            StatusRow row = new StatusRow(job.id(), job.name(), reason.name(), queue.queued(),
                    queue.byLane().isEmpty() ? null : ctx.json().write(queue.byLane()), oldestMs,
                    park.waitingForDecision(), queue.deferred(), park.forked(), park.packaged(),
                    lease == null ? null : lease.runId(), lease == null ? null : lease.nodeId(), counts.arrived(),
                    counts.departed(), counts.failedTechnical(), jam, alert.state().name(), alert.since(),
                    alert.pending() == null ? null : alert.pending().name(), alert.pendingCycles(),
                    alert.pendingSince(), version, now);
            if (old != null && row.sameContent(old)) {
                row = new StatusRow(row.jobId(), row.stepName(), row.statusReason(), row.queued(), row.queuedByLane(),
                        row.oldestQueuedMs(), row.waitingForDecision(), row.deferred(), row.forked(), row.packaged(),
                        row.activeRunId(), row.activeNodeId(), row.arrived5m(), row.departed5m(),
                        row.failedTechnical5m(), row.jamScore(), row.alertState(), row.alertSince(),
                        row.pendingState(), row.pendingCycles(), row.pendingSince(), old.projectionVersion(),
                        old.updatedAt());
                if (Objects.equals(row.pendingState(), old.pendingState())
                        && row.pendingCycles() == old.pendingCycles()) {
                    continue;
                }
            }
            store.saveStatus(c, ns, row, old != null);
            if (alert.changed()) {
                announce(c, job, row, alert, settings, now);
            }
        }
        for (UUID gone : stored.keySet()) {
            if (!seen.contains(gone)) {
                store.deleteStatus(c, ns, gone);
            }
        }
    }

    private static Alert alert(StatusRow old, Instant now) {
        if (old == null) {
            return Alert.initial(now);
        }
        return new Alert(AlertState.valueOf(old.alertState()), old.alertSince(),
                old.pendingState() == null ? null : AlertState.valueOf(old.pendingState()), old.pendingCycles(),
                old.pendingSince(), false, AlertState.valueOf(old.alertState()));
    }

    private void announce(Connection c, JobDefinition job, StatusRow row, Alert alert,
                          DtpConfig.Monitoring settings, Instant now) throws SQLException {
        Object event = switch (alert.state()) {
            case CONGESTED -> new StepCongested(job.name(), row.jamScore(), row.statusReason(), row.queued(),
                    Duration.ofMillis(row.oldestQueuedMs() == null ? 0 : row.oldestQueuedMs()), alert.since());
            case FAILING -> new TechnicalFailureSpike(job.name(), row.failedTechnical5m(), settings.failureWindow(),
                    store.errorTypes(c, ctx.namespace(), job.id(), now.minus(settings.failureWindow())),
                    alert.since());
            case OK -> new StepRecovered(job.name(), alert.previous().name(), alert.since());
        };
        LOG.log(System.Logger.Level.INFO, "step {0}: {1} -> {2} (jam score {3}, {4} queued, {5} technical failures)",
                job.name(), alert.previous(), alert.state(), Integer.toString(row.jamScore()),
                Integer.toString(row.queued()), Integer.toString(row.failedTechnical5m()));
        ctx.publishAfterCommit(event);
    }

    private static Map<UUID, Queue> queues(List<QueueRow> rows) {
        Map<UUID, Queue> queues = new HashMap<>();
        for (QueueRow r : rows) {
            Queue q = queues.getOrDefault(r.jobId(), new Queue(0, 0, null, new TreeMap<>()));
            Map<String, Integer> lanes = new TreeMap<>(q.byLane());
            if (r.queued() > 0) {
                lanes.merge(r.lane(), r.queued(), Integer::sum);
            }
            Instant oldest = q.oldestSince() == null || (r.oldestSince() != null
                    && r.oldestSince().isBefore(q.oldestSince())) ? r.oldestSince() : q.oldestSince();
            queues.put(r.jobId(), new Queue(q.queued() + r.queued(), q.deferred() + r.deferred(), oldest, lanes));
        }
        return queues;
    }

    private Set<String> capabilities(Connection c) throws SQLException {
        Set<String> capabilities = new HashSet<>();
        for (String json : ctx.nodes().activeCapabilities(c, ctx.namespace())) {
            List<?> names = ctx.json().read(json, List.class);
            if (names != null) {
                names.forEach(name -> capabilities.add(String.valueOf(name)));
            }
        }
        return capabilities;
    }

    private boolean slotClosed(JobDefinition job, Instant now) {
        if (!job.hasSlots()) {
            return false;
        }
        return ProcessingSlots.window(job.slots(), ctx.catalog()::calendar, job.zone(), now)
                              .map(window -> !window.contains(now)).orElse(true);
    }
}
