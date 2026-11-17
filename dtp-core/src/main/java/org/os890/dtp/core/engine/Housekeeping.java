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

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.os890.dtp.api.event.JobDefined;
import org.os890.dtp.api.event.JobRevised;
import org.os890.dtp.api.event.LeaseExpired;
import org.os890.dtp.api.event.NodeLost;
import org.os890.dtp.api.event.RunRequeued;
import org.os890.dtp.api.event.WindowDefined;
import org.os890.dtp.core.domain.BuiltInOption;
import org.os890.dtp.core.domain.CalendarDefinition;
import org.os890.dtp.core.domain.CronExpression;
import org.os890.dtp.core.domain.CronSchedule;
import org.os890.dtp.core.domain.DecisionKind;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.ProcessingSlots;
import org.os890.dtp.core.domain.SubjectType;
import org.os890.dtp.core.domain.TakeoverPolicy;
import org.os890.dtp.core.domain.TriggerType;
import org.os890.dtp.core.jdbc.JobStore;
import org.os890.dtp.core.jdbc.LeaseStore.ExpiredLease;
import org.os890.dtp.core.jdbc.NodeStore;
import org.os890.dtp.core.jdbc.PackageStore;
import org.os890.dtp.core.jdbc.RunStore;
import org.os890.dtp.core.jdbc.WindowStore;

/**
 * The tasks of the housekeeping leader (DD-3): creating lease rows, planning time and inbox triggers (cron slots with
 * their business calendar) and decision fallbacks, moving runs that missed their processing slot, taking over expired
 * leases, marking dead nodes, coordinating collection windows and announcing new definitions. Every action is
 * idempotent (run keys) or conditional (tokens), so two leaders for a short moment do no harm.
 */
public class Housekeeping {

    private static final System.Logger LOG = System.getLogger(Housekeeping.class.getName());

    private static final int MAX_CRON_SLOTS_PER_TICK = 10;

    private final EngineContext ctx;
    private final RunPlanner planner;
    private final Decisions decisions;
    private final FlowInstances instances;
    private final PackageStore packages;
    private final Map<String, CronExpression> crons = new ConcurrentHashMap<>();
    private final Map<UUID, Instant> examinedSlots = new ConcurrentHashMap<>();
    private final WindowCoordinator windows;
    private final Acknowledgements acknowledgements;
    private final WindowStore windowStore = new WindowStore();

    public Housekeeping(EngineContext ctx, RunPlanner planner) {
        this.ctx = ctx;
        this.planner = planner;
        this.decisions = new Decisions(ctx);
        this.acknowledgements = new Acknowledgements(ctx, new ItemRouting(ctx));
        this.instances = new FlowInstances(ctx);
        this.packages = new PackageStore(ctx.dialect());
        this.windows = new WindowCoordinator(ctx, planner, decisions);
    }

    /** One housekeeping cycle. Each step runs in its own transaction, so one failure doesn't block the others. */
    public void tick() {
        guarded("definitions", this::announceDefinitions);
        guarded("lease rows", () -> ctx.tx().required(() -> ctx.tx().withConnection(
                c -> ctx.leases().createMissing(c, ctx.namespace()))));
        for (JobDefinition job : ctx.catalog().all()) {
            if (!job.enabled()) {
                continue;
            }
            if (job.cronExpression() != null) {
                guarded("cron of " + job.name(), () -> planCron(job));
            }
            if (job.asap()) {
                guarded("asap of " + job.name(), () -> planContinuous(job, TriggerType.ASAP, 0));
            }
            if (job.inboxMinItems() != null) {
                guarded("inbox of " + job.name(), () -> planContinuous(job, TriggerType.INBOX, job.inboxMinItems()));
            }
        }
        guarded("decision fallbacks", () -> decisions.planFallbacks(planner));
        guarded("collection windows", this::coordinateWindows);
        guarded("missed processing slots", this::moveMissedSlots);
        guarded("lease takeover", this::reapLeases);
        guarded("acknowledgement timeouts", acknowledgements::timeOut);
        guarded("dead nodes", this::markDeadNodes);
    }

    /**
     * Announces new job revisions and windows once (slice 26), whatever wrote them: a definition script (DD-15), the
     * builder or a flow registration. A revision written ahead of the current one waits until it is current.
     */
    private Object announceDefinitions() {
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            Instant now = ctx.clock().now(c);
            for (JobStore.UnannouncedRevision r : ctx.jobs().unannounced(c, ctx.namespace())) {
                if (!ctx.jobs().announce(c, r.jobId(), r.revision(), now)) {
                    continue;
                }
                if (r.previousRevision() == null) {
                    LOG.log(System.Logger.Level.INFO, "job {0} defined (revision {1}) by {2}", r.job(), r.revision(),
                            r.changedBy());
                    ctx.publishAfterCommit(new JobDefined(r.job(), r.revision(), r.handlerClass(), r.changedBy(),
                            r.changedAt()));
                } else {
                    LOG.log(System.Logger.Level.INFO, "job {0} revised from revision {1} to {2} by {3}", r.job(),
                            r.previousRevision(), r.revision(), r.changedBy());
                    ctx.publishAfterCommit(new JobRevised(r.job(), r.previousRevision(), r.revision(),
                            r.handlerClass(), r.changedBy(), r.changedAt()));
                }
            }
            for (WindowStore.UnannouncedWindow w : windowStore.unannounced(c, ctx.namespace())) {
                if (windowStore.announce(c, w.id(), now)) {
                    ctx.publishAfterCommit(new WindowDefined(w.name(), ctx.catalog().byId(w.collectorJobId()).name(),
                            w.scheduleCron(), w.zone(), w.createdAt()));
                }
            }
            return null;
        }));
    }

    private Object planCron(JobDefinition job) {
        CronExpression cron = crons.computeIfAbsent(job.cronExpression(), CronExpression::parse);
        CalendarDefinition calendar = job.cronCalendarId() == null ? null
                : ctx.catalog().calendar(job.cronCalendarId());
        Instant examined = ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            Instant now = ctx.clock().now(c);
            // a new job starts with its first slot after its creation; slots already examined by this leader (e.g.
            // skipped or collapsed by the calendar) aren't examined again
            Instant last = ctx.runs().lastCronSlot(c, job.id()).orElseGet(() -> createdAt(job));
            Instant seen = examinedSlots.get(job.id());
            if (seen != null && seen.isAfter(last)) {
                last = seen;
            }
            Instant lower = CronSchedule.lowerBound(calendar, job.calendarShift(), now, ctx.config().misfireGrace());
            CronSchedule.Due due = CronSchedule.due(cron, job.zone(), calendar, job.calendarShift(),
                    last.isAfter(lower) ? last : lower, now, ctx.config().misfireGrace(), MAX_CRON_SLOTS_PER_TICK);
            for (CronSchedule.Occurrence occurrence : due.occurrences()) {
                if (job.cronFlowInstance()) {
                    instances.startScheduled(c, job, occurrence, now);
                } else {
                    planner.plan(c, job, new RunPlanner.Trigger(TriggerType.CRON,
                            "cron:" + job.id() + ":" + occurrence.at().getEpochSecond(), "cron slot " + occurrence.at()
                            + (occurrence.at().equals(occurrence.slot()) ? "" : " (moved from " + occurrence.slot()
                            + ")"), null, occurrence.at(), null, occurrence.slot()), now);
                }
            }
            return due.examined();
        }));
        if (examined != null) {
            // only after the commit: a rolled back slot is planned again
            examinedSlots.merge(job.id(), examined, (a, b) -> a.isAfter(b) ? a : b);
        }
        return null;
    }

    /** Each window in its own transaction: one failing window doesn't block the others. */
    private Object coordinateWindows() {
        for (var window : windows.windows()) {
            guarded("window " + window.name(), () -> {
                windows.cycle(window);
                return null;
            });
        }
        return null;
    }

    private Object moveMissedSlots() {
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            Instant now = ctx.clock().now(c);
            for (RunStore.MissedSlot missed : ctx.runs().missedSlots(c, ctx.namespace(), now)) {
                JobDefinition job = ctx.catalog().revision(missed.jobId(), missed.revision());
                if (!job.hasSlots()) {
                    continue;
                }
                ProcessingSlots.Window window = planner.slotWindow(job, now);
                if (ctx.runs().moveToSlot(c, missed.runId(), window.start(), window.end(), now)) {
                    LOG.log(System.Logger.Level.INFO, "run {0} of job {1} missed its processing slot, moved to {2}",
                            missed.runId(), job.name(), window.start());
                }
            }
            return null;
        }));
    }

    private Instant createdAt(JobDefinition job) {
        return ctx.tx().withConnection(c -> ctx.jobs().createdAt(c, job.id()));
    }

    private Object planContinuous(JobDefinition job, TriggerType type, int minItems) {
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            if (!ctx.runs().openRunStates(c, job.id()).isEmpty()) {
                return null;
            }
            Instant now = ctx.clock().now(c);
            // pending packages (carried over, or due again after a failure) are work of the job as well
            if (minItems > 0 && ctx.items().countDue(c, job.id(), now) < minItems
                    && packages.countDue(c, job.id(), now) == 0) {
                return null;
            }
            String previous = ctx.runs().latestRunId(c, job.id()).map(UUID::toString).orElse("first");
            planner.plan(c, job, new RunPlanner.Trigger(type, type.name().toLowerCase(java.util.Locale.ROOT) + ":"
                    + job.id() + ":" + previous, null, null, null), now);
            return null;
        }));
    }

    private Object markDeadNodes() {
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            for (NodeStore.LostNode node : ctx.nodes().markDead(c, ctx.namespace(),
                    ctx.clock().now(c).minus(ctx.config().deadAfter()))) {
                LOG.log(System.Logger.Level.WARNING, "node {0} ({1}) is lost, its last heartbeat was at {2}",
                        node.name(), node.id(), node.lastHeartbeat());
                ctx.publishAfterCommit(new NodeLost(node.id(), node.name(), node.color(), node.lastHeartbeat()));
            }
            return null;
        }));
    }

    private Object reapLeases() {
        var expired = ctx.tx().withConnection(c -> ctx.leases().findExpired(c, ctx.namespace(), ctx.clock().now(c)));
        for (ExpiredLease lease : expired) {
            boolean rerun = ctx.catalog().byId(lease.jobId()).takeover() == TakeoverPolicy.RERUN;
            boolean taken = ctx.tx().required(() -> ctx.tx().withConnection(c -> {
                Instant now = ctx.clock().now(c);
                if (!ctx.leases().expire(c, lease, now)) {
                    return false;
                }
                // read before the takeover: it still names the node whose lease expired
                ctx.publishAfterCommit(new LeaseExpired(ctx.runRef(c, lease.runId()), rerun));
                boolean active = ctx.runs().afterLeaseExpired(c, lease.runId(), rerun, now);
                if (active && rerun) {
                    ctx.publishAfterCommit(new RunRequeued(ctx.runRef(c, lease.runId()), RunRequeued.Cause.TAKEOVER,
                            ctx.config().nodeName()));
                }
                if (active && !rerun) {
                    // takeover policy ABANDON: an operator decides (DD-5)
                    decisions.request(c, new Decisions.NewRequest(DecisionKind.CHOICE, SubjectType.RUN,
                            lease.runId(), lease.jobId(), lease.runId(), "lease of node " + lease.owner()
                            + " expired, takeover policy ABANDON", Decisions.builtIn(BuiltInOption.RUN), null, null,
                            Decisions.OPERATORS), now);
                }
                return true;
            }));
            if (taken) {
                ctx.telemetry().leaseTakenOver(ctx.catalog().byId(lease.jobId()).name());
                LOG.log(System.Logger.Level.INFO, "took over the expired lease of run {0} (node {1}): {2}",
                        lease.runId(), lease.owner(), rerun ? "RERUN" : "ABANDON");
            }
        }
        return null;
    }

    @FunctionalInterface
    private interface Step {
        Object run();
    }

    private static void guarded(String name, Step step) {
        try {
            step.run();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "housekeeping step '" + name + "' failed", e);
        }
    }
}
