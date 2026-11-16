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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.WindowOccurrence;
import org.os890.dtp.api.event.WindowClosed;
import org.os890.dtp.api.event.WindowOpened;
import org.os890.dtp.api.event.WindowQuiescing;
import org.os890.dtp.core.domain.BuiltInOption;
import org.os890.dtp.core.domain.CalendarDefinition;
import org.os890.dtp.core.domain.CalendarShift;
import org.os890.dtp.core.domain.CronExpression;
import org.os890.dtp.core.domain.CronSchedule;
import org.os890.dtp.core.domain.DecisionKind;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.OccurrenceState;
import org.os890.dtp.core.domain.RunState;
import org.os890.dtp.core.domain.SubjectType;
import org.os890.dtp.core.domain.TriggerType;
import org.os890.dtp.core.domain.WindowDefinition;
import org.os890.dtp.core.jdbc.RunStore.RunRecord;
import org.os890.dtp.core.jdbc.WindowStore;
import org.os890.dtp.core.jdbc.WindowStore.OccurrenceRow;

/**
 * The window coordinator of the housekeeping leader (docs/design/05-coordination-flows.md §5.6): starts occurrences
 * from the schedule, stops the paused jobs softly, opens the occurrence with a cut-off once they are idle, plans the
 * collector run and closes the occurrence after it. A quiesce timeout and a failed collector run wait for a decision
 * of operations. Each window is one transaction per cycle, every change is conditional on the expected state.
 *
 * <p>The paused jobs are guaranteed to be idle while the collector runs: the claimer doesn't start their runs while
 * an occurrence is active, and an occurrence opens at the earliest {@link #SETTLE} after it started quiescing, when
 * claims that read the occurrence before it existed have committed long ago (their lease is visible then).
 */
public class WindowCoordinator {

    private static final System.Logger LOG = System.getLogger(WindowCoordinator.class.getName());

    /** Minimum time between quiescing and opening, see the class comment. */
    static final Duration SETTLE = Duration.ofSeconds(1);

    private final EngineContext ctx;
    private final RunPlanner planner;
    private final Decisions decisions;
    private final WindowStore store = new WindowStore();
    private final Map<String, CronExpression> crons = new ConcurrentHashMap<>();

    public WindowCoordinator(EngineContext ctx, RunPlanner planner, Decisions decisions) {
        this.ctx = ctx;
        this.planner = planner;
        this.decisions = decisions;
    }

    /**
     * The windows of the namespace.
     *
     * @return the windows
     */
    public List<WindowDefinition> windows() {
        return ctx.tx().withConnection(c -> store.loadAll(c, ctx.namespace()));
    }

    /**
     * One cycle for one window: start a due occurrence, then move the active one on.
     *
     * @param window the window
     */
    public void cycle(WindowDefinition window) {
        ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            Instant now = ctx.clock().now(c);
            store.lockWindow(c, window.id());
            Optional<OccurrenceRow> active = store.active(c, window.id());
            if (active.isEmpty() && window.enabled() && window.scheduleCron() != null) {
                Optional<Instant> slot = dueSlot(c, window, now);
                if (slot.isPresent()) {
                    Optional<UUID> started = store.startQuiescing(c, window.id(), slot.get(), null, now);
                    if (started.isPresent()) {
                        LOG.log(System.Logger.Level.INFO, "window {0}: occurrence for {1} started", window.name(),
                                slot.get());
                        ctx.publishAfterCommit(new WindowQuiescing(occurrence(store.load(c, started.get())
                                .orElseThrow()), null));
                    }
                    active = store.active(c, window.id());
                }
            }
            if (active.isPresent()) {
                advance(c, window, active.get(), now);
            }
            return null;
        }));
    }

    /** The latest slot of the schedule that is due and has no occurrence yet (older ones collapse into it). */
    private Optional<Instant> dueSlot(Connection c, WindowDefinition window, Instant now) throws SQLException {
        CronExpression cron = crons.computeIfAbsent(window.scheduleCron(), CronExpression::parse);
        CalendarDefinition calendar = window.calendarId() == null ? null : ctx.catalog().calendar(window.calendarId());
        Instant last = store.lastScheduledAt(c, window.id()).orElse(window.createdAt());
        Instant lower = CronSchedule.lowerBound(calendar, CalendarShift.SKIP, now, ctx.config().misfireGrace());
        CronSchedule.Due due = CronSchedule.due(cron, window.zone(), calendar, CalendarShift.SKIP,
                last.isAfter(lower) ? last : lower, now, ctx.config().misfireGrace(), Integer.MAX_VALUE);
        return due.occurrences().isEmpty() ? Optional.empty()
                : Optional.of(due.occurrences().getLast().at());
    }

    private void advance(Connection c, WindowDefinition window, OccurrenceRow occurrence, Instant now)
            throws SQLException {
        switch (occurrence.state()) {
            case QUIESCING -> quiesce(c, window, occurrence, now);
            case OPEN -> collect(c, window, occurrence, now);
            default -> {
                // waiting for a decision
            }
        }
    }

    private void quiesce(Connection c, WindowDefinition window, OccurrenceRow occurrence, Instant now)
            throws SQLException {
        Set<UUID> paused = paused(window);
        int asked = store.requestStops(c, paused);
        if (asked > 0) {
            LOG.log(System.Logger.Level.INFO, "window {0}: {1} run(s) of paused jobs asked to stop", window.name(),
                    asked);
        }
        boolean settled = !now.isBefore(occurrence.stateChangedAt().plus(SETTLE));
        if (occurrence.forceOpen() || settled && store.activeRuns(c, paused) == 0) {
            if (store.open(c, occurrence.id(), now)) {
                LOG.log(System.Logger.Level.INFO, "window {0}: occurrence {1} open, cut-off {2}", window.name(),
                        occurrence.seq(), now);
                OccurrenceRow opened = store.load(c, occurrence.id()).orElseThrow();
                ctx.publishAfterCommit(new WindowOpened(occurrence(opened), occurrence.forceOpen(),
                        planCollector(c, window, opened, now)));
            }
            return;
        }
        if (!now.isBefore(occurrence.stateChangedAt().plus(window.quiesceTimeout()))) {
            waitForDecision(c, window, occurrence, "window " + window.name() + " #" + occurrence.seq()
                    + ": the paused jobs didn't stop within " + window.quiesceTimeout(),
                    BuiltInOption.WINDOW_QUIESCE, null, now);
        }
    }

    private void collect(Connection c, WindowDefinition window, OccurrenceRow occurrence, Instant now)
            throws SQLException {
        if (occurrence.collectorRunId() == null) {
            planCollector(c, window, occurrence, now); // after the decision RETRY
            return;
        }
        RunRecord run = ctx.runs().load(c, occurrence.collectorRunId());
        if (run.state() != RunState.FINISHED) {
            return;
        }
        if (run.outcome() == RunOutcome.FAILED || run.outcome() == RunOutcome.CANCELLED) {
            waitForDecision(c, window, occurrence, "window " + window.name() + " #" + occurrence.seq()
                    + ": the collector run ended " + run.outcome(), BuiltInOption.WINDOW_COLLECTOR, run.id(), now);
            return;
        }
        // packages the collector left pending (slot end, limits) are carried over to its next run (D33)
        if (store.transition(c, occurrence.id(), OccurrenceState.OPEN, OccurrenceState.CLOSED, now)) {
            LOG.log(System.Logger.Level.INFO, "window {0}: occurrence {1} closed ({2})", window.name(),
                    occurrence.seq(), run.outcome());
            ctx.publishAfterCommit(new WindowClosed(occurrence(occurrence), false, run.outcome()));
        }
    }

    /** Plans the collector run of an open occurrence; {@code null} if it can't be planned (a decision follows). */
    private UUID planCollector(Connection c, WindowDefinition window, OccurrenceRow occurrence, Instant now)
            throws SQLException {
        JobDefinition collector = ctx.catalog().byId(window.collectorJobId());
        String key = "window:" + occurrence.id() + ":" + (occurrence.collectorAttempts() + 1);
        RunPlanner.Planned planned = planner.plan(c, collector, new RunPlanner.Trigger(TriggerType.WINDOW, key,
                "window " + window.name() + " #" + occurrence.seq(), null, null), now);
        if (planned.runId() == null) {
            // a disabled collector: the occurrence waits for a decision instead of staying open forever
            waitForDecision(c, window, store.load(c, occurrence.id()).orElseThrow(), "window " + window.name()
                    + " #" + occurrence.seq() + ": the collector " + collector.name() + " can't be planned",
                    BuiltInOption.WINDOW_COLLECTOR, null, now);
            return null;
        }
        store.assignRun(c, planned.runId(), occurrence.id());
        store.collectorPlanned(c, occurrence.id(), planned.runId());
        return planned.runId();
    }

    /**
     * An occurrence as the API names it.
     *
     * @param row the occurrence
     * @return the API view
     */
    static WindowOccurrence occurrence(OccurrenceRow row) {
        return new WindowOccurrence(row.id(), row.window(), row.seq(), row.from(), row.cutOff());
    }

    private void waitForDecision(Connection c, WindowDefinition window, OccurrenceRow occurrence, String reason,
                                 List<BuiltInOption> options, UUID runId, Instant now) throws SQLException {
        if (!store.transition(c, occurrence.id(), occurrence.state(), OccurrenceState.WAITING_FOR_DECISION, now)) {
            return;
        }
        LOG.log(System.Logger.Level.WARNING, reason);
        decisions.request(c, new Decisions.NewRequest(DecisionKind.CHOICE, SubjectType.WINDOW_OCCURRENCE,
                occurrence.id(), window.collectorJobId(), runId, reason, Decisions.builtIn(options), null, null,
                Decisions.OPERATORS), now);
    }

    /** The paused jobs, without the collector itself. */
    private static Set<UUID> paused(WindowDefinition window) {
        Set<UUID> paused = new HashSet<>(window.pausedJobIds());
        paused.remove(window.collectorJobId());
        return paused;
    }
}
