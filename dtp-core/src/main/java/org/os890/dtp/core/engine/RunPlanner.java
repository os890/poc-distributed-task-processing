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
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.core.domain.ConcurrencyPolicy;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.ProcessingSlots;
import org.os890.dtp.core.domain.RunState;
import org.os890.dtp.core.domain.TriggerType;
import org.os890.dtp.core.jdbc.RunStore;

/**
 * Turns a trigger occurrence into at most one run (unique run key), applying the job's concurrency policy
 * (docs/design/05-coordination-flows.md §5.1). Used by the leader's planner, by transitions and by external triggers.
 */
public class RunPlanner {

    private final EngineContext ctx;

    public RunPlanner(EngineContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Result of planning.
     *
     * @param runId   the planned (or already existing) run, or {@code null} if the trigger was coalesced
     * @param created {@code true} if a new run row was inserted
     */
    public record Planned(UUID runId, boolean created) {
    }

    /**
     * A trigger occurrence.
     *
     * @param type        what triggered
     * @param runKey      idempotency key of the occurrence
     * @param triggeredBy details, e.g. the predecessor run
     * @param parameters  run parameters as JSON, or {@code null} for the job's defaults
     * @param notBefore   earliest start, or {@code null}
     * @param flowInstanceId the flow instance of the new run, or {@code null}
     * @param cronSlot    the cron slot of a cron trigger (before a calendar shift), or {@code null}
     */
    public record Trigger(TriggerType type, String runKey, String triggeredBy, String parameters, Instant notBefore,
                          UUID flowInstanceId, Instant cronSlot) {

        /**
         * A trigger that isn't a cron slot.
         *
         * @param type           what triggered
         * @param runKey         idempotency key of the occurrence
         * @param triggeredBy    details
         * @param parameters     run parameters as JSON, or {@code null}
         * @param notBefore      earliest start, or {@code null}
         * @param flowInstanceId the flow instance of the new run, or {@code null}
         */
        public Trigger(TriggerType type, String runKey, String triggeredBy, String parameters, Instant notBefore,
                       UUID flowInstanceId) {
            this(type, runKey, triggeredBy, parameters, notBefore, flowInstanceId, null);
        }

        /**
         * A trigger outside a flow instance.
         *
         * @param type        what triggered
         * @param runKey      idempotency key of the occurrence
         * @param triggeredBy details
         * @param parameters  run parameters as JSON, or {@code null}
         * @param notBefore   earliest start, or {@code null}
         */
        public Trigger(TriggerType type, String runKey, String triggeredBy, String parameters, Instant notBefore) {
            this(type, runKey, triggeredBy, parameters, notBefore, null, null);
        }
    }

    /**
     * Plans a run in the current transaction.
     *
     * @param c       the connection
     * @param job     the job
     * @param trigger the trigger occurrence
     * @param now     the DB time
     * @return the result
     * @throws SQLException on DB errors
     */
    public Planned plan(Connection c, JobDefinition job, Trigger trigger, Instant now) throws SQLException {
        RunStore runs = ctx.runs();
        if (!job.enabled()) {
            return new Planned(null, false);
        }
        List<RunState> open = runs.openRunStates(c, job.id());
        RunState state = RunState.PENDING;
        RunOutcome outcome = null;
        // a fallback run belongs to one decision request, a run of a flow instance to its business key (e.g. the
        // business date): never skipped or coalesced, the job lease runs them one after the other
        ConcurrencyPolicy policy = trigger.type() == TriggerType.FALLBACK || trigger.flowInstanceId() != null
                ? ConcurrencyPolicy.QUEUE_ALL : job.concurrency();
        if (!open.isEmpty()) {
            switch (policy) {
                case SKIP -> {
                    state = RunState.FINISHED;
                    outcome = RunOutcome.SKIPPED;
                }
                case QUEUE_ONE -> {
                    if (open.contains(RunState.PENDING)) {
                        return new Planned(null, false);
                    }
                }
                case QUEUE_ALL -> {
                    // another pending run
                }
                default -> throw new IllegalStateException("unknown policy " + policy);
            }
        }
        String parameters = trigger.parameters() != null ? trigger.parameters() : job.defaultParameters();
        Instant notBefore = trigger.notBefore();
        Instant slotEnd = null;
        if (state == RunState.PENDING && job.hasSlots()) {
            // the run starts within a processing slot and is asked to stop at its end (§5.1, D39)
            Instant earliest = notBefore != null && notBefore.isAfter(now) ? notBefore : now;
            ProcessingSlots.Window window = slotWindow(job, earliest);
            notBefore = window.start().isAfter(earliest) ? window.start() : notBefore;
            slotEnd = window.end();
        }
        UUID id = UUID.randomUUID();
        boolean created = runs.insertIfAbsent(c, new RunStore.NewRun(id, ctx.namespace(), trigger.runKey(), job.id(),
                job.revision(), job.handlerClass(), state, outcome, trigger.type(), trigger.triggeredBy(), parameters,
                notBefore, trigger.flowInstanceId(), trigger.cronSlot(), slotEnd), now);
        if (created) {
            return new Planned(id, true);
        }
        return new Planned(runs.findByKey(c, ctx.namespace(), trigger.runKey()).orElse(null), false);
    }

    /**
     * The processing slot window of a job that is open at the given point in time, or else the next one.
     *
     * @param job the job, with slots
     * @param at  the point in time
     * @return the window
     * @throws IllegalStateException if no slot opens within {@link ProcessingSlots#HORIZON}
     */
    public ProcessingSlots.Window slotWindow(JobDefinition job, Instant at) {
        return ProcessingSlots.window(job.slots(), ctx.catalog()::calendar, job.zone(), at)
                              .orElseThrow(() -> new IllegalStateException("job " + job.name()
                                      + " has no processing slot within " + ProcessingSlots.HORIZON.toDays()
                                      + " days after " + at));
    }
}
