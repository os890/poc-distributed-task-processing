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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.StopReason;
import org.os890.dtp.api.event.JobDisabled;
import org.os890.dtp.api.event.JobEnabled;
import org.os890.dtp.api.event.RunFinished;
import org.os890.dtp.api.event.RunRef;
import org.os890.dtp.api.event.RunStopRequested;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.TriggerType;
import org.os890.dtp.core.jdbc.DecisionStore;
import org.os890.dtp.core.jdbc.RunStore;
import org.os890.dtp.core.jdbc.Sql;

/**
 * Operations on job definitions and runs by name, for the REST adapter and operators (mission §12): the definitions
 * with their triggers and revisions, enabling and disabling, triggering a run with JSON parameters, cancelling a run.
 * Definitions themselves stay SQL (D1): new revisions are written by Flyway scripts.
 */
public class JobAdmin {

    private static final System.Logger LOG = System.getLogger(JobAdmin.class.getName());

    private final EngineContext ctx;
    private final RunPlanner planner;
    private final FlowInstances instances;
    private final DecisionStore decisions = new DecisionStore();

    public JobAdmin(EngineContext ctx, RunPlanner planner) {
        this.ctx = ctx;
        this.planner = planner;
        this.instances = new FlowInstances(ctx);
    }

    /**
     * A job with its current revision.
     *
     * @param name          the name
     * @param revision      the current revision
     * @param enabled       disabled jobs aren't planned
     * @param handlerClass  the process step
     * @param triggers      e.g. {@code cron 0 0 18 ? * MON-FRI}, {@code inbox 1}, {@code external}, {@code asap}
     * @param retryMax      retries
     * @param retryBackoff  backoff of a retry
     * @param maxItemVisits loop limit, or {@code null}
     * @param ackRequired   sent packages wait for an acknowledgement
     * @param ackTimeout    the timeout of an acknowledgement, or {@code null}
     * @param slots         number of processing slots
     */
    public record JobView(String name, int revision, boolean enabled, String handlerClass, List<String> triggers,
                          int retryMax, Duration retryBackoff, Integer maxItemVisits, boolean ackRequired,
                          Duration ackTimeout, int slots) {
    }

    /**
     * A revision of a job.
     *
     * @param revision     the revision
     * @param handlerClass the process step
     * @param changedBy    who wrote it
     * @param changedAt    when
     */
    public record RevisionView(int revision, String handlerClass, String changedBy, Instant changedAt) {
    }

    /**
     * The jobs of the namespace.
     *
     * @return the jobs, by name
     */
    public List<JobView> jobs() {
        return ctx.catalog().all().stream().sorted(Comparator.comparing(JobDefinition::name)).map(JobAdmin::view)
                  .toList();
    }

    /**
     * The revisions of a job, newest first.
     *
     * @param name the job name
     * @return the revisions
     */
    public List<RevisionView> revisions(String name) {
        JobDefinition job = byName(name);
        return ctx.tx().withConnection(c -> Sql.query(c, "SELECT revision, handler_class, changed_by, changed_at"
                + " FROM dtp_job_revision WHERE job_id = ? ORDER BY revision DESC",
                rs -> new RevisionView(rs.getInt("revision"), rs.getString("handler_class"),
                        rs.getString("changed_by"), Sql.instant(rs, "changed_at")), job.id()));
    }

    /**
     * Enables or disables a job: a disabled job isn't planned, running runs finish.
     *
     * @param name    the job name
     * @param enabled the new state
     * @param by      who
     * @return {@code true} if the state changed
     */
    public boolean setEnabled(String name, boolean enabled, String by) {
        Objects.requireNonNull(by, "by");
        JobDefinition job = byName(name);
        boolean changed = ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            if (Sql.update(c, "UPDATE dtp_job SET enabled = ?, updated_at = ? WHERE id = ? AND enabled <> ?", enabled,
                    ctx.clock().now(c), job.id(), enabled) != 1) {
                return false;
            }
            ctx.publishAfterCommit(enabled ? new JobEnabled(name, by) : new JobDisabled(name, by));
            return true;
        }));
        if (changed) {
            ctx.catalog().invalidate();
            LOG.log(System.Logger.Level.INFO, "job {0} {1} by {2}", name, enabled ? "enabled" : "disabled", by);
        }
        return changed;
    }

    /**
     * Triggers a run of a job that allows external triggers. The parameters are checked against the parameter type
     * of the job handler if this node has it.
     *
     * @param name           the job name
     * @param parameters     the run parameters as JSON, or {@code null} for none
     * @param idempotencyKey a second trigger with the same key plans no second run, or {@code null}
     * @return the run
     */
    public RunHandle trigger(String name, String parameters, String idempotencyKey) {
        JobDefinition job = byName(name);
        if (!job.external()) {
            throw new DtpException("job " + name + " doesn't allow external triggers (trigger_external)");
        }
        String json = parameters == null || parameters.isBlank() ? "{}" : parameters;
        ctx.steps().entrySet().stream().filter(e -> e.getKey().getName().equals(job.handlerClass())).findFirst()
           .map(Map.Entry::getValue).ifPresent(step -> validate(name, step, json));
        String key = idempotencyKey != null ? idempotencyKey : UUID.randomUUID().toString();
        RunPlanner.Planned planned = ctx.tx().required(() -> ctx.tx().withConnection(c -> planner.plan(c, job,
                new RunPlanner.Trigger(TriggerType.EXTERNAL, "external:" + job.id() + ":" + key, key, json, null),
                ctx.clock().now(c))));
        return new RunHandle(planned.runId() == null ? null : planned.runId().toString(), planned.created());
    }

    /**
     * Cancels a run: a pending run or a run that waits for a decision is finished as {@code CANCELLED} (the open
     * decision request is withdrawn), a running one is asked to stop softly.
     *
     * @param runId the run
     * @return {@code true} if the run was pending, waiting or running
     */
    public boolean cancel(UUID runId) {
        return cancel(runId, null);
    }

    /**
     * Cancels a run: a pending run or a run that waits for a decision is finished as {@code CANCELLED} (the open
     * decision request is withdrawn), a running one is asked to stop softly.
     *
     * @param runId the run
     * @param by    who cancels it, or {@code null}
     * @return {@code true} if the run was pending, waiting or running
     */
    public boolean cancel(UUID runId, String by) {
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            // the lock order of a cancel: the open requests about the run, its flow instance, the run
            List<UUID> requests = decisions.lockOpenOfRuns(c, List.of(runId));
            instances.lockForSuccessors(c, ctx.runs().load(c, runId).flowInstanceId());
            Instant now = ctx.clock().now(c);
            RunStore.Cancelled cancelled = ctx.runs().cancel(c, runId, now);
            if (cancelled != RunStore.Cancelled.NONE) {
                for (UUID request : requests) {
                    decisions.withdraw(c, request, by, "run cancelled", now);
                }
                RunRef run = ctx.runRef(c, runId);
                ctx.publishAfterCommit(cancelled == RunStore.Cancelled.FINISHED
                        ? new RunFinished(run, RunOutcome.CANCELLED, null, StopReason.CANCEL, null)
                        : new RunStopRequested(run, StopReason.CANCEL, by));
            }
            instances.runEnded(c, runId, now);
            return cancelled != RunStore.Cancelled.NONE;
        }));
    }

    private void validate(String name, StepInfo step, String json) {
        Class<?> type = step.parameterType();
        if (type == null || type == NoParams.class) {
            return;
        }
        try {
            ctx.json().read(json, type);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("parameters of job " + name + " don't match "
                    + type.getSimpleName() + ": " + e.getMessage(), e);
        }
    }

    private JobDefinition byName(String name) {
        return ctx.catalog().all().stream().filter(job -> job.name().equals(name)).findFirst()
                  .orElseThrow(() -> new IllegalArgumentException("unknown job " + name));
    }

    private static JobView view(JobDefinition job) {
        List<String> triggers = new ArrayList<>();
        if (job.asap()) {
            triggers.add("asap");
        }
        if (job.cronExpression() != null) {
            triggers.add("cron " + job.cronExpression() + (job.cronZone() == null ? "" : " " + job.cronZone()));
        }
        if (job.inboxMinItems() != null) {
            triggers.add("inbox " + job.inboxMinItems());
        }
        if (job.external()) {
            triggers.add("external");
        }
        if (!job.joinMembers().isEmpty()) {
            triggers.add("join of " + job.joinMembers().size());
        }
        return new JobView(job.name(), job.revision(), job.enabled(), job.handlerClass(), triggers, job.retryMax(),
                job.retryBackoff(), job.maxItemVisits(), job.ackRequired(), job.ackTimeout(), job.slots().size());
    }
}
