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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.api.FlowInstance;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.event.FlowInstanceCancelled;
import org.os890.dtp.api.event.FlowInstanceCompleted;
import org.os890.dtp.api.event.FlowInstanceStarted;
import org.os890.dtp.api.event.JoinFired;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.domain.CronSchedule;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.JoinMember;
import org.os890.dtp.core.domain.RunState;
import org.os890.dtp.core.domain.TriggerType;
import org.os890.dtp.core.jdbc.DecisionStore;
import org.os890.dtp.core.jdbc.FlowInstanceStore;
import org.os890.dtp.core.jdbc.FlowInstanceStore.ArrivalRow;
import org.os890.dtp.core.jdbc.FlowInstanceStore.Cancel;
import org.os890.dtp.core.jdbc.FlowInstanceStore.InstanceRow;
import org.os890.dtp.core.jdbc.FlowInstanceStore.InstanceRun;
import org.os890.dtp.core.jdbc.FlowInstanceStore.JoinRow;
import org.os890.dtp.core.jdbc.RunStore.RunRecord;

/**
 * Flow instances (DD-4) and run joins (DD-9). A flow instance groups the runs of one end-to-end process, e.g. the end
 * of day of a business date: the root run and its successors. A run join plans its target once per instance when all
 * members finished with an accepted outcome. Every finishing run of an instance locks the instance row, then the row of
 * each join it is a member of: concurrent arrivals are serialized, so exactly one of them fires the join and exactly
 * one of them completes the instance. An operator can cancel an open instance ({@link #cancel}).
 * <p>
 * Lock order: the open decision requests about runs, then the instance, then its runs. A decision about a run locks
 * its request first, a finishing run locks its instance before its run row.
 */
public class FlowInstances {

    private static final System.Logger LOG = System.getLogger(FlowInstances.class.getName());

    private final EngineContext ctx;
    private final RunPlanner planner;
    private final FlowInstanceStore store;
    private final DecisionStore decisions = new DecisionStore();

    public FlowInstances(EngineContext ctx) {
        this.ctx = ctx;
        this.planner = new RunPlanner(ctx);
        this.store = new FlowInstanceStore(ctx.dialect());
    }

    /**
     * A flow instance with its runs and joins.
     *
     * @param id          the instance
     * @param name        the flow name: the root job
     * @param businessKey the business key
     * @param state       OPEN, COMPLETED or CANCELLED
     * @param startedAt   when it was started
     * @param completedAt when it completed, or {@code null}
     * @param cancel      who cancelled it, when and why, or {@code null}
     * @param runs        its runs, in the order they were planned
     * @param joins       its run joins
     */
    public record InstanceView(UUID id, String name, String businessKey, String state, Instant startedAt,
                               Instant completedAt, CancelView cancel, List<RunView> runs, List<JoinView> joins) {
    }

    /**
     * The cancel of an instance.
     *
     * @param at     when
     * @param by     who
     * @param reason why
     */
    public record CancelView(Instant at, String by, String reason) {
    }

    /**
     * What a cancel did.
     *
     * @param runs      the runs cancelled or asked to stop
     * @param withdrawn the decision requests withdrawn
     */
    public record Cancelled(int runs, int withdrawn) {
    }

    /**
     * A run of an instance.
     *
     * @param id          the run
     * @param job         the job
     * @param state       the state
     * @param outcome     the outcome, or {@code null}
     * @param triggerType what created it
     * @param triggeredBy details of the trigger
     * @param createdAt   when it was planned
     * @param finishedAt  when it finished, or {@code null}
     */
    public record RunView(UUID id, String job, String state, String outcome, String triggerType, String triggeredBy,
                          Instant createdAt, Instant finishedAt) {
    }

    /**
     * A run join of an instance.
     *
     * @param target     the join target
     * @param fired      {@code true} once the target's run was planned
     * @param runId      the run of the target, or {@code null}
     * @param arrivals   the latest run of each member that finished
     * @param waitingFor the members without an accepted outcome yet
     */
    public record JoinView(String target, boolean fired, UUID runId, List<ArrivalView> arrivals,
                           List<String> waitingFor) {
    }

    /**
     * The latest run of a member.
     *
     * @param member   the member job
     * @param outcome  the outcome of its latest run
     * @param accepted {@code true} if the outcome counts
     * @param runId    the run
     */
    public record ArrivalView(String member, String outcome, boolean accepted, UUID runId) {
    }

    /**
     * Starts an instance in the caller's transaction, or returns the existing one.
     *
     * @param c           the connection
     * @param root        the root job
     * @param businessKey the business key
     * @param parameters  the parameters of the root run as JSON
     * @param now         the DB time
     * @return the root run, {@code created} only for a new instance
     * @throws SQLException on DB errors
     */
    RunPlanner.Planned start(Connection c, JobDefinition root, String businessKey, String parameters, Instant now)
            throws SQLException {
        return start(c, root, businessKey, new RunPlanner.Trigger(TriggerType.EXTERNAL, null,
                "flow instance " + root.name() + " " + businessKey, parameters, null, null), now);
    }

    /**
     * Starts the instance of a cron slot of a root job with {@code cron_flow_instance} in the caller's transaction:
     * the business key is the business date of the slot, the root run gets the job's default parameters. Further
     * slots of the same business date find the existing instance.
     *
     * @param c          the connection
     * @param root       the root job
     * @param occurrence the due cron slot
     * @param now        the DB time
     * @return the root run, {@code created} only for a new instance
     * @throws SQLException on DB errors
     */
    RunPlanner.Planned startScheduled(Connection c, JobDefinition root, CronSchedule.Occurrence occurrence,
                                      Instant now) throws SQLException {
        String businessKey = occurrence.businessDate().toString();
        return start(c, root, businessKey, new RunPlanner.Trigger(TriggerType.CRON, null, "cron slot "
                + occurrence.at() + ", flow instance " + root.name() + " " + businessKey, null, occurrence.at(), null,
                occurrence.slot()), now);
    }

    private RunPlanner.Planned start(Connection c, JobDefinition root, String businessKey, RunPlanner.Trigger trigger,
                                     Instant now) throws SQLException {
        if (!root.enabled()) {
            throw new IllegalStateException("job " + root.name() + " is disabled");
        }
        InstanceRow row = new InstanceRow(UUID.randomUUID(), ctx.namespace(), root.name(), businessKey, root.id(),
                "OPEN", now, null, null);
        boolean created = store.insertIfAbsent(c, row);
        InstanceRow instance = created ? row : store.find(c, ctx.namespace(), root.name(), businessKey).orElseThrow();
        RunPlanner.Planned run = planner.plan(c, root, new RunPlanner.Trigger(trigger.type(),
                "flow:" + instance.id() + ":root", trigger.triggeredBy(), trigger.parameters(), trigger.notBefore(),
                instance.id(), trigger.cronSlot()), now);
        if (created) {
            ctx.publishAfterCommit(new FlowInstanceStarted(api(instance)));
        }
        return new RunPlanner.Planned(run.runId(), created && run.created());
    }

    /**
     * Called in the transaction that finished a run (also when a decision or a cancel finished it): records the
     * arrival at the run joins the job is a member of, fires the joins whose members all arrived, and completes the
     * instance when nothing is left: no pending or active run, no waiting join and no failed run that wasn't run again.
     * An instance with a failed run stays open for an operator ({@link #rerun}). A run that stopped early with work
     * left ({@code DEFERRED}: end of its slot, its limit, a drain) doesn't arrive at its joins: it's continued by a
     * new run of the same job with the same parameters in the same instance ({@code continue:<run>}), which arrives
     * once it finishes. If the job is disabled, the deferred run keeps the instance open like a failed one.
     *
     * @param c     the connection
     * @param runId the finished run
     * @param now   the DB time
     * @throws SQLException on DB errors
     */
    public void runEnded(Connection c, UUID runId, Instant now) throws SQLException {
        RunRecord run = ctx.runs().load(c, runId);
        if (run.flowInstanceId() == null || run.state() != RunState.FINISHED) {
            return;
        }
        InstanceRow instance = store.lock(c, run.flowInstanceId()).orElseThrow();
        if (!instance.isOpen()) {
            LOG.log(instance.cancel() != null ? System.Logger.Level.INFO : System.Logger.Level.WARNING,
                    "run {0} ended ({1}) after its flow instance {2} {3} was {4}", runId, run.outcome(),
                    instance.flowName(), instance.businessKey(), instance.state().toLowerCase(Locale.ROOT));
            return;
        }
        if (run.outcome() == RunOutcome.DEFERRED) {
            continueDeferred(c, instance, run, now);
        } else {
            for (JobDefinition target : ctx.catalog().all()) {
                if (target.joinMember(run.jobId()).isPresent()) {
                    arrive(c, instance, target, run, now);
                }
            }
        }
        if (store.countOpenRuns(c, instance.id()) == 0 && store.countWaitingJoins(c, instance.id()) == 0
                && store.countUnresolvedRuns(c, instance.id()) == 0 && store.complete(c, instance.id(), now)) {
            ctx.publishAfterCommit(new FlowInstanceCompleted(api(instance)));
        }
    }

    private void continueDeferred(Connection c, InstanceRow instance, RunRecord run, Instant now)
            throws SQLException {
        JobDefinition job = ctx.catalog().byId(run.jobId());
        String why = run.stopReason() == null ? "" : " (" + run.stopReason() + ")";
        // a job with processing slots continues in its next slot: the planner places the run there
        RunPlanner.Planned planned = planner.plan(c, job, new RunPlanner.Trigger(TriggerType.CONTINUATION,
                "continue:" + run.id(), "continuation of " + run.id() + why, run.parameters(), null, instance.id()),
                now);
        if (planned.runId() == null) {
            LOG.log(System.Logger.Level.WARNING, "run {0} of the disabled job {1} deferred work of flow instance {2}"
                    + " {3}: the instance stays open until the run is run again", run.id(), job.name(),
                    instance.flowName(), instance.businessKey());
        }
    }

    private void arrive(Connection c, InstanceRow instance, JobDefinition target, RunRecord run, Instant now)
            throws SQLException {
        store.ensureJoin(c, target.id(), instance.id(), now);
        JoinRow join = store.lockJoin(c, target.id(), instance.id());
        if (join.fired()) {
            LOG.log(System.Logger.Level.INFO, "run {0} arrived after the join of {1} fired, ignored", run.id(),
                    target.name());
            return;
        }
        store.arrive(c, target.id(), instance.id(), new ArrivalRow(run.jobId(), run.id(), run.outcome(), now));
        List<ArrivalRow> arrivals = store.arrivals(c, target.id(), instance.id());
        if (!waitingFor(target, arrivals).isEmpty()) {
            return;
        }
        RunPlanner.Planned planned = planner.plan(c, target, new RunPlanner.Trigger(TriggerType.JOIN,
                "join:" + target.id() + ":" + instance.id(), "join of " + memberNames(target), null, null,
                instance.id()), now);
        if (planned.runId() != null) {
            store.fire(c, target.id(), instance.id(), planned.runId(), now);
            ctx.publishAfterCommit(new JoinFired(api(instance), target.name(), planned.runId()));
        }
    }

    /**
     * Runs a finished run of an open instance again, e.g. a failed member of a join: a new run of the same job with
     * the same parameters in the same instance. Its arrival replaces the one of the old run. Idempotent per run.
     *
     * @param runId the finished run
     * @param by    who asked
     * @return the new run
     */
    public UUID rerun(UUID runId, String by) {
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            RunRecord run = ctx.runs().load(c, runId);
            if (run.flowInstanceId() == null) {
                throw new DtpException("run " + runId + " isn't part of a flow instance");
            }
            if (run.state() != RunState.FINISHED) {
                throw new DtpException("run " + runId + " didn't finish yet");
            }
            InstanceRow instance = store.lock(c, run.flowInstanceId()).orElseThrow();
            if (!instance.isOpen()) {
                throw new DtpException("flow instance " + instance.flowName() + " " + instance.businessKey()
                        + " is " + instance.state().toLowerCase(Locale.ROOT) + " already");
            }
            JobDefinition job = ctx.catalog().byId(run.jobId());
            RunPlanner.Planned planned = planner.plan(c, job, new RunPlanner.Trigger(TriggerType.RERUN,
                    "rerun:" + runId, "rerun of " + runId + " by " + by, run.parameters(), null, instance.id()),
                    ctx.clock().now(c));
            if (planned.runId() == null) {
                throw new DtpException("job " + job.name() + " is disabled");
            }
            return planned.runId();
        }));
    }

    /**
     * Cancels an open instance, e.g. the end of day of a business date that was started by mistake: its pending runs
     * and the runs that wait for a decision finish as {@code CANCELLED}, its running runs are asked to stop softly
     * (they end as {@code CANCELLED} once their handler noticed it, or with their own outcome if they finished before),
     * the open decision requests about its runs are withdrawn. Nothing is planned in the instance any more: no
     * transitions, no joins, no continuations, no reruns. Its business key stays taken: starting it again finds the
     * cancelled instance.
     *
     * @param instanceId the instance
     * @param by         who cancels it
     * @param reason     why (required)
     * @return what the cancel did
     * @throws DtpException if the instance completed or was cancelled already
     */
    public Cancelled cancel(UUID instanceId, String by, String reason) {
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("a cancel needs a reason");
        }
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            if (store.load(c, instanceId).isEmpty()) {
                throw new IllegalArgumentException("unknown flow instance " + instanceId);
            }
            List<UUID> runIds = store.runs(c, instanceId).stream().map(InstanceRun::id).toList();
            List<UUID> requests = decisions.lockOpenOfRuns(c, runIds);
            InstanceRow instance = store.lock(c, instanceId).orElseThrow();
            Instant now = ctx.clock().now(c);
            if (!store.cancel(c, instanceId, new Cancel(now, by, reason))) {
                throw new DtpException("flow instance " + instance.flowName() + " " + instance.businessKey()
                        + " is " + instance.state().toLowerCase(Locale.ROOT) + " already");
            }
            int runs = store.cancelRuns(c, instanceId, now);
            int withdrawn = 0;
            for (UUID request : requests) {
                if (decisions.withdraw(c, request, by, "flow instance cancelled: " + reason, now)) {
                    withdrawn++;
                }
            }
            LOG.log(System.Logger.Level.INFO, "flow instance {0} {1} cancelled by {2} ({3}): {4} runs cancelled or"
                    + " asked to stop, {5} decision requests withdrawn", instance.flowName(), instance.businessKey(),
                    by, reason, runs, withdrawn);
            ctx.publishAfterCommit(new FlowInstanceCancelled(api(instance), by, reason));
            return new Cancelled(runs, withdrawn);
        }));
    }

    /**
     * Locks the instance of a finishing run before its run row (the lock order of {@link #cancel}).
     *
     * @param c          the connection
     * @param instanceId the instance, or {@code null} outside an instance
     * @return {@code false} if the instance completed or was cancelled: nothing is planned in it any more
     * @throws SQLException on DB errors
     */
    boolean lockForSuccessors(Connection c, UUID instanceId) throws SQLException {
        return instanceId == null || store.lock(c, instanceId).map(InstanceRow::isOpen).orElse(false);
    }

    /**
     * The latest instances, newest first.
     *
     * @param limit at most this many
     * @return the instances
     */
    public List<InstanceView> latest(int limit) {
        return ctx.tx().withConnection(c -> {
            List<InstanceView> views = new ArrayList<>();
            for (InstanceRow row : store.latest(c, ctx.namespace(), limit)) {
                views.add(view(c, row));
            }
            return views;
        });
    }

    /**
     * An instance.
     *
     * @param id the instance
     * @return the instance
     */
    public InstanceView get(UUID id) {
        return ctx.tx().withConnection(c -> view(c, store.load(c, id).orElseThrow(
                () -> new IllegalArgumentException("unknown flow instance " + id))));
    }

    /**
     * An instance by flow name and business key.
     *
     * @param name        the flow name: the root job
     * @param businessKey the business key
     * @return the instance, if it exists
     */
    public Optional<InstanceView> find(String name, String businessKey) {
        return ctx.tx().withConnection(c -> {
            Optional<InstanceRow> row = store.find(c, ctx.namespace(), name, businessKey);
            return row.isEmpty() ? Optional.<InstanceView>empty() : Optional.of(view(c, row.get()));
        });
    }

    /**
     * The instance of a run.
     *
     * @param runId the run
     * @return the instance, empty outside an instance
     */
    Optional<FlowInstance> ofRun(UUID runId) {
        return ctx.tx().withConnection(c -> store.ofRun(c, runId).map(FlowInstances::api));
    }

    private InstanceView view(Connection c, InstanceRow row) throws SQLException {
        List<RunView> runs = store.runs(c, row.id()).stream().map(r -> new RunView(r.id(), jobName(r.jobId()),
                r.state(), r.outcome(), r.triggerType(), r.triggeredBy(), r.createdAt(), r.finishedAt())).toList();
        List<JoinView> joins = new ArrayList<>();
        for (JoinRow join : store.joins(c, row.id())) {
            JobDefinition target = ctx.catalog().byId(join.targetJobId());
            List<ArrivalRow> arrivals = store.arrivals(c, join.targetJobId(), row.id());
            List<ArrivalView> arrived = arrivals.stream().map(a -> new ArrivalView(jobName(a.memberJobId()),
                    a.outcome().name(), target.joinMember(a.memberJobId()).map(m -> m.accepts(a.outcome()))
                                              .orElse(false), a.runId())).toList();
            joins.add(new JoinView(target.name(), join.fired(), join.runId(), arrived,
                    join.fired() ? List.of() : waitingFor(target, arrivals)));
        }
        CancelView cancel = row.cancel() == null ? null
                : new CancelView(row.cancel().at(), row.cancel().by(), row.cancel().reason());
        return new InstanceView(row.id(), row.flowName(), row.businessKey(), row.state(), row.startedAt(),
                row.completedAt(), cancel, runs, joins);
    }

    private List<String> waitingFor(JobDefinition target, List<ArrivalRow> arrivals) {
        List<String> waiting = new ArrayList<>();
        for (JoinMember member : target.joinMembers()) {
            boolean arrived = arrivals.stream()
                    .anyMatch(a -> a.memberJobId().equals(member.jobId()) && member.accepts(a.outcome()));
            if (!arrived) {
                waiting.add(jobName(member.jobId()));
            }
        }
        return waiting;
    }

    private String memberNames(JobDefinition target) {
        return String.join(", ", target.joinMembers().stream().map(m -> jobName(m.jobId())).toList());
    }

    private String jobName(UUID jobId) {
        return ctx.catalog().byId(jobId).name();
    }

    private static FlowInstance api(InstanceRow row) {
        return new FlowInstance(row.id(), row.flowName(), row.businessKey());
    }
}
