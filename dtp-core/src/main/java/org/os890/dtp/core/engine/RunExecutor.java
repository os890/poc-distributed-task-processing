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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.StopReason;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.LeaseLostException;
import org.os890.dtp.core.domain.RouteModel;
import org.os890.dtp.core.domain.TransitionDefinition;
import org.os890.dtp.core.domain.TriggerType;
import org.os890.dtp.core.jdbc.RunStore;
import org.os890.dtp.core.jdbc.RunStore.RunRecord;
import org.os890.dtp.core.spi.Telemetry;

/**
 * Executes claimed runs on virtual threads and finishes them: outcome (docs/design/04-lifecycles.md §4.1), fenced
 * finish with lease release, and the transitions to successor jobs in the same transaction.
 */
public class RunExecutor {

    private static final System.Logger LOG = System.getLogger(RunExecutor.class.getName());

    private final EngineContext ctx;
    private final RunPlanner planner;
    private final ItemRouting routing;
    private final Runnable onFinished;
    private final FlowRouting flows;
    private final Map<UUID, RunContext> active = new ConcurrentHashMap<>();
    private final ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();

    private volatile boolean abandoned;

    public RunExecutor(EngineContext ctx, RunPlanner planner, ItemRouting routing, FlowRouting flows,
                       Runnable onFinished) {
        this.ctx = ctx;
        this.planner = planner;
        this.routing = routing;
        this.onFinished = onFinished;
        this.flows = flows;
        this.instances = new FlowInstances(ctx);
    }

    private final FlowInstances instances;

    /** How an execution ended; {@code decision} is the request the run waits for instead of finishing. */
    private record Result(RunOutcome outcome, String resultType, String resultStatus, String error, UUID decision) {

        Result(RunOutcome outcome, String resultType, String resultStatus, String error) {
            this(outcome, resultType, resultStatus, error, null);
        }
    }

    /** Number of runs this node can still start. */
    public int freeSlots() {
        return ctx.config().maxConcurrentRuns() - active.size();
    }

    /** The runs currently executed by this node. */
    public Collection<RunContext> active() {
        return List.copyOf(active.values());
    }

    /**
     * Starts a claimed run.
     *
     * @param claimed the run
     */
    public void submit(ClaimedRun claimed) {
        JobDefinition job = ctx.catalog().revision(claimed.jobId(), claimed.revision());
        RunContext run = new RunContext(ctx, claimed, job, ctx.step(claimed.handlerClass()));
        active.put(claimed.runId(), run);
        threads.submit(() -> execute(run));
    }

    /**
     * Asks all runs to stop softly and waits for them.
     *
     * @param reason  the stop reason
     * @param timeout maximum wait in milliseconds
     */
    public void stopAll(StopReason reason, long timeout) {
        active.values().forEach(run -> run.requestStop(reason));
        threads.shutdown();
        try {
            if (!threads.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
                LOG.log(System.Logger.Level.WARNING, "{0} runs didn't stop within {1} ms, their leases expire",
                        active.size(), timeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Test support: simulates a crashed or partitioned node. No new runs start, running handlers continue as zombies
     * whose commits are fenced once their leases were taken over.
     */
    public void abandon() {
        abandoned = true;
        threads.shutdown();
    }

    private void execute(RunContext run) {
        UUID runId = run.claimed().runId();
        try {
            Result result;
            try {
                result = run(run);
            } catch (LeaseLostException e) {
                throw e;
            } catch (RuntimeException | Error e) {
                LOG.log(System.Logger.Level.WARNING, "run " + runId + " of job " + run.job().name() + " failed", e);
                result = new Result(RunOutcome.FAILED, null, null, ctx.json().write(Map.of("type",
                        e.getClass().getName(),
                        "message", String.valueOf(e.getMessage()))));
            }
            finish(run, result);
        } catch (LeaseLostException e) {
            LOG.log(System.Logger.Level.INFO, "run {0} of job {1} was fenced: {2}", runId, run.job().name(),
                    e.getMessage());
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR, "finishing run " + runId + " failed, its lease expires", e);
        } finally {
            active.remove(runId);
            if (!abandoned) {
                onFinished.run();
            }
        }
    }

    private Result run(RunContext run) {
        ProcessStep step = ctx.registry().instance(run.step().stepClass());
        if (step instanceof ItemProcessor<?, ?> processor) {
            boolean remaining = new ItemProcessorDriver(run, routing, processor).run();
            return new Result(outcome(run, remaining), null, null, null);
        }
        JobHandler<Object, ?> handler = handler(step);
        run.onStop(() -> handler.onStop(run.stopReason()));
        Object parameters = parameters(run);
        JobResult<?> result = run.unitOfWork(false, () -> {
            try (Telemetry.Span span = ctx.telemetry().startUnitOfWork(run.job().name(), run.claimed().runId(),
                    run.claimed().attempt(), List.of())) {
                try {
                    return handler.execute(new JobContextImpl<>(run, routing, parameters));
                } catch (RuntimeException | Error e) {
                    span.failed(e);
                    throw e;
                }
            }
        });
        if (result == null) {
            throw new IllegalStateException(handler.getClass().getName() + ".execute returned null");
        }
        return new Result(outcome(run, run.stopReason() != null), RouteModel.typeOf(result.status()),
                result.status().name(), null, run.decisionRequest());
    }

    @SuppressWarnings("unchecked")
    private static JobHandler<Object, ?> handler(ProcessStep step) {
        return (JobHandler<Object, ?>) step;
    }

    private Object parameters(RunContext run) {
        Class<?> type = run.step().parameterType();
        if (type == NoParams.class) {
            return new NoParams();
        }
        RunRecord record = ctx.tx().withConnection(c -> ctx.runs().load(c, run.claimed().runId()));
        return ctx.json().read(record.parameters(), type);
    }

    private RunOutcome outcome(RunContext run, boolean remainingWork) {
        StopReason stop = run.stopReason();
        if (stop == StopReason.CANCEL) {
            return RunOutcome.CANCELLED;
        }
        if (stop != null && remainingWork) {
            return RunOutcome.DEFERRED;
        }
        int issues = ctx.tx().withConnection(c -> ctx.runs().countIssues(c, run.claimed().runId()));
        return issues > 0 ? RunOutcome.COMPLETED_WITH_ISSUES : RunOutcome.SUCCEEDED;
    }

    private void finish(RunContext run, Result result) {
        if (result.decision() != null) {
            waitForDecision(run, result.decision());
            return;
        }
        ClaimedRun claimed = run.claimed();
        ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            var now = ctx.clock().now(c);
            RunStore.RunEnd end = new RunStore.RunEnd(result.outcome(), result.resultStatus(),
                    run.runInfo().toJson(), result.error(), run.stopReason());
            if (!ctx.runs().finish(c, claimed.runId(), claimed.token(), claimed.attempt(), end, now)) {
                throw new LeaseLostException(claimed.runId(), claimed.token());
            }
            // successors belong to the flow instance of the run (DD-4)
            UUID flowInstance = ctx.runs().load(c, claimed.runId()).flowInstanceId();
            for (TransitionDefinition transition : run.job().transitions()) {
                if (transition.matches(result.outcome(), result.resultStatus())) {
                    JobDefinition target = ctx.catalog().byId(transition.targetJobId());
                    planner.plan(c, target, new RunPlanner.Trigger(TriggerType.TRANSITION,
                            "transition:" + claimed.runId() + ":" + target.id(), claimed.runId().toString(), null,
                            null, flowInstance), now);
                }
            }
            instances.runEnded(c, claimed.runId(), now);
            boolean succeeded = result.outcome() == RunOutcome.SUCCEEDED
                    || result.outcome() == RunOutcome.COMPLETED_WITH_ISSUES;
            if (flows != null && succeeded && result.resultStatus() != null) {
                flows.runSucceeded(c, run.job(), claimed.runId(), result.resultType(), result.resultStatus(), now);
            }
            // the release is the fence check of the finish (DD-12): the last statement before the commit
            if (!ctx.leases().release(c, claimed.jobId(), claimed.runId(), claimed.token())) {
                throw new LeaseLostException(claimed.runId(), claimed.token());
            }
            return null;
        }));
        ctx.telemetry().runFinished(run.job().name(), result.outcome().name(), run.elapsed());
        LOG.log(System.Logger.Level.DEBUG, "run {0} of job {1} finished: {2}", claimed.runId(), run.job().name(),
                result.outcome());
    }

    /**
     * The handler requested a decision: the run waits for it instead of finishing. No transitions, no join arrival;
     * the decision plans the run again or finishes it.
     */
    private void waitForDecision(RunContext run, UUID decision) {
        ClaimedRun claimed = run.claimed();
        ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            if (!ctx.runs().waitForDecision(c, claimed.runId(), claimed.token(), claimed.attempt(),
                    run.runInfo().toJson(), ctx.clock().now(c))) {
                throw new LeaseLostException(claimed.runId(), claimed.token());
            }
            if (!ctx.leases().release(c, claimed.jobId(), claimed.runId(), claimed.token())) {
                throw new LeaseLostException(claimed.runId(), claimed.token());
            }
            return null;
        }));
        LOG.log(System.Logger.Level.INFO, "run {0} of job {1} waits for decision request {2}", claimed.runId(),
                run.job().name(), decision);
    }
}
