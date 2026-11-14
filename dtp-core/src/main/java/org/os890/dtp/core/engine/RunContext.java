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
import java.util.concurrent.atomic.AtomicLong;

import org.os890.dtp.api.StopReason;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.LeaseLostException;
import org.os890.dtp.core.spi.Transactions;

/**
 * State of a run on the executing node: the lease it holds, soft stop requests and fenced units of work.
 */
public class RunContext {

    private final EngineContext engine;
    private final ClaimedRun claimed;
    private final JobDefinition job;
    private final StepInfo step;
    private final RunInfoImpl runInfo;
    private static final ThreadLocal<RunContext> CURRENT = new ThreadLocal<>();

    private final ThreadLocal<Instant> unitNow = new ThreadLocal<>();

    private final AtomicLong processedItems = new AtomicLong();

    private volatile StopReason stopReason;
    private volatile Runnable onStop;

    public RunContext(EngineContext engine, ClaimedRun claimed, JobDefinition job, StepInfo step) {
        this.engine = engine;
        this.claimed = claimed;
        this.job = job;
        this.step = step;
        this.runInfo = new RunInfoImpl(engine.json());
    }

    public EngineContext engine() {
        return engine;
    }

    public ClaimedRun claimed() {
        return claimed;
    }

    public JobDefinition job() {
        return job;
    }

    public StepInfo step() {
        return step;
    }

    public RunInfoImpl runInfo() {
        return runInfo;
    }

    /** {@code false} once a soft stop was requested. */
    public boolean shouldContinue() {
        return stopReason == null;
    }

    /** The requested stop, or {@code null}. */
    public StopReason stopReason() {
        return stopReason;
    }

    /**
     * Requests a soft stop. Running work is never interrupted, the run stops at the next unit of work.
     *
     * @param reason the reason
     */
    public void requestStop(StopReason reason) {
        if (stopReason == null) {
            stopReason = reason;
            Runnable callback = onStop;
            if (callback != null) {
                callback.run();
            }
        }
    }

    /**
     * Counts processed items; once the item limit of the job is reached, the run is asked to stop
     * ({@code LIMIT_REACHED}).
     *
     * @param count the number of items just processed
     */
    public void countItems(long count) {
        long total = processedItems.addAndGet(count);
        Long limit = job.itemLimit();
        if (limit != null && total >= limit) {
            requestStop(StopReason.LIMIT_REACHED);
        }
    }

    /**
     * Callback for stop requests, e.g. {@code JobHandler#onStop}.
     *
     * @param callback the callback
     */
    public void onStop(Runnable callback) {
        this.onStop = callback;
    }

    /**
     * Runs a fenced unit of work (DD-12): the fence check is the last statement before the commit.
     *
     * @param requiresNew {@code true} to suspend an active transaction (e.g. an inbox batch within a handler)
     * @param work        the work
     * @param <T>         result type
     * @return the result
     * @throws LeaseLostException if the run lost its lease, the unit of work is rolled back
     */
    public <T> T unitOfWork(boolean requiresNew, Transactions.Work<T> work) {
        Transactions.Work<T> fenced = () -> {
            Instant outer = unitNow.get();
            RunContext outerRun = CURRENT.get();
            unitNow.set(engine.now());
            CURRENT.set(this);
            try {
                T result = work.run();
                fence();
                return result;
            } finally {
                if (outerRun == null) {
                    CURRENT.remove();
                } else {
                    CURRENT.set(outerRun);
                }
                if (outer == null) {
                    unitNow.remove();
                } else {
                    unitNow.set(outer);
                }
            }
        };
        return requiresNew ? engine.tx().requiresNew(fenced) : engine.tx().required(fenced);
    }

    /**
     * The run whose unit of work is executed by the current thread, e.g. for beans that call {@code Items} or fire
     * flow events within a handler.
     *
     * @return the run, or {@code null} outside of runs
     */
    public static RunContext current() {
        return CURRENT.get();
    }

    /** The DB time of the current unit of work (read once per unit), or the current DB time. */
    public Instant now() {
        Instant now = unitNow.get();
        return now != null ? now : engine.now();
    }

    private void fence() {
        boolean holds = engine.tx().withConnection(c -> engine.leases().fence(c, claimed.jobId(), claimed.runId(),
                claimed.token()));
        if (!holds) {
            requestStop(StopReason.LEASE_LOST);
            throw new LeaseLostException(claimed.runId(), claimed.token());
        }
    }
}
