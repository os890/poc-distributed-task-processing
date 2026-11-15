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
package org.os890.dtp.core.spi;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Port for telemetry metrics and traces (docs/design/09-monitoring-heatmap.md §9.8, DD-33). The engine reports what
 * happened on this node; item results and run ends only after the commit of their transaction, never for a rolled
 * back one. Attributes are bounded: steps, lanes, outcomes, kinds, groups and exception classes, never item ids,
 * business keys or messages. Implementations must be fast and must not throw. All methods do nothing by default.
 */
public interface Telemetry {

    /** Reports nothing, the default. */
    Telemetry NONE = new Telemetry() {
    };

    /**
     * {@code true} if this telemetry records traces: the engine then reads the trace contexts of the items it
     * processes, to link the spans of the units of work to them.
     *
     * @return whether traces are recorded
     */
    default boolean tracing() {
        return false;
    }

    /**
     * An item left a step (committed).
     *
     * @param step        the job name
     * @param lane        the lane, or {@code null}
     * @param result      {@code routed}, {@code routed_back}, {@code forked}, {@code packaged}, {@code sent},
     *                    {@code completed},
     *                    {@code discarded}, {@code deferred}, {@code escalated} or {@code failed}
     * @param failureKind {@code BUSINESS} or {@code TECHNICAL} for {@code failed}, else {@code null}
     */
    default void itemProcessed(String step, String lane, String result, String failureKind) {
    }

    /**
     * An item waited in the inbox of a step until its processing started.
     *
     * @param step the job name
     * @param lane the lane, or {@code null}
     * @param wait the time since it was due
     */
    default void itemWaited(String step, String lane, Duration wait) {
    }

    /**
     * A technical failure of an item (committed).
     *
     * @param step      the job name
     * @param errorType the exception class
     */
    default void technicalFailure(String step, String errorType) {
    }

    /**
     * A unit of work committed.
     *
     * @param step     the job name
     * @param duration its duration, incl. the commit
     */
    default void unitOfWork(String step, Duration duration) {
    }

    /**
     * A unit of work was rolled back.
     *
     * @param step   the job name
     * @param reason {@code exception} or {@code lease_lost}
     */
    default void rollback(String step, String reason) {
    }

    /**
     * A run attempt of this node ended (committed).
     *
     * @param step     the job name
     * @param outcome  the run outcome
     * @param duration the time since the claim
     */
    default void runFinished(String step, String outcome, Duration duration) {
    }

    /**
     * This node claimed a run.
     *
     * @param lag the time since the run was due
     */
    default void claimed(Duration lag) {
    }

    /** This node lost a claim race (the lease was taken, or the DB lock timed out). */
    default void claimConflict() {
    }

    /**
     * This node renewed the lease of a run.
     *
     * @param step the job name
     */
    default void leaseRenewed(String step) {
    }

    /**
     * A run of this node lost its lease.
     *
     * @param step the job name
     */
    default void leaseLost(String step) {
    }

    /**
     * The leader took over the expired lease of a run.
     *
     * @param step the job name
     */
    default void leaseTakenOver(String step) {
    }

    /**
     * This node was elected leader of a role.
     *
     * @param role {@code housekeeping} or {@code projection}
     */
    default void leaderElected(String role) {
    }

    /**
     * The receiver acknowledged a sent work package (committed).
     *
     * @param step      the job that sent it
     * @param sinceSent the time since it was sent
     */
    default void packageAcknowledged(String step, Duration sinceSent) {
    }

    /**
     * A sent work package was rejected, or not acknowledged in time (committed).
     *
     * @param step   the job that sent it
     * @param reason {@code rejected} or {@code timeout}
     */
    default void packageRejected(String step, String reason) {
    }

    /**
     * A decision request was created (committed).
     *
     * @param step  the requesting job
     * @param kind  {@code CHOICE} or {@code APPROVAL}
     * @param group the responsible group, or {@code null}
     */
    default void decisionCreated(String step, String kind, String group) {
    }

    /**
     * A decision request was decided (committed).
     *
     * @param step  the requesting job
     * @param kind  {@code CHOICE} or {@code APPROVAL}
     * @param group the responsible group, or {@code null}
     */
    default void decisionDecided(String step, String kind, String group) {
    }

    /**
     * The cluster-wide state of the monitoring read model, reported by the leader of the projection after each
     * cycle. {@code null} when this node doesn't lead the projection (any more): it must stop reporting the cluster
     * gauges at once, another node reports them.
     *
     * @param state the state, or {@code null}
     */
    default void clusterState(ClusterState state) {
    }

    /**
     * Starts the span of a unit of work and makes it current on this thread until it is closed.
     *
     * @param step    the job name
     * @param runId   the run
     * @param attempt the run attempt
     * @param links   W3C {@code traceparent}s of the items processed in the unit, to link the span to
     * @return the span
     */
    default Span startUnitOfWork(String step, UUID runId, int attempt, List<String> links) {
        return Span.NONE;
    }

    /**
     * The W3C {@code traceparent} of the span that is current on this thread, stored with new items.
     *
     * @return the traceparent, or {@code null}
     */
    default String currentTraceParent() {
        return null;
    }

    /**
     * The trace id of the span that is current on this thread, stored with journal entries.
     *
     * @return the trace id, or {@code null}
     */
    default String currentTraceId() {
        return null;
    }

    /** The span of a unit of work. */
    interface Span extends AutoCloseable {

        /** No span. */
        Span NONE = new Span() {
        };

        /**
         * The number of items of the unit.
         *
         * @param count the items
         */
        default void items(int count) {
        }

        /**
         * Marks the span as failed.
         *
         * @param error the error
         */
        default void failed(Throwable error) {
        }

        /** Ends the span. */
        @Override
        default void close() {
        }
    }
}
