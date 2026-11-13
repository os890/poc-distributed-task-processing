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
package org.os890.dtp.api;

import java.time.Duration;
import java.util.Optional;

/**
 * An item in a job's inbox. Every operation is recorded in the item's journey.
 *
 * <p>Exactly one of the routing operations ({@code routeTo}, {@code routeBack}, {@code defer}, {@code fail},
 * {@code complete}, {@code discard}) has to be called per hand-over.
 *
 * <p>Slice 1 of the PoC: fork/join, approvals and decision requests are not implemented yet. An item that exceeds
 * its retries or its loop limit waits in state {@code WAITING_FOR_DECISION}.
 *
 * @param <E>  the tracked entity type
 * @param <ID> its id type
 */
public interface TrackedItem<E, ID> {

    /** Type-safe reference to the entity. */
    ItemRef<E, ID> ref();

    /** Business key given when the item was tracked. */
    String businessKey();

    /** Lane of the item (name of the {@link ItemLane} given when it was tracked). */
    String lane();

    /** Reason given by the job that routed the item into this inbox, if any. */
    Optional<String> arrivalReason();

    /**
     * How often this item has entered the given job's inbox (loop protection).
     *
     * @param job the process step class of the job
     * @return the number of visits
     */
    int visits(Class<? extends ProcessStep> job);

    /**
     * Routes the item into the inbox of the given job.
     *
     * @param job the process step class of the target job
     */
    void routeTo(Class<? extends ProcessStep> job);

    /**
     * Routes the item into the inbox of the given job, with a reason the target can read.
     *
     * @param job    the process step class of the target job
     * @param reason the reason
     */
    void routeTo(Class<? extends ProcessStep> job, String reason);

    /**
     * Routes the item to an earlier stage. Recorded as {@code ROUTED_BACK} in the journey.
     *
     * @param job    the process step class of the target job
     * @param reason the reason
     */
    void routeBack(Class<? extends ProcessStep> job, String reason);

    /**
     * Keeps the item in this inbox until the delay has passed. Not a failure, no retry is counted.
     *
     * @param delay  how long to wait
     * @param reason the reason
     */
    void defer(Duration delay, String reason);

    /**
     * Business failure: counts as a failed attempt and is retried according to the job's retry policy.
     *
     * @param reason the reason
     */
    void fail(String reason);

    /** Completes the item's journey. */
    void complete();

    /**
     * Ends the item's journey without completing it.
     *
     * @param reason the reason
     */
    void discard(String reason);
}
