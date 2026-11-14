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
 * <p>Exactly one of the routing operations ({@code signal}, {@code routeTo}, {@code routeBack}, {@code defer},
 * {@code fail}, {@code complete}, {@code discard}) has to be called per hand-over. {@code signal} lets the flow
 * definition decide, the others route manually.
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
     * Reports a signal: the flow definition in the DB decides where the item goes next (see {@link FlowDefinition}).
     * The flow version is pinned per item, routes can be overruled by operators.
     *
     * @param signal the signal
     */
    void signal(Signal signal);

    /**
     * Reports a signal with a reason the next step can read.
     *
     * @param signal the signal
     * @param reason the reason
     */
    void signal(Signal signal, String reason);

    /**
     * Manual routing: routes the item into the inbox of the given job, without a flow definition.
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

    /**
     * Hands the item over to a person (DD-5): it waits in state {@code WAITING_FOR_DECISION} until a user decides
     * {@code RETRY} (back into this step's inbox), {@code RESET_VISITS} or {@code DISCARD}.
     *
     * @param reason the reason shown to the user
     */
    void escalate(String reason);

    /**
     * Asks a user to choose one of the options (plus {@code DISCARD}). The item comes back into this step's inbox,
     * which reads the choice with {@link #decision(Class)} (DD-19).
     *
     * @param reason  the reason shown to the user
     * @param options the options, an enum
     * @param <O>     the option type
     */
    <O extends Enum<O> & DecisionOption> void requestDecision(String reason, Class<O> options);

    /**
     * Same as {@link #requestDecision(String, Class)}, for the members of a team only (DD-24).
     *
     * @param reason  the reason shown to the user
     * @param options the options, an enum
     * @param team    the responsible team
     * @param <O>     the option type
     */
    <O extends Enum<O> & DecisionOption> void requestDecision(String reason, Class<O> options, Responsibility team);

    /**
     * Asks for an approval (DD-16). The item comes back into this step's inbox once the quorum is reached or a user
     * rejected, the step reads the result with {@link #approval()}.
     *
     * @param approval the quorum and the team
     * @param reason   the reason shown to the users
     */
    void requestApproval(Approval approval, String reason);

    /**
     * The decision that brought the item back into this inbox.
     *
     * @param options the option type of the request
     * @param <O>     the option type
     * @return the chosen option, empty if the item didn't come back from a decision of this type
     */
    <O extends Enum<O> & DecisionOption> Optional<O> decision(Class<O> options);

    /**
     * The approval that brought the item back into this inbox.
     *
     * @return the result, empty if the item didn't come back from an approval
     */
    Optional<ApprovalResult> approval();

    /** Completes the item's journey. */
    void complete();

    /**
     * Ends the item's journey without completing it.
     *
     * @param reason the reason
     */
    void discard(String reason);
}
