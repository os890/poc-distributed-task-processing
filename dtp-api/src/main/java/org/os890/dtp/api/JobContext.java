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

import java.util.Optional;

/**
 * Everything a handler needs during a run. Only valid inside {@link JobHandler#execute(JobContext)}.
 *
 * @param <P> type of the run parameters
 */
public interface JobContext<P> {

    /** Typed run parameters (deserialized from the trigger or the definition's defaults). */
    P parameters();

    /** Custom, typed information about this run (visible in the run history). */
    RunInfo runInfo();

    /** {@code false} once a soft stop was requested. Inbox iteration checks this automatically. */
    boolean shouldContinue();

    /**
     * The flow instance of this run, e.g. to read the business date of an end of day (empty outside an instance).
     *
     * @return the flow instance
     */
    Optional<FlowInstance> flowInstance();

    /**
     * The window occurrence this run collects for, e.g. to select the changes up to its cut-off (empty for runs that
     * aren't collector runs of a collection window).
     *
     * @return the window occurrence
     */
    Optional<WindowOccurrence> window();

    /**
     * Asks a user to choose one of the options (plus {@code FAIL}, unless the enum has its own), e.g. to sign off an
     * end of day. When {@code execute} returns, the run waits in state {@code WAITING_FOR_DECISION} instead of
     * finishing (its result is ignored, transitions and joins wait too). The decision plans the same run again: its
     * handler is executed once more and reads the choice with {@link #decision(Class)}. {@code FAIL} finishes the
     * run as failed. The request is part of the unit of work of {@code execute}: if it throws, there is no request.
     * Once per execution.
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
     * Asks for an approval (DD-16), e.g. a four-eyes sign-off. The run waits like with
     * {@link #requestDecision(String, Class)} until the quorum is reached or a user rejected, then it's executed again
     * and reads the result with {@link #approval()}.
     *
     * @param approval the quorum and the team
     * @param reason   the reason shown to the users
     */
    void requestApproval(Approval approval, String reason);

    /**
     * The latest decision about this run.
     *
     * @param options the option type of the request
     * @param <O>     the option type
     * @return the chosen option, empty if no request of this type was decided for this run (a rerun is a new run)
     */
    <O extends Enum<O> & DecisionOption> Optional<O> decision(Class<O> options);

    /**
     * The latest approval about this run.
     *
     * @return the result, empty if no approval was decided for this run
     */
    Optional<ApprovalResult> approval();

    /** Tracks new items within this run. */
    Items items();

    /**
     * The inbox of this job for the given item type (only the owning run reads it).
     *
     * @param type the item type
     * @param <E>  the tracked entity type
     * @param <ID> its id type
     * @return the inbox
     */
    <E, ID> Inbox<E, ID> inbox(ItemType<E, ID> type);

    /**
     * The work packages of this job for the given item type.
     *
     * @param type the item type
     * @param <E>  the tracked entity type
     * @param <ID> its id type
     * @return the packages
     */
    <E, ID> WorkPackages<E, ID> packages(ItemType<E, ID> type);
}
