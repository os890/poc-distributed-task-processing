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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.os890.dtp.api.BranchResult;
import org.os890.dtp.api.Signal;
import org.os890.dtp.core.domain.BranchState;
import org.os890.dtp.core.domain.BuiltInOption;
import org.os890.dtp.core.domain.DecisionKind;
import org.os890.dtp.core.domain.FailureKind;
import org.os890.dtp.core.domain.ItemEventType;
import org.os890.dtp.core.domain.ItemState;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.RouteModel;
import org.os890.dtp.core.domain.SubjectType;
import org.os890.dtp.core.jdbc.ItemStore;
import org.os890.dtp.core.jdbc.ItemStore.BranchRow;
import org.os890.dtp.core.jdbc.ItemStore.InboxEntry;
import org.os890.dtp.core.jdbc.ItemStore.ItemEvent;
import org.os890.dtp.core.jdbc.ItemStore.ItemRow;

/**
 * Item routing and failure handling. Every change is recorded in the item's journey and runs in the current
 * transaction, i.e. within the fenced unit of work of the run (or the caller's transaction outside of runs).
 */
public class ItemRouting {

    private static final int STACK_FRAMES_FOR_HASH = 5;

    private final EngineContext ctx;
    private final Decisions decisions;

    private FlowRouting flows;

    public ItemRouting(EngineContext ctx) {
        this.ctx = ctx;
        this.decisions = new Decisions(ctx);
    }

    /** The decision requests. */
    public Decisions decisions() {
        return decisions;
    }

    /**
     * Attaches the flow routing that resolves signals.
     *
     * @param flowRouting the flow routing
     */
    public void attach(FlowRouting flowRouting) {
        this.flows = flowRouting;
    }

    /**
     * Routes an item according to the flow definition.
     *
     * @param item   the item
     * @param signal the signal
     * @param reason the reason, or {@code null}
     * @param run    the current run, or {@code null}
     */
    public void signal(ItemRow item, Signal signal, String reason, RunContext run) {
        if (flows == null) {
            throw new IllegalStateException("flows aren't enabled on this node");
        }
        flows.signal(item, Objects.requireNonNull(signal, "signal"), reason, run);
    }

    /**
     * Moves an item into a job's inbox, honoring the job's loop limit.
     *
     * @param item   the item
     * @param target the target job
     * @param reason the reason, or {@code null}
     * @param back   {@code true} for a back-routing
     * @param run    the current run, or {@code null}
     */
    public void routeTo(ItemRow item, JobDefinition target, String reason, boolean back, RunContext run) {
        inTx(run, (c, now) -> moveTo(c, now, item, item.currentJobId(), target, reason,
                back ? ItemEventType.ROUTED_BACK : ItemEventType.ROUTED, run));
    }

    /**
     * Forks an item (DD-22): one branch per branch step, each in the inbox of its step. The last branch that
     * finishes moves the item into the inbox of the join step.
     *
     * @param item     the item
     * @param branches the branch steps
     * @param join     the join step
     * @param reason   the reason, or {@code null}
     * @param run      the current run, or {@code null}
     */
    public void fork(ItemRow item, List<JobDefinition> branches, JobDefinition join, String reason, RunContext run) {
        String why = "fork into " + branches.stream().map(JobDefinition::name).collect(Collectors.joining(", "))
                + ", join at " + join.name() + (reason == null ? "" : " (" + reason + ")");
        inTx(run, (c, now) -> {
            ctx.items().fork(c, item, branches.stream().map(JobDefinition::id).toList(), join.id(), reason, now);
            ctx.items().append(c, event(item, ItemEventType.FORKED, item.currentJobId(), join.id(), why, run), now);
        });
    }

    /**
     * Finishes a branch with the signal the branch step reported. The item row lock serializes the branches of an
     * item, so exactly one of them, the last one, moves the item into the inbox of the join step.
     *
     * @param item   the item
     * @param branch the branch
     * @param signal the result
     * @param reason the reason, or {@code null}
     * @param run    the current run, or {@code null}
     */
    public void finishBranch(ItemRow item, BranchRow branch, Signal signal, String reason, RunContext run) {
        Objects.requireNonNull(signal, "signal");
        inTx(run, (c, now) -> {
            ItemStore items = ctx.items();
            ItemRow locked = items.lock(c, item.id());
            if (locked.state() != ItemState.FORKED) {
                throw new IllegalStateException("item " + item.businessKey() + " isn't forked any more ("
                        + locked.state() + ")");
            }
            int open = items.finishBranch(c, branch, RouteModel.typeOf(signal), signal.name(), reason, ctx.nodeId(),
                    now);
            if (open < 0) {
                throw new IllegalStateException("branch " + jobName(branch.branchJobId()) + " of item "
                        + item.businessKey() + " isn't queued any more");
            }
            items.append(c, event(item, ItemEventType.BRANCH_FINISHED, branch.branchJobId(), branch.joinJobId(),
                    signal.name() + (reason == null ? "" : ": " + reason), run), now);
            if (open == 0) {
                moveTo(c, now, locked, branch.branchJobId(), ctx.catalog().byId(branch.joinJobId()),
                        "all branches finished", ItemEventType.JOINED, run);
            }
        });
    }

    private void moveTo(Connection c, Instant now, ItemRow item, UUID from, JobDefinition target, String reason,
                        ItemEventType type, RunContext run) throws SQLException {
        ItemStore items = ctx.items();
        int visits = items.incrementVisits(c, item.id(), target.id());
        items.route(c, item.id(), target.id(), reason, now);
        items.append(c, event(item, type, from, target.id(), reason, run), now);
        if (target.maxItemVisits() != null && visits > target.maxItemVisits()) {
            String why = "visit " + visits + " of " + target.name() + " exceeds max_item_visits "
                    + target.maxItemVisits();
            items.setState(c, item.id(), ItemState.WAITING_FOR_DECISION, 0, now);
            UUID request = decisions.request(c, new Decisions.NewRequest(DecisionKind.CHOICE, SubjectType.ITEM,
                    item.id(), target.id(), runId(run), why, Decisions.builtIn(BuiltInOption.ITEM_LOOP), null, null,
                    null), now);
            items.append(c, event(item, ItemEventType.LOOP_LIMIT_EXCEEDED, from, target.id(), why, run)
                    .withDecision(request), now);
        }
    }

    /**
     * Completes the journey of an item.
     *
     * @param item the item
     * @param run  the current run, or {@code null}
     */
    public void complete(ItemRow item, RunContext run) {
        inTx(run, (c, now) -> {
            ctx.items().setState(c, item.id(), ItemState.COMPLETED, item.attempts(), now);
            ctx.items().append(c, event(item, ItemEventType.COMPLETED, item.currentJobId(), null, null, run), now);
        });
    }

    /**
     * Ends the journey of an item without completing it.
     *
     * @param item   the item
     * @param reason the reason
     * @param run    the current run, or {@code null}
     */
    public void discard(ItemRow item, String reason, RunContext run) {
        inTx(run, (c, now) -> {
            ctx.items().setState(c, item.id(), ItemState.DISCARDED, item.attempts(), now);
            ctx.items().append(c, event(item, ItemEventType.DISCARDED, item.currentJobId(), null, reason, run), now);
        });
    }

    /**
     * Keeps an item in its inbox until the delay has passed.
     *
     * @param item   the item
     * @param branch the branch the step handles, or {@code null}
     * @param delay  the delay
     * @param reason the reason
     * @param run    the current run, or {@code null}
     */
    public void defer(ItemRow item, BranchRow branch, Duration delay, String reason, RunContext run) {
        inTx(run, (c, now) -> {
            if (branch != null) {
                ctx.items().postponeBranch(c, branch, branch.attempts(), now.plus(delay), now);
            } else {
                ctx.items().postpone(c, item.id(), item.attempts(), now.plus(delay), now);
            }
            ctx.items().append(c, event(item, ItemEventType.DEFERRED, from(item, branch), null, reason, run), now);
        });
    }

    /**
     * Records a failed attempt and schedules a retry, or lets the item wait for a decision once the retries of the
     * job are exhausted.
     *
     * @param item   the item
     * @param job    the job whose retry policy applies
     * @param kind   business or technical failure
     * @param reason the reason (the message of a technical failure)
     * @param error  the exception of a technical failure, or {@code null}
     * @param run    the current run, or {@code null}
     */
    public void fail(ItemRow item, JobDefinition job, FailureKind kind, String reason, Throwable error,
                     RunContext run) {
        fail(item, null, job, kind, reason, error, run);
    }

    /**
     * Records a failed attempt of an item or of one of its branches.
     *
     * @param item   the item
     * @param branch the branch the step handles, or {@code null}
     * @param job    the job whose retry policy applies
     * @param kind   business or technical failure
     * @param reason the reason (the message of a technical failure)
     * @param error  the exception of a technical failure, or {@code null}
     * @param run    the current run, or {@code null}
     */
    public void fail(ItemRow item, BranchRow branch, JobDefinition job, FailureKind kind, String reason,
                     Throwable error, RunContext run) {
        inTx(run, (c, now) -> {
            ItemStore items = ctx.items();
            UUID from = from(item, branch);
            int attempts = (branch != null ? branch.attempts() : item.attempts()) + 1;
            items.append(c, new ItemEvent(item.id(), item.namespace(), ItemEventType.FAILED, from, null, runId(run),
                    runAttempt(run), ctx.nodeId(), reason, kind, error == null ? null : error.getClass().getName(),
                    error == null ? null : hash(error), null, null), now);
            if (attempts <= job.retryMax()) {
                Instant next = now.plus(job.retryBackoff().multipliedBy(attempts));
                if (branch != null) {
                    items.postponeBranch(c, branch, attempts, next, now);
                } else {
                    items.postpone(c, item.id(), attempts, next, now);
                }
                items.append(c, event(item, ItemEventType.RETRY_SCHEDULED, from, null, "retry " + attempts + " of "
                        + job.retryMax() + " at " + next, run), now);
            } else {
                String why = "retries exhausted (" + job.retryMax() + ")" + (reason == null ? "" : ": " + reason);
                waitForDecision(c, item, branch, attempts, now);
                // technical failures go to operations, business failures to the business (DD-29)
                UUID request = decisions.request(c, new Decisions.NewRequest(DecisionKind.CHOICE, SubjectType.ITEM,
                        item.id(), job.id(), runId(run), why, Decisions.builtIn(BuiltInOption.ITEM), null, null,
                        kind == FailureKind.TECHNICAL ? Decisions.OPERATORS : null), now);
                items.append(c, event(item, ItemEventType.ESCALATED, from, null, why, run).withDecision(request),
                        now);
            }
        });
    }

    /**
     * Hands an item over to a person: it waits for a decision, the result comes back to the requesting job.
     *
     * @param entry      the item, or the branch the step handles
     * @param kind       CHOICE or APPROVAL
     * @param options    the options
     * @param optionType the enum of custom options, or {@code null}
     * @param quorum     the quorum of an approval, or {@code null}
     * @param team       the responsible team, or {@code null}
     * @param reason     the reason
     * @param run        the current run, or {@code null}
     */
    public void requestDecision(InboxEntry entry, DecisionKind kind, List<Decisions.OptionView> options,
                                Class<?> optionType, Integer quorum, String team, String reason, RunContext run) {
        ItemRow item = entry.item();
        BranchRow branch = entry.branch();
        UUID from = from(item, branch);
        UUID requestingJob = from != null ? from : run != null ? run.job().id() : null;
        if (requestingJob == null) {
            throw new IllegalStateException("item " + item.businessKey() + " is in no step, route it first");
        }
        inTx(run, (c, now) -> {
            waitForDecision(c, item, branch, branch != null ? branch.attempts() : item.attempts(), now);
            UUID request = decisions.request(c, new Decisions.NewRequest(kind, SubjectType.ITEM, item.id(),
                    requestingJob, runId(run), reason, options, optionType, quorum, team), now);
            ctx.items().append(c, event(item, ItemEventType.ESCALATED, from, null, reason, run)
                    .withDecision(request), now);
        });
    }

    private void waitForDecision(Connection c, ItemRow item, BranchRow branch, int attempts, Instant now)
            throws SQLException {
        if (branch != null) {
            ctx.items().branchWaitsForDecision(c, branch, attempts, now);
        } else {
            ctx.items().setState(c, item.id(), ItemState.WAITING_FOR_DECISION, attempts, now);
        }
    }

    /**
     * Records that an item is processed again because its batch was rolled back (another item failed).
     *
     * @param item   the item
     * @param branch the branch the step handles, or {@code null}
     * @param run    the current run
     */
    public void reprocessed(ItemRow item, BranchRow branch, RunContext run) {
        inTx(run, (c, now) -> ctx.items().append(c, event(item, ItemEventType.REPROCESSED, from(item, branch), null,
                "batch rollback", run), now));
    }

    /**
     * Records that an item was tracked.
     *
     * @param item the new item
     * @param run  the current run, or {@code null}
     */
    public void tracked(ItemRow item, RunContext run) {
        inTx(run, (c, now) -> ctx.items().append(c, event(item, ItemEventType.TRACKED, null, null, null, run), now));
    }

    /**
     * The results of the finished branches of the last fork of an item.
     *
     * @param item the item
     * @return the results, in the order of the fork definition
     */
    public List<BranchResult> branchResults(ItemRow item) {
        return ctx.tx().withConnection(c -> {
            List<BranchResult> results = new ArrayList<>();
            for (BranchRow branch : ctx.items().branches(c, item.id())) {
                if (branch.state() == BranchState.FINISHED) {
                    results.add(new BranchResult(ctx.catalog().byId(branch.branchJobId()).handlerClass(),
                            branch.signalType(), branch.signal(), branch.reason()));
                }
            }
            return results;
        });
    }

    /**
     * Visits of an item in a job's inbox.
     *
     * @param item  the item
     * @param jobId the job
     * @return the visits
     */
    public int visits(ItemRow item, UUID jobId) {
        return ctx.tx().withConnection(c -> ctx.items().visits(c, item.id(), jobId));
    }

    /**
     * Hash of the exception class and the top stack frames, to group technical failures (DD-29).
     *
     * @param error the exception
     * @return a short hex hash
     */
    public static String hash(Throwable error) {
        String frames = Arrays.stream(error.getStackTrace())
                              .limit(STACK_FRAMES_FOR_HASH)
                              .map(StackTraceElement::toString)
                              .collect(Collectors.joining("|", error.getClass().getName() + "|", ""));
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(frames.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @FunctionalInterface
    private interface ItemWork {
        void run(Connection c, Instant now) throws SQLException;
    }

    private void inTx(RunContext run, ItemWork work) {
        ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            work.run(c, run != null ? run.now() : ctx.clock().now(c));
            return null;
        }));
    }

    private ItemEvent event(ItemRow item, ItemEventType type, UUID fromJobId, UUID toJobId, String reason,
                            RunContext run) {
        return new ItemEvent(item.id(), item.namespace(), type, fromJobId, toJobId, runId(run), runAttempt(run),
                ctx.nodeId(), reason, null, null, null, null, null);
    }

    private static UUID from(ItemRow item, BranchRow branch) {
        return branch != null ? branch.branchJobId() : item.currentJobId();
    }

    private String jobName(UUID jobId) {
        return ctx.catalog().byId(jobId).name();
    }

    private static UUID runId(RunContext run) {
        return run == null ? null : run.claimed().runId();
    }

    private static Integer runAttempt(RunContext run) {
        return run == null ? null : run.claimed().attempt();
    }
}
