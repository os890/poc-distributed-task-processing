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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.api.Approval;
import org.os890.dtp.api.ApprovalResult;
import org.os890.dtp.api.BranchResult;
import org.os890.dtp.api.DecisionOption;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.ItemType;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.api.Responsibility;
import org.os890.dtp.api.Signal;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.domain.BuiltInOption;
import org.os890.dtp.core.domain.DecisionKind;
import org.os890.dtp.core.domain.FailureKind;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.jdbc.ItemStore.BranchRow;
import org.os890.dtp.core.jdbc.ItemStore.InboxEntry;
import org.os890.dtp.core.jdbc.ItemStore.ItemRow;

/**
 * A tracked item handed over to application code, or one of its parallel branches. Exactly one routing operation is
 * allowed per hand-over.
 *
 * @param <E>  the entity type
 * @param <ID> the id type
 */
public class TrackedItemImpl<E, ID> implements TrackedItem<E, ID> {

    private final EngineContext ctx;
    private final ItemRouting routing;
    private final ItemRow row;
    private final ItemType<E, ID> type;
    private final RunContext run;
    private final BranchRow branch;

    private boolean handled;

    public TrackedItemImpl(EngineContext ctx, ItemRouting routing, ItemRow row, ItemType<E, ID> type,
                           RunContext run) {
        this(ctx, routing, new InboxEntry(row, null), type, run);
    }

    /**
     * Hands over an inbox entry: an item, or a branch of a forked item.
     *
     * @param ctx     the engine
     * @param routing the item routing
     * @param entry   the entry
     * @param type    the item type
     * @param run     the run, or {@code null}
     */
    public TrackedItemImpl(EngineContext ctx, ItemRouting routing, InboxEntry entry, ItemType<E, ID> type,
                           RunContext run) {
        this.ctx = ctx;
        this.routing = routing;
        this.row = entry.item();
        this.branch = entry.branch();
        this.type = type;
        this.run = run;
    }

    /** The underlying row. */
    public ItemRow row() {
        return row;
    }

    /** {@code true} once a routing operation was called. */
    public boolean isHandled() {
        return handled;
    }

    @Override
    public ItemRef<E, ID> ref() {
        ID id = IdCodec.decode(row.refId(), type.idType());
        return row.refVersion() == 0 ? ItemRef.of(type, id) : ItemRef.of(type, id, row.refVersion());
    }

    @Override
    public String businessKey() {
        return row.businessKey();
    }

    @Override
    public String lane() {
        return row.lane();
    }

    @Override
    public Optional<String> arrivalReason() {
        return Optional.ofNullable(branch != null ? branch.arrivalReason() : row.arrivalReason());
    }

    @Override
    public int visits(Class<? extends ProcessStep> job) {
        return routing.visits(row, ctx.catalog().byStep(job).id());
    }

    @Override
    public void signal(Signal signal) {
        signal(signal, null);
    }

    @Override
    public void signal(Signal signal, String reason) {
        markHandled();
        if (branch != null) {
            routing.finishBranch(row, branch, signal, reason, run);
        } else {
            routing.signal(row, signal, reason, run);
        }
    }

    @Override
    public void routeTo(Class<? extends ProcessStep> job) {
        routeTo(job, null);
    }

    @Override
    public void routeTo(Class<? extends ProcessStep> job, String reason) {
        notInBranch("routeTo");
        markHandled();
        routing.routeTo(row, ctx.catalog().byStep(job), reason, false, run);
    }

    @Override
    public void routeBack(Class<? extends ProcessStep> job, String reason) {
        notInBranch("routeBack");
        markHandled();
        routing.routeTo(row, ctx.catalog().byStep(job), Objects.requireNonNull(reason, "reason"), true, run);
    }

    @Override
    public void defer(Duration delay, String reason) {
        markHandled();
        routing.defer(row, branch, Objects.requireNonNull(delay, "delay"), reason, run);
    }

    @Override
    public void fail(String reason) {
        markHandled();
        routing.fail(row, branch, currentJob(), FailureKind.BUSINESS, reason, null, run);
    }

    @Override
    public void escalate(String reason) {
        markHandled();
        routing.requestDecision(entry(), DecisionKind.CHOICE, Decisions.builtIn(BuiltInOption.ITEM), null, null,
                null, Objects.requireNonNull(reason, "reason"), run);
    }

    @Override
    public <O extends Enum<O> & DecisionOption> void requestDecision(String reason, Class<O> options) {
        requestDecision(reason, options, null);
    }

    @Override
    public <O extends Enum<O> & DecisionOption> void requestDecision(String reason, Class<O> options,
                                                                     Responsibility team) {
        markHandled();
        routing.requestDecision(entry(), DecisionKind.CHOICE, Decisions.optionsOf(options), options, null,
                team == null ? null : team.group(), Objects.requireNonNull(reason, "reason"), run);
    }

    @Override
    public void requestApproval(Approval approval, String reason) {
        markHandled();
        routing.requestDecision(entry(), DecisionKind.APPROVAL, Decisions.builtIn(List.of(BuiltInOption.APPROVED,
                BuiltInOption.REJECTED)), null, approval.requiredApprovals(),
                approval.team() == null ? null : approval.team().group(), Objects.requireNonNull(reason, "reason"),
                run);
    }

    @Override
    public <O extends Enum<O> & DecisionOption> Optional<O> decision(Class<O> options) {
        return routing.decisions().decisionOf(decisionId(), options);
    }

    @Override
    public Optional<ApprovalResult> approval() {
        return routing.decisions().approvalOf(decisionId());
    }

    @Override
    public boolean isBranch() {
        return branch != null;
    }

    @Override
    public List<BranchResult> branchResults() {
        return routing.branchResults(row);
    }

    @Override
    public void complete() {
        notInBranch("complete");
        markHandled();
        routing.complete(row, run);
    }

    @Override
    public void discard(String reason) {
        notInBranch("discard");
        markHandled();
        routing.discard(row, reason, run);
    }

    @Override
    public String toString() {
        return "TrackedItem[" + type.name() + " " + row.refId() + ", " + row.businessKey()
                + (branch == null ? "" : ", branch " + ctx.catalog().byId(branch.branchJobId()).name()) + "]";
    }

    private JobDefinition currentJob() {
        UUID job = branch != null ? branch.branchJobId() : row.currentJobId();
        if (job == null) {
            throw new IllegalStateException(this + " is in no inbox yet, route it first");
        }
        return ctx.catalog().byId(job);
    }

    private InboxEntry entry() {
        return new InboxEntry(row, branch);
    }

    private UUID decisionId() {
        return branch != null ? branch.decisionId() : row.decisionId();
    }

    private void notInBranch(String operation) {
        if (branch != null) {
            throw new IllegalStateException(this + " is a branch of a fork: " + operation + " isn't allowed, signal"
                    + " the result of the branch (or defer, fail, escalate)");
        }
    }

    /** Marks the item as routed, only one routing operation is allowed. */
    void markHandled() {
        if (handled) {
            throw new IllegalStateException(this + " was routed already");
        }
        handled = true;
    }
}
