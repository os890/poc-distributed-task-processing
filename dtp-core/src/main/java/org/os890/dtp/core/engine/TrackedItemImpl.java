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

import org.os890.dtp.api.Approval;
import org.os890.dtp.api.ApprovalResult;
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
import org.os890.dtp.core.jdbc.ItemStore.ItemRow;

/**
 * A tracked item handed over to application code. Exactly one routing operation is allowed per hand-over.
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

    private boolean handled;

    public TrackedItemImpl(EngineContext ctx, ItemRouting routing, ItemRow row, ItemType<E, ID> type,
                           RunContext run) {
        this.ctx = ctx;
        this.routing = routing;
        this.row = row;
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
        return Optional.ofNullable(row.arrivalReason());
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
        routing.signal(row, signal, reason, run);
    }

    @Override
    public void routeTo(Class<? extends ProcessStep> job) {
        routeTo(job, null);
    }

    @Override
    public void routeTo(Class<? extends ProcessStep> job, String reason) {
        markHandled();
        routing.routeTo(row, ctx.catalog().byStep(job), reason, false, run);
    }

    @Override
    public void routeBack(Class<? extends ProcessStep> job, String reason) {
        markHandled();
        routing.routeTo(row, ctx.catalog().byStep(job), Objects.requireNonNull(reason, "reason"), true, run);
    }

    @Override
    public void defer(Duration delay, String reason) {
        markHandled();
        routing.defer(row, Objects.requireNonNull(delay, "delay"), reason, run);
    }

    @Override
    public void fail(String reason) {
        markHandled();
        routing.fail(row, currentJob(), FailureKind.BUSINESS, reason, null, run);
    }

    @Override
    public void escalate(String reason) {
        markHandled();
        routing.requestDecision(row, DecisionKind.CHOICE, Decisions.builtIn(BuiltInOption.ITEM), null, null, null,
                Objects.requireNonNull(reason, "reason"), run);
    }

    @Override
    public <O extends Enum<O> & DecisionOption> void requestDecision(String reason, Class<O> options) {
        requestDecision(reason, options, null);
    }

    @Override
    public <O extends Enum<O> & DecisionOption> void requestDecision(String reason, Class<O> options,
                                                                     Responsibility team) {
        markHandled();
        routing.requestDecision(row, DecisionKind.CHOICE, Decisions.optionsOf(options), options, null,
                team == null ? null : team.group(), Objects.requireNonNull(reason, "reason"), run);
    }

    @Override
    public void requestApproval(Approval approval, String reason) {
        markHandled();
        routing.requestDecision(row, DecisionKind.APPROVAL, Decisions.builtIn(List.of(BuiltInOption.APPROVED,
                BuiltInOption.REJECTED)), null, approval.requiredApprovals(),
                approval.team() == null ? null : approval.team().group(), Objects.requireNonNull(reason, "reason"),
                run);
    }

    @Override
    public <O extends Enum<O> & DecisionOption> Optional<O> decision(Class<O> options) {
        return routing.decisions().decisionOf(row.decisionId(), options);
    }

    @Override
    public Optional<ApprovalResult> approval() {
        return routing.decisions().approvalOf(row.decisionId());
    }

    @Override
    public void complete() {
        markHandled();
        routing.complete(row, run);
    }

    @Override
    public void discard(String reason) {
        markHandled();
        routing.discard(row, reason, run);
    }

    @Override
    public String toString() {
        return "TrackedItem[" + type.name() + " " + row.refId() + ", " + row.businessKey() + "]";
    }

    private JobDefinition currentJob() {
        if (row.currentJobId() == null) {
            throw new IllegalStateException(this + " is in no inbox yet, route it first");
        }
        return ctx.catalog().byId(row.currentJobId());
    }

    /** Marks the item as routed, only one routing operation is allowed. */
    void markHandled() {
        if (handled) {
            throw new IllegalStateException(this + " was routed already");
        }
        handled = true;
    }
}
