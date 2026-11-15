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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.api.Approval;
import org.os890.dtp.api.ApprovalResult;
import org.os890.dtp.api.DecisionOption;
import org.os890.dtp.api.FlowInstance;
import org.os890.dtp.api.Inbox;
import org.os890.dtp.api.ItemType;
import org.os890.dtp.api.Items;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.Responsibility;
import org.os890.dtp.api.RunInfo;
import org.os890.dtp.api.WindowOccurrence;
import org.os890.dtp.api.WorkPackages;
import org.os890.dtp.core.domain.BuiltInOption;
import org.os890.dtp.core.domain.DecisionKind;
import org.os890.dtp.core.domain.SubjectType;
import org.os890.dtp.core.jdbc.WindowStore;

/**
 * The context of a {@code JobHandler} run.
 *
 * @param <P> the parameter type
 */
public class JobContextImpl<P> implements JobContext<P> {

    private final RunContext run;
    private final ItemRouting routing;
    private final P parameters;

    public JobContextImpl(RunContext run, ItemRouting routing, P parameters) {
        this.run = run;
        this.routing = routing;
        this.parameters = parameters;
    }

    @Override
    public P parameters() {
        return parameters;
    }

    @Override
    public RunInfo runInfo() {
        return run.runInfo();
    }

    @Override
    public boolean shouldContinue() {
        return run.shouldContinue();
    }

    @Override
    public Optional<FlowInstance> flowInstance() {
        return new FlowInstances(run.engine()).ofRun(run.claimed().runId());
    }

    @Override
    public Optional<WindowOccurrence> window() {
        return run.engine().tx().withConnection(c -> new WindowStore().ofRun(c, run.claimed().runId()))
                  .map(o -> new WindowOccurrence(o.id(), o.window(), o.seq(), o.from(), o.cutOff()));
    }

    @Override
    public Items items() {
        return new ItemsImpl(run.engine(), routing, run);
    }

    @Override
    public <E, ID> Inbox<E, ID> inbox(ItemType<E, ID> type) {
        return new InboxImpl<>(run, routing, type, false);
    }

    @Override
    public <E, ID> WorkPackages<E, ID> packages(ItemType<E, ID> type) {
        return new WorkPackagesImpl<>(run, routing, type);
    }

    @Override
    public <O extends Enum<O> & DecisionOption> void requestDecision(String reason, Class<O> options) {
        requestDecision(reason, options, null);
    }

    @Override
    public <O extends Enum<O> & DecisionOption> void requestDecision(String reason, Class<O> options,
                                                                     Responsibility team) {
        request(DecisionKind.CHOICE, Decisions.optionsOfRun(options), options, null,
                team == null ? null : team.group(), reason);
    }

    @Override
    public void requestApproval(Approval approval, String reason) {
        request(DecisionKind.APPROVAL, Decisions.builtIn(List.of(BuiltInOption.APPROVED, BuiltInOption.REJECTED)),
                null, approval.requiredApprovals(), approval.team() == null ? null : approval.team().group(),
                reason);
    }

    @Override
    public <O extends Enum<O> & DecisionOption> Optional<O> decision(Class<O> options) {
        return routing.decisions().decisionOfRun(run.claimed().runId(), options);
    }

    @Override
    public Optional<ApprovalResult> approval() {
        return routing.decisions().approvalOfRun(run.claimed().runId());
    }

    private void request(DecisionKind kind, List<Decisions.OptionView> options, Class<?> optionType,
                         Integer requiredApprovals, String team, String reason) {
        Objects.requireNonNull(reason, "reason");
        if (run.decisionRequest() != null) {
            throw new IllegalStateException("run " + run.claimed().runId() + " requested a decision already");
        }
        UUID runId = run.claimed().runId();
        // in the unit of work of execute: rolled back with it
        UUID id = run.engine().tx().required(() -> run.engine().tx().withConnection(c -> routing.decisions().request(c,
                new Decisions.NewRequest(kind, SubjectType.RUN, runId, run.job().id(), runId, reason, options,
                        optionType, requiredApprovals, team), run.engine().clock().now(c))));
        run.decisionRequested(id);
    }
}
