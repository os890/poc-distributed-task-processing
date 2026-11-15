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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.FlowEvent;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.Signal;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.domain.FlowModel;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.RouteKind;
import org.os890.dtp.core.domain.RouteModel;
import org.os890.dtp.core.domain.TriggerType;
import org.os890.dtp.core.jdbc.FlowStore;
import org.os890.dtp.core.jdbc.FlowStore.FlowRow;
import org.os890.dtp.core.jdbc.FlowStore.RouteRow;
import org.os890.dtp.core.jdbc.FlowStore.VersionRow;
import org.os890.dtp.core.jdbc.ItemStore.ItemRow;

/**
 * Resolves signals with the flow definitions in the DB. An item is pinned to the version of the flow that was active
 * when it reported its first signal (or when its flow was started), so a newly activated version applies to new items
 * only. An override of a hop takes precedence over the route of the version.
 */
public class FlowRouting {

    private final EngineContext ctx;
    private final ItemRouting routing;
    private final RunPlanner planner;
    private final Map<Class<? extends FlowDefinition>, String> flowNames;

    /**
     * Creates the routing, attach it to the item routing via {@link ItemRouting#attach(FlowRouting)}.
     *
     * @param ctx       the engine
     * @param routing   the item routing
     * @param planner   plans the successors of typed run results
     * @param flowNames the names of the flows defined on this node
     */
    public FlowRouting(EngineContext ctx, ItemRouting routing, RunPlanner planner,
                       Map<Class<? extends FlowDefinition>, String> flowNames) {
        this.ctx = ctx;
        this.routing = routing;
        this.planner = planner;
        this.flowNames = Map.copyOf(flowNames);
    }

    /**
     * Name of a flow.
     *
     * @param flow the definition class
     * @return the name
     */
    public String flowName(Class<? extends FlowDefinition> flow) {
        String name = flowNames.get(flow);
        return name != null ? name : FlowModel.defaultName(flow);
    }

    /**
     * Handles a flow event (fired via CDI or passed directly).
     *
     * @param event the event
     * @return the started item for {@link FlowEvent.StartFlow}, the signalled item otherwise
     */
    public TrackedItem<?, ?> handle(FlowEvent event) {
        return switch (event) {
            case FlowEvent.StartFlow start -> start(start);
            case FlowEvent.ItemSignal signal -> {
                if (!(signal.item() instanceof TrackedItemImpl<?, ?> item)) {
                    throw new IllegalArgumentException(signal.item() + " isn't tracked by dtp");
                }
                item.signal(signal.signal(), signal.reason());
                yield item;
            }
        };
    }

    /**
     * Starts a flow: tracks the entity, pins it to the active version and puts it into the inbox of the entry step.
     *
     * @param start the event
     * @return the tracked item
     */
    public TrackedItem<?, ?> start(FlowEvent.StartFlow start) {
        RunContext run = RunContext.current();
        String flow = flowName(start.flow());
        return ctx.tx().required(() -> {
            String lane = start.lane() == null ? null : start.lane().name();
            VersionRow version = ctx.tx().withConnection(c -> activeVersion(c, flow, lane, now(c, run)));
            ItemsImpl items = new ItemsImpl(ctx, routing, run);
            TrackedItemImpl<?, ?> item = (TrackedItemImpl<?, ?>) track(items, start.ref(), start);
            ItemRow pinned = pin(item.row(), version.id(), run);
            routing.routeTo(pinned, ctx.catalog().byId(version.entryJobId()), "flow " + flow + " started", false,
                    run);
            item.markHandled();
            return item;
        });
    }

    /**
     * Resolves a signal and routes the item.
     *
     * @param item   the item
     * @param signal the signal
     * @param reason the reason, or {@code null} (the signal name)
     * @param run    the current run, or {@code null}
     */
    public void signal(ItemRow item, Signal signal, String reason, RunContext run) {
        String why = reason != null ? reason : signal.name();
        ctx.tx().required(() -> {
            Resolved resolved = ctx.tx().withConnection(c -> resolve(c, item, signal, run));
            ItemRow pinned = item.flowVersionId() == null || !item.flowVersionId().equals(resolved.versionId())
                    ? pin(item, resolved.versionId(), run) : item;
            apply(pinned, resolved, why, run);
            return null;
        });
    }

    /**
     * Plans the successors of a typed run result ({@code onResult} routes) of the active flow versions that contain
     * the job. Runs in the transaction that finishes the run.
     *
     * @param c          the connection
     * @param job        the job whose run finished
     * @param runId      the run
     * @param resultType class of the result status
     * @param result     the result status name
     * @param now        the DB time
     * @throws SQLException on DB errors
     */
    public void runSucceeded(Connection c, JobDefinition job, UUID runId, String resultType, String result,
                             Instant now) throws SQLException {
        FlowStore flows = ctx.flows();
        for (UUID versionId : flows.activeVersionsContaining(c, ctx.namespace(), job.id(), now)) {
            for (RouteRow route : flows.runRoutes(c, versionId, job.id(), resultType, result)) {
                JobDefinition target = ctx.catalog().byId(route.targetJobId());
                planner.plan(c, target, new RunPlanner.Trigger(TriggerType.TRANSITION,
                        "flow-transition:" + runId + ":" + target.id(), runId.toString(), null, null), now);
            }
        }
    }

    private record Resolved(UUID versionId, RouteRow route, List<UUID> branches) {

        Resolved(UUID versionId, RouteRow route) {
            this(versionId, route, List.of());
        }
    }

    private Resolved resolve(Connection c, ItemRow item, Signal signal, RunContext run) throws SQLException {
        FlowStore flows = ctx.flows();
        Instant now = now(c, run);
        UUID fromJob = item.currentJobId() != null ? item.currentJobId() : run != null ? run.job().id() : null;
        if (fromJob == null) {
            throw new IllegalStateException("item " + item.businessKey() + " is in no step, start its flow via"
                    + " FlowEvent.start");
        }
        UUID versionId = item.flowVersionId();
        if (versionId == null) {
            List<UUID> candidates = flows.activeVersionsContaining(c, ctx.namespace(), fromJob, item.lane(), now);
            if (candidates.size() != 1) {
                throw new IllegalStateException("item " + item.businessKey() + ": " + candidates.size()
                        + " active flows contain the step " + jobName(fromJob) + ", start the flow via"
                        + " FlowEvent.start");
            }
            versionId = candidates.getFirst();
        }
        VersionRow version = flows.version(c, versionId);
        Optional<RouteRow> route = flows.activeOverride(c, version.flowId(), fromJob, signal.name(), now);
        if (route.isEmpty()) {
            route = flows.itemRoute(c, versionId, fromJob, RouteModel.typeOf(signal), signal.name());
        }
        RouteRow found = route.orElseThrow(() -> new IllegalStateException("flow version " + version.hash()
                + " has no route for " + signal.name() + " of " + jobName(fromJob)));
        if (found.kind() == RouteKind.FLOW) {
            VersionRow target = activeVersion(c, flows.flow(c, found.targetFlowId()).name(), item.lane(), now);
            return new Resolved(target.id(), new RouteRow(found.id(), fromJob, found.signalType(), found.signal(),
                    RouteKind.FLOW, target.entryJobId(), found.targetFlowId(), found.override()));
        }
        if (found.kind() == RouteKind.FORK) {
            return new Resolved(versionId, found, flows.branches(c, found.id()));
        }
        return new Resolved(versionId, found);
    }

    private void apply(ItemRow item, Resolved resolved, String reason, RunContext run) {
        RouteRow route = resolved.route();
        switch (route.kind()) {
            case TO, FLOW -> routing.routeTo(item, ctx.catalog().byId(route.targetJobId()), reason, false, run);
            case BACK -> routing.routeTo(item, ctx.catalog().byId(route.targetJobId()), reason, true, run);
            case COMPLETE -> routing.complete(item, run);
            case DISCARD -> routing.discard(item, reason, run);
            case FORK -> routing.fork(item, resolved.branches().stream().map(ctx.catalog()::byId).toList(),
                    ctx.catalog().byId(route.targetJobId()), reason, run);
            default -> throw new IllegalStateException(route.kind() + " isn't an item route");
        }
    }

    private ItemRow pin(ItemRow item, UUID versionId, RunContext run) {
        ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            ctx.items().pin(c, item.id(), versionId, now(c, run));
            return null;
        }));
        return item.pinnedTo(versionId);
    }

    /** The version of a flow that is active for a lane (an activation of the lane, else for all lanes). */
    private VersionRow activeVersion(Connection c, String flow, String lane, Instant now) throws SQLException {
        FlowStore flows = ctx.flows();
        FlowRow row = flows.flow(c, ctx.namespace(), flow)
                           .orElseThrow(() -> new DtpException("unknown flow " + flow));
        UUID versionId = flows.activeVersion(c, row.id(), lane, now)
                              .orElseThrow(() -> new DtpException("flow " + flow + " has no active version"));
        return flows.version(c, versionId);
    }

    private String jobName(UUID jobId) {
        return ctx.catalog().byId(jobId).name();
    }

    private Instant now(Connection c, RunContext run) throws SQLException {
        return run != null ? run.now() : ctx.clock().now(c);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static TrackedItem<?, ?> track(ItemsImpl items, ItemRef<?, ?> ref, FlowEvent.StartFlow start) {
        return items.track((ItemRef) ref, start.businessKey(), start.lane());
    }
}
