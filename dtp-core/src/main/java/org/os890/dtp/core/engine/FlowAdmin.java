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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.domain.ItemEventType;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.RouteKind;
import org.os890.dtp.core.jdbc.FlowStore.ActivationRow;
import org.os890.dtp.core.jdbc.FlowStore.FlowRow;
import org.os890.dtp.core.jdbc.FlowStore.RouteRow;
import org.os890.dtp.core.jdbc.FlowStore.VersionRow;
import org.os890.dtp.core.jdbc.ItemStore.ItemEvent;
import org.os890.dtp.core.jdbc.ItemStore.ItemRow;
import org.os890.dtp.core.spi.Transactions;

/**
 * Operator actions on flows: activate a version (also an older one, i.e. a switch back), overrule single hops and
 * move queued items to another version. Every action is recorded (who, when, why) and takes effect cluster-wide with
 * the next signal, no restart or deployment is needed.
 */
public class FlowAdmin {

    private static final int MIGRATION_LIMIT = 10_000;

    private final EngineContext ctx;

    public FlowAdmin(EngineContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Result of a migration.
     *
     * @param migrated items pinned to the new version
     * @param skipped  items whose step isn't part of the new version (they stay on the old version)
     */
    public record Migration(int migrated, int skipped) {
    }

    /**
     * Activates a version of a flow. Refused if no active node can run one of its steps.
     *
     * @param flow       the flow name
     * @param hash       the version hash (or a unique prefix)
     * @param activeFrom from when, or {@code null} for now
     * @param by         who
     * @param reason     why
     */
    public void activate(String flow, String hash, Instant activeFrom, String by, String reason) {
        Objects.requireNonNull(by, "by");
        inTx(c -> {
            Instant now = ctx.clock().now(c);
            FlowRow row = flow(c, flow);
            VersionRow version = version(c, row, hash);
            List<String> missing = missingCapabilities(c, version.id());
            if (!missing.isEmpty()) {
                throw new DtpException("version " + version.hash() + " of flow " + flow
                        + " can't be activated, no active node can run " + missing);
            }
            ctx.flows().activate(c, row.id(), new ActivationRow(version.id(), activeFrom != null ? activeFrom : now,
                    false, by, reason, now));
            return null;
        });
    }

    /**
     * A redirected hop.
     *
     * @param flow    the flow name
     * @param fromJob the job that reports the signal
     * @param signal  the signal name
     * @param kind    what happens instead (not {@link RouteKind#RUN})
     * @param target  the target job ({@code TO}, {@code BACK}) or flow ({@code FLOW}), otherwise {@code null}
     */
    public record Redirect(String flow, String fromJob, String signal, RouteKind kind, String target) {
    }

    /**
     * Overrules one hop of a flow (all versions) until it is ended or expires.
     *
     * @param redirect   the hop and its new target
     * @param activeFrom from when, or {@code null} for now
     * @param until      until when, or {@code null}
     * @param by         who
     * @param reason     why
     * @return the override id
     */
    public UUID override(Redirect redirect, Instant activeFrom, Instant until, String by, String reason) {
        String flow = redirect.flow();
        String target = redirect.target();
        RouteKind kind = Objects.requireNonNull(redirect.kind(), "kind");
        if (!kind.isItemRoute()) {
            throw new IllegalArgumentException("only item routes can be overruled");
        }
        Objects.requireNonNull(by, "by");
        return inTx(c -> {
            Instant now = ctx.clock().now(c);
            FlowRow row = flow(c, flow);
            UUID targetJob = kind == RouteKind.TO || kind == RouteKind.BACK ? job(target).id() : null;
            UUID targetFlow = kind == RouteKind.FLOW ? flow(c, target).id() : null;
            RouteRow route = new RouteRow(null, job(redirect.fromJob()).id(), null,
                    Objects.requireNonNull(redirect.signal(), "signal"),
                    kind, targetJob, targetFlow, true);
            return ctx.flows().insertOverride(c, row.id(), route, activeFrom != null ? activeFrom : now, until, by,
                    reason, now);
        });
    }

    /**
     * Ends an override.
     *
     * @param id the override
     * @return {@code true} if it was in effect or planned
     */
    public boolean endOverride(UUID id) {
        return inTx(c -> ctx.flows().endOverride(c, id, ctx.clock().now(c)));
    }

    /**
     * Pins the queued items of a version to another version of the same flow, e.g. to apply a fixed route to items
     * that wait already. Items in a running batch are skipped.
     *
     * @param flow     the flow name
     * @param fromHash the old version
     * @param toHash   the new version
     * @param by       who
     * @return the result
     */
    public Migration migrateQueued(String flow, String fromHash, String toHash, String by) {
        return inTx(c -> {
            Instant now = ctx.clock().now(c);
            FlowRow row = flow(c, flow);
            VersionRow from = version(c, row, fromHash);
            VersionRow to = version(c, row, toHash);
            Set<UUID> steps = new HashSet<>(ctx.flows().steps(c, to.id()));
            int migrated = 0;
            int skipped = 0;
            for (ItemRow item : ctx.items().lockQueuedOfVersion(c, from.id(), MIGRATION_LIMIT)) {
                if (item.currentJobId() == null || !steps.contains(item.currentJobId())) {
                    skipped++;
                    continue;
                }
                ctx.items().pin(c, item.id(), to.id(), now);
                ctx.items().append(c, new ItemEvent(item.id(), item.namespace(), ItemEventType.MIGRATED,
                        item.currentJobId(), item.currentJobId(), null, null, ctx.nodeId(),
                        "flow " + flow + " " + from.hash() + " -> " + to.hash() + " by " + by, null, null, null), now);
                migrated++;
            }
            return new Migration(migrated, skipped);
        });
    }

    private List<String> missingCapabilities(Connection c, UUID versionId) throws SQLException {
        Set<String> capable = new HashSet<>();
        for (String json : ctx.nodes().activeCapabilities(c, ctx.namespace())) {
            if (json != null) {
                capable.addAll(List.of(ctx.json().read(json, String[].class)));
            }
        }
        List<String> missing = new ArrayList<>();
        for (UUID jobId : ctx.flows().steps(c, versionId)) {
            JobDefinition job = ctx.catalog().byId(jobId);
            if (!capable.contains(job.handlerClass())) {
                missing.add(job.handlerClass());
            }
        }
        return missing;
    }

    private FlowRow flow(Connection c, String name) throws SQLException {
        return ctx.flows().flow(c, ctx.namespace(), Objects.requireNonNull(name, "flow"))
                  .orElseThrow(() -> new DtpException("unknown flow " + name));
    }

    private VersionRow version(Connection c, FlowRow flow, String hash) throws SQLException {
        return ctx.flows().version(c, flow.id(), Objects.requireNonNull(hash, "hash"))
                  .orElseThrow(() -> new DtpException("flow " + flow.name() + " has no (unique) version " + hash));
    }

    private JobDefinition job(String name) {
        Objects.requireNonNull(name, "job");
        return ctx.catalog().all().stream().filter(j -> j.name().equals(name)).findFirst()
                  .orElseThrow(() -> new DtpException("unknown job " + name));
    }

    private <T> T inTx(Transactions.ConnectionWork<T> work) {
        return ctx.tx().required(() -> ctx.tx().withConnection(work));
    }
}
