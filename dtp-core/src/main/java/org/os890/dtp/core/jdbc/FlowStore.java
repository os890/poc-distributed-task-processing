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
package org.os890.dtp.core.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.core.domain.RouteKind;

/**
 * Flows ({@code dtp_flow*}, {@code dtp_route_override}): immutable versions, append-only activations and overrides.
 */
public class FlowStore {

    private final SqlDialect dialect;

    public FlowStore(SqlDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * A flow.
     *
     * @param id              the flow id
     * @param name            the name
     * @param definitionClass the class of the Java definition, if any
     */
    public record FlowRow(UUID id, String name, String definitionClass) {
    }

    /**
     * A version of a flow.
     *
     * @param id         the version id
     * @param flowId     the flow
     * @param hash       content hash
     * @param origin     CODE or MANUAL
     * @param entryJobId the entry step
     * @param createdBy  who created it
     * @param createdAt  when
     */
    public record VersionRow(UUID id, UUID flowId, String hash, String origin, UUID entryJobId, String createdBy,
                             Instant createdAt) {
    }

    /**
     * A route of a version, or an override.
     *
     * @param id           the row id
     * @param fromJobId    the step that reports the signal
     * @param signalType   class of the signal ({@code null} for overrides: any type)
     * @param signal       the signal name
     * @param kind         what happens
     * @param targetJobId  the target step
     * @param targetFlowId the target flow
     * @param override     {@code true} for an override
     */
    public record RouteRow(UUID id, UUID fromJobId, String signalType, String signal, RouteKind kind,
                           UUID targetJobId, UUID targetFlowId, boolean override) {
    }

    /**
     * An activation.
     *
     * @param versionId   the activated version
     * @param activeFrom  from when
     * @param generated   {@code true} if the framework activated a new code version
     * @param activatedBy who
     * @param reason      why
     * @param createdAt   when the activation was recorded
     */
    public record ActivationRow(UUID versionId, Instant activeFrom, boolean generated, String activatedBy,
                                String reason, Instant createdAt) {
    }

    /**
     * The flows of a namespace.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the flows
     * @throws SQLException on DB errors
     */
    public List<FlowRow> flows(Connection c, String namespace) throws SQLException {
        return Sql.query(c, "SELECT id, name, definition_class FROM dtp_flow WHERE namespace = ? ORDER BY name",
                rs -> new FlowRow(Sql.uuid(rs, "id"), rs.getString("name"), rs.getString("definition_class")),
                namespace);
    }

    /**
     * A flow by name.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param name      the name
     * @return the flow, if it exists
     * @throws SQLException on DB errors
     */
    public Optional<FlowRow> flow(Connection c, String namespace, String name) throws SQLException {
        return Sql.queryOne(c, "SELECT id, name, definition_class FROM dtp_flow WHERE namespace = ? AND name = ?",
                rs -> new FlowRow(Sql.uuid(rs, "id"), rs.getString("name"), rs.getString("definition_class")),
                namespace, name);
    }

    /**
     * A flow by id.
     *
     * @param c      the connection
     * @param flowId the flow
     * @return the flow
     * @throws SQLException on DB errors
     */
    public FlowRow flow(Connection c, UUID flowId) throws SQLException {
        return Sql.queryOne(c, "SELECT id, name, definition_class FROM dtp_flow WHERE id = ?",
                rs -> new FlowRow(Sql.uuid(rs, "id"), rs.getString("name"), rs.getString("definition_class")), flowId)
                  .orElseThrow(() -> new SQLException("unknown flow " + flowId));
    }

    /**
     * Creates a flow unless it exists, and records the class of its Java definition.
     *
     * @param c               the connection
     * @param namespace       the namespace
     * @param name            the name
     * @param definitionClass the class of the definition, or {@code null}
     * @param now             the DB time
     * @return the flow id
     * @throws SQLException on DB errors
     */
    public UUID ensureFlow(Connection c, String namespace, String name, String definitionClass, Instant now)
            throws SQLException {
        Optional<FlowRow> existing = flow(c, namespace, name);
        if (existing.isPresent()) {
            if (definitionClass != null && !definitionClass.equals(existing.get().definitionClass())) {
                Sql.update(c, "UPDATE dtp_flow SET definition_class = ? WHERE id = ?", definitionClass,
                        existing.get().id());
            }
            return existing.get().id();
        }
        UUID id = UUID.randomUUID();
        String sql = "INSERT INTO dtp_flow (id, namespace, name, definition_class, created_at) VALUES (?, ?, ?, ?, ?)";
        Sql.update(c, dialect.supportsOnConflictDoNothing() ? sql + " ON CONFLICT (namespace, name) DO NOTHING" : sql,
                id, namespace, name, definitionClass, now);
        return flow(c, namespace, name).orElseThrow().id();
    }

    /**
     * A version by hash.
     *
     * @param c      the connection
     * @param flowId the flow
     * @param hash   the content hash (or a unique prefix)
     * @return the version, if it exists
     * @throws SQLException on DB errors
     */
    public Optional<VersionRow> version(Connection c, UUID flowId, String hash) throws SQLException {
        List<VersionRow> rows = Sql.query(c, "SELECT * FROM dtp_flow_version WHERE flow_id = ? AND hash LIKE ?",
                FlowStore::version, flowId, hash + "%");
        return rows.size() == 1 ? Optional.of(rows.getFirst()) : Optional.empty();
    }

    /**
     * A version by id.
     *
     * @param c         the connection
     * @param versionId the version
     * @return the version
     * @throws SQLException on DB errors
     */
    public VersionRow version(Connection c, UUID versionId) throws SQLException {
        return Sql.queryOne(c, "SELECT * FROM dtp_flow_version WHERE id = ?", FlowStore::version, versionId)
                  .orElseThrow(() -> new SQLException("unknown flow version " + versionId));
    }

    /**
     * All versions of a flow, newest first.
     *
     * @param c      the connection
     * @param flowId the flow
     * @return the versions
     * @throws SQLException on DB errors
     */
    public List<VersionRow> versions(Connection c, UUID flowId) throws SQLException {
        return Sql.query(c, "SELECT * FROM dtp_flow_version WHERE flow_id = ? ORDER BY created_at DESC, id",
                FlowStore::version, flowId);
    }

    /**
     * Inserts a version with its steps.
     *
     * @param c       the connection
     * @param version the version
     * @param steps   the jobs of its steps
     * @throws SQLException on DB errors, e.g. if another node inserted the same version meanwhile
     */
    public void insertVersion(Connection c, VersionRow version, List<UUID> steps) throws SQLException {
        Sql.update(c, "INSERT INTO dtp_flow_version (id, flow_id, hash, origin, entry_job_id, created_by, created_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?)", version.id(), version.flowId(), version.hash(), version.origin(),
                version.entryJobId(), version.createdBy(), version.createdAt());
        for (UUID step : steps) {
            Sql.update(c, "INSERT INTO dtp_flow_step (version_id, job_id) VALUES (?, ?)", version.id(), step);
        }
    }

    /**
     * Inserts a route of a version.
     *
     * @param c         the connection
     * @param versionId the version
     * @param route     the route
     * @param branches  the branch steps of a FORK, otherwise empty
     * @throws SQLException on DB errors
     */
    public void insertRoute(Connection c, UUID versionId, RouteRow route, List<UUID> branches) throws SQLException {
        UUID id = UUID.randomUUID();
        Sql.update(c, "INSERT INTO dtp_flow_route (id, version_id, from_job_id, signal_type, signal, kind,"
                + " target_job_id, target_flow_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", id, versionId,
                route.fromJobId(), route.signalType(), route.signal(), route.kind(), route.targetJobId(),
                route.targetFlowId());
        for (int i = 0; i < branches.size(); i++) {
            Sql.update(c, "INSERT INTO dtp_flow_route_branch (route_id, position, job_id) VALUES (?, ?, ?)", id, i,
                    branches.get(i));
        }
    }

    /**
     * The branch steps of a FORK route, in the order of the definition.
     *
     * @param c       the connection
     * @param routeId the route
     * @return the jobs of the branch steps
     * @throws SQLException on DB errors
     */
    public List<UUID> branches(Connection c, UUID routeId) throws SQLException {
        return Sql.query(c, "SELECT job_id FROM dtp_flow_route_branch WHERE route_id = ? ORDER BY position",
                rs -> Sql.uuid(rs, "job_id"), routeId);
    }

    /**
     * The jobs of the steps of a version.
     *
     * @param c         the connection
     * @param versionId the version
     * @return the job ids
     * @throws SQLException on DB errors
     */
    public List<UUID> steps(Connection c, UUID versionId) throws SQLException {
        return Sql.query(c, "SELECT job_id FROM dtp_flow_step WHERE version_id = ?", rs -> Sql.uuid(rs, "job_id"),
                versionId);
    }

    /**
     * The routes of a version.
     *
     * @param c         the connection
     * @param versionId the version
     * @return the routes
     * @throws SQLException on DB errors
     */
    public List<RouteRow> routes(Connection c, UUID versionId) throws SQLException {
        return Sql.query(c, "SELECT * FROM dtp_flow_route WHERE version_id = ? ORDER BY signal_type, signal",
                rs -> route(rs, false), versionId);
    }

    /**
     * The item route of a signal.
     *
     * @param c          the connection
     * @param versionId  the version
     * @param fromJobId  the step that reports the signal
     * @param signalType class of the signal
     * @param signal     the signal name
     * @return the route, if the version has one
     * @throws SQLException on DB errors
     */
    public Optional<RouteRow> itemRoute(Connection c, UUID versionId, UUID fromJobId, String signalType,
                                        String signal) throws SQLException {
        return Sql.queryOne(c, "SELECT * FROM dtp_flow_route WHERE version_id = ? AND from_job_id = ?"
                + " AND signal_type = ? AND signal = ? AND kind <> 'RUN'", rs -> route(rs, false), versionId,
                fromJobId, signalType, signal);
    }

    /**
     * The successors of a typed run result.
     *
     * @param c          the connection
     * @param versionId  the version
     * @param fromJobId  the step whose run finished
     * @param resultType class of the result status
     * @param result     the result status name
     * @return the routes
     * @throws SQLException on DB errors
     */
    public List<RouteRow> runRoutes(Connection c, UUID versionId, UUID fromJobId, String resultType, String result)
            throws SQLException {
        return Sql.query(c, "SELECT * FROM dtp_flow_route WHERE version_id = ? AND from_job_id = ?"
                + " AND signal_type = ? AND signal = ? AND kind = 'RUN'", rs -> route(rs, false), versionId,
                fromJobId, resultType, result);
    }

    /**
     * The active version of a flow: the latest activation that is in effect.
     *
     * @param c      the connection
     * @param flowId the flow
     * @param now    the DB time
     * @return the version id, if the flow has an active version
     * @throws SQLException on DB errors
     */
    public Optional<UUID> activeVersion(Connection c, UUID flowId, Instant now) throws SQLException {
        return Sql.queryOne(c, "SELECT version_id FROM dtp_flow_activation WHERE flow_id = ? AND active_from <= ?"
                + " ORDER BY active_from DESC, created_at DESC LIMIT 1", rs -> Sql.uuid(rs, "version_id"), flowId, now);
    }

    /**
     * The activations of a flow, newest first.
     *
     * @param c      the connection
     * @param flowId the flow
     * @return the activations
     * @throws SQLException on DB errors
     */
    public List<ActivationRow> activations(Connection c, UUID flowId) throws SQLException {
        return Sql.query(c, "SELECT * FROM dtp_flow_activation WHERE flow_id = ? ORDER BY created_at DESC, id",
                rs -> new ActivationRow(Sql.uuid(rs, "version_id"), Sql.instant(rs, "active_from"),
                        rs.getBoolean("generated"), rs.getString("activated_by"), rs.getString("reason"),
                        Sql.instant(rs, "created_at")), flowId);
    }

    /**
     * Records an activation.
     *
     * @param c          the connection
     * @param flowId     the flow
     * @param activation the activation
     * @throws SQLException on DB errors
     */
    public void activate(Connection c, UUID flowId, ActivationRow activation) throws SQLException {
        Sql.update(c, "INSERT INTO dtp_flow_activation (id, flow_id, version_id, active_from, generated,"
                + " activated_by, reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", UUID.randomUUID(), flowId,
                activation.versionId(), activation.activeFrom(), activation.generated(), activation.activatedBy(),
                activation.reason(), activation.createdAt());
    }

    /**
     * The active versions of all flows of a namespace that contain a step.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param jobId     the step
     * @param now       the DB time
     * @return the version ids
     * @throws SQLException on DB errors
     */
    public List<UUID> activeVersionsContaining(Connection c, String namespace, UUID jobId, Instant now)
            throws SQLException {
        List<UUID> flows = Sql.query(c, "SELECT DISTINCT v.flow_id FROM dtp_flow_version v"
                + " JOIN dtp_flow_step s ON s.version_id = v.id JOIN dtp_flow f ON f.id = v.flow_id"
                + " WHERE f.namespace = ? AND s.job_id = ?", rs -> Sql.uuid(rs, "flow_id"), namespace, jobId);
        List<UUID> result = new ArrayList<>();
        for (UUID flow : flows) {
            Optional<UUID> active = activeVersion(c, flow, now);
            if (active.isPresent() && steps(c, active.get()).contains(jobId)) {
                result.add(active.get());
            }
        }
        return result;
    }

    /**
     * The override in effect for a hop of a flow, if any (the most recent one wins).
     *
     * @param c         the connection
     * @param flowId    the flow
     * @param fromJobId the step that reports the signal
     * @param signal    the signal name
     * @param now       the DB time
     * @return the override
     * @throws SQLException on DB errors
     */
    public Optional<RouteRow> activeOverride(Connection c, UUID flowId, UUID fromJobId, String signal, Instant now)
            throws SQLException {
        return Sql.queryOne(c, "SELECT * FROM dtp_route_override WHERE flow_id = ? AND from_job_id = ? AND signal = ?"
                + " AND active_from <= ? AND (active_until IS NULL OR active_until > ?)"
                + " ORDER BY created_at DESC LIMIT 1", rs -> route(rs, true), flowId, fromJobId, signal, now, now);
    }

    /**
     * The overrides of a flow that are in effect or planned.
     *
     * @param c      the connection
     * @param flowId the flow
     * @param now    the DB time
     * @return the overrides
     * @throws SQLException on DB errors
     */
    public List<RouteRow> currentOverrides(Connection c, UUID flowId, Instant now) throws SQLException {
        return Sql.query(c, "SELECT * FROM dtp_route_override WHERE flow_id = ?"
                + " AND (active_until IS NULL OR active_until > ?) ORDER BY created_at", rs -> route(rs, true),
                flowId, now);
    }

    /**
     * Records an override.
     *
     * @param c          the connection
     * @param flowId     the flow
     * @param route      the redirected hop
     * @param activeFrom from when
     * @param until      until when, or {@code null}
     * @param createdBy  who
     * @param reason     why
     * @param now        the DB time
     * @return the override id
     * @throws SQLException on DB errors
     */
    public UUID insertOverride(Connection c, UUID flowId, RouteRow route, Instant activeFrom, Instant until,
                               String createdBy, String reason, Instant now) throws SQLException {
        UUID id = UUID.randomUUID();
        Sql.update(c, "INSERT INTO dtp_route_override (id, flow_id, from_job_id, signal, kind, target_job_id,"
                + " target_flow_id, active_from, active_until, created_by, reason, created_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", id, flowId, route.fromJobId(), route.signal(),
                route.kind(), route.targetJobId(), route.targetFlowId(), activeFrom, until, createdBy, reason, now);
        return id;
    }

    /**
     * Ends an override.
     *
     * @param c   the connection
     * @param id  the override
     * @param now the DB time
     * @return {@code true} if it was in effect or planned
     * @throws SQLException on DB errors
     */
    public boolean endOverride(Connection c, UUID id, Instant now) throws SQLException {
        return Sql.update(c, "UPDATE dtp_route_override SET active_until = ? WHERE id = ?"
                + " AND (active_until IS NULL OR active_until > ?)", now, id, now) == 1;
    }

    private static VersionRow version(ResultSet rs) throws SQLException {
        return new VersionRow(Sql.uuid(rs, "id"), Sql.uuid(rs, "flow_id"), rs.getString("hash"),
                rs.getString("origin"), Sql.uuid(rs, "entry_job_id"), rs.getString("created_by"),
                Sql.instant(rs, "created_at"));
    }

    private static RouteRow route(ResultSet rs, boolean override) throws SQLException {
        return new RouteRow(Sql.uuid(rs, "id"), Sql.uuid(rs, "from_job_id"),
                override ? null : rs.getString("signal_type"), rs.getString("signal"),
                RouteKind.valueOf(rs.getString("kind")), Sql.uuid(rs, "target_job_id"),
                Sql.uuid(rs, "target_flow_id"), override);
    }
}
