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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.api.RunOutcome;

/**
 * Flow instances and run joins ({@code dtp_flow_instance}, {@code dtp_join_instance}, {@code dtp_join_arrival}).
 */
public class FlowInstanceStore {

    private static final String SELECT = "SELECT * FROM dtp_flow_instance";

    private final SqlDialect dialect;

    public FlowInstanceStore(SqlDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * A flow instance.
     *
     * @param id          the id
     * @param namespace   the namespace
     * @param flowName    the name: the root job
     * @param businessKey the business key
     * @param rootJobId   the root job
     * @param state       OPEN, COMPLETED or CANCELLED
     * @param startedAt   when it was started
     * @param completedAt when it completed, or {@code null}
     * @param cancel      who cancelled it, when and why, or {@code null}
     */
    public record InstanceRow(UUID id, String namespace, String flowName, String businessKey, UUID rootJobId,
                              String state, Instant startedAt, Instant completedAt, Cancel cancel) {

        /** {@code true} until it completed or was cancelled. */
        public boolean isOpen() {
            return "OPEN".equals(state);
        }
    }

    /**
     * The cancel of a flow instance.
     *
     * @param at     when
     * @param by     who
     * @param reason why
     */
    public record Cancel(Instant at, String by, String reason) {
    }

    /**
     * A run of a flow instance.
     *
     * @param id          the run
     * @param jobId       the job
     * @param state       the run state
     * @param outcome     the outcome, or {@code null}
     * @param triggerType what created it
     * @param triggeredBy details of the trigger
     * @param createdAt   when it was planned
     * @param finishedAt  when it finished, or {@code null}
     */
    public record InstanceRun(UUID id, UUID jobId, String state, String outcome, String triggerType,
                              String triggeredBy, Instant createdAt, Instant finishedAt) {
    }

    /**
     * A run join of a flow instance.
     *
     * @param targetJobId the join target
     * @param fired       {@code true} once the target's run was planned
     * @param runId       the run of the target, or {@code null}
     * @param firedAt     when it fired, or {@code null}
     */
    public record JoinRow(UUID targetJobId, boolean fired, UUID runId, Instant firedAt) {
    }

    /**
     * The arrival of a member at a run join: its latest run.
     *
     * @param memberJobId the member
     * @param runId       its latest run
     * @param outcome     the outcome of that run
     * @param arrivedAt   when
     */
    public record ArrivalRow(UUID memberJobId, UUID runId, RunOutcome outcome, Instant arrivedAt) {
    }

    /**
     * Inserts an instance unless one with the same name and business key exists.
     *
     * @param c        the connection
     * @param instance the instance
     * @return {@code true} if it was inserted
     * @throws SQLException on DB errors
     */
    public boolean insertIfAbsent(Connection c, InstanceRow instance) throws SQLException {
        String sql = "INSERT INTO dtp_flow_instance (id, namespace, flow_name, business_key, root_job_id, state,"
                + " started_at) VALUES (?, ?, ?, ?, ?, 'OPEN', ?)";
        Object[] params = {instance.id(), instance.namespace(), instance.flowName(), instance.businessKey(),
            instance.rootJobId(), instance.startedAt()};
        if (dialect.supportsOnConflictDoNothing()) {
            return Sql.update(c, sql + " ON CONFLICT (namespace, flow_name, business_key) DO NOTHING", params) == 1;
        }
        try {
            return Sql.update(c, sql, params) == 1;
        } catch (SQLException e) {
            if (Sql.UNIQUE_VIOLATION.equals(e.getSQLState())) {
                return false;
            }
            throw e;
        }
    }

    /**
     * An instance by name and business key.
     *
     * @param c           the connection
     * @param namespace   the namespace
     * @param flowName    the name
     * @param businessKey the business key
     * @return the instance, if it exists
     * @throws SQLException on DB errors
     */
    public Optional<InstanceRow> find(Connection c, String namespace, String flowName, String businessKey)
            throws SQLException {
        return Sql.queryOne(c, SELECT + " WHERE namespace = ? AND flow_name = ? AND business_key = ?",
                FlowInstanceStore::map, namespace, flowName, businessKey);
    }

    /**
     * An instance.
     *
     * @param c  the connection
     * @param id the instance
     * @return the instance, if it exists
     * @throws SQLException on DB errors
     */
    public Optional<InstanceRow> load(Connection c, UUID id) throws SQLException {
        return Sql.queryOne(c, SELECT + " WHERE id = ?", FlowInstanceStore::map, id);
    }

    /**
     * Locks an instance: serializes the finishing runs of the instance, so exactly one of them sees that nothing is
     * left and completes it (DD-4).
     *
     * @param c  the connection
     * @param id the instance
     * @return the instance, if it exists
     * @throws SQLException on DB errors
     */
    public Optional<InstanceRow> lock(Connection c, UUID id) throws SQLException {
        return Sql.queryOne(c, SELECT + " WHERE id = ? FOR UPDATE", FlowInstanceStore::map, id);
    }

    /**
     * The instances of a namespace, newest first.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param limit     at most this many
     * @return the instances
     * @throws SQLException on DB errors
     */
    public List<InstanceRow> latest(Connection c, String namespace, int limit) throws SQLException {
        return Sql.query(c, SELECT + " WHERE namespace = ? ORDER BY started_at DESC, id LIMIT ?",
                FlowInstanceStore::map, namespace, limit);
    }

    /**
     * Completes an instance.
     *
     * @param c   the connection
     * @param id  the instance
     * @param now the DB time
     * @return {@code false} if it completed already
     * @throws SQLException on DB errors
     */
    public boolean complete(Connection c, UUID id, Instant now) throws SQLException {
        return Sql.update(c, "UPDATE dtp_flow_instance SET state = 'COMPLETED', completed_at = ? WHERE id = ?"
                + " AND state = 'OPEN'", now, id) == 1;
    }

    /**
     * Cancels an open instance.
     *
     * @param c      the connection
     * @param id     the instance
     * @param cancel who, when and why
     * @return {@code false} if it isn't open
     * @throws SQLException on DB errors
     */
    public boolean cancel(Connection c, UUID id, Cancel cancel) throws SQLException {
        return Sql.update(c, "UPDATE dtp_flow_instance SET state = 'CANCELLED', cancelled_at = ?, cancelled_by = ?,"
                + " cancel_reason = ? WHERE id = ? AND state = 'OPEN'", cancel.at(), cancel.by(), cancel.reason(),
                id) == 1;
    }

    /**
     * Cancels the runs of an instance in one statement: pending runs and runs waiting for a decision finish as
     * {@code CANCELLED}, running ones are asked to stop softly ({@code STOPPING}, stop reason {@code CANCEL}). One
     * statement, so a run that changes its state meanwhile (claimed, waiting for a decision) is still caught: the DB
     * evaluates the condition again on the new row version once the other transaction committed.
     *
     * @param c   the connection
     * @param id  the instance
     * @param now the DB time
     * @return the number of runs cancelled or asked to stop
     * @throws SQLException on DB errors
     */
    public int cancelRuns(Connection c, UUID id, Instant now) throws SQLException {
        return Sql.update(c, "UPDATE dtp_run SET"
                + " outcome = CASE WHEN state IN ('RUNNING', 'STOPPING') THEN outcome ELSE 'CANCELLED' END,"
                + " finished_at = CASE WHEN state IN ('RUNNING', 'STOPPING') THEN finished_at ELSE ? END,"
                + " state = CASE WHEN state IN ('RUNNING', 'STOPPING') THEN 'STOPPING' ELSE 'FINISHED' END,"
                + " stop_reason = 'CANCEL'"
                + " WHERE flow_instance_id = ?"
                + " AND state IN ('PENDING', 'RUNNING', 'STOPPING', 'WAITING_FOR_DECISION')",
                now, id);
    }

    /**
     * Number of pending or active runs of an instance.
     *
     * @param c  the connection
     * @param id the instance
     * @return the count
     * @throws SQLException on DB errors
     */
    public int countOpenRuns(Connection c, UUID id) throws SQLException {
        return Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_run WHERE flow_instance_id = ? AND state IN ('PENDING',"
                + " 'RUNNING', 'STOPPING', 'WAITING_FOR_DECISION')", rs -> rs.getInt(1), id).orElse(0);
    }

    /**
     * Number of run joins of an instance that wait for members.
     *
     * @param c  the connection
     * @param id the instance
     * @return the count
     * @throws SQLException on DB errors
     */
    public int countWaitingJoins(Connection c, UUID id) throws SQLException {
        return Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_join_instance WHERE flow_instance_id = ? AND fired = FALSE",
                rs -> rs.getInt(1), id).orElse(0);
    }

    /**
     * Number of runs of an instance that ended without their work and weren't followed up: failed or deferred runs
     * without a rerun ({@code rerun:<run>}) or a continuation ({@code continue:<run>}). They keep the instance open.
     *
     * @param c  the connection
     * @param id the instance
     * @return the count
     * @throws SQLException on DB errors
     */
    public int countUnresolvedRuns(Connection c, UUID id) throws SQLException {
        return Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_run r WHERE r.flow_instance_id = ?"
                + " AND r.outcome IN ('FAILED', 'DEFERRED')"
                + " AND NOT EXISTS (SELECT 1 FROM dtp_run x WHERE x.namespace = r.namespace"
                + " AND x.run_key IN ('rerun:' || CAST(r.id AS VARCHAR(36)),"
                + " 'continue:' || CAST(r.id AS VARCHAR(36))))",
                rs -> rs.getInt(1), id).orElse(0);
    }

    /**
     * The runs of an instance, in the order they were planned.
     *
     * @param c  the connection
     * @param id the instance
     * @return the runs
     * @throws SQLException on DB errors
     */
    public List<InstanceRun> runs(Connection c, UUID id) throws SQLException {
        return Sql.query(c, "SELECT id, job_id, state, outcome, trigger_type, triggered_by, created_at, finished_at"
                + " FROM dtp_run WHERE flow_instance_id = ? ORDER BY created_at, id", rs -> new InstanceRun(
                        Sql.uuid(rs, "id"), Sql.uuid(rs, "job_id"), rs.getString("state"), rs.getString("outcome"),
                        rs.getString("trigger_type"), rs.getString("triggered_by"), Sql.instant(rs, "created_at"),
                        Sql.instant(rs, "finished_at")), id);
    }

    /**
     * The instance of a run.
     *
     * @param c     the connection
     * @param runId the run
     * @return the instance, empty outside an instance
     * @throws SQLException on DB errors
     */
    public Optional<InstanceRow> ofRun(Connection c, UUID runId) throws SQLException {
        return Sql.queryOne(c, "SELECT f.* FROM dtp_flow_instance f JOIN dtp_run r ON r.flow_instance_id = f.id"
                + " WHERE r.id = ?", FlowInstanceStore::map, runId);
    }

    /**
     * Creates the row of a run join unless it exists. Another member arriving at the same time may insert it first
     * (H2: unique violation, PostgreSQL: {@code ON CONFLICT DO NOTHING}).
     *
     * @param c          the connection
     * @param targetJob  the join target
     * @param instanceId the flow instance
     * @param now        the DB time
     * @throws SQLException on DB errors
     */
    public void ensureJoin(Connection c, UUID targetJob, UUID instanceId, Instant now) throws SQLException {
        String sql = "INSERT INTO dtp_join_instance (target_job_id, flow_instance_id, fired, created_at)"
                + " VALUES (?, ?, FALSE, ?)";
        if (dialect.supportsOnConflictDoNothing()) {
            Sql.update(c, sql + " ON CONFLICT (target_job_id, flow_instance_id) DO NOTHING", targetJob, instanceId,
                    now);
            return;
        }
        try {
            Sql.update(c, sql, targetJob, instanceId, now);
        } catch (SQLException e) {
            if (!Sql.UNIQUE_VIOLATION.equals(e.getSQLState())) {
                throw e;
            }
        }
    }

    /**
     * Locks the row of a run join: serializes the members arriving at the same time (DD-9).
     *
     * @param c          the connection
     * @param targetJob  the join target
     * @param instanceId the flow instance
     * @return the join
     * @throws SQLException on DB errors
     */
    public JoinRow lockJoin(Connection c, UUID targetJob, UUID instanceId) throws SQLException {
        return Sql.queryOne(c, "SELECT * FROM dtp_join_instance WHERE target_job_id = ? AND flow_instance_id = ?"
                + " FOR UPDATE", FlowInstanceStore::join, targetJob, instanceId)
                  .orElseThrow(() -> new SQLException("no join of " + targetJob + " in " + instanceId));
    }

    /**
     * The run joins of an instance.
     *
     * @param c          the connection
     * @param instanceId the instance
     * @return the joins
     * @throws SQLException on DB errors
     */
    public List<JoinRow> joins(Connection c, UUID instanceId) throws SQLException {
        return Sql.query(c, "SELECT * FROM dtp_join_instance WHERE flow_instance_id = ? ORDER BY created_at",
                FlowInstanceStore::join, instanceId);
    }

    /**
     * Records the latest run of a member (the caller holds the lock of the join).
     *
     * @param c          the connection
     * @param targetJob  the join target
     * @param instanceId the flow instance
     * @param arrival    the arrival
     * @throws SQLException on DB errors
     */
    public void arrive(Connection c, UUID targetJob, UUID instanceId, ArrivalRow arrival) throws SQLException {
        int updated = Sql.update(c, "UPDATE dtp_join_arrival SET run_id = ?, outcome = ?, arrived_at = ?"
                + " WHERE target_job_id = ? AND flow_instance_id = ? AND member_job_id = ?", arrival.runId(),
                arrival.outcome(), arrival.arrivedAt(), targetJob, instanceId, arrival.memberJobId());
        if (updated == 0) {
            Sql.update(c, "INSERT INTO dtp_join_arrival (target_job_id, flow_instance_id, member_job_id, run_id,"
                    + " outcome, arrived_at) VALUES (?, ?, ?, ?, ?, ?)", targetJob, instanceId, arrival.memberJobId(),
                    arrival.runId(), arrival.outcome(), arrival.arrivedAt());
        }
    }

    /**
     * The arrivals at a run join.
     *
     * @param c          the connection
     * @param targetJob  the join target
     * @param instanceId the flow instance
     * @return the arrivals
     * @throws SQLException on DB errors
     */
    public List<ArrivalRow> arrivals(Connection c, UUID targetJob, UUID instanceId) throws SQLException {
        return Sql.query(c, "SELECT * FROM dtp_join_arrival WHERE target_job_id = ? AND flow_instance_id = ?"
                + " ORDER BY arrived_at, member_job_id", rs -> new ArrivalRow(Sql.uuid(rs, "member_job_id"),
                        Sql.uuid(rs, "run_id"), RunOutcome.valueOf(rs.getString("outcome")),
                        Sql.instant(rs, "arrived_at")), targetJob, instanceId);
    }

    /**
     * Marks a run join as fired.
     *
     * @param c          the connection
     * @param targetJob  the join target
     * @param instanceId the flow instance
     * @param runId      the run of the target
     * @param now        the DB time
     * @throws SQLException on DB errors
     */
    public void fire(Connection c, UUID targetJob, UUID instanceId, UUID runId, Instant now) throws SQLException {
        Sql.update(c, "UPDATE dtp_join_instance SET fired = TRUE, run_id = ?, fired_at = ? WHERE target_job_id = ?"
                + " AND flow_instance_id = ?", runId, now, targetJob, instanceId);
    }

    private static InstanceRow map(ResultSet rs) throws SQLException {
        return new InstanceRow(Sql.uuid(rs, "id"), rs.getString("namespace"), rs.getString("flow_name"),
                rs.getString("business_key"), Sql.uuid(rs, "root_job_id"), rs.getString("state"),
                Sql.instant(rs, "started_at"), Sql.instant(rs, "completed_at"),
                rs.getString("cancelled_by") == null ? null : new Cancel(Sql.instant(rs, "cancelled_at"),
                        rs.getString("cancelled_by"), rs.getString("cancel_reason")));
    }

    private static JoinRow join(ResultSet rs) throws SQLException {
        return new JoinRow(Sql.uuid(rs, "target_job_id"), rs.getBoolean("fired"), Sql.uuid(rs, "run_id"),
                Sql.instant(rs, "fired_at"));
    }
}
