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
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.StopReason;
import org.os890.dtp.core.domain.RunState;
import org.os890.dtp.core.domain.TriggerType;

/** Runs and their attempts ({@code dtp_run}, {@code dtp_run_attempt}). */
public class RunStore {

    private final SqlDialect dialect;

    public RunStore(SqlDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * A run to insert.
     *
     * @param id           the run id
     * @param namespace    the namespace
     * @param runKey       idempotency key of the trigger occurrence
     * @param jobId        the job
     * @param revision     the job revision
     * @param handlerClass the handler class (for the capability filter)
     * @param state        PENDING, or FINISHED for skipped triggers
     * @param outcome      outcome of a FINISHED run, otherwise {@code null}
     * @param triggerType  what created the run
     * @param triggeredBy  details of the trigger
     * @param parameters   run parameters as JSON, or {@code null}
     * @param notBefore    earliest start, or {@code null}
     * @param flowInstanceId the flow instance, or {@code null}
     * @param cronSlot     the cron slot of a run planned by a cron trigger, or {@code null}
     * @param slotEnd      end of the processing slot, or {@code null}
     */
    public record NewRun(UUID id, String namespace, String runKey, UUID jobId, int revision, String handlerClass,
                         RunState state, RunOutcome outcome, TriggerType triggerType, String triggeredBy,
                         String parameters, Instant notBefore, UUID flowInstanceId, Instant cronSlot,
                         Instant slotEnd) {
    }

    /**
     * A pending run whose processing slot ended before it started.
     *
     * @param runId    the run
     * @param jobId    its job
     * @param revision its job revision
     */
    public record MissedSlot(UUID runId, UUID jobId, int revision) {
    }

    /**
     * A pending run that may be claimed.
     *
     * @param runId the run
     * @param jobId its job
     */
    public record Candidate(UUID runId, UUID jobId) {
    }

    /**
     * A run as loaded for execution.
     *
     * @param id           the run
     * @param jobId        the job
     * @param revision     the job revision
     * @param handlerClass the handler class
     * @param state        the state
     * @param stopReason   requested stop, if any
     * @param parameters   run parameters as JSON, or {@code null}
     * @param attempt      current attempt
     * @param outcome      the outcome of a finished run, or {@code null}
     * @param flowInstanceId the flow instance, or {@code null}
     * @param slotEnd      end of the processing slot, or {@code null}
     */
    public record RunRecord(UUID id, UUID jobId, int revision, String handlerClass, RunState state,
                            StopReason stopReason, String parameters, int attempt, RunOutcome outcome,
                            UUID flowInstanceId, Instant slotEnd) {
    }

    /**
     * Inserts a run unless a run with the same key exists (idempotent triggers).
     *
     * @param c   the connection
     * @param run the run
     * @param now the DB time
     * @return {@code true} if the run was inserted
     * @throws SQLException on DB errors
     */
    public boolean insertIfAbsent(Connection c, NewRun run, Instant now) throws SQLException {
        String sql = "INSERT INTO dtp_run (id, namespace, run_key, job_id, job_revision, handler_class, state,"
                + " outcome, trigger_type, triggered_by, parameters, not_before, created_at, finished_at,"
                + " flow_instance_id, cron_slot, slot_end) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Object[] params = {run.id(), run.namespace(), run.runKey(), run.jobId(), run.revision(), run.handlerClass(),
            run.state(), run.outcome(), run.triggerType(), run.triggeredBy(), run.parameters(), run.notBefore(), now,
            run.state() == RunState.FINISHED ? now : null, run.flowInstanceId(), run.cronSlot(), run.slotEnd()};
        if (dialect.supportsOnConflictDoNothing()) {
            return Sql.update(c, sql + " ON CONFLICT (namespace, run_key) DO NOTHING", params) == 1;
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
     * Id of the run with the given key.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param runKey    the run key
     * @return the run id, if it exists
     * @throws SQLException on DB errors
     */
    public Optional<UUID> findByKey(Connection c, String namespace, String runKey) throws SQLException {
        return Sql.queryOne(c, "SELECT id FROM dtp_run WHERE namespace = ? AND run_key = ?", rs -> Sql.uuid(rs, "id"),
                namespace, runKey);
    }

    /**
     * States of the pending and active runs of a job.
     *
     * @param c     the connection
     * @param jobId the job
     * @return the states, one per run
     * @throws SQLException on DB errors
     */
    public List<RunState> openRunStates(Connection c, UUID jobId) throws SQLException {
        return Sql.query(c, "SELECT state FROM dtp_run WHERE job_id = ? AND state IN ('PENDING', 'RUNNING',"
                + " 'STOPPING')", rs -> RunState.valueOf(rs.getString("state")), jobId);
    }

    /**
     * Id of the most recently created run of a job (part of the run keys of continuous triggers).
     *
     * @param c     the connection
     * @param jobId the job
     * @return the run id, if any
     * @throws SQLException on DB errors
     */
    public Optional<UUID> latestRunId(Connection c, UUID jobId) throws SQLException {
        return Sql.queryOne(c, "SELECT id FROM dtp_run WHERE job_id = ? ORDER BY created_at DESC, id DESC"
                + " LIMIT 1", rs -> Sql.uuid(rs, "id"), jobId);
    }

    /**
     * Latest planned cron slot of a job (before a calendar shift).
     *
     * @param c     the connection
     * @param jobId the job
     * @return the slot, if any
     * @throws SQLException on DB errors
     */
    public Optional<Instant> lastCronSlot(Connection c, UUID jobId) throws SQLException {
        return Sql.queryOne(c, "SELECT MAX(cron_slot) AS slot FROM dtp_run WHERE job_id = ?",
                rs -> Sql.instant(rs, "slot"), jobId);
    }

    /**
     * Pending runs whose processing slot ended before they started, e.g. because the cluster was down or busy.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param now       the DB time
     * @return the runs
     * @throws SQLException on DB errors
     */
    public List<MissedSlot> missedSlots(Connection c, String namespace, Instant now) throws SQLException {
        return Sql.query(c, "SELECT id, job_id, job_revision FROM dtp_run WHERE namespace = ? AND state = 'PENDING'"
                + " AND slot_end <= ?", rs -> new MissedSlot(Sql.uuid(rs, "id"), Sql.uuid(rs, "job_id"),
                        rs.getInt("job_revision")), namespace, now);
    }

    /**
     * Moves a pending run whose slot ended to the next slot.
     *
     * @param c         the connection
     * @param runId     the run
     * @param notBefore start of the next slot
     * @param slotEnd   end of the next slot
     * @param now       the DB time
     * @return {@code false} if the run started or its slot was moved meanwhile
     * @throws SQLException on DB errors
     */
    public boolean moveToSlot(Connection c, UUID runId, Instant notBefore, Instant slotEnd, Instant now)
            throws SQLException {
        return Sql.update(c, "UPDATE dtp_run SET not_before = ?, slot_end = ? WHERE id = ? AND state = 'PENDING'"
                + " AND slot_end <= ?", notBefore, slotEnd, runId, now) == 1;
    }

    /**
     * Pending runs whose job lease is free, oldest first, at most one per job.
     *
     * @param c              the connection
     * @param namespace      the namespace
     * @param now            the DB time
     * @param handlerClasses capabilities of the claiming node
     * @param limit          maximum number of candidates
     * @return the candidates
     * @throws SQLException on DB errors
     */
    public List<Candidate> claimCandidates(Connection c, String namespace, Instant now,
                                           Collection<String> handlerClasses, int limit) throws SQLException {
        if (handlerClasses.isEmpty()) {
            return List.of();
        }
        String in = String.join(", ", handlerClasses.stream().map(h -> "?").toList());
        List<Object> params = new ArrayList<>(List.of(namespace, now, now));
        params.addAll(handlerClasses);
        params.add(limit);
        List<Candidate> rows = Sql.query(c, "SELECT r.id, r.job_id FROM dtp_run r"
                + " JOIN dtp_job_lease l ON l.job_id = r.job_id"
                + " WHERE r.namespace = ? AND r.state = 'PENDING' AND l.run_id IS NULL"
                + " AND (r.not_before IS NULL OR r.not_before <= ?) AND (r.slot_end IS NULL OR r.slot_end > ?)"
                + " AND r.handler_class IN (" + in + ")"
                + " ORDER BY r.priority DESC, r.created_at, r.id LIMIT ?",
                rs -> new Candidate(Sql.uuid(rs, "id"), Sql.uuid(rs, "job_id")), params.toArray());
        List<Candidate> onePerJob = new ArrayList<>();
        for (Candidate candidate : rows) {
            if (onePerJob.stream().noneMatch(other -> other.jobId().equals(candidate.jobId()))) {
                onePerJob.add(candidate);
            }
        }
        return onePerJob;
    }

    /**
     * Marks a claimed run as running and opens a new attempt.
     *
     * @param c     the connection
     * @param runId the run
     * @param node  the claiming node
     * @param token the fencing token of the acquired lease
     * @param now   the DB time
     * @return the attempt number, or empty if the run isn't pending anymore
     * @throws SQLException on DB errors
     */
    public Optional<Integer> start(Connection c, UUID runId, UUID node, long token, Instant now) throws SQLException {
        int updated = Sql.update(c, "UPDATE dtp_run SET state = 'RUNNING', attempt = attempt + 1, owner_node_id = ?,"
                + " fencing_token = ?, started_at = COALESCE(started_at, ?) WHERE id = ? AND state = 'PENDING'",
                node, token, now, runId);
        if (updated != 1) {
            return Optional.empty();
        }
        int attempt = Sql.queryOne(c, "SELECT attempt FROM dtp_run WHERE id = ?", rs -> rs.getInt(1), runId)
                         .orElseThrow();
        Sql.update(c, "INSERT INTO dtp_run_attempt (run_id, attempt, node_id, fencing_token, started_at)"
                + " VALUES (?, ?, ?, ?, ?)", runId, attempt, node, token, now);
        return Optional.of(attempt);
    }

    /**
     * Loads a run.
     *
     * @param c     the connection
     * @param runId the run
     * @return the run
     * @throws SQLException on DB errors
     */
    public RunRecord load(Connection c, UUID runId) throws SQLException {
        return Sql.queryOne(c, "SELECT id, job_id, job_revision, handler_class, state, stop_reason, parameters,"
                + " attempt, outcome, flow_instance_id, slot_end FROM dtp_run WHERE id = ?", rs -> {
                    String stop = rs.getString("stop_reason");
                    String outcome = rs.getString("outcome");
                    return new RunRecord(Sql.uuid(rs, "id"), Sql.uuid(rs, "job_id"), rs.getInt("job_revision"),
                            rs.getString("handler_class"), RunState.valueOf(rs.getString("state")),
                            stop == null ? null : StopReason.valueOf(stop), rs.getString("parameters"),
                            rs.getInt("attempt"), outcome == null ? null : RunOutcome.valueOf(outcome),
                            Sql.uuid(rs, "flow_instance_id"), Sql.instant(rs, "slot_end"));
                }, runId).orElseThrow(() -> new SQLException("unknown run " + runId));
    }

    /**
     * How a run ended.
     *
     * @param outcome      the outcome
     * @param resultStatus the typed result status, or {@code null}
     * @param info         custom run info as JSON, or {@code null}
     * @param error        error as JSON, or {@code null}
     * @param stopReason   why the run stopped early, e.g. {@code SLOT_END}, or {@code null}
     */
    public record RunEnd(RunOutcome outcome, String resultStatus, String info, String error, StopReason stopReason) {
    }

    /**
     * Finishes a run (fenced by its token).
     *
     * @param c       the connection
     * @param runId   the run
     * @param token   the fencing token of the run
     * @param attempt the current attempt
     * @param end     how the run ended
     * @param now     the DB time
     * @return {@code false} if the run isn't owned with this token anymore
     * @throws SQLException on DB errors
     */
    public boolean finish(Connection c, UUID runId, long token, int attempt, RunEnd end, Instant now)
            throws SQLException {
        int updated = Sql.update(c, "UPDATE dtp_run SET state = 'FINISHED', outcome = ?, result_status = ?,"
                + " info = ?, error = ?, finished_at = ?, stop_reason = COALESCE(?, stop_reason) WHERE id = ?"
                + " AND fencing_token = ? AND state IN ('RUNNING', 'STOPPING')", end.outcome(), end.resultStatus(),
                end.info(), end.error(), now, end.stopReason(), runId, token);
        endAttempt(c, runId, attempt, "FINISHED", now);
        return updated == 1;
    }

    /**
     * Whether the attempt with the given fencing token finished the run itself (not a takeover or a decision).
     *
     * @param c     the connection
     * @param runId the run
     * @param token the fencing token of the attempt
     * @return {@code true} if that attempt finished the run
     * @throws SQLException on DB errors
     */
    public boolean finishedWith(Connection c, UUID runId, long token) throws SQLException {
        return Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_run_attempt WHERE run_id = ? AND fencing_token = ?"
                + " AND end_reason = 'FINISHED'", rs -> rs.getInt(1), runId, token).orElse(0) == 1;
    }

    /**
     * Handles a run whose lease was taken over: closes its open attempt and re-queues it (RERUN) or lets it wait for
     * a decision (ABANDON).
     *
     * @param c       the connection
     * @param runId   the run
     * @param rerun   {@code true} for RERUN, {@code false} for ABANDON
     * @param now     the DB time
     * @return {@code true} if the run was active
     * @throws SQLException on DB errors
     */
    public boolean afterLeaseExpired(Connection c, UUID runId, boolean rerun, Instant now) throws SQLException {
        Sql.update(c, "UPDATE dtp_run_attempt SET ended_at = ?, end_reason = 'LEASE_EXPIRED'"
                + " WHERE run_id = ? AND ended_at IS NULL", now, runId);
        String state = rerun ? "PENDING" : "WAITING_FOR_DECISION";
        return Sql.update(c, "UPDATE dtp_run SET state = ?, owner_node_id = NULL, stop_reason = NULL"
                + " WHERE id = ? AND state IN ('RUNNING', 'STOPPING')", state, runId) == 1;
    }

    /**
     * Applies the decision about an abandoned run: planned again ({@code PENDING}) or finished with an outcome.
     *
     * @param c       the connection
     * @param runId   the run
     * @param outcome the outcome, or {@code null} to plan the run again
     * @param now     the DB time
     * @return {@code false} if the run doesn't wait for a decision
     * @throws SQLException on DB errors
     */
    public boolean decided(Connection c, UUID runId, RunOutcome outcome, Instant now) throws SQLException {
        if (outcome == null) {
            return Sql.update(c, "UPDATE dtp_run SET state = 'PENDING' WHERE id = ? AND state = 'WAITING_FOR_DECISION'",
                    runId) == 1;
        }
        return Sql.update(c, "UPDATE dtp_run SET state = 'FINISHED', outcome = ?, finished_at = ? WHERE id = ?"
                + " AND state = 'WAITING_FOR_DECISION'", outcome, now, runId) == 1;
    }

    /**
     * Stop requests of runs (e.g. a cancel via the API), by run id.
     *
     * @param c      the connection
     * @param runIds the runs
     * @return the runs with a requested stop
     * @throws SQLException on DB errors
     */
    public List<RunRecord> stopRequests(Connection c, Collection<UUID> runIds) throws SQLException {
        List<RunRecord> result = new ArrayList<>();
        for (UUID runId : runIds) {
            RunRecord run = load(c, runId);
            if (run.stopReason() != null) {
                result.add(run);
            }
        }
        return result;
    }

    /**
     * Requests a soft stop of a run, or cancels it if it didn't start yet.
     *
     * @param c     the connection
     * @param runId the run
     * @param now   the DB time
     * @return {@code true} if the run was pending or running
     * @throws SQLException on DB errors
     */
    public boolean cancel(Connection c, UUID runId, Instant now) throws SQLException {
        int pending = Sql.update(c, "UPDATE dtp_run SET state = 'FINISHED', outcome = 'CANCELLED', finished_at = ?"
                + " WHERE id = ? AND state = 'PENDING'", now, runId);
        if (pending == 1) {
            return true;
        }
        return Sql.update(c, "UPDATE dtp_run SET state = 'STOPPING', stop_reason = 'CANCEL'"
                + " WHERE id = ? AND state = 'RUNNING'", runId) == 1;
    }

    /**
     * Number of issues of a run: journal entries of discarded or escalated items, and work packages it left waiting
     * for a decision.
     *
     * @param c     the connection
     * @param runId the run
     * @return the number of issues
     * @throws SQLException on DB errors
     */
    public int countIssues(Connection c, UUID runId) throws SQLException {
        int items = Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_item_event WHERE run_id = ? AND event_type IN"
                + " ('DISCARDED', 'ESCALATED', 'LOOP_LIMIT_EXCEEDED')", rs -> rs.getInt(1), runId).orElse(0);
        int packages = Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_work_package WHERE processed_by_run_id = ?"
                + " AND state IN ('WAITING_FOR_DECISION', 'DISCARDED')", rs -> rs.getInt(1), runId).orElse(0);
        return items + packages;
    }

    private static void endAttempt(Connection c, UUID runId, int attempt, String reason, Instant now)
            throws SQLException {
        Sql.update(c, "UPDATE dtp_run_attempt SET ended_at = ?, end_reason = ? WHERE run_id = ? AND attempt = ?",
                now, reason, runId, attempt);
    }
}
