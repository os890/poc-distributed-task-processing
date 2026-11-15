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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * The tables of the monitoring read model (docs/design/09-monitoring-heatmap.md §9.3): what the projector reads from
 * the processing tables and writes into the views. Clients read the views with {@code MonitoringQueries}.
 */
public class MonitoringStore {

    /** The name of the projection in {@code dtp_projection_state}. */
    public static final String PROJECTION = "monitoring";

    /** Journal entries after which an item has left a step. */
    private static final String DEPARTURES = "'ROUTED', 'ROUTED_BACK', 'FORKED', 'COMPLETED', 'DISCARDED', 'PACKAGED',"
            + " 'BRANCH_FINISHED'";

    /** Journal entries after which an item is in the inbox of a step ({@code to_job_id}). */
    private static final String ARRIVALS = "'ROUTED', 'ROUTED_BACK', 'PACKAGED', 'JOINED'";

    /**
     * A journal entry of the recompute horizon.
     *
     * @param itemId      the item
     * @param type        the entry type
     * @param fromJobId   the step it left or that wrote it
     * @param toJobId     the step it went to
     * @param reason      the reason (the message of a technical failure)
     * @param failureKind BUSINESS or TECHNICAL for {@code FAILED}
     * @param errorType   the exception class of a technical failure
     * @param errorHash   the hash of its top stack frames
     * @param occurredAt  when
     * @param arrivedAt   for a departure: when the item arrived at the step it left
     */
    public record EventRow(UUID itemId, String type, UUID fromJobId, UUID toJobId, String reason, String failureKind,
                           String errorType, String errorHash, Instant occurredAt, Instant arrivedAt) {
    }

    /**
     * Counters of a step within one minute.
     *
     * @param jobId           the step
     * @param minute          the start of the minute
     * @param arrived         items that arrived in its inbox
     * @param departed        items that left it
     * @param routedBack      of these, routed back
     * @param completed       completed by it
     * @param discarded       discarded by it
     * @param escalated       decision requests created by it
     * @param failedBusiness  business failures
     * @param failedTechnical technical failures
     * @param maxWaitMs       longest time from arrival to departure
     * @param sumWaitMs       sum of these times
     */
    public record StepMinute(UUID jobId, Instant minute, int arrived, int departed, int routedBack, int completed,
                             int discarded, int escalated, int failedBusiness, int failedTechnical, long maxWaitMs,
                             long sumWaitMs) {
    }

    /**
     * Items that moved from one step to another within one minute.
     *
     * @param fromJobId the source
     * @param toJobId   the target
     * @param backRoute routed back
     * @param minute    the start of the minute
     * @param items     the number of items
     */
    public record EdgeMinute(UUID fromJobId, UUID toJobId, boolean backRoute, Instant minute, int items) {
    }

    /**
     * Technical failures of a step and error type within one minute.
     *
     * @param jobId         the step
     * @param errorType     the exception class
     * @param errorHash     the hash of the top stack frames
     * @param minute        the start of the minute
     * @param occurrences   the number
     * @param sampleMessage one of the messages
     * @param sampleItemId  one of the items
     */
    public record FailureMinute(UUID jobId, String errorType, String errorHash, Instant minute, int occurrences,
                                String sampleMessage, UUID sampleItemId) {
    }

    /**
     * Queued items of a step and lane.
     *
     * @param jobId       the step
     * @param lane        the lane, {@code ""} without
     * @param queued      due items
     * @param deferred    items waiting for their next attempt
     * @param oldestSince since when the oldest due item is due (queued, or its next attempt after a failure)
     */
    public record QueueRow(UUID jobId, String lane, int queued, int deferred, Instant oldestSince) {
    }

    /**
     * Items of a step that wait for something else than a run.
     *
     * @param waitingForDecision items and branches waiting for a person
     * @param forked             items waiting for their branches
     * @param packaged           items in pending work packages
     */
    public record Parked(int waitingForDecision, int forked, int packaged) {
    }

    /**
     * Counters of a step within the recent window, read from the journal.
     *
     * @param arrived         items that arrived
     * @param departed        items that left
     * @param failedTechnical technical failures
     */
    public record Recent(int arrived, int departed, int failedTechnical) {
    }

    /**
     * The run that holds the lease of a job.
     *
     * @param runId  the run
     * @param nodeId its node
     */
    public record ActiveLease(UUID runId, UUID nodeId) {
    }

    /**
     * A row of {@code dtp_view_step_status}.
     *
     * @param jobId              the step
     * @param stepName           the job name
     * @param statusReason       why it does (not) process
     * @param queued             due items
     * @param queuedByLane       JSON object lane to due items
     * @param oldestQueuedMs     age of the oldest due item
     * @param waitingForDecision items waiting for a person
     * @param deferred           items waiting for their next attempt
     * @param forked             items waiting for their branches
     * @param packaged           items in pending packages
     * @param activeRunId        the run holding the lease
     * @param activeNodeId       its node
     * @param arrived5m          arrivals within the recent window
     * @param departed5m         departures within the recent window
     * @param failedTechnical5m  technical failures within the recent window
     * @param jamScore           0 to 100
     * @param alertState         OK, CONGESTED or FAILING
     * @param alertSince         since when
     * @param pendingState       the alert state the step is moving to
     * @param pendingCycles      cycles the pending state held
     * @param pendingSince       since when it held
     * @param projectionVersion  the cycle that changed the row last
     * @param updatedAt          when it changed last
     */
    public record StatusRow(UUID jobId, String stepName, String statusReason, int queued, String queuedByLane,
                            Long oldestQueuedMs, int waitingForDecision, int deferred, int forked, int packaged,
                            UUID activeRunId, UUID activeNodeId, int arrived5m, int departed5m,
                            int failedTechnical5m, int jamScore, String alertState, Instant alertSince,
                            String pendingState, int pendingCycles, Instant pendingSince, long projectionVersion,
                            Instant updatedAt) {

        /**
         * Compares what clients see (everything but the bookkeeping of the projector).
         *
         * @param other the stored row, or {@code null}
         * @return {@code true} if a client would see the same
         */
        public boolean sameContent(StatusRow other) {
            return other != null && stepName.equals(other.stepName) && statusReason.equals(other.statusReason)
                    && queued == other.queued && Objects.equals(queuedByLane, other.queuedByLane)
                    && Objects.equals(oldestQueuedMs, other.oldestQueuedMs)
                    && waitingForDecision == other.waitingForDecision && deferred == other.deferred
                    && forked == other.forked && packaged == other.packaged
                    && Objects.equals(activeRunId, other.activeRunId)
                    && Objects.equals(activeNodeId, other.activeNodeId) && arrived5m == other.arrived5m
                    && departed5m == other.departed5m && failedTechnical5m == other.failedTechnical5m
                    && jamScore == other.jamScore && alertState.equals(other.alertState)
                    && Objects.equals(alertSince, other.alertSince);
        }
    }

    /**
     * Locks the state of the projection and returns the version of the new cycle: a second projector that runs at
     * the same time (a leader change) waits for the first one.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the version of this cycle
     * @throws SQLException on DB errors
     */
    public long nextVersion(Connection c, String namespace) throws SQLException {
        Long version = Sql.queryOne(c, "SELECT version FROM dtp_projection_state WHERE namespace = ? AND projection = ?"
                + " FOR UPDATE", rs -> rs.getLong(1), namespace, PROJECTION).orElse(null);
        if (version == null) {
            Sql.update(c, "INSERT INTO dtp_projection_state (namespace, projection, version) VALUES (?, ?, 0)",
                    namespace, PROJECTION);
            return 1;
        }
        return version + 1;
    }

    /**
     * Stores the end of a cycle.
     *
     * @param c              the connection
     * @param namespace      the namespace
     * @param version        the version of the cycle
     * @param projectedUntil the DB time of the cycle
     * @param term           the leader term
     * @throws SQLException on DB errors
     */
    public void finishCycle(Connection c, String namespace, long version, Instant projectedUntil, long term)
            throws SQLException {
        Sql.update(c, "UPDATE dtp_projection_state SET version = ?, projected_until = ?, term = ?"
                + " WHERE namespace = ? AND projection = ?", version, projectedUntil, term, namespace, PROJECTION);
    }

    /**
     * The journal entries since a point in time, departures with the arrival of the item at the step it left.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param since     the start (inclusive)
     * @return the entries
     * @throws SQLException on DB errors
     */
    public List<EventRow> events(Connection c, String namespace, Instant since) throws SQLException {
        return Sql.query(c, "SELECT e.item_id, e.event_type, e.from_job_id, e.to_job_id, e.reason, e.failure_kind,"
                + " e.error_type, e.error_hash, e.occurred_at, CASE WHEN e.event_type IN (" + DEPARTURES + ")"
                + " AND e.from_job_id IS NOT NULL THEN (SELECT MAX(a.occurred_at) FROM dtp_item_event a"
                + " WHERE a.item_id = e.item_id AND a.seq < e.seq AND (a.event_type = 'TRACKED'"
                + " OR (e.event_type = 'BRANCH_FINISHED' AND a.event_type = 'FORKED')"
                + " OR (e.event_type <> 'BRANCH_FINISHED' AND a.event_type IN (" + ARRIVALS + ")"
                + " AND a.to_job_id = e.from_job_id))) END AS arrived_at"
                + " FROM dtp_item_event e WHERE e.namespace = ? AND e.occurred_at >= ?",
                rs -> new EventRow(Sql.uuid(rs, "item_id"), rs.getString("event_type"), Sql.uuid(rs, "from_job_id"),
                        Sql.uuid(rs, "to_job_id"), rs.getString("reason"), rs.getString("failure_kind"),
                        rs.getString("error_type"), rs.getString("error_hash"), Sql.instant(rs, "occurred_at"),
                        Sql.instant(rs, "arrived_at")), namespace, since);
    }

    /**
     * The branch steps of the items forked since a point in time (the journal names only the join step).
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param since     the start (inclusive)
     * @return the branch steps per item
     * @throws SQLException on DB errors
     */
    public Map<UUID, List<UUID>> branchSteps(Connection c, String namespace, Instant since) throws SQLException {
        Map<UUID, List<UUID>> branches = new HashMap<>();
        Sql.query(c, "SELECT DISTINCT b.item_id, b.branch_job_id, b.position FROM dtp_item_branch b"
                + " JOIN dtp_item_event e ON e.item_id = b.item_id WHERE e.namespace = ? AND e.event_type = 'FORKED'"
                + " AND e.occurred_at >= ? ORDER BY b.item_id, b.position", rs -> {
                    branches.computeIfAbsent(Sql.uuid(rs, "item_id"), id -> new ArrayList<>())
                            .add(Sql.uuid(rs, "branch_job_id"));
                    return null;
                }, namespace, since);
        return branches;
    }

    /**
     * Replaces the minute buckets from a minute on (the recompute horizon).
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param from      the first minute to replace
     * @param steps     the step buckets
     * @param edges     the edge buckets
     * @param failures  the failure buckets
     * @throws SQLException on DB errors
     */
    public void replaceMinutes(Connection c, String namespace, Instant from, List<StepMinute> steps,
                               List<EdgeMinute> edges, List<FailureMinute> failures) throws SQLException {
        Sql.update(c, "DELETE FROM dtp_view_step_minute WHERE namespace = ? AND minute_at >= ?", namespace, from);
        Sql.update(c, "DELETE FROM dtp_view_edge_minute WHERE namespace = ? AND minute_at >= ?", namespace, from);
        Sql.update(c, "DELETE FROM dtp_view_failure_minute WHERE namespace = ? AND minute_at >= ?", namespace, from);
        for (StepMinute m : steps) {
            Sql.update(c, "INSERT INTO dtp_view_step_minute (namespace, job_id, minute_at, arrived, departed,"
                    + " routed_back, completed, discarded, escalated, failed_business, failed_technical, max_wait_ms,"
                    + " sum_wait_ms)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", namespace, m.jobId(), m.minute(), m.arrived(),
                    m.departed(), m.routedBack(), m.completed(), m.discarded(), m.escalated(), m.failedBusiness(),
                    m.failedTechnical(), m.maxWaitMs(), m.sumWaitMs());
        }
        for (EdgeMinute m : edges) {
            Sql.update(c, "INSERT INTO dtp_view_edge_minute (namespace, from_job_id, to_job_id, back_route, minute_at,"
                    + " items) VALUES (?, ?, ?, ?, ?, ?)", namespace, m.fromJobId(), m.toJobId(), m.backRoute(),
                    m.minute(), m.items());
        }
        for (FailureMinute m : failures) {
            Sql.update(c, "INSERT INTO dtp_view_failure_minute (namespace, job_id, error_type, error_hash, minute_at,"
                    + " occurrences, sample_message, sample_item_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", namespace,
                    m.jobId(), m.errorType(), m.errorHash(), m.minute(), m.occurrences(), m.sampleMessage(),
                    m.sampleItemId());
        }
    }

    /**
     * Deletes the minute buckets before the retention.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param before    the first minute to keep
     * @throws SQLException on DB errors
     */
    public void deleteBefore(Connection c, String namespace, Instant before) throws SQLException {
        Sql.update(c, "DELETE FROM dtp_view_step_minute WHERE namespace = ? AND minute_at < ?", namespace, before);
        Sql.update(c, "DELETE FROM dtp_view_edge_minute WHERE namespace = ? AND minute_at < ?", namespace, before);
        Sql.update(c, "DELETE FROM dtp_view_failure_minute WHERE namespace = ? AND minute_at < ?", namespace, before);
    }

    /**
     * Arrivals, departures and technical failures per step since a point in time, read from the journal.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param since     the start of the recent window
     * @return the counters per step
     * @throws SQLException on DB errors
     */
    public Map<UUID, Recent> recent(Connection c, String namespace, Instant since) throws SQLException {
        Map<UUID, int[]> counts = new HashMap<>();
        Sql.query(c, "SELECT from_job_id, SUM(CASE WHEN event_type IN (" + DEPARTURES + ") THEN 1 ELSE 0 END) AS dep,"
                + " SUM(CASE WHEN event_type = 'FAILED' AND failure_kind = 'TECHNICAL' THEN 1 ELSE 0 END) AS tech"
                + " FROM dtp_item_event WHERE namespace = ? AND occurred_at >= ? AND from_job_id IS NOT NULL"
                + " GROUP BY from_job_id", rs -> {
                    int[] c3 = counts.computeIfAbsent(Sql.uuid(rs, "from_job_id"), id -> new int[3]);
                    c3[1] = rs.getInt("dep");
                    c3[2] = rs.getInt("tech");
                    return null;
                }, namespace, since);
        Sql.query(c, "SELECT to_job_id, COUNT(*) AS arr FROM dtp_item_event WHERE namespace = ? AND occurred_at >= ?"
                + " AND event_type IN (" + ARRIVALS + ") AND to_job_id IS NOT NULL GROUP BY to_job_id", rs -> {
                    counts.computeIfAbsent(Sql.uuid(rs, "to_job_id"), id -> new int[3])[0] = rs.getInt("arr");
                    return null;
                }, namespace, since);
        Map<UUID, Recent> recent = new HashMap<>();
        counts.forEach((job, c3) -> recent.put(job, new Recent(c3[0], c3[1], c3[2])));
        return recent;
    }

    /**
     * The exception classes of the technical failures of a step since a point in time, most frequent first.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param jobId     the step
     * @param since     the start
     * @return the exception classes
     * @throws SQLException on DB errors
     */
    public List<String> errorTypes(Connection c, String namespace, UUID jobId, Instant since) throws SQLException {
        return Sql.query(c, "SELECT error_type, COUNT(*) AS n FROM dtp_item_event WHERE namespace = ?"
                + " AND occurred_at >= ? AND from_job_id = ? AND event_type = 'FAILED' AND failure_kind = 'TECHNICAL'"
                + " AND error_type IS NOT NULL GROUP BY error_type ORDER BY n DESC, error_type",
                rs -> rs.getString("error_type"), namespace, since, jobId);
    }

    /**
     * Queued items per step and lane: due ones (with the oldest), and deferred ones; branches of forked items count
     * at their branch step.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param now       the DB time
     * @return the queues
     * @throws SQLException on DB errors
     */
    public List<QueueRow> queues(Connection c, String namespace, Instant now) throws SQLException {
        List<QueueRow> rows = new ArrayList<>(Sql.query(c, "SELECT current_job_id AS job_id,"
                + " COALESCE(lane, '') AS lane, " + queueColumns("") + " FROM dtp_item WHERE namespace = ?"
                + " AND state = 'QUEUED' AND current_job_id IS NOT NULL GROUP BY current_job_id, COALESCE(lane, '')",
                rs -> queue(rs), now, now, now, namespace));
        rows.addAll(Sql.query(c, "SELECT b.branch_job_id AS job_id, COALESCE(i.lane, '') AS lane, "
                + queueColumns("b.") + " FROM dtp_item_branch b JOIN dtp_item i ON i.id = b.item_id"
                + " WHERE b.namespace = ? AND b.state = 'QUEUED' GROUP BY b.branch_job_id, COALESCE(i.lane, '')",
                rs -> queue(rs), now, now, now, namespace));
        return rows;
    }

    private static String queueColumns(String alias) {
        String due = "(" + alias + "next_attempt_at IS NULL OR " + alias + "next_attempt_at <= ?)";
        return "SUM(CASE WHEN " + due + " THEN 1 ELSE 0 END) AS queued, SUM(CASE WHEN " + due
                + " THEN 0 ELSE 1 END) AS deferred, MIN(CASE WHEN " + due + " THEN COALESCE(" + alias
                + "next_attempt_at, " + alias + "updated_at) END) AS oldest";
    }

    private static QueueRow queue(ResultSet rs) throws SQLException {
        return new QueueRow(Sql.uuid(rs, "job_id"), rs.getString("lane"), rs.getInt("queued"), rs.getInt("deferred"),
                Sql.instant(rs, "oldest"));
    }

    /**
     * Items per step that wait for a person, for their branches or in a pending package.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the counters per step
     * @throws SQLException on DB errors
     */
    public Map<UUID, Parked> parked(Connection c, String namespace) throws SQLException {
        Map<UUID, int[]> counts = new HashMap<>();
        Sql.query(c, "SELECT current_job_id, state, COUNT(*) AS n FROM dtp_item WHERE namespace = ?"
                + " AND state IN ('WAITING_FOR_DECISION', 'FORKED', 'PACKAGED') AND current_job_id IS NOT NULL"
                + " GROUP BY current_job_id, state", rs -> {
                    int[] c3 = counts.computeIfAbsent(Sql.uuid(rs, "current_job_id"), id -> new int[3]);
                    switch (rs.getString("state")) {
                        case "WAITING_FOR_DECISION" -> c3[0] += rs.getInt("n");
                        case "FORKED" -> c3[1] += rs.getInt("n");
                        default -> c3[2] += rs.getInt("n");
                    }
                    return null;
                }, namespace);
        Sql.query(c, "SELECT branch_job_id, COUNT(*) AS n FROM dtp_item_branch WHERE namespace = ?"
                + " AND state = 'WAITING_FOR_DECISION' GROUP BY branch_job_id", rs -> {
                    counts.computeIfAbsent(Sql.uuid(rs, "branch_job_id"), id -> new int[3])[0] += rs.getInt("n");
                    return null;
                }, namespace);
        Map<UUID, Parked> parked = new HashMap<>();
        counts.forEach((job, c3) -> parked.put(job, new Parked(c3[0], c3[1], c3[2])));
        return parked;
    }

    /**
     * The runs that hold a lease, per job.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the leases
     * @throws SQLException on DB errors
     */
    public Map<UUID, ActiveLease> activeLeases(Connection c, String namespace) throws SQLException {
        Map<UUID, ActiveLease> leases = new HashMap<>();
        Sql.query(c, "SELECT job_id, run_id, owner_node_id FROM dtp_job_lease WHERE namespace = ?"
                + " AND run_id IS NOT NULL", rs -> {
                    leases.put(Sql.uuid(rs, "job_id"), new ActiveLease(Sql.uuid(rs, "run_id"),
                            Sql.uuid(rs, "owner_node_id")));
                    return null;
                }, namespace);
        return leases;
    }

    /**
     * The jobs paused by an active occurrence of a collection window.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the paused jobs
     * @throws SQLException on DB errors
     */
    public Set<UUID> pausedJobs(Connection c, String namespace) throws SQLException {
        return new HashSet<>(Sql.query(c, "SELECT DISTINCT p.job_id FROM dtp_window_paused_job p"
                + " JOIN dtp_window w ON w.id = p.window_id JOIN dtp_window_occurrence o ON o.window_id = w.id"
                + " WHERE w.namespace = ? AND o.state IN ('QUIESCING', 'OPEN', 'WAITING_FOR_DECISION')",
                rs -> Sql.uuid(rs, "job_id"), namespace));
    }

    /**
     * The jam thresholds of the current revisions ({@code expected_max_queue_age}).
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the thresholds of the jobs that have one
     * @throws SQLException on DB errors
     */
    public Map<UUID, Duration> expectedMaxQueueAges(Connection c, String namespace) throws SQLException {
        Map<UUID, Duration> ages = new HashMap<>();
        Sql.query(c, "SELECT j.id, r.expected_max_queue_age FROM dtp_job j JOIN dtp_job_revision r"
                + " ON r.job_id = j.id AND r.revision = j.current_revision WHERE j.namespace = ?"
                + " AND r.expected_max_queue_age IS NOT NULL", rs -> ages.put(Sql.uuid(rs, "id"),
                Duration.parse(rs.getString("expected_max_queue_age"))), namespace);
        return ages;
    }

    /**
     * The stored status rows.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the rows per step
     * @throws SQLException on DB errors
     */
    public Map<UUID, StatusRow> statuses(Connection c, String namespace) throws SQLException {
        Map<UUID, StatusRow> rows = new LinkedHashMap<>();
        Sql.query(c, "SELECT * FROM dtp_view_step_status WHERE namespace = ?", rs -> {
            StatusRow row = status(rs);
            rows.put(row.jobId(), row);
            return null;
        }, namespace);
        return rows;
    }

    /**
     * Maps a row of {@code dtp_view_step_status}.
     *
     * @param rs the result set
     * @return the row
     * @throws SQLException on DB errors
     */
    public static StatusRow status(ResultSet rs) throws SQLException {
        long oldest = rs.getLong("oldest_queued_ms");
        Long oldestMs = rs.wasNull() ? null : oldest;
        return new StatusRow(Sql.uuid(rs, "job_id"), rs.getString("step_name"), rs.getString("status_reason"),
                rs.getInt("queued"), rs.getString("queued_by_lane"), oldestMs, rs.getInt("waiting_for_decision"),
                rs.getInt("deferred"), rs.getInt("forked"), rs.getInt("packaged"), Sql.uuid(rs, "active_run_id"),
                Sql.uuid(rs, "active_node_id"), rs.getInt("arrived_5m"), rs.getInt("departed_5m"),
                rs.getInt("failed_technical_5m"), rs.getInt("jam_score"), rs.getString("alert_state"),
                Sql.instant(rs, "alert_since"), rs.getString("pending_state"), rs.getInt("pending_cycles"),
                Sql.instant(rs, "pending_since"), rs.getLong("projection_version"), Sql.instant(rs, "updated_at"));
    }

    /**
     * Inserts or replaces a status row.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param row       the row
     * @param exists    {@code true} if a row of the step exists
     * @throws SQLException on DB errors
     */
    public void saveStatus(Connection c, String namespace, StatusRow row, boolean exists) throws SQLException {
        Object[] values = {row.stepName(), row.statusReason(), row.queued(), row.queuedByLane(), row.oldestQueuedMs(),
            row.waitingForDecision(), row.deferred(), row.forked(), row.packaged(), row.activeRunId(),
            row.activeNodeId(), row.arrived5m(), row.departed5m(), row.failedTechnical5m(), row.jamScore(),
            row.alertState(), row.alertSince(), row.pendingState(), row.pendingCycles(), row.pendingSince(),
            row.projectionVersion(), row.updatedAt(), namespace, row.jobId()};
        if (exists) {
            Sql.update(c, "UPDATE dtp_view_step_status SET step_name = ?, status_reason = ?, queued = ?,"
                    + " queued_by_lane = ?, oldest_queued_ms = ?, waiting_for_decision = ?, deferred = ?, forked = ?,"
                    + " packaged = ?, active_run_id = ?, active_node_id = ?, arrived_5m = ?, departed_5m = ?,"
                    + " failed_technical_5m = ?, jam_score = ?, alert_state = ?, alert_since = ?, pending_state = ?,"
                    + " pending_cycles = ?, pending_since = ?, projection_version = ?, updated_at = ?"
                    + " WHERE namespace = ? AND job_id = ?", values);
        } else {
            Sql.update(c, "INSERT INTO dtp_view_step_status (step_name, status_reason, queued, queued_by_lane,"
                    + " oldest_queued_ms, waiting_for_decision, deferred, forked, packaged, active_run_id,"
                    + " active_node_id, arrived_5m, departed_5m, failed_technical_5m, jam_score, alert_state,"
                    + " alert_since, pending_state, pending_cycles, pending_since, projection_version, updated_at,"
                    + " namespace, job_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                    + " ?, ?)", values);
        }
    }

    /**
     * Deletes the status rows of jobs that don't exist anymore.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param jobId     the job
     * @throws SQLException on DB errors
     */
    public void deleteStatus(Connection c, String namespace, UUID jobId) throws SQLException {
        Sql.update(c, "DELETE FROM dtp_view_step_status WHERE namespace = ? AND job_id = ?", namespace, jobId);
    }
}
