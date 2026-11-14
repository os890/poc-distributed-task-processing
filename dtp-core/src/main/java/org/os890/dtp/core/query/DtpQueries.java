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
package org.os890.dtp.core.query;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.spi.Transactions;

/** Read-only views for operators, REST resources and tests. */
public class DtpQueries {

    private final Transactions tx;
    private final String namespace;

    public DtpQueries(Transactions tx, String namespace) {
        this.tx = tx;
        this.namespace = namespace;
    }

    /**
     * One entry of an item's journey.
     *
     * @param seq         order of the entries
     * @param occurredAt  DB time
     * @param type        event type
     * @param fromJob     name of the source job
     * @param toJob       name of the target job
     * @param runId       the run
     * @param runAttempt  the run attempt
     * @param node        name of the node
     * @param reason      the reason
     * @param failureKind business or technical failure
     * @param errorType   exception class of a technical failure
     */
    public record JourneyEntry(long seq, Instant occurredAt, String type, String fromJob, String toJob, UUID runId,
                               Integer runAttempt, String node, String reason, String failureKind,
                               String errorType) {
    }

    /**
     * A run.
     *
     * @param id           the run
     * @param job          the job name
     * @param state        the state
     * @param outcome      the outcome
     * @param resultStatus the typed result status
     * @param triggerType  the trigger type
     * @param attempt      the attempt
     * @param node         name of the (last) owning node
     * @param createdAt    planning time
     * @param startedAt    start of the first attempt
     * @param finishedAt   end time
     * @param info         custom run info as JSON
     */
    public record RunView(UUID id, String job, String state, String outcome, String resultStatus, String triggerType,
                          int attempt, String node, Instant createdAt, Instant startedAt, Instant finishedAt,
                          String info) {
    }

    /**
     * A node.
     *
     * @param id          the node id
     * @param name        the node name
     * @param state       the state
     * @param heartbeatAt last heartbeat
     * @param leader      {@code true} if it leads the housekeeping
     */
    public record NodeView(UUID id, String name, String state, Instant heartbeatAt, boolean leader) {
    }

    /**
     * Number of items per job and state.
     *
     * @param job   the job name ({@code null} if the item is in no inbox)
     * @param state the item state
     * @param count the number of items
     */
    public record ItemCount(String job, String state, int count) {
    }

    /**
     * The journey of an item, in order.
     *
     * @param itemType    name of the item type
     * @param businessKey the business key
     * @return the entries
     */
    public List<JourneyEntry> journey(String itemType, String businessKey) {
        return tx.withConnection(c -> Sql.query(c, "SELECT e.seq, e.occurred_at, e.event_type, fj.name AS from_job,"
                + " tj.name AS to_job, e.run_id, e.run_attempt, n.name AS node, e.reason, e.failure_kind, e.error_type"
                + " FROM dtp_item_event e JOIN dtp_item i ON i.id = e.item_id"
                + " LEFT JOIN dtp_job fj ON fj.id = e.from_job_id LEFT JOIN dtp_job tj ON tj.id = e.to_job_id"
                + " LEFT JOIN dtp_node n ON n.id = e.node_id"
                + " WHERE i.namespace = ? AND i.item_type = ? AND i.business_key = ? ORDER BY e.seq",
                rs -> new JourneyEntry(rs.getLong("seq"), Sql.instant(rs, "occurred_at"), rs.getString("event_type"),
                        rs.getString("from_job"), rs.getString("to_job"), Sql.uuid(rs, "run_id"),
                        Sql.integer(rs, "run_attempt"), rs.getString("node"), rs.getString("reason"),
                        rs.getString("failure_kind"), rs.getString("error_type")),
                namespace, itemType, businessKey));
    }

    /**
     * The most recent runs.
     *
     * @param limit maximum number of runs
     * @return the runs, newest first
     */
    public List<RunView> runs(int limit) {
        return tx.withConnection(c -> Sql.query(c, "SELECT r.id, j.name AS job, r.state, r.outcome, r.result_status,"
                + " r.trigger_type, r.attempt, n.name AS node, r.created_at, r.started_at, r.finished_at, r.info"
                + " FROM dtp_run r JOIN dtp_job j ON j.id = r.job_id LEFT JOIN dtp_node n ON n.id = r.owner_node_id"
                + " WHERE r.namespace = ? ORDER BY r.created_at DESC, r.id LIMIT ?",
                rs -> new RunView(Sql.uuid(rs, "id"), rs.getString("job"), rs.getString("state"),
                        rs.getString("outcome"), rs.getString("result_status"), rs.getString("trigger_type"),
                        rs.getInt("attempt"), rs.getString("node"), Sql.instant(rs, "created_at"),
                        Sql.instant(rs, "started_at"), Sql.instant(rs, "finished_at"), rs.getString("info")),
                namespace, limit));
    }

    /**
     * The nodes of the namespace.
     *
     * @return the nodes
     */
    public List<NodeView> nodes() {
        return tx.withConnection(c -> Sql.query(c, "SELECT n.id, n.name, n.state, n.heartbeat_at,"
                + " CASE WHEN l.holder_node_id = n.id THEN TRUE ELSE FALSE END AS leader FROM dtp_node n"
                + " LEFT JOIN dtp_leader l ON l.namespace = n.namespace AND l.role = 'housekeeping'"
                + " WHERE n.namespace = ? ORDER BY n.started_at",
                rs -> new NodeView(Sql.uuid(rs, "id"), rs.getString("name"), rs.getString("state"),
                        Sql.instant(rs, "heartbeat_at"), rs.getBoolean("leader")), namespace));
    }

    /**
     * Number of items per job and state.
     *
     * @return the counts
     */
    public List<ItemCount> itemCounts() {
        return tx.withConnection(c -> {
            List<ItemCount> counts = new ArrayList<>(Sql.query(c, "SELECT j.name AS job, i.state, COUNT(*) AS cnt"
                    + " FROM dtp_item i LEFT JOIN dtp_job j ON j.id = i.current_job_id WHERE i.namespace = ?"
                    + " GROUP BY j.name, i.state ORDER BY j.name, i.state",
                    rs -> new ItemCount(rs.getString("job"), rs.getString("state"), rs.getInt("cnt")), namespace));
            // the open branches of forked items, by branch step
            counts.addAll(Sql.query(c, "SELECT j.name AS job, b.state, COUNT(*) AS cnt FROM dtp_item_branch b"
                    + " JOIN dtp_item i ON i.id = b.item_id JOIN dtp_job j ON j.id = b.branch_job_id"
                    + " WHERE b.namespace = ? AND i.state = 'FORKED' AND b.state IN ('QUEUED', 'WAITING_FOR_DECISION')"
                    + " GROUP BY j.name, b.state ORDER BY j.name, b.state",
                    rs -> new ItemCount(rs.getString("job"), "BRANCH_" + rs.getString("state"), rs.getInt("cnt")),
                    namespace));
            return counts;
        });
    }

    /**
     * {@code true} if no run is pending or active and no inbox with an inbox trigger has due items.
     *
     * @return {@code true} if the cluster is idle
     */
    public boolean isIdle() {
        return tx.withConnection(c -> {
            int open = Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_run WHERE namespace = ? AND state IN ('PENDING',"
                    + " 'RUNNING', 'STOPPING')", rs -> rs.getInt(1), namespace).orElse(0);
            int due = Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_item i JOIN dtp_job j ON j.id = i.current_job_id"
                    + " JOIN dtp_job_revision r ON r.job_id = j.id AND r.revision = j.current_revision"
                    + " WHERE i.namespace = ? AND i.state = 'QUEUED' AND r.inbox_min_items IS NOT NULL"
                    + " AND (i.next_attempt_at IS NULL OR i.next_attempt_at <= CURRENT_TIMESTAMP)",
                    rs -> rs.getInt(1), namespace).orElse(0);
            int branches = Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_item_branch b"
                    + " JOIN dtp_job j ON j.id = b.branch_job_id"
                    + " JOIN dtp_job_revision r ON r.job_id = j.id AND r.revision = j.current_revision"
                    + " WHERE b.namespace = ? AND b.state = 'QUEUED' AND r.inbox_min_items IS NOT NULL"
                    + " AND (b.next_attempt_at IS NULL OR b.next_attempt_at <= CURRENT_TIMESTAMP)",
                    rs -> rs.getInt(1), namespace).orElse(0);
            return open == 0 && due == 0 && branches == 0;
        });
    }
}
