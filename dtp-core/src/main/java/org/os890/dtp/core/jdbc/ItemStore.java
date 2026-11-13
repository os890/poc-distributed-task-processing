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

import org.os890.dtp.core.domain.FailureKind;
import org.os890.dtp.core.domain.ItemEventType;
import org.os890.dtp.core.domain.ItemState;

/** Tracked items, their visit counters and their journey ({@code dtp_item*}). */
public class ItemStore {

    private static final String COLUMNS = "id, namespace, item_type, ref_id, ref_version, business_key, lane,"
            + " current_job_id, inbox_seq, state, arrival_reason, attempts";

    private final SqlDialect dialect;

    public ItemStore(SqlDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * An item row.
     *
     * @param id            the item id
     * @param namespace     the namespace
     * @param itemType      name of the item type
     * @param refId         encoded id of the referenced entity
     * @param refVersion    version of the referenced entity (0 = unversioned)
     * @param businessKey   the business key
     * @param lane          the lane, or {@code null}
     * @param currentJobId  the job whose inbox holds the item
     * @param inboxSeq      position in the inbox
     * @param state         the state
     * @param arrivalReason reason of the last routing
     * @param attempts      failed attempts in the current inbox
     */
    public record ItemRow(UUID id, String namespace, String itemType, String refId, long refVersion,
                          String businessKey, String lane, UUID currentJobId, Long inboxSeq, ItemState state,
                          String arrivalReason, int attempts) {
    }

    /**
     * A journal entry.
     *
     * @param itemId      the item
     * @param namespace   the namespace
     * @param type        the event type
     * @param fromJobId   source job, or {@code null}
     * @param toJobId     target job, or {@code null}
     * @param runId       the run, or {@code null} outside of runs
     * @param runAttempt  the run attempt, or {@code null}
     * @param nodeId      the node
     * @param reason      the reason, or {@code null}
     * @param failureKind business or technical failure, or {@code null}
     * @param errorType   exception class of a technical failure, or {@code null}
     * @param errorHash   hash of the top stack frames, or {@code null}
     */
    public record ItemEvent(UUID itemId, String namespace, ItemEventType type, UUID fromJobId, UUID toJobId,
                            UUID runId, Integer runAttempt, UUID nodeId, String reason, FailureKind failureKind,
                            String errorType, String errorHash) {
    }

    /**
     * A query of inbox items.
     *
     * @param namespace      the namespace
     * @param jobId          the job
     * @param itemType       only items of this type, or {@code null} for all
     * @param afterSeq       only items behind this inbox position, or {@code null}
     * @param includeNotDue  also items that are deferred and not yet due
     * @param byLanePriority order by lane priority first
     * @param limit          batch size
     */
    public record InboxQuery(String namespace, UUID jobId, String itemType, Long afterSeq, boolean includeNotDue,
                             boolean byLanePriority, int limit) {
    }

    /**
     * Tracks a new item (not yet in any inbox).
     *
     * @param c            the connection
     * @param item         the new item (state QUEUED, in no inbox)
     * @param lanePriority the lane priority
     * @param now          the DB time
     * @throws SQLException on DB errors, e.g. if the item is tracked already
     */
    public void insert(Connection c, ItemRow item, int lanePriority, Instant now) throws SQLException {
        Sql.update(c, "INSERT INTO dtp_item (id, namespace, item_type, ref_id, ref_version, business_key, lane,"
                + " lane_priority, state, attempts, created_at, updated_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)", item.id(), item.namespace(), item.itemType(),
                item.refId(), item.refVersion(), item.businessKey(), item.lane(), lanePriority, item.state(), now, now);
    }

    /**
     * Locks the next due items of an inbox. Locked rows are skipped, so parallel batches get disjoint items.
     *
     * @param c     the connection of the unit of work
     * @param query the query
     * @param now   the DB time
     * @return the locked items
     * @throws SQLException on DB errors
     */
    public List<ItemRow> lockInboxBatch(Connection c, InboxQuery query, Instant now) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT " + COLUMNS + " FROM dtp_item WHERE namespace = ?"
                + " AND current_job_id = ? AND state = 'QUEUED'");
        List<Object> params = new ArrayList<>(List.of(query.namespace(), query.jobId()));
        if (query.itemType() != null) {
            sql.append(" AND item_type = ?");
            params.add(query.itemType());
        }
        if (query.afterSeq() != null) {
            sql.append(" AND inbox_seq > ?");
            params.add(query.afterSeq());
        }
        if (!query.includeNotDue()) {
            sql.append(" AND (next_attempt_at IS NULL OR next_attempt_at <= ?)");
            params.add(now);
        }
        sql.append(query.byLanePriority() ? " ORDER BY lane_priority DESC, inbox_seq" : " ORDER BY inbox_seq");
        sql.append(" LIMIT ? FOR UPDATE SKIP LOCKED");
        params.add(query.limit());
        return Sql.query(c, sql.toString(), ItemStore::map, params.toArray());
    }

    /**
     * Locks one item if it is still queued and due in the given inbox.
     *
     * @param c      the connection
     * @param itemId the item
     * @param jobId  the inbox
     * @param now    the DB time
     * @return the item, if it is still queued and due there
     * @throws SQLException on DB errors
     */
    public Optional<ItemRow> lockQueued(Connection c, UUID itemId, UUID jobId, Instant now) throws SQLException {
        return Sql.queryOne(c, "SELECT " + COLUMNS + " FROM dtp_item WHERE id = ? AND current_job_id = ?"
                + " AND state = 'QUEUED' AND (next_attempt_at IS NULL OR next_attempt_at <= ?) FOR UPDATE",
                ItemStore::map, itemId, jobId, now);
    }

    /**
     * Number of queued items in an inbox that are due.
     *
     * @param c     the connection
     * @param jobId the inbox
     * @param now   the DB time
     * @return the number of due items
     * @throws SQLException on DB errors
     */
    public int countDue(Connection c, UUID jobId, Instant now) throws SQLException {
        return Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_item WHERE current_job_id = ? AND state = 'QUEUED'"
                + " AND (next_attempt_at IS NULL OR next_attempt_at <= ?)", rs -> rs.getInt(1), jobId, now).orElse(0);
    }

    /**
     * Moves an item into the inbox of a job (at its end).
     *
     * @param c      the connection
     * @param itemId the item
     * @param jobId  the target job
     * @param reason the routing reason, or {@code null}
     * @param now    the DB time
     * @throws SQLException on DB errors
     */
    public void route(Connection c, UUID itemId, UUID jobId, String reason, Instant now) throws SQLException {
        Sql.update(c, "UPDATE dtp_item SET current_job_id = ?, inbox_seq = " + dialect.nextValue("dtp_inbox_seq")
                + ", state = 'QUEUED', arrival_reason = ?, attempts = 0, next_attempt_at = NULL, updated_at = ?"
                + " WHERE id = ?", jobId, reason, now, itemId);
    }

    /**
     * Keeps an item queued but not due before the given time.
     *
     * @param c             the connection
     * @param itemId        the item
     * @param attempts      failed attempts so far
     * @param nextAttemptAt when the item is due again
     * @param now           the DB time
     * @throws SQLException on DB errors
     */
    public void postpone(Connection c, UUID itemId, int attempts, Instant nextAttemptAt, Instant now)
            throws SQLException {
        Sql.update(c, "UPDATE dtp_item SET state = 'QUEUED', attempts = ?, next_attempt_at = ?, updated_at = ?"
                + " WHERE id = ?", attempts, nextAttemptAt, now, itemId);
    }

    /**
     * Sets the state of an item.
     *
     * @param c        the connection
     * @param itemId   the item
     * @param state    the new state
     * @param attempts failed attempts so far
     * @param now      the DB time
     * @throws SQLException on DB errors
     */
    public void setState(Connection c, UUID itemId, ItemState state, int attempts, Instant now) throws SQLException {
        Sql.update(c, "UPDATE dtp_item SET state = ?, attempts = ?, next_attempt_at = NULL, updated_at = ?"
                + " WHERE id = ?", state, attempts, now, itemId);
    }

    /**
     * Counts a visit of an item in a job's inbox. The caller holds the lock of the item row.
     *
     * @param c      the connection
     * @param itemId the item
     * @param jobId  the job
     * @return the visits including this one
     * @throws SQLException on DB errors
     */
    public int incrementVisits(Connection c, UUID itemId, UUID jobId) throws SQLException {
        int updated = Sql.update(c, "UPDATE dtp_item_visit SET visits = visits + 1 WHERE item_id = ? AND job_id = ?",
                itemId, jobId);
        if (updated == 0) {
            Sql.update(c, "INSERT INTO dtp_item_visit (item_id, job_id, visits) VALUES (?, ?, 1)", itemId, jobId);
            return 1;
        }
        return visits(c, itemId, jobId);
    }

    /**
     * Visits of an item in a job's inbox.
     *
     * @param c      the connection
     * @param itemId the item
     * @param jobId  the job
     * @return the number of visits
     * @throws SQLException on DB errors
     */
    public int visits(Connection c, UUID itemId, UUID jobId) throws SQLException {
        return Sql.queryOne(c, "SELECT visits FROM dtp_item_visit WHERE item_id = ? AND job_id = ?",
                rs -> rs.getInt(1), itemId, jobId).orElse(0);
    }

    /**
     * Appends an entry to the journey of an item.
     *
     * @param c     the connection
     * @param event the entry
     * @param now   the DB time
     * @throws SQLException on DB errors
     */
    public void append(Connection c, ItemEvent event, Instant now) throws SQLException {
        Sql.update(c, "INSERT INTO dtp_item_event (id, seq, namespace, item_id, event_type, from_job_id, to_job_id,"
                + " run_id, run_attempt, node_id, reason, failure_kind, error_type, error_hash, occurred_at)"
                + " VALUES (?, " + dialect.nextValue("dtp_event_seq") + ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                UUID.randomUUID(), event.namespace(), event.itemId(), event.type(), event.fromJobId(),
                event.toJobId(), event.runId(), event.runAttempt(), event.nodeId(), truncate(event.reason()),
                event.failureKind(), event.errorType(), event.errorHash(), now);
    }

    private static String truncate(String reason) {
        return reason == null || reason.length() <= 1000 ? reason : reason.substring(0, 997) + "...";
    }

    private static ItemRow map(ResultSet rs) throws SQLException {
        long seq = rs.getLong("inbox_seq");
        Long inboxSeq = rs.wasNull() ? null : seq;
        return new ItemRow(Sql.uuid(rs, "id"), rs.getString("namespace"), rs.getString("item_type"),
                rs.getString("ref_id"), rs.getLong("ref_version"), rs.getString("business_key"),
                rs.getString("lane"), Sql.uuid(rs, "current_job_id"), inboxSeq,
                ItemState.valueOf(rs.getString("state")), rs.getString("arrival_reason"), rs.getInt("attempts"));
    }
}
