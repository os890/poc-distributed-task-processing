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

/** Work packages ({@code dtp_work_package}, docs/design/04-lifecycles.md §4.4). */
public class PackageStore {

    private static final String COLUMNS = "id, namespace, job_id, item_type, group_key, seq, state, attempts,"
            + " item_count, created_by_run_id, created_by_job_id, processed_by_run_id, next_attempt_at, last_error,"
            + " info, created_at, updated_at, ack_reference, sent_at, ack_due_at";

    private final SqlDialect dialect;

    public PackageStore(SqlDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * A work package.
     *
     * @param id               the package
     * @param namespace        the namespace
     * @param jobId            the job that processes it
     * @param itemType         the type of its items
     * @param groupKey         the group key, or {@code null}
     * @param seq              increasing number, the order of processing
     * @param state            PENDING, SENT, DONE, WAITING_FOR_DECISION or DISCARDED
     * @param attempts         failed attempts
     * @param itemCount        number of items
     * @param createdByRunId   the run that created it
     * @param createdByJobId   the job that created it, another job than {@code jobId} after a handover
     * @param processedByRunId the run that processed it last, or {@code null}
     * @param nextAttemptAt    earliest next attempt after a failure, or {@code null}
     * @param lastError        the reason of the last failure, or {@code null}
     * @param info             custom information as JSON, or {@code null}
     * @param createdAt        creation time
     * @param updatedAt        last change
     * @param ackReference     the reference the receiver acknowledges, or {@code null}
     * @param sentAt           when it was sent last, or {@code null}
     * @param ackDueAt         until when the acknowledgement is expected, or {@code null}
     */
    public record PackageRow(UUID id, String namespace, UUID jobId, String itemType, String groupKey, long seq,
                             String state, int attempts, int itemCount, UUID createdByRunId, UUID createdByJobId,
                             UUID processedByRunId,
                             Instant nextAttemptAt, String lastError, String info, Instant createdAt,
                             Instant updatedAt, String ackReference, Instant sentAt, Instant ackDueAt) {
    }

    /**
     * Inserts a new, empty pending package.
     *
     * @param c         the connection
     * @param id        the package id
     * @param namespace the namespace
     * @param jobId     the job that processes it
     * @param itemType  the item type
     * @param groupKey  the group key, or {@code null}
     * @param creator   the creating run and its job
     * @param now       the DB time
     * @throws SQLException on DB errors
     */
    public void insert(Connection c, UUID id, String namespace, UUID jobId, String itemType, String groupKey,
                       Creator creator, Instant now) throws SQLException {
        Sql.update(c, "INSERT INTO dtp_work_package (id, namespace, job_id, item_type, group_key, seq, state,"
                + " created_by_run_id, created_by_job_id, window_occurrence_id, created_at, updated_at)"
                + " VALUES (?, ?, ?, ?, ?, " + dialect.nextValue("dtp_package_seq") + ", 'PENDING', ?, ?, ?, ?, ?)",
                id, namespace, jobId, itemType, groupKey, creator.runId(), creator.jobId(),
                creator.windowOccurrenceId(), now, now);
    }

    /**
     * The run that creates packages, its job and the window occurrence it collects for.
     *
     * @param runId              the run
     * @param jobId              its job
     * @param windowOccurrenceId the window occurrence of a collector run, or {@code null}
     */
    public record Creator(UUID runId, UUID jobId, UUID windowOccurrenceId) {
    }

    /**
     * Adds to the item count of a package.
     *
     * @param c     the connection
     * @param id    the package
     * @param count the number of added items
     * @throws SQLException on DB errors
     */
    public void addItems(Connection c, UUID id, int count) throws SQLException {
        Sql.update(c, "UPDATE dtp_work_package SET item_count = item_count + ? WHERE id = ?", count, id);
    }

    /**
     * Locks the oldest pending, due package of a job.
     *
     * @param c        the connection of the unit of work
     * @param jobId    the job
     * @param itemType the item type
     * @param now      the DB time
     * @return the package, if any
     * @throws SQLException on DB errors
     */
    public Optional<PackageRow> lockNextPending(Connection c, UUID jobId, String itemType, Instant now)
            throws SQLException {
        return Sql.queryOne(c, "SELECT " + COLUMNS + " FROM dtp_work_package WHERE job_id = ? AND item_type = ?"
                + " AND state = 'PENDING' AND (next_attempt_at IS NULL OR next_attempt_at <= ?) ORDER BY seq"
                + " LIMIT 1 FOR UPDATE SKIP LOCKED", PackageStore::map, jobId, itemType, now);
    }

    /**
     * Locks a package.
     *
     * @param c  the connection
     * @param id the package
     * @return the package, if it exists
     * @throws SQLException on DB errors
     */
    public Optional<PackageRow> lock(Connection c, UUID id) throws SQLException {
        return Sql.queryOne(c, "SELECT " + COLUMNS + " FROM dtp_work_package WHERE id = ? FOR UPDATE",
                PackageStore::map, id);
    }

    /**
     * Loads a package.
     *
     * @param c  the connection
     * @param id the package
     * @return the package
     * @throws SQLException on DB errors
     */
    public PackageRow load(Connection c, UUID id) throws SQLException {
        return Sql.queryOne(c, "SELECT " + COLUMNS + " FROM dtp_work_package WHERE id = ?", PackageStore::map, id)
                  .orElseThrow(() -> new SQLException("unknown work package " + id));
    }

    /**
     * Marks a package as done.
     *
     * @param c     the connection
     * @param id    the package
     * @param runId the processing run
     * @param info  custom information as JSON, or {@code null}
     * @param now   the DB time
     * @throws SQLException on DB errors
     */
    public void done(Connection c, UUID id, UUID runId, String info, Instant now) throws SQLException {
        done(c, id, runId, info, null, now);
    }

    /**
     * Marks a package as done, sent with a reference (without acknowledgement).
     *
     * @param c         the connection
     * @param id        the package
     * @param runId     the processing run
     * @param info      custom information as JSON, or {@code null}
     * @param reference the reference of the sent package, or {@code null}
     * @param now       the DB time
     * @throws SQLException on DB errors
     */
    public void done(Connection c, UUID id, UUID runId, String info, String reference, Instant now)
            throws SQLException {
        if (reference == null) {
            Sql.update(c, "UPDATE dtp_work_package SET state = 'DONE', processed_by_run_id = ?, info = ?,"
                    + " next_attempt_at = NULL, updated_at = ? WHERE id = ?", runId, info, now, id);
        } else {
            Sql.update(c, "UPDATE dtp_work_package SET state = 'DONE', processed_by_run_id = ?, info = ?,"
                    + " ack_reference = ?, sent_at = ?, next_attempt_at = NULL, updated_at = ? WHERE id = ?", runId,
                    info, reference, now, now, id);
        }
    }

    /**
     * Marks a package as sent: it waits for the acknowledgement of the receiver.
     *
     * @param c         the connection
     * @param id        the package
     * @param runId     the processing run
     * @param info      custom information as JSON, or {@code null}
     * @param reference the reference the receiver acknowledges
     * @param ackDueAt  until when the acknowledgement is expected, or {@code null}
     * @param now       the DB time
     * @throws SQLException on DB errors
     */
    public void sent(Connection c, UUID id, UUID runId, String info, String reference, Instant ackDueAt,
                     Instant now) throws SQLException {
        Sql.update(c, "UPDATE dtp_work_package SET state = 'SENT', processed_by_run_id = ?, info = ?,"
                + " ack_reference = ?, sent_at = ?, ack_due_at = ?, next_attempt_at = NULL, updated_at = ?"
                + " WHERE id = ?", runId, info, reference, now, ackDueAt, now, id);
    }

    /**
     * Locks the sent package of a job with a reference (the most recent one if a reference was reused).
     *
     * @param c         the connection
     * @param jobId     the job
     * @param reference the reference
     * @return the package, if one waits for its acknowledgement
     * @throws SQLException on DB errors
     */
    public Optional<PackageRow> lockSent(Connection c, UUID jobId, String reference) throws SQLException {
        return Sql.queryOne(c, "SELECT " + COLUMNS + " FROM dtp_work_package WHERE job_id = ? AND ack_reference = ?"
                + " AND state = 'SENT' ORDER BY seq DESC LIMIT 1 FOR UPDATE", PackageStore::map, jobId, reference);
    }

    /**
     * Whether a job has a package with a reference, in any state.
     *
     * @param c         the connection
     * @param jobId     the job
     * @param reference the reference
     * @return {@code true} if it has one
     * @throws SQLException on DB errors
     */
    public boolean exists(Connection c, UUID jobId, String reference) throws SQLException {
        return Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_work_package WHERE job_id = ? AND ack_reference = ?",
                rs -> rs.getInt(1), jobId, reference).orElse(0) > 0;
    }

    /**
     * Marks a sent package as acknowledged (done).
     *
     * @param c   the connection
     * @param id  the package
     * @param by  who acknowledged it, or {@code null}
     * @param now the DB time
     * @return {@code false} if it isn't sent (any more)
     * @throws SQLException on DB errors
     */
    public boolean acknowledged(Connection c, UUID id, String by, Instant now) throws SQLException {
        return Sql.update(c, "UPDATE dtp_work_package SET state = 'DONE', acknowledged_at = ?, acknowledged_by = ?,"
                + " ack_due_at = NULL, updated_at = ? WHERE id = ? AND state = 'SENT'", now, by, now, id) == 1;
    }

    /**
     * The sent packages whose acknowledgement is overdue.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param now       the DB time
     * @param limit     maximum number
     * @return the package ids
     * @throws SQLException on DB errors
     */
    public List<UUID> overdue(Connection c, String namespace, Instant now, int limit) throws SQLException {
        return Sql.query(c, "SELECT id FROM dtp_work_package WHERE namespace = ? AND state = 'SENT'"
                + " AND ack_due_at <= ? ORDER BY ack_due_at LIMIT ?", rs -> Sql.uuid(rs, "id"), namespace, now, limit);
    }

    /**
     * Records a failed attempt: pending again with a later due time, or waiting for a decision.
     *
     * @param c             the connection
     * @param id            the package
     * @param runId         the processing run, or {@code null} outside of runs (a rejection)
     * @param attempts      failed attempts including this one
     * @param nextAttemptAt the next attempt, or {@code null} if it waits for a decision
     * @param error         the reason
     * @param now           the DB time
     * @throws SQLException on DB errors
     */
    public void failed(Connection c, UUID id, UUID runId, int attempts, Instant nextAttemptAt, String error,
                       Instant now) throws SQLException {
        Sql.update(c, "UPDATE dtp_work_package SET state = ?, attempts = ?,"
                + " processed_by_run_id = COALESCE(?, processed_by_run_id), next_attempt_at = ?, last_error = ?,"
                + " ack_due_at = NULL, updated_at = ? WHERE id = ?",
                nextAttemptAt == null ? "WAITING_FOR_DECISION" : "PENDING", attempts, runId, nextAttemptAt,
                truncate(error), now, id);
    }

    /**
     * Applies a decision about a package that waits for one: pending again (attempts reset) or discarded.
     *
     * @param c       the connection
     * @param id      the package
     * @param discard {@code true} to discard it
     * @param now     the DB time
     * @return {@code false} if the package doesn't wait for a decision
     * @throws SQLException on DB errors
     */
    public boolean decided(Connection c, UUID id, boolean discard, Instant now) throws SQLException {
        return Sql.update(c, "UPDATE dtp_work_package SET state = ?, attempts = 0, next_attempt_at = NULL,"
                + " updated_at = ? WHERE id = ? AND state = 'WAITING_FOR_DECISION'", discard ? "DISCARDED" : "PENDING",
                now, id) == 1;
    }

    /**
     * Number of pending, due packages of a job (inbox trigger).
     *
     * @param c     the connection
     * @param jobId the job
     * @param now   the DB time
     * @return the number of packages
     * @throws SQLException on DB errors
     */
    public int countDue(Connection c, UUID jobId, Instant now) throws SQLException {
        return Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_work_package WHERE job_id = ? AND state = 'PENDING'"
                + " AND (next_attempt_at IS NULL OR next_attempt_at <= ?)", rs -> rs.getInt(1), jobId, now).orElse(0);
    }

    private static String truncate(String value) {
        return value == null || value.length() <= 1000 ? value : value.substring(0, 1000);
    }

    private static PackageRow map(ResultSet rs) throws SQLException {
        return new PackageRow(Sql.uuid(rs, "id"), rs.getString("namespace"), Sql.uuid(rs, "job_id"),
                rs.getString("item_type"), rs.getString("group_key"), rs.getLong("seq"), rs.getString("state"),
                rs.getInt("attempts"), rs.getInt("item_count"), Sql.uuid(rs, "created_by_run_id"),
                Sql.uuid(rs, "created_by_job_id"),
                Sql.uuid(rs, "processed_by_run_id"), Sql.instant(rs, "next_attempt_at"), rs.getString("last_error"),
                rs.getString("info"), Sql.instant(rs, "created_at"), Sql.instant(rs, "updated_at"),
                rs.getString("ack_reference"), Sql.instant(rs, "sent_at"), Sql.instant(rs, "ack_due_at"));
    }
}
