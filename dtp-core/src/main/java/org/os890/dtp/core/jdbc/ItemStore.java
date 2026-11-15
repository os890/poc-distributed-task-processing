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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.core.domain.BranchState;
import org.os890.dtp.core.domain.FailureKind;
import org.os890.dtp.core.domain.ItemEventType;
import org.os890.dtp.core.domain.ItemState;
import org.os890.dtp.core.spi.Telemetry;

/** Tracked items, their visit counters and their journey ({@code dtp_item*}). */
public class ItemStore {

    private static final String COLUMNS = "id, namespace, item_type, ref_id, ref_version, business_key, lane,"
            + " current_job_id, inbox_seq, state, arrival_reason, attempts, flow_version_id, decision_request_id,"
            + " lane_priority";
    private static final String BRANCH_COLUMNS = "item_id, branch_job_id, join_job_id, position, inbox_seq, state,"
            + " arrival_reason, attempts, decision_request_id, signal_type, signal, reason";

    private final SqlDialect dialect;
    private final Telemetry telemetry;

    public ItemStore(SqlDialect dialect) {
        this(dialect, Telemetry.NONE);
    }

    /**
     * Creates the store.
     *
     * @param dialect   the SQL dialect
     * @param telemetry the trace context of new items and journal entries
     */
    public ItemStore(SqlDialect dialect, Telemetry telemetry) {
        this.telemetry = telemetry;
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
     * @param flowVersionId the flow version the item is pinned to, or {@code null}
     * @param decisionId    the decision that brought the item back into its inbox, or {@code null}
     * @param lanePriority  the priority of the lane
     */
    public record ItemRow(UUID id, String namespace, String itemType, String refId, long refVersion,
                          String businessKey, String lane, UUID currentJobId, Long inboxSeq, ItemState state,
                          String arrivalReason, int attempts, UUID flowVersionId, UUID decisionId, int lanePriority) {

        /**
         * Same item, pinned to a flow version.
         *
         * @param versionId the flow version
         * @return a copy
         */
        public ItemRow pinnedTo(UUID versionId) {
            return new ItemRow(id, namespace, itemType, refId, refVersion, businessKey, lane, currentJobId, inboxSeq,
                    state, arrivalReason, attempts, versionId, decisionId, lanePriority);
        }
    }

    /**
     * A parallel branch of an item (DD-22).
     *
     * @param itemId        the item
     * @param branchJobId   the branch step, whose inbox holds the branch while it is queued
     * @param joinJobId     the join step
     * @param position      position in the fork definition
     * @param inboxSeq      position in the inbox of the branch step
     * @param state         the state
     * @param arrivalReason the reason of the fork, or of the decision that brought the branch back
     * @param attempts      failed attempts
     * @param decisionId    the decision that brought the branch back into its inbox, or {@code null}
     * @param signalType    class of the reported signal (FINISHED only)
     * @param signal        the reported signal (FINISHED only)
     * @param reason        the reason given with the signal
     */
    public record BranchRow(UUID itemId, UUID branchJobId, UUID joinJobId, int position, long inboxSeq,
                            BranchState state, String arrivalReason, int attempts, UUID decisionId,
                            String signalType, String signal, String reason) {
    }

    /**
     * An entry of an inbox: an item, or a branch of a forked item in the inbox of a branch step.
     *
     * @param item   the item
     * @param branch the branch, or {@code null} for an item in the inbox of a step
     */
    public record InboxEntry(ItemRow item, BranchRow branch) {

        /** Position in the inbox. */
        public long seq() {
            return branch != null ? branch.inboxSeq() : item.inboxSeq();
        }

        /** The step whose inbox holds the entry. */
        public UUID jobId() {
            return branch != null ? branch.branchJobId() : item.currentJobId();
        }
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
     * @param decisionId  the decision request the entry is about, or {@code null}
     * @param packageId   the work package the entry is about, or {@code null}
     */
    public record ItemEvent(UUID itemId, String namespace, ItemEventType type, UUID fromJobId, UUID toJobId,
                            UUID runId, Integer runAttempt, UUID nodeId, String reason, FailureKind failureKind,
                            String errorType, String errorHash, UUID decisionId, UUID packageId) {

        /**
         * The same entry, linked to a decision request.
         *
         * @param id the decision request
         * @return the linked entry
         */
        public ItemEvent withDecision(UUID id) {
            return new ItemEvent(itemId, namespace, type, fromJobId, toJobId, runId, runAttempt, nodeId, reason,
                    failureKind, errorType, errorHash, id, packageId);
        }
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
     * @param c    the connection
     * @param item the new item (state QUEUED, in no inbox)
     * @param now  the DB time
     * @throws SQLException on DB errors, e.g. if the item is tracked already
     */
    public void insert(Connection c, ItemRow item, Instant now) throws SQLException {
        Sql.update(c, "INSERT INTO dtp_item (id, namespace, item_type, ref_id, ref_version, business_key, lane,"
                + " lane_priority, state, attempts, trace_parent, created_at, updated_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)", item.id(), item.namespace(), item.itemType(),
                item.refId(), item.refVersion(), item.businessKey(), item.lane(), item.lanePriority(), item.state(),
                telemetry.currentTraceParent(), now, now);
    }

    /**
     * Assigns a queued item of the job to a work package (state {@code PACKAGED}).
     *
     * @param c         the connection
     * @param itemId    the item, locked by the caller
     * @param jobId     the job of the package, the item moves to it
     * @param packageId the package
     * @param now       the DB time
     * @throws SQLException on DB errors
     */
    public void assignToPackage(Connection c, UUID itemId, UUID jobId, UUID packageId, Instant now)
            throws SQLException {
        Sql.update(c, "UPDATE dtp_item SET state = 'PACKAGED', current_job_id = ?, work_package_id = ?, updated_at = ?"
                + " WHERE id = ?", jobId, packageId, now, itemId);
    }

    /**
     * Whether a reference is tracked already.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param itemType  the item type
     * @param refId     the encoded id
     * @param version   the version, 0 without
     * @return {@code true} if an item with this reference exists
     * @throws SQLException on DB errors
     */
    public boolean isTracked(Connection c, String namespace, String itemType, String refId, long version)
            throws SQLException {
        return Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_item WHERE namespace = ? AND item_type = ? AND ref_id = ?"
                + " AND ref_version = ?", rs -> rs.getInt(1), namespace, itemType, refId, version).orElse(0) > 0;
    }

    /**
     * Tracks a new item directly in a work package of a job (state {@code PACKAGED}).
     *
     * @param c         the connection
     * @param item      the new item
     * @param jobId     the job of the package
     * @param packageId the package
     * @param now       the DB time
     * @throws SQLException on DB errors, e.g. if the item is tracked already
     */
    public void insertPackaged(Connection c, ItemRow item, UUID jobId, UUID packageId, Instant now)
            throws SQLException {
        Sql.update(c, "INSERT INTO dtp_item (id, namespace, item_type, ref_id, ref_version, business_key, lane,"
                + " lane_priority, current_job_id, inbox_seq, state, attempts, work_package_id, trace_parent,"
                + " created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, " + dialect.nextValue("dtp_inbox_seq")
                + ", 'PACKAGED', 0, ?, ?, ?, ?)", item.id(), item.namespace(), item.itemType(), item.refId(),
                item.refVersion(), item.businessKey(), item.lane(), item.lanePriority(), jobId, packageId,
                telemetry.currentTraceParent(), now, now);
    }

    /**
     * The items of a pending work package, locked.
     *
     * @param c         the connection
     * @param packageId the package
     * @return the items still assigned to it, in inbox order
     * @throws SQLException on DB errors
     */
    public List<ItemRow> lockPackaged(Connection c, UUID packageId) throws SQLException {
        return Sql.query(c, "SELECT " + COLUMNS + " FROM dtp_item WHERE work_package_id = ? AND state = 'PACKAGED'"
                + " ORDER BY inbox_seq FOR UPDATE", ItemStore::map, packageId);
    }

    /**
     * Locks the next due entries of an inbox: items routed into the inbox and branches of forked items. Locked rows
     * are skipped, so parallel batches get disjoint entries.
     *
     * @param c     the connection of the unit of work
     * @param query the query
     * @param now   the DB time
     * @return the locked entries, in inbox order
     * @throws SQLException on DB errors
     */
    public List<InboxEntry> lockInboxBatch(Connection c, InboxQuery query, Instant now) throws SQLException {
        return lockInbox(c, query, now);
    }

    /**
     * When the entries of an inbox became due, and the trace context of their items (telemetry).
     *
     * @param c       the connection
     * @param jobId   the job whose inbox the entries are in
     * @param entries the entries, locked
     * @return per item id
     * @throws SQLException on DB errors
     */
    public Map<UUID, InboxTrace> traces(Connection c, UUID jobId, List<InboxEntry> entries) throws SQLException {
        Map<UUID, InboxTrace> traces = new HashMap<>();
        if (entries.isEmpty()) {
            return traces;
        }
        List<UUID> ids = entries.stream().map(e -> e.item().id()).distinct().toList();
        String marks = String.join(", ", Collections.nCopies(ids.size(), "?"));
        Map<UUID, String> parents = new HashMap<>();
        Map<UUID, Instant> due = new HashMap<>();
        Sql.query(c, "SELECT id, trace_parent, COALESCE(next_attempt_at, updated_at) AS due_at FROM dtp_item"
                + " WHERE id IN (" + marks + ")", rs -> {
                    parents.put(Sql.uuid(rs, "id"), rs.getString("trace_parent"));
                    return due.put(Sql.uuid(rs, "id"), Sql.instant(rs, "due_at"));
                }, ids.toArray());
        if (entries.stream().anyMatch(e -> e.branch() != null)) {
            List<Object> params = new ArrayList<>(ids);
            params.add(jobId);
            Sql.query(c, "SELECT item_id, COALESCE(next_attempt_at, updated_at) AS due_at FROM dtp_item_branch"
                    + " WHERE item_id IN (" + marks + ") AND branch_job_id = ?",
                    rs -> due.put(Sql.uuid(rs, "item_id"), Sql.instant(rs, "due_at")), params.toArray());
        }
        for (UUID id : ids) {
            traces.put(id, new InboxTrace(due.get(id), parents.get(id)));
        }
        return traces;
    }

    /**
     * When an inbox entry became due, and the trace context of its item.
     *
     * @param dueAt       arrival, or the next attempt after a retry or a deferral
     * @param traceParent the W3C traceparent stored with the item, or {@code null}
     */
    public record InboxTrace(Instant dueAt, String traceParent) {
    }

    private List<InboxEntry> lockInbox(Connection c, InboxQuery query, Instant now) throws SQLException {
        List<InboxEntry> entries = new ArrayList<>();
        StringBuilder where = new StringBuilder();
        List<Object> params = new ArrayList<>(List.of(query.namespace(), query.jobId()));
        if (query.itemType() != null) {
            where.append(" AND item_type = ?");
            params.add(query.itemType());
        }
        if (query.afterSeq() != null) {
            where.append(" AND inbox_seq > ?");
            params.add(query.afterSeq());
        }
        if (!query.includeNotDue()) {
            where.append(" AND (next_attempt_at IS NULL OR next_attempt_at <= ?)");
            params.add(now);
        }
        String order = query.byLanePriority() ? " ORDER BY lane_priority DESC, inbox_seq" : " ORDER BY inbox_seq";
        params.add(query.limit());
        for (ItemRow item : Sql.query(c, "SELECT " + COLUMNS + " FROM dtp_item WHERE namespace = ?"
                + " AND current_job_id = ? AND state = 'QUEUED'" + where + order + " LIMIT ? FOR UPDATE SKIP LOCKED",
                ItemStore::map, params.toArray())) {
            entries.add(new InboxEntry(item, null));
        }
        List<BranchRow> branches = Sql.query(c, "SELECT " + BRANCH_COLUMNS + " FROM dtp_item_branch"
                + " WHERE namespace = ? AND branch_job_id = ? AND state = 'QUEUED'" + where + order
                + " LIMIT ? FOR UPDATE SKIP LOCKED", ItemStore::branch, params.toArray());
        if (branches.isEmpty()) {
            return entries;
        }
        Map<UUID, ItemRow> items = loadAll(c, branches.stream().map(BranchRow::itemId).toList());
        for (BranchRow branch : branches) {
            ItemRow item = items.get(branch.itemId());
            entries.add(new InboxEntry(item, branch));
        }
        Comparator<InboxEntry> bySeq = Comparator.comparingLong(InboxEntry::seq);
        Comparator<InboxEntry> byLane = Comparator.comparingInt((InboxEntry e) -> e.item().lanePriority()).reversed();
        entries.sort(query.byLanePriority() ? byLane.thenComparing(bySeq) : bySeq);
        // the surplus stays locked until the commit and is handed over by a later batch
        return entries.size() > query.limit() ? new ArrayList<>(entries.subList(0, query.limit())) : entries;
    }

    /**
     * Locks one entry if it is still queued and due in the given inbox.
     *
     * @param c      the connection
     * @param itemId the item
     * @param jobId  the inbox
     * @param now    the DB time
     * @return the item or its branch, if it is still queued and due there
     * @throws SQLException on DB errors
     */
    public Optional<InboxEntry> lockQueued(Connection c, UUID itemId, UUID jobId, Instant now) throws SQLException {
        Optional<ItemRow> item = Sql.queryOne(c, "SELECT " + COLUMNS + " FROM dtp_item WHERE id = ?"
                + " AND current_job_id = ? AND state = 'QUEUED' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)"
                + " FOR UPDATE", ItemStore::map, itemId, jobId, now);
        if (item.isPresent()) {
            return Optional.of(new InboxEntry(item.get(), null));
        }
        Optional<BranchRow> branch = Sql.queryOne(c, "SELECT " + BRANCH_COLUMNS + " FROM dtp_item_branch"
                + " WHERE item_id = ? AND branch_job_id = ? AND state = 'QUEUED'"
                + " AND (next_attempt_at IS NULL OR next_attempt_at <= ?) FOR UPDATE", ItemStore::branch, itemId,
                jobId, now);
        if (branch.isEmpty()) {
            return Optional.empty();
        }
        ItemRow owner = load(c, itemId);
        return Optional.of(new InboxEntry(owner, branch.get()));
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
        int items = Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_item WHERE current_job_id = ? AND state = 'QUEUED'"
                + " AND (next_attempt_at IS NULL OR next_attempt_at <= ?)", rs -> rs.getInt(1), jobId, now).orElse(0);
        int branches = Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_item_branch WHERE branch_job_id = ?"
                + " AND state = 'QUEUED' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)", rs -> rs.getInt(1),
                jobId, now).orElse(0);
        return items + branches;
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
                + ", state = 'QUEUED', arrival_reason = ?, attempts = 0, next_attempt_at = NULL,"
                + " decision_request_id = NULL, open_branches = NULL, updated_at = ? WHERE id = ?", jobId, reason, now,
                itemId);
    }

    /**
     * Re-queues an item after a decision (DD-19): at the end of the inbox of the requesting job, with the decision
     * attached, without counting a visit.
     *
     * @param c         the connection
     * @param itemId    the item
     * @param jobId     the requesting job
     * @param requestId the decision request
     * @param reason    the arrival reason
     * @param now       the DB time
     * @throws SQLException on DB errors
     */
    public void requeueAfterDecision(Connection c, UUID itemId, UUID jobId, UUID requestId, String reason,
                                     Instant now) throws SQLException {
        route(c, itemId, jobId, reason, now);
        Sql.update(c, "UPDATE dtp_item SET decision_request_id = ? WHERE id = ?", requestId, itemId);
    }

    /**
     * Resets the visits of an item in a job's inbox (decision {@code RESET_VISITS}).
     *
     * @param c      the connection
     * @param itemId the item
     * @param jobId  the job
     * @throws SQLException on DB errors
     */
    public void resetVisits(Connection c, UUID itemId, UUID jobId) throws SQLException {
        Sql.update(c, "DELETE FROM dtp_item_visit WHERE item_id = ? AND job_id = ?", itemId, jobId);
    }

    /**
     * An item by id.
     *
     * @param c      the connection
     * @param itemId the item
     * @return the item
     * @throws SQLException on DB errors
     */
    public ItemRow load(Connection c, UUID itemId) throws SQLException {
        return Sql.queryOne(c, "SELECT " + COLUMNS + " FROM dtp_item WHERE id = ?", ItemStore::map, itemId)
                  .orElseThrow(() -> new SQLException("unknown item " + itemId));
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
     * Pins an item to a flow version: its signals are resolved with the routes of that version.
     *
     * @param c         the connection
     * @param itemId    the item
     * @param versionId the flow version
     * @param now       the DB time
     * @throws SQLException on DB errors
     */
    public void pin(Connection c, UUID itemId, UUID versionId, Instant now) throws SQLException {
        Sql.update(c, "UPDATE dtp_item SET flow_version_id = ?, updated_at = ? WHERE id = ?", versionId, now, itemId);
    }

    /**
     * Locks queued items pinned to a flow version (items in a running batch are skipped).
     *
     * @param c         the connection
     * @param versionId the flow version
     * @param limit     maximum number of items
     * @return the locked items
     * @throws SQLException on DB errors
     */
    public List<ItemRow> lockQueuedOfVersion(Connection c, UUID versionId, int limit) throws SQLException {
        return Sql.query(c, "SELECT " + COLUMNS + " FROM dtp_item WHERE flow_version_id = ? AND state = 'QUEUED'"
                + " ORDER BY inbox_seq LIMIT ? FOR UPDATE SKIP LOCKED", ItemStore::map, versionId, limit);
    }

    /**
     * Forks an item (DD-22): replaces the branches of an earlier fork, puts one branch per branch step into its inbox
     * and sets the item to {@code FORKED}. The item stays assigned to the forking step until it is joined.
     *
     * @param c            the connection
     * @param item       the item
     * @param branchJobs the branch steps
     * @param joinJobId  the join step
     * @param reason     the arrival reason of the branches
     * @param now        the DB time
     * @throws SQLException on DB errors
     */
    public void fork(Connection c, ItemRow item, List<UUID> branchJobs, UUID joinJobId, String reason, Instant now)
            throws SQLException {
        Sql.update(c, "DELETE FROM dtp_item_branch WHERE item_id = ?", item.id());
        for (int i = 0; i < branchJobs.size(); i++) {
            Sql.update(c, "INSERT INTO dtp_item_branch (item_id, branch_job_id, join_job_id, position, namespace,"
                    + " item_type, lane_priority, inbox_seq, state, arrival_reason, attempts, created_at, updated_at)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, " + dialect.nextValue("dtp_inbox_seq") + ", 'QUEUED', ?, 0, ?, ?)",
                    item.id(), branchJobs.get(i), joinJobId, i, item.namespace(), item.itemType(), item.lanePriority(),
                    reason, now, now);
        }
        Sql.update(c, "UPDATE dtp_item SET state = 'FORKED', open_branches = ?, attempts = 0, next_attempt_at = NULL,"
                + " decision_request_id = NULL, updated_at = ? WHERE id = ?", branchJobs.size(), now, item.id());
    }

    /**
     * Locks an item row, e.g. to serialize the completions of its branches.
     *
     * @param c      the connection
     * @param itemId the item
     * @return the item
     * @throws SQLException on DB errors
     */
    public ItemRow lock(Connection c, UUID itemId) throws SQLException {
        return Sql.queryOne(c, "SELECT " + COLUMNS + " FROM dtp_item WHERE id = ? FOR UPDATE",
                ItemStore::map, itemId).orElseThrow(() -> new SQLException("unknown item " + itemId));
    }

    /**
     * Finishes a branch with its result. The caller holds the lock of the item row.
     *
     * @param c          the connection
     * @param branch     the branch
     * @param signalType class of the signal
     * @param signal     the signal
     * @param reason     the reason, or {@code null}
     * @param nodeId     the node
     * @param now        the DB time
     * @return the number of branches that are still open, or -1 if the branch wasn't queued (any more)
     * @throws SQLException on DB errors
     */
    public int finishBranch(Connection c, BranchRow branch, String signalType, String signal, String reason,
                            UUID nodeId, Instant now) throws SQLException {
        int finished = Sql.update(c, "UPDATE dtp_item_branch SET state = 'FINISHED', signal_type = ?, signal = ?,"
                + " reason = ?, finished_by_node_id = ?, finished_at = ?, next_attempt_at = NULL, updated_at = ?"
                + " WHERE item_id = ? AND branch_job_id = ? AND state = 'QUEUED'", signalType, signal,
                truncate(reason), nodeId, now, now, branch.itemId(), branch.branchJobId());
        if (finished == 0) {
            return -1;
        }
        Sql.update(c, "UPDATE dtp_item SET open_branches = open_branches - 1, updated_at = ? WHERE id = ?", now,
                branch.itemId());
        return Sql.queryOne(c, "SELECT open_branches FROM dtp_item WHERE id = ?", rs -> rs.getInt(1),
                branch.itemId()).orElseThrow();
    }

    /**
     * Keeps a branch queued but not due before the given time.
     *
     * @param c             the connection
     * @param branch        the branch
     * @param attempts      failed attempts so far
     * @param nextAttemptAt when the branch is due again
     * @param now           the DB time
     * @throws SQLException on DB errors
     */
    public void postponeBranch(Connection c, BranchRow branch, int attempts, Instant nextAttemptAt, Instant now)
            throws SQLException {
        Sql.update(c, "UPDATE dtp_item_branch SET state = 'QUEUED', attempts = ?, next_attempt_at = ?, updated_at = ?"
                + " WHERE item_id = ? AND branch_job_id = ? AND state = 'QUEUED'", attempts, nextAttemptAt, now,
                branch.itemId(), branch.branchJobId());
    }

    /**
     * Lets a branch wait for a decision.
     *
     * @param c        the connection
     * @param branch   the branch
     * @param attempts failed attempts so far
     * @param now      the DB time
     * @throws SQLException on DB errors
     */
    public void branchWaitsForDecision(Connection c, BranchRow branch, int attempts, Instant now)
            throws SQLException {
        Sql.update(c, "UPDATE dtp_item_branch SET state = 'WAITING_FOR_DECISION', attempts = ?, next_attempt_at = NULL,"
                + " updated_at = ? WHERE item_id = ? AND branch_job_id = ? AND state = 'QUEUED'", attempts, now,
                branch.itemId(), branch.branchJobId());
    }

    /**
     * Re-queues a branch after a decision, with the decision attached (DD-19).
     *
     * @param c           the connection
     * @param itemId      the item
     * @param branchJobId the branch step
     * @param requestId   the decision request
     * @param reason      the arrival reason
     * @param now         the DB time
     * @return {@code true} if the branch waited for a decision
     * @throws SQLException on DB errors
     */
    public boolean requeueBranchAfterDecision(Connection c, UUID itemId, UUID branchJobId, UUID requestId,
                                              String reason, Instant now) throws SQLException {
        return Sql.update(c, "UPDATE dtp_item_branch SET state = 'QUEUED', inbox_seq = "
                + dialect.nextValue("dtp_inbox_seq") + ", attempts = 0, next_attempt_at = NULL,"
                + " decision_request_id = ?, arrival_reason = ?, updated_at = ? WHERE item_id = ? AND branch_job_id = ?"
                + " AND state = 'WAITING_FOR_DECISION'", requestId, truncate(reason), now, itemId, branchJobId) == 1;
    }

    /**
     * Cancels the open branches of an item, e.g. when it is discarded.
     *
     * @param c      the connection
     * @param itemId the item
     * @param now    the DB time
     * @throws SQLException on DB errors
     */
    public void cancelBranches(Connection c, UUID itemId, Instant now) throws SQLException {
        Sql.update(c, "UPDATE dtp_item_branch SET state = 'CANCELLED', next_attempt_at = NULL, updated_at = ?"
                + " WHERE item_id = ? AND state IN ('QUEUED', 'WAITING_FOR_DECISION')", now, itemId);
    }

    /**
     * A branch of an item.
     *
     * @param c           the connection
     * @param itemId      the item
     * @param branchJobId the branch step
     * @return the branch, if the last fork of the item had this branch
     * @throws SQLException on DB errors
     */
    public Optional<BranchRow> branch(Connection c, UUID itemId, UUID branchJobId) throws SQLException {
        return Sql.queryOne(c, "SELECT " + BRANCH_COLUMNS + " FROM dtp_item_branch WHERE item_id = ?"
                + " AND branch_job_id = ?", ItemStore::branch, itemId, branchJobId);
    }

    /**
     * The branches of the last fork of an item, in the order of the fork definition.
     *
     * @param c      the connection
     * @param itemId the item
     * @return the branches
     * @throws SQLException on DB errors
     */
    public List<BranchRow> branches(Connection c, UUID itemId) throws SQLException {
        return Sql.query(c, "SELECT " + BRANCH_COLUMNS + " FROM dtp_item_branch WHERE item_id = ? ORDER BY position",
                ItemStore::branch, itemId);
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
                + " run_id, run_attempt, node_id, reason, failure_kind, error_type, error_hash, decision_request_id,"
                + " work_package_id, trace_id, occurred_at)"
                + " VALUES (?, " + dialect.nextValue("dtp_event_seq")
                + ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                UUID.randomUUID(), event.namespace(), event.itemId(), event.type(), event.fromJobId(),
                event.toJobId(), event.runId(), event.runAttempt(), event.nodeId(), truncate(event.reason()),
                event.failureKind(), event.errorType(), event.errorHash(), event.decisionId(), event.packageId(),
                telemetry.currentTraceId(), now);
    }

    private Map<UUID, ItemRow> loadAll(Connection c, List<UUID> ids) throws SQLException {
        String marks = String.join(", ", Collections.nCopies(ids.size(), "?"));
        Map<UUID, ItemRow> items = new HashMap<>();
        for (ItemRow item : Sql.query(c, "SELECT " + COLUMNS + " FROM dtp_item WHERE id IN (" + marks
                + ")", ItemStore::map, ids.toArray())) {
            items.put(item.id(), item);
        }
        return items;
    }

    private static String truncate(String reason) {
        return reason == null || reason.length() <= 1000 ? reason : reason.substring(0, 997) + "...";
    }

    private static BranchRow branch(ResultSet rs) throws SQLException {
        return new BranchRow(Sql.uuid(rs, "item_id"), Sql.uuid(rs, "branch_job_id"), Sql.uuid(rs, "join_job_id"),
                rs.getInt("position"), rs.getLong("inbox_seq"), BranchState.valueOf(rs.getString("state")),
                rs.getString("arrival_reason"), rs.getInt("attempts"), Sql.uuid(rs, "decision_request_id"),
                rs.getString("signal_type"), rs.getString("signal"), rs.getString("reason"));
    }

    private static ItemRow map(ResultSet rs) throws SQLException {
        long seq = rs.getLong("inbox_seq");
        Long inboxSeq = rs.wasNull() ? null : seq;
        return new ItemRow(Sql.uuid(rs, "id"), rs.getString("namespace"), rs.getString("item_type"),
                rs.getString("ref_id"), rs.getLong("ref_version"), rs.getString("business_key"),
                rs.getString("lane"), Sql.uuid(rs, "current_job_id"), inboxSeq,
                ItemState.valueOf(rs.getString("state")), rs.getString("arrival_reason"), rs.getInt("attempts"),
                Sql.uuid(rs, "flow_version_id"), Sql.uuid(rs, "decision_request_id"), rs.getInt("lane_priority"));
    }
}
