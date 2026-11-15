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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.core.jdbc.DbClock;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.jdbc.SqlDialect;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.spi.Transactions;

/**
 * The journey diagram of an item (docs/design/09-monitoring-heatmap.md §9.9): folds the item's journal entries into a
 * graph of the steps it visited, with its decisions, work packages, retries and where it is now. Reads the journal by
 * item (indexed), the decision requests and packages of the item, and the step status of the monitoring read model;
 * it adds nothing to the processing path.
 */
public class JourneyQueries {

    private final Transactions tx;
    private final String namespace;
    private final DtpQueries queries;
    private final DbClock clock;

    /**
     * Creates the queries.
     *
     * @param tx        the transactions
     * @param namespace the namespace
     * @param queries   the queries of the journal
     */
    public JourneyQueries(Transactions tx, String namespace, DtpQueries queries) {
        this.tx = tx;
        this.namespace = namespace;
        this.queries = queries;
        this.clock = new DbClock(tx.withConnection(SqlDialect::detect));
    }

    /** How much the diagram shows. */
    public enum Detail {
        /** Steps, decisions, packages and annotations. */
        FULL,
        /** Steps only, without decisions, packages and annotations. */
        STEPS
    }

    /**
     * Options of a journey diagram.
     *
     * @param detail    how much the diagram shows
     * @param technical {@code true} to include node names and error messages (for operators only)
     * @param zone      the time zone of the times in the labels
     */
    public record Options(Detail detail, boolean technical, ZoneId zone) {

        /**
         * Validates the options.
         *
         * @param detail    how much the diagram shows
         * @param technical {@code true} to include node names and error messages (for operators only)
         * @param zone      the time zone of the times in the labels
         */
        public Options {
            Objects.requireNonNull(detail, "detail");
            Objects.requireNonNull(zone, "zone");
        }

        /**
         * All details except the technical ones, in the time zone of the JVM.
         *
         * @return the options
         */
        public static Options defaults() {
            return new Options(Detail.FULL, false, ZoneId.systemDefault());
        }
    }

    /** The item. */
    record ItemInfo(UUID id, String itemType, String businessKey, long version, String state, String currentJob,
                    Instant updatedAt, Instant nextAttemptAt, UUID packageId) {
    }

    /** A decision request about the item. */
    record DecisionInfo(UUID id, String kind, String job, Integer requiredApprovals, String group, String state,
                        int approvals) {
    }

    /** A work package the item was put into. */
    record PackageInfo(UUID id, long seq, String groupKey, String state, String job, String info, int attempts,
                       String lastError, Instant sentAt, Instant ackDueAt) {
    }

    /** An open branch of the item. */
    record BranchInfo(String job, String state, Instant updatedAt, Instant nextAttemptAt) {
    }

    /** Everything the folding needs. */
    record Context(ItemInfo item, List<JourneyEntry> entries, Map<String, String> handlers,
                   Map<UUID, DecisionInfo> decisions, Map<UUID, PackageInfo> packages, List<BranchInfo> openBranches,
                   Map<String, String> statusReasons, Instant now) {
    }

    /**
     * The journey of an item as a graph. If several versions of the item reference have the business key, the most
     * recent one.
     *
     * @param itemType    name of the item type
     * @param businessKey the business key
     * @param options     the options
     * @return the graph, or empty if there is no such item
     */
    public Optional<JourneyGraph> graph(String itemType, String businessKey, Options options) {
        return item("i.item_type = ? AND i.business_key = ?", itemType, businessKey)
                .map(item -> JourneyFolder.fold(context(item), options));
    }

    /**
     * The journey of an item as a graph.
     *
     * @param itemId  the item
     * @param options the options
     * @return the graph, or empty if there is no such item
     */
    public Optional<JourneyGraph> graph(UUID itemId, Options options) {
        return item("i.id = ?", itemId).map(item -> JourneyFolder.fold(context(item), options));
    }

    /**
     * The journey of an item as Mermaid source.
     *
     * @param itemType    name of the item type
     * @param businessKey the business key
     * @param options     the options
     * @return the diagram, or empty if there is no such item
     */
    public Optional<String> diagram(String itemType, String businessKey, Options options) {
        return graph(itemType, businessKey, options).map(JourneyGraph::mermaid);
    }

    private Optional<ItemInfo> item(String where, Object... params) {
        Object[] all = new Object[params.length + 1];
        all[0] = namespace;
        System.arraycopy(params, 0, all, 1, params.length);
        return tx.withConnection(c -> Sql.query(c, "SELECT i.id, i.item_type, i.business_key, i.ref_version, i.state,"
                + " j.name AS job, i.updated_at, i.next_attempt_at, i.work_package_id"
                + " FROM dtp_item i LEFT JOIN dtp_job j ON j.id = i.current_job_id WHERE i.namespace = ? AND " + where
                + " ORDER BY i.created_at DESC, i.ref_version DESC",
                rs -> new ItemInfo(Sql.uuid(rs, "id"), rs.getString("item_type"), rs.getString("business_key"),
                        rs.getLong("ref_version"), rs.getString("state"), rs.getString("job"),
                        Sql.instant(rs, "updated_at"), Sql.instant(rs, "next_attempt_at"),
                        Sql.uuid(rs, "work_package_id")), all))
                 .stream().findFirst();
    }

    private Context context(ItemInfo item) {
        List<JourneyEntry> entries = queries.journey(item.id());
        return tx.withConnection(c -> new Context(item, entries, handlers(c), decisions(c, item.id()),
                packages(c, item), branches(c, item.id()), statusReasons(c), clock.now(c)));
    }

    private Map<String, String> handlers(Connection c) throws SQLException {
        Map<String, String> handlers = new HashMap<>();
        Sql.query(c, "SELECT j.name, r.handler_class FROM dtp_job j JOIN dtp_job_revision r ON r.job_id = j.id"
                + " AND r.revision = j.current_revision WHERE j.namespace = ?",
                rs -> handlers.put(rs.getString("name"), rs.getString("handler_class")), namespace);
        return handlers;
    }

    private Map<UUID, DecisionInfo> decisions(Connection c, UUID itemId) throws SQLException {
        Map<UUID, DecisionInfo> decisions = new HashMap<>();
        Sql.query(c, "SELECT d.id, d.kind, j.name AS job, d.required_approvals, d.responsible_group, d.state,"
                + " (SELECT COUNT(*) FROM dtp_decision_vote v WHERE v.request_id = d.id AND v.vote = 'APPROVE')"
                + " AS approvals FROM dtp_decision_request d JOIN dtp_job j ON j.id = d.requested_by_job_id"
                + " WHERE d.namespace = ? AND d.subject_type = 'ITEM' AND d.subject_id = ?",
                rs -> decisions.put(Sql.uuid(rs, "id"), new DecisionInfo(Sql.uuid(rs, "id"), rs.getString("kind"),
                        rs.getString("job"), Sql.integer(rs, "required_approvals"), rs.getString("responsible_group"),
                        rs.getString("state"), rs.getInt("approvals"))), namespace, itemId);
        return decisions;
    }

    private Map<UUID, PackageInfo> packages(Connection c, ItemInfo item) throws SQLException {
        Map<UUID, PackageInfo> packages = new HashMap<>();
        Sql.query(c, "SELECT p.id, p.seq, p.group_key, p.state, j.name AS job, p.info, p.attempts, p.last_error,"
                + " p.sent_at, p.ack_due_at"
                + " FROM dtp_work_package p JOIN dtp_job j ON j.id = p.job_id WHERE p.id IN (SELECT e.work_package_id"
                + " FROM dtp_item_event e WHERE e.item_id = ? AND e.work_package_id IS NOT NULL) OR p.id = ?",
                rs -> packages.put(Sql.uuid(rs, "id"), new PackageInfo(Sql.uuid(rs, "id"), rs.getLong("seq"),
                        rs.getString("group_key"), rs.getString("state"), rs.getString("job"), rs.getString("info"),
                        rs.getInt("attempts"), rs.getString("last_error"), Sql.instant(rs, "sent_at"),
                        Sql.instant(rs, "ack_due_at"))), item.id(), item.packageId());
        return packages;
    }

    private List<BranchInfo> branches(Connection c, UUID itemId) throws SQLException {
        return Sql.query(c, "SELECT j.name AS job, b.state, b.updated_at, b.next_attempt_at"
                + " FROM dtp_item_branch b JOIN dtp_job j ON j.id = b.branch_job_id WHERE b.item_id = ?"
                + " AND b.state IN ('QUEUED', 'WAITING_FOR_DECISION') ORDER BY b.position",
                rs -> new BranchInfo(rs.getString("job"), rs.getString("state"), Sql.instant(rs, "updated_at"),
                        Sql.instant(rs, "next_attempt_at")), itemId);
    }

    private Map<String, String> statusReasons(Connection c) throws SQLException {
        Map<String, String> reasons = new HashMap<>();
        Sql.query(c, "SELECT step_name, status_reason FROM dtp_view_step_status WHERE namespace = ?",
                rs -> reasons.put(rs.getString("step_name"), rs.getString("status_reason")), namespace);
        return reasons;
    }
}
