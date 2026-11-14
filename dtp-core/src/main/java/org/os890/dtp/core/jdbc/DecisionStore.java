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

import org.os890.dtp.core.domain.DecisionKind;
import org.os890.dtp.core.domain.SubjectType;

/** Decision requests and votes ({@code dtp_decision_request}, {@code dtp_decision_vote}). */
public class DecisionStore {

    private static final String SELECT = "SELECT * FROM dtp_decision_request";

    // a disabled fallback job doesn't block the others, its overdue fallbacks are planned once it is enabled again
    private static final String DUE_FALLBACKS = " JOIN dtp_job j ON j.id = r.fallback_job_id AND j.enabled = TRUE"
            + " WHERE r.state = 'OPEN' AND r.fallback_due_at <= ? AND r.fallback_run_id IS NULL AND r.namespace = ?";

    /**
     * A decision request.
     *
     * @param id                the id
     * @param namespace         the namespace
     * @param kind              CHOICE or APPROVAL
     * @param subjectType       what waits
     * @param subjectId         the item or run
     * @param requestedByJobId  the job the decided item goes back to
     * @param runId             the run that requested it, or {@code null}
     * @param reason            why
     * @param options           JSON array of {@code {name, label}}
     * @param optionType        enum class of custom options, or {@code null}
     * @param requiredApprovals quorum of an approval
     * @param responsibleGroup  the responsible team, or {@code null}
     * @param state             OPEN or DECIDED
     * @param decidedOption     the decision
     * @param decidedBy         who decided
     * @param decidedAt         when
     * @param comment           comment of the decision
     * @param createdAt         when it was requested
     * @param fallbackJobId     job of the fallback run, or {@code null}
     * @param fallbackDueAt     when the fallback run is due, or {@code null}
     * @param fallbackRunId     the planned fallback run, or {@code null}
     */
    public record RequestRow(UUID id, String namespace, DecisionKind kind, SubjectType subjectType, UUID subjectId,
                             UUID requestedByJobId, UUID runId, String reason, String options, String optionType,
                             Integer requiredApprovals, String responsibleGroup, String state, String decidedOption,
                             String decidedBy, Instant decidedAt, String comment, Instant createdAt,
                             UUID fallbackJobId, Instant fallbackDueAt, UUID fallbackRunId) {

        /** {@code true} while nobody decided. */
        public boolean isOpen() {
            return "OPEN".equals(state);
        }
    }

    /**
     * A vote.
     *
     * @param voter   the user
     * @param vote    APPROVE or REJECT
     * @param comment the comment
     * @param votedAt when
     */
    public record VoteRow(String voter, String vote, String comment, Instant votedAt) {
    }

    /**
     * Inserts a request (state OPEN).
     *
     * @param c       the connection
     * @param request the request
     * @throws SQLException on DB errors
     */
    public void insert(Connection c, RequestRow request) throws SQLException {
        Sql.update(c, "INSERT INTO dtp_decision_request (id, namespace, kind, subject_type, subject_id,"
                + " requested_by_job_id, run_id, reason, options, option_type, required_approvals, responsible_group,"
                + " state, created_at, fallback_job_id, fallback_due_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?)", request.id(),
                request.namespace(), request.kind(), request.subjectType(), request.subjectId(),
                request.requestedByJobId(), request.runId(), request.reason(), request.options(),
                request.optionType(), request.requiredApprovals(), request.responsibleGroup(), request.createdAt(),
                request.fallbackJobId(), request.fallbackDueAt());
    }

    /**
     * A request.
     *
     * @param c  the connection
     * @param id the request
     * @return the request, if it exists
     * @throws SQLException on DB errors
     */
    public Optional<RequestRow> load(Connection c, UUID id) throws SQLException {
        return Sql.queryOne(c, SELECT + " WHERE id = ?", DecisionStore::map, id);
    }

    /**
     * Locks a request, to serialize decisions and votes.
     *
     * @param c  the connection
     * @param id the request
     * @return the request, if it exists
     * @throws SQLException on DB errors
     */
    public Optional<RequestRow> lock(Connection c, UUID id) throws SQLException {
        return Sql.queryOne(c, SELECT + " WHERE id = ? FOR UPDATE", DecisionStore::map, id);
    }

    /**
     * The open requests of a namespace, oldest first.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the requests
     * @throws SQLException on DB errors
     */
    public List<RequestRow> open(Connection c, String namespace) throws SQLException {
        return Sql.query(c, SELECT + " WHERE namespace = ? AND state = 'OPEN' ORDER BY created_at, id",
                DecisionStore::map, namespace);
    }

    /**
     * The requests of a subject, newest first.
     *
     * @param c         the connection
     * @param subjectId the item or run
     * @return the requests
     * @throws SQLException on DB errors
     */
    public List<RequestRow> ofSubject(Connection c, UUID subjectId) throws SQLException {
        return Sql.query(c, SELECT + " WHERE subject_id = ? ORDER BY created_at DESC, id", DecisionStore::map,
                subjectId);
    }

    /**
     * Records the decision.
     *
     * @param c       the connection
     * @param id      the request
     * @param option  the decided option
     * @param by      who
     * @param comment the comment
     * @param now     the DB time
     * @return {@code false} if it was decided already
     * @throws SQLException on DB errors
     */
    public boolean decide(Connection c, UUID id, String option, String by, String comment, Instant now)
            throws SQLException {
        return Sql.update(c, "UPDATE dtp_decision_request SET state = 'DECIDED', decided_option = ?, decided_by = ?,"
                + " decided_at = ?, decision_comment = ? WHERE id = ? AND state = 'OPEN'", option, by, now, comment,
                id) == 1;
    }

    /**
     * Open requests whose fallback run is due and not planned yet, the longest overdue first. Only requests whose
     * fallback job is enabled.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param now       the DB time
     * @param limit     at most this many
     * @return the request ids
     * @throws SQLException on DB errors
     */
    public List<UUID> dueFallbacks(Connection c, String namespace, Instant now, int limit) throws SQLException {
        return Sql.query(c, "SELECT r.id FROM dtp_decision_request r" + DUE_FALLBACKS
                + " ORDER BY r.fallback_due_at, r.id LIMIT ?", rs -> Sql.uuid(rs, "id"), now, namespace, limit);
    }

    /**
     * Records the planned fallback run, only once.
     *
     * @param c     the connection
     * @param id    the request
     * @param runId the run
     * @return {@code false} if it was decided or planned meanwhile
     * @throws SQLException on DB errors
     */
    public boolean fallbackPlanned(Connection c, UUID id, UUID runId) throws SQLException {
        return Sql.update(c, "UPDATE dtp_decision_request SET fallback_run_id = ? WHERE id = ? AND state = 'OPEN'"
                + " AND fallback_run_id IS NULL", runId, id) == 1;
    }

    /**
     * Records a vote.
     *
     * @param c         the connection
     * @param requestId the request
     * @param vote      the vote
     * @throws SQLException on DB errors, e.g. a second vote of the same user
     */
    public void insertVote(Connection c, UUID requestId, VoteRow vote) throws SQLException {
        Sql.update(c, "INSERT INTO dtp_decision_vote (request_id, voter, vote, vote_comment, voted_at)"
                + " VALUES (?, ?, ?, ?, ?)", requestId, vote.voter(), vote.vote(), vote.comment(), vote.votedAt());
    }

    /**
     * The votes of a request, oldest first.
     *
     * @param c         the connection
     * @param requestId the request
     * @return the votes
     * @throws SQLException on DB errors
     */
    public List<VoteRow> votes(Connection c, UUID requestId) throws SQLException {
        return Sql.query(c, "SELECT * FROM dtp_decision_vote WHERE request_id = ? ORDER BY voted_at, voter",
                rs -> new VoteRow(rs.getString("voter"), rs.getString("vote"), rs.getString("vote_comment"),
                        Sql.instant(rs, "voted_at")), requestId);
    }

    private static RequestRow map(ResultSet rs) throws SQLException {
        return new RequestRow(Sql.uuid(rs, "id"), rs.getString("namespace"),
                DecisionKind.valueOf(rs.getString("kind")), SubjectType.valueOf(rs.getString("subject_type")),
                Sql.uuid(rs, "subject_id"), Sql.uuid(rs, "requested_by_job_id"), Sql.uuid(rs, "run_id"),
                rs.getString("reason"), rs.getString("options"), rs.getString("option_type"),
                Sql.integer(rs, "required_approvals"), rs.getString("responsible_group"), rs.getString("state"),
                rs.getString("decided_option"), rs.getString("decided_by"), Sql.instant(rs, "decided_at"),
                rs.getString("decision_comment"), Sql.instant(rs, "created_at"), Sql.uuid(rs, "fallback_job_id"),
                Sql.instant(rs, "fallback_due_at"), Sql.uuid(rs, "fallback_run_id"));
    }
}
