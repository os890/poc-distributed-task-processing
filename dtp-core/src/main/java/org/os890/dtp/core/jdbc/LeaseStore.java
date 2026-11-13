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
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;

/**
 * Job leases (DD-1): one row per job. The fencing token is incremented on every acquire and every expiry, so a
 * node that lost its lease can't commit anymore (DD-12).
 */
public class LeaseStore {

    private final SqlDialect dialect;

    public LeaseStore(SqlDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * An expired lease.
     *
     * @param jobId the job
     * @param runId the run that held the lease
     * @param owner the node that held the lease
     * @param token the fencing token
     */
    public record ExpiredLease(UUID jobId, UUID runId, UUID owner, long token) {
    }

    /**
     * Creates the missing lease rows of a namespace.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return number of created rows
     * @throws SQLException on DB errors
     */
    public int createMissing(Connection c, String namespace) throws SQLException {
        String sql = "INSERT INTO dtp_job_lease (job_id, namespace, fencing_token) SELECT j.id, j.namespace, 0"
                + " FROM dtp_job j WHERE j.namespace = ?"
                + " AND NOT EXISTS (SELECT 1 FROM dtp_job_lease l WHERE l.job_id = j.id)";
        if (dialect.supportsOnConflictDoNothing()) {
            return Sql.update(c, sql + " ON CONFLICT (job_id) DO NOTHING", namespace);
        }
        try {
            return Sql.update(c, sql, namespace);
        } catch (SQLException e) {
            if (Sql.UNIQUE_VIOLATION.equals(e.getSQLState())) {
                return 0;
            }
            throw e;
        }
    }

    /**
     * Acquires the free lease of a job for a run.
     *
     * @param c          the connection
     * @param jobId      the job
     * @param runId      the run
     * @param node       the claiming node
     * @param leaseUntil end of the lease
     * @return the new fencing token, or empty if the lease isn't free
     * @throws SQLException on DB errors
     */
    public OptionalLong acquire(Connection c, UUID jobId, UUID runId, UUID node, Instant leaseUntil)
            throws SQLException {
        int updated = Sql.update(c, "UPDATE dtp_job_lease SET run_id = ?, owner_node_id = ?, lease_until = ?,"
                + " fencing_token = fencing_token + 1 WHERE job_id = ? AND run_id IS NULL", runId, node, leaseUntil,
                jobId);
        if (updated != 1) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(Sql.queryOne(c, "SELECT fencing_token FROM dtp_job_lease WHERE job_id = ?",
                rs -> rs.getLong(1), jobId).orElseThrow());
    }

    /**
     * Renews a lease. A lease that expired but wasn't taken over yet can still be renewed (same token).
     *
     * @param c          the connection
     * @param jobId      the job
     * @param runId      the run
     * @param token      the fencing token of the run
     * @param leaseUntil new end of the lease
     * @return {@code false} if the lease is lost
     * @throws SQLException on DB errors
     */
    public boolean renew(Connection c, UUID jobId, UUID runId, long token, Instant leaseUntil) throws SQLException {
        return Sql.update(c, "UPDATE dtp_job_lease SET lease_until = ? WHERE job_id = ? AND run_id = ?"
                + " AND fencing_token = ?", leaseUntil, jobId, runId, token) == 1;
    }

    /**
     * Fence check (DD-12): the last statement of every unit of work of a run. It locks the lease row until the
     * commit, so the lease can't be taken over between the check and the commit.
     *
     * @param c     the connection of the unit of work
     * @param jobId the job
     * @param runId the run
     * @param token the fencing token of the run
     * @return {@code false} if the run lost its lease
     * @throws SQLException on DB errors
     */
    public boolean fence(Connection c, UUID jobId, UUID runId, long token) throws SQLException {
        return Sql.update(c, "UPDATE dtp_job_lease SET fencing_token = fencing_token WHERE job_id = ? AND run_id = ?"
                + " AND fencing_token = ?", jobId, runId, token) == 1;
    }

    /**
     * Releases a lease when its run finishes. Also a fence check.
     *
     * @param c     the connection
     * @param jobId the job
     * @param runId the run
     * @param token the fencing token of the run
     * @return {@code false} if the run lost its lease
     * @throws SQLException on DB errors
     */
    public boolean release(Connection c, UUID jobId, UUID runId, long token) throws SQLException {
        return Sql.update(c, "UPDATE dtp_job_lease SET run_id = NULL, owner_node_id = NULL, lease_until = NULL"
                + " WHERE job_id = ? AND run_id = ? AND fencing_token = ?", jobId, runId, token) == 1;
    }

    /**
     * Expired leases of a namespace.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param now       the DB time
     * @return the expired leases
     * @throws SQLException on DB errors
     */
    public List<ExpiredLease> findExpired(Connection c, String namespace, Instant now) throws SQLException {
        return Sql.query(c, "SELECT job_id, run_id, owner_node_id, fencing_token FROM dtp_job_lease"
                        + " WHERE namespace = ? AND run_id IS NOT NULL AND lease_until < ?",
                rs -> new ExpiredLease(Sql.uuid(rs, "job_id"), Sql.uuid(rs, "run_id"), Sql.uuid(rs, "owner_node_id"),
                        rs.getLong("fencing_token")), namespace, now);
    }

    /**
     * Takes over an expired lease: frees it and increments the token, so the old owner is fenced.
     *
     * @param c     the connection
     * @param lease the expired lease
     * @param now   the DB time
     * @return {@code false} if the lease was renewed or taken over meanwhile
     * @throws SQLException on DB errors
     */
    public boolean expire(Connection c, ExpiredLease lease, Instant now) throws SQLException {
        return Sql.update(c, "UPDATE dtp_job_lease SET run_id = NULL, owner_node_id = NULL, lease_until = NULL,"
                + " fencing_token = fencing_token + 1 WHERE job_id = ? AND fencing_token = ? AND lease_until < ?",
                lease.jobId(), lease.token(), now) == 1;
    }
}
