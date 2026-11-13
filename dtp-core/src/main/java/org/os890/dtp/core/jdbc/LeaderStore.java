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
import java.util.OptionalLong;
import java.util.UUID;

/**
 * JDBC leader election ({@code dtp_leader}): one row per namespace and role, every new election increments the term.
 */
public class LeaderStore {

    private final SqlDialect dialect;

    public LeaderStore(SqlDialect dialect) {
        this.dialect = dialect;
    }

    /**
     * Creates the row of a role if it is missing.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param role      the role, e.g. {@code housekeeping}
     * @throws SQLException on DB errors
     */
    public void ensureRole(Connection c, String namespace, String role) throws SQLException {
        String sql = "INSERT INTO dtp_leader (namespace, role, term) SELECT ?, ?, 0"
                + " WHERE NOT EXISTS (SELECT 1 FROM dtp_leader WHERE namespace = ? AND role = ?)";
        if (dialect.supportsOnConflictDoNothing()) {
            Sql.update(c, sql + " ON CONFLICT (namespace, role) DO NOTHING", namespace, role, namespace, role);
            return;
        }
        try {
            Sql.update(c, sql, namespace, role, namespace, role);
        } catch (SQLException e) {
            if (!Sql.UNIQUE_VIOLATION.equals(e.getSQLState())) {
                throw e;
            }
        }
    }

    /**
     * Tries to become the leader of a role.
     *
     * @param c          the connection
     * @param namespace  the namespace
     * @param role       the role
     * @param node       the candidate
     * @param leaseUntil end of the leadership lease
     * @param now        the DB time
     * @return the new term, or empty if another node leads
     * @throws SQLException on DB errors
     */
    public OptionalLong tryAcquire(Connection c, String namespace, String role, UUID node, Instant leaseUntil,
                                   Instant now) throws SQLException {
        int updated = Sql.update(c, "UPDATE dtp_leader SET holder_node_id = ?, lease_until = ?, term = term + 1,"
                + " acquired_at = ? WHERE namespace = ? AND role = ? AND (holder_node_id IS NULL OR lease_until < ?)",
                node, leaseUntil, now, namespace, role, now);
        if (updated != 1) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(Sql.queryOne(c, "SELECT term FROM dtp_leader WHERE namespace = ? AND role = ?",
                rs -> rs.getLong(1), namespace, role).orElseThrow());
    }

    /**
     * Renews the leadership.
     *
     * @param c          the connection
     * @param namespace  the namespace
     * @param role       the role
     * @param node       the leader
     * @param term       its term
     * @param leaseUntil new end of the leadership lease
     * @return {@code false} if the leadership is lost
     * @throws SQLException on DB errors
     */
    public boolean renew(Connection c, String namespace, String role, UUID node, long term, Instant leaseUntil)
            throws SQLException {
        return Sql.update(c, "UPDATE dtp_leader SET lease_until = ? WHERE namespace = ? AND role = ?"
                + " AND holder_node_id = ? AND term = ?", leaseUntil, namespace, role, node, term) == 1;
    }

    /**
     * Resigns, e.g. on shutdown, so another node is elected without waiting for the lease to expire.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param role      the role
     * @param node      the leader
     * @param term      its term
     * @throws SQLException on DB errors
     */
    public void resign(Connection c, String namespace, String role, UUID node, long term) throws SQLException {
        Sql.update(c, "UPDATE dtp_leader SET holder_node_id = NULL, lease_until = NULL WHERE namespace = ?"
                + " AND role = ? AND holder_node_id = ? AND term = ?", namespace, role, node, term);
    }
}
