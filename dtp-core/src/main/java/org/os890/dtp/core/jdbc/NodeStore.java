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
import java.util.UUID;

/** Cluster membership ({@code dtp_node}). */
public class NodeStore {

    /**
     * Registers a node.
     *
     * @param c            the connection
     * @param id           the node id
     * @param namespace    the namespace
     * @param name         a human-readable name
     * @param host         the host name
     * @param capabilities handler classes as JSON array
     * @param now          the DB time
     * @throws SQLException on DB errors
     */
    public void register(Connection c, UUID id, String namespace, String name, String host, String capabilities,
                         Instant now) throws SQLException {
        Sql.update(c, "INSERT INTO dtp_node (id, namespace, name, host, state, capabilities, started_at, heartbeat_at)"
                + " VALUES (?, ?, ?, ?, 'ACTIVE', ?, ?, ?)", id, namespace, name, host, capabilities, now, now);
    }

    /**
     * Heartbeat of a node. A node that was declared dead (e.g. after a long pause) becomes active again; its leases
     * were taken over meanwhile, so its old runs are fenced.
     *
     * @param c       the connection
     * @param id      the node id
     * @param state   ACTIVE or DRAINING
     * @param now     the DB time
     * @throws SQLException on DB errors
     */
    public void heartbeat(Connection c, UUID id, String state, Instant now) throws SQLException {
        Sql.update(c, "UPDATE dtp_node SET heartbeat_at = ?, state = ? WHERE id = ?", now, state, id);
    }

    /**
     * Sets the state of a node.
     *
     * @param c     the connection
     * @param id    the node id
     * @param state the state
     * @throws SQLException on DB errors
     */
    public void setState(Connection c, UUID id, String state) throws SQLException {
        Sql.update(c, "UPDATE dtp_node SET state = ? WHERE id = ?", state, id);
    }

    /**
     * Marks nodes without a recent heartbeat as dead.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param cutOff    heartbeats before this time are too old
     * @return number of nodes marked as dead
     * @throws SQLException on DB errors
     */
    public int markDead(Connection c, String namespace, Instant cutOff) throws SQLException {
        return Sql.update(c, "UPDATE dtp_node SET state = 'DEAD' WHERE namespace = ? AND state IN ('ACTIVE',"
                + " 'DRAINING') AND heartbeat_at < ?", namespace, cutOff);
    }
}
