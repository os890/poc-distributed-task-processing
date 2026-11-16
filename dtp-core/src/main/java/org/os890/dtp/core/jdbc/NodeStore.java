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
import java.util.UUID;

/** Cluster membership ({@code dtp_node}). */
public class NodeStore {

    /**
     * A node to register.
     *
     * @param id           the node id
     * @param namespace    the namespace
     * @param name         a human-readable name
     * @param host         the host name
     * @param color        the color of its release (red/green), or {@code null}
     * @param appVersion   the version of the application, or {@code null}
     * @param state        {@code ACTIVE}, or {@code STANDBY} while another color is active
     * @param capabilities handler classes as JSON array
     * @param flowVersions the flow versions it runs as JSON object (flow name to hash)
     */
    public record Registration(UUID id, String namespace, String name, String host, String color, String appVersion,
                               String state, String capabilities, String flowVersions) {
    }

    /**
     * Registers a node.
     *
     * @param c    the connection
     * @param node the node
     * @param now  the DB time
     * @throws SQLException on DB errors
     */
    public void register(Connection c, Registration node, Instant now) throws SQLException {
        Sql.update(c, "INSERT INTO dtp_node (id, namespace, name, host, color, app_version, state, capabilities,"
                + " flow_versions, started_at, heartbeat_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", node.id(),
                node.namespace(), node.name(), node.host(), node.color(), node.appVersion(), node.state(),
                node.capabilities(), node.flowVersions(), now, now);
    }

    /**
     * Heartbeat of a node. A node that was declared dead (e.g. after a long pause) becomes active again; its leases
     * were taken over meanwhile, so its old runs are fenced.
     *
     * @param c       the connection
     * @param id      the node id
     * @param state   ACTIVE, STANDBY or DRAINING
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
                + " 'STANDBY', 'DRAINING') AND heartbeat_at < ?", namespace, cutOff);
    }

    /**
     * The capabilities (JSON arrays of step classes) of the active nodes of a namespace.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return one JSON array per node
     * @throws SQLException on DB errors
     */
    public List<String> activeCapabilities(Connection c, String namespace) throws SQLException {
        return Sql.query(c, "SELECT capabilities FROM dtp_node WHERE namespace = ? AND state = 'ACTIVE'",
                rs -> rs.getString("capabilities"), namespace);
    }

    /**
     * A live node of a color.
     *
     * @param id           the node id
     * @param name         its name
     * @param state        {@code ACTIVE} or {@code STANDBY}
     * @param capabilities its handler classes as JSON array
     * @param flowVersions its flow versions as JSON object, or {@code null}
     */
    public record ColorNode(UUID id, String name, String state, String capabilities, String flowVersions) {
    }

    /**
     * The live nodes of a color: active or on standby, with a recent heartbeat (draining nodes don't count).
     *
     * @param c          the connection
     * @param namespace  the namespace
     * @param color      the color
     * @param aliveAfter heartbeats before this time are too old
     * @return the nodes
     * @throws SQLException on DB errors
     */
    public List<ColorNode> liveNodes(Connection c, String namespace, String color, Instant aliveAfter)
            throws SQLException {
        return Sql.query(c, "SELECT id, name, state, capabilities, flow_versions FROM dtp_node WHERE namespace = ?"
                + " AND color = ? AND state IN ('ACTIVE', 'STANDBY') AND heartbeat_at >= ? ORDER BY started_at",
                rs -> new ColorNode(Sql.uuid(rs, "id"), rs.getString("name"), rs.getString("state"),
                        rs.getString("capabilities"), rs.getString("flow_versions")), namespace, color, aliveAfter);
    }

    /**
     * Applies a switch of the active color to the states of the nodes at once (their heartbeats keep them): nodes of
     * the active color are {@code ACTIVE}, all others {@code STANDBY}; without an active color all are {@code ACTIVE}.
     * Draining, left and dead nodes keep their state.
     *
     * @param c           the connection
     * @param namespace   the namespace
     * @param activeColor the active color, or {@code null}
     * @return number of nodes that are active now
     * @throws SQLException on DB errors
     */
    public int applyActiveColor(Connection c, String namespace, String activeColor) throws SQLException {
        if (activeColor == null) {
            return Sql.update(c, "UPDATE dtp_node SET state = 'ACTIVE' WHERE namespace = ? AND state IN ('ACTIVE',"
                    + " 'STANDBY')", namespace);
        }
        Sql.update(c, "UPDATE dtp_node SET state = 'STANDBY' WHERE namespace = ? AND state = 'ACTIVE'"
                + " AND (color IS NULL OR color <> ?)", namespace, activeColor);
        return Sql.update(c, "UPDATE dtp_node SET state = 'ACTIVE' WHERE namespace = ? AND state IN ('ACTIVE',"
                + " 'STANDBY') AND color = ?", namespace, activeColor);
    }
}
