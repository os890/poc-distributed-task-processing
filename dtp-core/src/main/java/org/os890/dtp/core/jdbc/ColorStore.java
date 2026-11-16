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
import java.util.Optional;
import java.util.UUID;

/** The active color of a namespace ({@code dtp_namespace_control}) and its switches ({@code dtp_color_switch}). */
public class ColorStore {

    /**
     * The current switch of a namespace.
     *
     * @param activeColor the active color, or {@code null}: all colors claim
     * @param switchedAt  when
     * @param switchedBy  who
     * @param reason      why, or {@code null}
     * @param unavailableSince since when the active color has no live node, or {@code null} (slice 24)
     */
    public record Control(String activeColor, Instant switchedAt, String switchedBy, String reason,
                          Instant unavailableSince) {

        /**
         * A new switch.
         *
         * @param activeColor the active color, or {@code null}
         * @param switchedAt  when
         * @param switchedBy  who
         * @param reason      why, or {@code null}
         */
        public Control(String activeColor, Instant switchedAt, String switchedBy, String reason) {
            this(activeColor, switchedAt, switchedBy, reason, null);
        }
    }

    /**
     * A switch of the active color.
     *
     * @param id             the id
     * @param previousColor  the color active before, or {@code null}
     * @param activeColor    the color active since, or {@code null}
     * @param switchedAt     when
     * @param switchedBy     who
     * @param reason         why, or {@code null}
     * @param activatedFlows the flow versions it activated as JSON array, or {@code null}
     */
    public record SwitchRow(UUID id, String previousColor, String activeColor, Instant switchedAt, String switchedBy,
                            String reason, String activatedFlows) {
    }

    /**
     * The active color, read on every heartbeat and claim round of a node.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the active color, or empty if all colors claim
     * @throws SQLException on DB errors
     */
    public Optional<String> activeColor(Connection c, String namespace) throws SQLException {
        return control(c, namespace, false).map(Control::activeColor);
    }

    /**
     * The current switch.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param lock      lock the row ({@code FOR UPDATE}), so concurrent switches run one after the other
     * @return the switch, or empty if the namespace never had an active color
     * @throws SQLException on DB errors
     */
    public Optional<Control> control(Connection c, String namespace, boolean lock) throws SQLException {
        return Sql.queryOne(c, "SELECT active_color, switched_at, switched_by, reason, unavailable_since"
                + " FROM dtp_namespace_control WHERE namespace = ?" + (lock ? " FOR UPDATE" : ""),
                rs -> new Control(rs.getString("active_color"), Sql.instant(rs, "switched_at"),
                        rs.getString("switched_by"), rs.getString("reason"), Sql.instant(rs, "unavailable_since")),
                namespace);
    }

    /**
     * Stores a switch: the current row and the history.
     *
     * @param c              the connection
     * @param namespace      the namespace
     * @param previousColor  the color active before
     * @param control        the new switch
     * @param activatedFlows the flow versions activated by it as JSON array
     * @throws SQLException on DB errors (a unique violation if the first switch of the namespace raced another)
     */
    public void switchTo(Connection c, String namespace, String previousColor, Control control, String activatedFlows)
            throws SQLException {
        int updated = Sql.update(c, "UPDATE dtp_namespace_control SET active_color = ?, switched_at = ?,"
                + " switched_by = ?, reason = ?, unavailable_since = NULL WHERE namespace = ?", control.activeColor(),
                control.switchedAt(), control.switchedBy(), control.reason(), namespace);
        if (updated == 0) {
            Sql.update(c, "INSERT INTO dtp_namespace_control (namespace, active_color, switched_at, switched_by,"
                    + " reason) VALUES (?, ?, ?, ?, ?)", namespace, control.activeColor(), control.switchedAt(),
                    control.switchedBy(), control.reason());
        }
        Sql.update(c, "INSERT INTO dtp_color_switch (id, namespace, previous_color, active_color, switched_at,"
                + " switched_by, reason, activated_flows) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", UUID.randomUUID(),
                namespace, previousColor, control.activeColor(), control.switchedAt(), control.switchedBy(),
                control.reason(), activatedFlows);
    }

    /**
     * Records that the active color has no live node, once for the cluster: only the first node that notices it
     * changes the row.
     *
     * @param c           the connection
     * @param namespace   the namespace
     * @param activeColor the active color as the node read it
     * @param since       the DB time
     * @return {@code true} if this call recorded it
     * @throws SQLException on DB errors
     */
    public boolean markUnavailable(Connection c, String namespace, String activeColor, Instant since)
            throws SQLException {
        return Sql.update(c, "UPDATE dtp_namespace_control SET unavailable_since = ? WHERE namespace = ?"
                + " AND active_color = ? AND unavailable_since IS NULL", since, namespace, activeColor) == 1;
    }

    /**
     * A node of the active color is live again.
     *
     * @param c           the connection
     * @param namespace   the namespace
     * @param activeColor the color of the node
     * @return {@code true} if the color was marked as unavailable
     * @throws SQLException on DB errors
     */
    public boolean markAvailable(Connection c, String namespace, String activeColor) throws SQLException {
        return Sql.update(c, "UPDATE dtp_namespace_control SET unavailable_since = NULL WHERE namespace = ?"
                + " AND active_color = ? AND unavailable_since IS NOT NULL", namespace, activeColor) == 1;
    }

    /**
     * The latest switches, newest first.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param limit     maximum number
     * @return the switches
     * @throws SQLException on DB errors
     */
    public List<SwitchRow> switches(Connection c, String namespace, int limit) throws SQLException {
        return Sql.query(c, "SELECT id, previous_color, active_color, switched_at, switched_by, reason,"
                + " activated_flows FROM dtp_color_switch WHERE namespace = ? ORDER BY switched_at DESC, id LIMIT ?",
                rs -> new SwitchRow(Sql.uuid(rs, "id"), rs.getString("previous_color"),
                        rs.getString("active_color"), Sql.instant(rs, "switched_at"), rs.getString("switched_by"),
                        rs.getString("reason"), rs.getString("activated_flows")), namespace, limit);
    }
}
