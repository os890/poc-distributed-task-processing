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
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.os890.dtp.core.domain.OccurrenceState;
import org.os890.dtp.core.domain.WindowDefinition;

/**
 * Collection windows and their occurrences ({@code dtp_window}, {@code dtp_window_paused_job},
 * {@code dtp_window_occurrence}, docs/design/04-lifecycles.md §4.6). Every state change is conditional on the expected
 * state, so a decision and the coordinator never overwrite each other.
 */
public class WindowStore {

    private static final String COLUMNS = "o.id, o.window_id, w.name AS window_name, o.seq, o.scheduled_at, o.state,"
            + " o.triggered_by, o.force_open, o.from_ts, o.cut_off, o.collector_run_id, o.collector_attempts,"
            + " o.state_changed_at";

    private static final String ACTIVE = "('QUIESCING', 'OPEN', 'WAITING_FOR_DECISION')";

    /**
     * An occurrence of a window.
     *
     * @param id                the occurrence
     * @param windowId          its window
     * @param window            the window's name
     * @param seq               its number within the window, from 1
     * @param scheduledAt       the slot of the schedule, or the time an operator started it
     * @param state             its state
     * @param triggeredBy       the operator who started it, {@code null} for the schedule
     * @param forceOpen         opens without waiting for the paused jobs (decision {@code FORCE_OPEN})
     * @param from              the cut-off of the last closed occurrence, {@code null} for the first
     * @param cutOff            its cut-off, once open
     * @param collectorRunId    the current collector run, {@code null} before it's planned
     * @param collectorAttempts number of collector runs planned
     * @param stateChangedAt    when the state changed last, the start of the quiesce timeout
     */
    public record OccurrenceRow(UUID id, UUID windowId, String window, long seq, Instant scheduledAt,
                                OccurrenceState state, String triggeredBy, boolean forceOpen, Instant from,
                                Instant cutOff, UUID collectorRunId, int collectorAttempts, Instant stateChangedAt) {
    }

    /**
     * The windows of a namespace with their paused jobs.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the windows
     * @throws SQLException on DB errors
     */
    public List<WindowDefinition> loadAll(Connection c, String namespace) throws SQLException {
        Map<UUID, Set<UUID>> paused = new HashMap<>();
        Sql.query(c, "SELECT p.window_id, p.job_id FROM dtp_window_paused_job p JOIN dtp_window w ON w.id = p.window_id"
                + " WHERE w.namespace = ?", rs -> paused.computeIfAbsent(Sql.uuid(rs, "window_id"),
                        k -> new HashSet<>()).add(Sql.uuid(rs, "job_id")), namespace);
        return Sql.query(c, "SELECT w.id, w.name, w.schedule_cron, COALESCE(k.zone, w.schedule_zone) AS zone,"
                + " w.calendar_id, w.collector_job_id, w.quiesce_timeout, w.enabled, w.created_at FROM dtp_window w"
                + " LEFT JOIN dtp_calendar k ON k.id = w.calendar_id WHERE w.namespace = ? ORDER BY w.name",
                rs -> new WindowDefinition(Sql.uuid(rs, "id"), rs.getString("name"), rs.getString("schedule_cron"),
                        ZoneId.of(rs.getString("zone")), Sql.uuid(rs, "calendar_id"),
                        Sql.uuid(rs, "collector_job_id"), paused.getOrDefault(Sql.uuid(rs, "id"), Set.of()),
                        Duration.parse(rs.getString("quiesce_timeout")), rs.getBoolean("enabled"),
                        Sql.instant(rs, "created_at")), namespace);
    }

    /**
     * Locks a window, so planning an occurrence and starting one by an operator don't overlap.
     *
     * @param c        the connection
     * @param windowId the window
     * @throws SQLException on DB errors
     */
    public void lockWindow(Connection c, UUID windowId) throws SQLException {
        Sql.queryOne(c, "SELECT id FROM dtp_window WHERE id = ? FOR UPDATE", rs -> rs.getObject(1), windowId);
    }

    /**
     * The active occurrence of a window (at most one: occurrences are created under the window's lock).
     *
     * @param c        the connection
     * @param windowId the window
     * @return the occurrence that is quiescing, open or waiting for a decision
     * @throws SQLException on DB errors
     */
    public Optional<OccurrenceRow> active(Connection c, UUID windowId) throws SQLException {
        return Sql.queryOne(c, "SELECT " + COLUMNS + " FROM dtp_window_occurrence o JOIN dtp_window w"
                + " ON w.id = o.window_id WHERE o.window_id = ? AND o.state IN " + ACTIVE, WindowStore::map, windowId);
    }

    /**
     * An occurrence.
     *
     * @param c  the connection
     * @param id the occurrence
     * @return the occurrence, if it exists
     * @throws SQLException on DB errors
     */
    public Optional<OccurrenceRow> load(Connection c, UUID id) throws SQLException {
        return Sql.queryOne(c, "SELECT " + COLUMNS + " FROM dtp_window_occurrence o JOIN dtp_window w"
                + " ON w.id = o.window_id WHERE o.id = ?", WindowStore::map, id);
    }

    /**
     * The latest slot an occurrence was created for.
     *
     * @param c        the connection
     * @param windowId the window
     * @return the latest {@code scheduled_at}, if any
     * @throws SQLException on DB errors
     */
    public Optional<Instant> lastScheduledAt(Connection c, UUID windowId) throws SQLException {
        return Sql.queryOne(c, "SELECT MAX(scheduled_at) AS at FROM dtp_window_occurrence WHERE window_id = ?",
                rs -> Sql.instant(rs, "at"), windowId);
    }

    /**
     * Creates an occurrence that starts quiescing, numbered after the last one. Call it with the window locked.
     *
     * @param c           the connection
     * @param windowId    the window
     * @param scheduledAt the slot of the schedule, or now for an operator
     * @param triggeredBy the operator, {@code null} for the schedule
     * @param now         the DB time
     * @return the new occurrence's id, empty if the slot has an occurrence already
     * @throws SQLException on DB errors
     */
    public Optional<UUID> startQuiescing(Connection c, UUID windowId, Instant scheduledAt, String triggeredBy,
                                         Instant now) throws SQLException {
        if (Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_window_occurrence WHERE window_id = ? AND scheduled_at = ?",
                rs -> rs.getInt(1), windowId, scheduledAt).orElse(0) > 0) {
            return Optional.empty();
        }
        long seq = Sql.queryOne(c, "SELECT COALESCE(MAX(seq), 0) + 1 FROM dtp_window_occurrence WHERE window_id = ?",
                rs -> rs.getLong(1), windowId).orElse(1L);
        UUID id = UUID.randomUUID();
        Sql.update(c, "INSERT INTO dtp_window_occurrence (id, window_id, seq, scheduled_at, state, triggered_by,"
                + " state_changed_at, created_at) VALUES (?, ?, ?, ?, 'QUIESCING', ?, ?, ?)", id, windowId, seq,
                scheduledAt, triggeredBy, now, now);
        return Optional.of(id);
    }

    /**
     * Opens a quiescing occurrence: the cut-off is recorded, {@code from} is the cut-off of the last closed one.
     *
     * @param c      the connection
     * @param id     the occurrence
     * @param cutOff the cut-off (the DB time)
     * @return {@code true} if it was quiescing
     * @throws SQLException on DB errors
     */
    public boolean open(Connection c, UUID id, Instant cutOff) throws SQLException {
        Instant from = Sql.queryOne(c, "SELECT MAX(p.cut_off) AS cut_off FROM dtp_window_occurrence p"
                + " JOIN dtp_window_occurrence o ON o.window_id = p.window_id WHERE o.id = ? AND p.state = 'CLOSED'",
                rs -> Sql.instant(rs, "cut_off"), id).orElse(null);
        return Sql.update(c, "UPDATE dtp_window_occurrence SET state = 'OPEN', from_ts = ?, cut_off = ?,"
                + " state_changed_at = ? WHERE id = ? AND state = 'QUIESCING'", from, cutOff, cutOff, id) == 1;
    }

    /**
     * Records the collector run planned for an open occurrence.
     *
     * @param c     the connection
     * @param id    the occurrence
     * @param runId the collector run
     * @return {@code true} if the occurrence is open
     * @throws SQLException on DB errors
     */
    public boolean collectorPlanned(Connection c, UUID id, UUID runId) throws SQLException {
        return Sql.update(c, "UPDATE dtp_window_occurrence SET collector_run_id = ?,"
                + " collector_attempts = collector_attempts + 1 WHERE id = ? AND state = 'OPEN'", runId, id) == 1;
    }

    /**
     * Changes the state of an occurrence if it is in the expected one.
     *
     * @param c        the connection
     * @param id       the occurrence
     * @param expected the expected state
     * @param next     the new state
     * @param now      the DB time
     * @return {@code true} if it was in the expected state
     * @throws SQLException on DB errors
     */
    public boolean transition(Connection c, UUID id, OccurrenceState expected, OccurrenceState next, Instant now)
            throws SQLException {
        return Sql.update(c, "UPDATE dtp_window_occurrence SET state = ?, state_changed_at = ? WHERE id = ?"
                + " AND state = ?", next, now, id, expected) == 1;
    }

    /**
     * Applies a decision on an occurrence waiting for one.
     *
     * @param c              the connection
     * @param id             the occurrence
     * @param next           QUIESCING (keep waiting, force open), OPEN (a new collector run) or SKIPPED
     * @param forceOpen      opens on the next cycle without waiting for the paused jobs
     * @param clearCollector a new collector run is planned on the next cycle
     * @param now            the DB time
     * @return {@code true} if it waited for a decision
     * @throws SQLException on DB errors
     */
    public boolean decided(Connection c, UUID id, OccurrenceState next, boolean forceOpen, boolean clearCollector,
                           Instant now) throws SQLException {
        return Sql.update(c, "UPDATE dtp_window_occurrence SET state = ?, force_open = ?, state_changed_at = ?,"
                + " collector_run_id = " + (clearCollector ? "NULL" : "collector_run_id")
                + " WHERE id = ? AND state = 'WAITING_FOR_DECISION'", next, forceOpen, now, id) == 1;
    }

    /**
     * Number of runs of the given jobs that hold their job's lease (running, stopping, or taken over soon).
     *
     * @param c      the connection
     * @param jobIds the jobs
     * @return the number of held leases
     * @throws SQLException on DB errors
     */
    public int activeRuns(Connection c, Set<UUID> jobIds) throws SQLException {
        if (jobIds.isEmpty()) {
            return 0;
        }
        String in = String.join(", ", jobIds.stream().map(j -> "?").toList());
        return Sql.queryOne(c, "SELECT COUNT(*) FROM dtp_job_lease WHERE run_id IS NOT NULL AND job_id IN (" + in
                + ")", rs -> rs.getInt(1), jobIds.toArray()).orElse(0);
    }

    /**
     * Asks the running runs of the given jobs to stop softly (stop reason {@code WINDOW}).
     *
     * @param c      the connection
     * @param jobIds the paused jobs
     * @return the number of runs asked to stop
     * @throws SQLException on DB errors
     */
    public int requestStops(Connection c, Set<UUID> jobIds) throws SQLException {
        if (jobIds.isEmpty()) {
            return 0;
        }
        String in = String.join(", ", jobIds.stream().map(j -> "?").toList());
        return Sql.update(c, "UPDATE dtp_run SET state = 'STOPPING', stop_reason = 'WINDOW' WHERE state = 'RUNNING'"
                + " AND job_id IN (" + in + ")", jobIds.toArray());
    }

    /**
     * Marks a run as the collector run of an occurrence.
     *
     * @param c            the connection
     * @param runId        the run
     * @param occurrenceId the occurrence
     * @throws SQLException on DB errors
     */
    public void assignRun(Connection c, UUID runId, UUID occurrenceId) throws SQLException {
        Sql.update(c, "UPDATE dtp_run SET window_occurrence_id = ? WHERE id = ?", occurrenceId, runId);
    }

    /**
     * The occurrence a run collects for.
     *
     * @param c     the connection
     * @param runId the run
     * @return the occurrence, if the run is a collector run
     * @throws SQLException on DB errors
     */
    public Optional<OccurrenceRow> ofRun(Connection c, UUID runId) throws SQLException {
        return Sql.queryOne(c, "SELECT " + COLUMNS + " FROM dtp_run r JOIN dtp_window_occurrence o"
                + " ON o.id = r.window_occurrence_id JOIN dtp_window w ON w.id = o.window_id WHERE r.id = ?",
                WindowStore::map, runId);
    }

    /**
     * The latest occurrences of a namespace.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @param limit     at most this many
     * @return the occurrences, newest first
     * @throws SQLException on DB errors
     */
    public List<OccurrenceRow> latest(Connection c, String namespace, int limit) throws SQLException {
        return Sql.query(c, "SELECT " + COLUMNS + " FROM dtp_window_occurrence o JOIN dtp_window w"
                + " ON w.id = o.window_id WHERE w.namespace = ? ORDER BY o.created_at DESC, o.seq DESC LIMIT ?",
                WindowStore::map, namespace, limit);
    }

    private static OccurrenceRow map(ResultSet rs) throws SQLException {
        return new OccurrenceRow(Sql.uuid(rs, "id"), Sql.uuid(rs, "window_id"), rs.getString("window_name"),
                rs.getLong("seq"), Sql.instant(rs, "scheduled_at"), OccurrenceState.valueOf(rs.getString("state")),
                rs.getString("triggered_by"), rs.getBoolean("force_open"), Sql.instant(rs, "from_ts"),
                Sql.instant(rs, "cut_off"), Sql.uuid(rs, "collector_run_id"), rs.getInt("collector_attempts"),
                Sql.instant(rs, "state_changed_at"));
    }
    /**
     * A window that wasn't announced yet (slice 26).
     *
     * @param id             the window
     * @param name           its name
     * @param collectorJobId its collector job
     * @param scheduleCron   its schedule, or {@code null}
     * @param zone           the zone of its schedule
     * @param createdAt      when it was written
     */
    public record UnannouncedWindow(UUID id, String name, UUID collectorJobId, String scheduleCron, String zone,
                                    Instant createdAt) {
    }

    /**
     * The windows not announced yet, oldest first.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the windows
     * @throws SQLException on DB errors
     */
    public List<UnannouncedWindow> unannounced(Connection c, String namespace) throws SQLException {
        return Sql.query(c, "SELECT w.id, w.name, w.collector_job_id, w.schedule_cron,"
                + " COALESCE(k.zone, w.schedule_zone) AS zone, w.created_at FROM dtp_window w"
                + " LEFT JOIN dtp_calendar k ON k.id = w.calendar_id WHERE w.namespace = ? AND w.announced_at IS NULL"
                + " ORDER BY w.created_at, w.name",
                rs -> new UnannouncedWindow(Sql.uuid(rs, "id"), rs.getString("name"),
                        Sql.uuid(rs, "collector_job_id"), rs.getString("schedule_cron"), rs.getString("zone"),
                        Sql.instant(rs, "created_at")), namespace);
    }

    /**
     * Marks a window as announced, once for the cluster.
     *
     * @param c        the connection
     * @param windowId the window
     * @param now      the DB time
     * @return {@code true} if this call marked it
     * @throws SQLException on DB errors
     */
    public boolean announce(Connection c, UUID windowId, Instant now) throws SQLException {
        return Sql.update(c, "UPDATE dtp_window SET announced_at = ? WHERE id = ? AND announced_at IS NULL", now,
                windowId) == 1;
    }
}
