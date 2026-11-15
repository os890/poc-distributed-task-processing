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
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.core.domain.CalendarShift;
import org.os890.dtp.core.domain.ConcurrencyPolicy;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.JoinMember;
import org.os890.dtp.core.domain.ProcessingSlot;
import org.os890.dtp.core.domain.TakeoverPolicy;
import org.os890.dtp.core.domain.TransitionDefinition;

/** Reads job definitions ({@code dtp_job}, {@code dtp_job_revision}, {@code dtp_transition}, {@code dtp_job_slot}). */
public class JobStore {

    private static final String SELECT = "SELECT j.id, j.namespace, j.name, j.enabled, r.revision, r.handler_class,"
            + " r.trigger_asap, r.cron_expression, r.cron_zone, r.trigger_external, r.inbox_min_items,"
            + " r.concurrency_policy, r.takeover_policy, r.retry_max, r.retry_backoff, r.max_item_visits,"
            + " r.lease_duration, r.default_parameters, r.fallback_job_id, r.fallback_delay, r.cron_calendar_id,"
            + " r.calendar_shift, r.cron_flow_instance, r.item_limit, r.time_limit, r.ack_required, r.ack_timeout"
            + " FROM dtp_job j JOIN dtp_job_revision r ON r.job_id = j.id";

    /**
     * Current revisions of all jobs of a namespace, with their transitions.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the definitions
     * @throws SQLException on DB errors
     */
    public List<JobDefinition> loadCurrent(Connection c, String namespace) throws SQLException {
        List<JobDefinition> jobs = Sql.query(c, SELECT + " WHERE j.namespace = ? AND r.revision = j.current_revision",
                JobStore::map, namespace);
        Map<String, List<TransitionDefinition>> transitions = new HashMap<>();
        Sql.query(c, "SELECT t.source_job_id, t.source_revision, t.on_outcome, t.on_result_status, t.target_job_id"
                        + " FROM dtp_transition t JOIN dtp_job j ON j.id = t.source_job_id"
                        + " AND j.current_revision = t.source_revision WHERE j.namespace = ?",
                rs -> transitions.computeIfAbsent(key(Sql.uuid(rs, "source_job_id"), rs.getInt("source_revision")),
                        k -> new ArrayList<>()).add(transition(rs)), namespace);
        Map<String, List<JoinMember>> members = new HashMap<>();
        Sql.query(c, "SELECT m.target_job_id, m.target_revision, m.member_job_id, m.accepted_outcomes"
                        + " FROM dtp_join_member m JOIN dtp_job j ON j.id = m.target_job_id"
                        + " AND j.current_revision = m.target_revision WHERE j.namespace = ?",
                rs -> members.computeIfAbsent(key(Sql.uuid(rs, "target_job_id"), rs.getInt("target_revision")),
                        k -> new ArrayList<>()).add(member(rs)), namespace);
        Map<String, List<ProcessingSlot>> slots = new HashMap<>();
        Sql.query(c, "SELECT s.job_id, s.revision, s.calendar_id, s.from_time, s.to_time FROM dtp_job_slot s"
                        + " JOIN dtp_job j ON j.id = s.job_id AND j.current_revision = s.revision WHERE j.namespace = ?"
                        + " ORDER BY s.from_time",
                rs -> slots.computeIfAbsent(key(Sql.uuid(rs, "job_id"), rs.getInt("revision")),
                        k -> new ArrayList<>()).add(slot(rs)), namespace);
        return jobs.stream()
                   .map(job -> job.withTransitions(transitions.getOrDefault(key(job.id(), job.revision()), List.of()))
                                  .withJoinMembers(members.getOrDefault(key(job.id(), job.revision()), List.of()))
                                  .withSlots(slots.getOrDefault(key(job.id(), job.revision()), List.of())))
                   .toList();
    }

    /**
     * One revision of a job, with its transitions.
     *
     * @param c        the connection
     * @param jobId    the job
     * @param revision the revision
     * @return the definition, if it exists
     * @throws SQLException on DB errors
     */
    public Optional<JobDefinition> load(Connection c, UUID jobId, int revision) throws SQLException {
        Optional<JobDefinition> job = Sql.queryOne(c, SELECT + " WHERE j.id = ? AND r.revision = ?", JobStore::map,
                jobId, revision);
        if (job.isEmpty()) {
            return job;
        }
        List<TransitionDefinition> transitions = Sql.query(c, "SELECT on_outcome, on_result_status, target_job_id"
                + " FROM dtp_transition WHERE source_job_id = ? AND source_revision = ?", JobStore::transition,
                jobId, revision);
        List<JoinMember> members = Sql.query(c, "SELECT member_job_id, accepted_outcomes FROM dtp_join_member"
                + " WHERE target_job_id = ? AND target_revision = ?", JobStore::member, jobId, revision);
        List<ProcessingSlot> slots = Sql.query(c, "SELECT calendar_id, from_time, to_time FROM dtp_job_slot"
                + " WHERE job_id = ? AND revision = ? ORDER BY from_time", JobStore::slot, jobId, revision);
        return Optional.of(job.get().withTransitions(transitions).withJoinMembers(members).withSlots(slots));
    }

    /**
     * A job generated for a step of a flow (DD-15 still applies to entry points: their triggers come from SQL).
     *
     * @param c             the connection
     * @param namespace     the namespace
     * @param name          the job name
     * @param handlerClass  the step class
     * @param inboxMinItems inbox trigger, or {@code null}
     * @param changedBy     who generated it
     * @param now           the DB time
     * @return the job id
     * @throws SQLException on DB errors
     */
    public UUID insertGenerated(Connection c, String namespace, String name, String handlerClass,
                                Integer inboxMinItems, String changedBy, java.time.Instant now) throws SQLException {
        UUID id = UUID.randomUUID();
        Sql.update(c, "INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at, updated_at)"
                + " VALUES (?, ?, ?, 1, TRUE, ?, ?)", id, namespace, name, now, now);
        Sql.update(c, "INSERT INTO dtp_job_revision (job_id, revision, handler_class, inbox_min_items, changed_by,"
                + " changed_at) VALUES (?, 1, ?, ?, ?, ?)", id, handlerClass, inboxMinItems, changedBy, now);
        return id;
    }

    /**
     * Creation time of a job.
     *
     * @param c     the connection
     * @param jobId the job
     * @return the creation time
     * @throws SQLException on DB errors
     */
    public java.time.Instant createdAt(Connection c, UUID jobId) throws SQLException {
        return Sql.queryOne(c, "SELECT created_at FROM dtp_job WHERE id = ?", rs -> Sql.instant(rs, "created_at"),
                jobId).orElseThrow(() -> new SQLException("unknown job " + jobId));
    }

    private static String key(UUID jobId, int revision) {
        return jobId + "#" + revision;
    }

    private static TransitionDefinition transition(ResultSet rs) throws SQLException {
        String outcome = rs.getString("on_outcome");
        return new TransitionDefinition(outcome == null ? null : RunOutcome.valueOf(outcome),
                rs.getString("on_result_status"), Sql.uuid(rs, "target_job_id"));
    }

    private static JoinMember member(ResultSet rs) throws SQLException {
        Set<RunOutcome> accepted = EnumSet.noneOf(RunOutcome.class);
        for (String outcome : rs.getString("accepted_outcomes").split(",")) {
            accepted.add(RunOutcome.valueOf(outcome.trim()));
        }
        return new JoinMember(Sql.uuid(rs, "member_job_id"), accepted);
    }

    private static ProcessingSlot slot(ResultSet rs) throws SQLException {
        return new ProcessingSlot(Sql.uuid(rs, "calendar_id"), rs.getObject("from_time", LocalTime.class),
                rs.getObject("to_time", LocalTime.class));
    }

    private static JobDefinition map(ResultSet rs) throws SQLException {
        String lease = rs.getString("lease_duration");
        String fallbackDelay = rs.getString("fallback_delay");
        String timeLimit = rs.getString("time_limit");
        String ackTimeout = rs.getString("ack_timeout");
        return new JobDefinition(Sql.uuid(rs, "id"), rs.getString("namespace"), rs.getString("name"),
                rs.getInt("revision"), rs.getBoolean("enabled"), rs.getString("handler_class"),
                rs.getBoolean("trigger_asap"), rs.getString("cron_expression"), rs.getString("cron_zone"),
                rs.getBoolean("trigger_external"), Sql.integer(rs, "inbox_min_items"),
                ConcurrencyPolicy.valueOf(rs.getString("concurrency_policy")),
                TakeoverPolicy.valueOf(rs.getString("takeover_policy")), rs.getInt("retry_max"),
                Duration.parse(rs.getString("retry_backoff")), Sql.integer(rs, "max_item_visits"),
                lease == null ? null : Duration.parse(lease), rs.getString("default_parameters"),
                Sql.uuid(rs, "fallback_job_id"), fallbackDelay == null ? null : Duration.parse(fallbackDelay),
                Sql.uuid(rs, "cron_calendar_id"), CalendarShift.valueOf(rs.getString("calendar_shift")),
                rs.getBoolean("cron_flow_instance"), List.of(), rs.getObject("item_limit", Long.class),
                timeLimit == null ? null : Duration.parse(timeLimit), rs.getBoolean("ack_required"),
                ackTimeout == null ? null : Duration.parse(ackTimeout), List.of(), List.of());
    }
}
