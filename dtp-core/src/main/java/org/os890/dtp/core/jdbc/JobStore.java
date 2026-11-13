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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.core.domain.ConcurrencyPolicy;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.TakeoverPolicy;
import org.os890.dtp.core.domain.TransitionDefinition;

/** Reads job definitions ({@code dtp_job}, {@code dtp_job_revision}, {@code dtp_transition}). */
public class JobStore {

    private static final String SELECT = "SELECT j.id, j.namespace, j.name, j.enabled, r.revision, r.handler_class,"
            + " r.trigger_asap, r.cron_expression, r.cron_zone, r.trigger_external, r.inbox_min_items,"
            + " r.concurrency_policy, r.takeover_policy, r.retry_max, r.retry_backoff, r.max_item_visits,"
            + " r.lease_duration, r.default_parameters"
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
        return jobs.stream()
                   .map(job -> job.withTransitions(transitions.getOrDefault(key(job.id(), job.revision()), List.of())))
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
        return Optional.of(job.get().withTransitions(transitions));
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

    private static JobDefinition map(ResultSet rs) throws SQLException {
        String lease = rs.getString("lease_duration");
        return new JobDefinition(Sql.uuid(rs, "id"), rs.getString("namespace"), rs.getString("name"),
                rs.getInt("revision"), rs.getBoolean("enabled"), rs.getString("handler_class"),
                rs.getBoolean("trigger_asap"), rs.getString("cron_expression"), rs.getString("cron_zone"),
                rs.getBoolean("trigger_external"), Sql.integer(rs, "inbox_min_items"),
                ConcurrencyPolicy.valueOf(rs.getString("concurrency_policy")),
                TakeoverPolicy.valueOf(rs.getString("takeover_policy")), rs.getInt("retry_max"),
                Duration.parse(rs.getString("retry_backoff")), Sql.integer(rs, "max_item_visits"),
                lease == null ? null : Duration.parse(lease), rs.getString("default_parameters"), List.of());
    }
}
