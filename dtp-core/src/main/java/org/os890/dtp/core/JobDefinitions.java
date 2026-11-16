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
package org.os890.dtp.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.core.domain.CalendarShift;
import org.os890.dtp.core.domain.ConcurrencyPolicy;
import org.os890.dtp.core.domain.TakeoverPolicy;
import org.os890.dtp.core.jdbc.DbClock;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.jdbc.SqlDialect;
import org.os890.dtp.core.spi.Transactions;

/**
 * Builder for job definitions, for tests and small applications (D23): larger applications keep their definitions in
 * SQL scripts ({@code db/dtp-definitions}, DD-15), which operators can read and change. Writes the same rows as such a
 * script, revision 1 of each job. The job is named like the simple name of its class.
 */
public class JobDefinitions {

    private static final String CHANGED_BY = "builder";

    private final Transactions tx;
    private final String namespace;
    private final DbClock clock;

    public JobDefinitions(Transactions tx, String namespace) {
        this(tx, namespace, null);
    }

    /**
     * A builder that stamps the definitions with the time of a clock, e.g. the shifted clock of a test cluster.
     *
     * @param tx        the transactions
     * @param namespace the namespace
     * @param clock     the clock, or {@code null} for the DB time
     */
    public JobDefinitions(Transactions tx, String namespace, DbClock clock) {
        this.tx = tx;
        this.namespace = namespace;
        this.clock = clock;
    }

    private Instant now(Connection c) throws SQLException {
        return (clock != null ? clock : new DbClock(SqlDialect.detect(c))).now(c);
    }

    /**
     * Starts a definition.
     *
     * @param step the process step
     * @return the builder
     */
    public Definition define(Class<? extends ProcessStep> step) {
        return new Definition(step);
    }

    /**
     * Saves a business calendar.
     *
     * @param name       the name
     * @param zone       the zone
     * @param weekdays   the business weekdays, e.g. {@code MON-FRI}
     * @param exceptions days that differ from their weekday: {@code false} for a holiday
     * @return the calendar id
     */
    public UUID calendar(String name, String zone, String weekdays, Map<LocalDate, Boolean> exceptions) {
        UUID id = UUID.randomUUID();
        tx.required(() -> tx.withConnection(c -> {
            Sql.update(c, "INSERT INTO dtp_calendar (id, namespace, name, zone, business_weekdays)"
                    + " VALUES (?, ?, ?, ?, ?)", id, namespace, name, zone, weekdays);
            for (Map.Entry<LocalDate, Boolean> exception : exceptions.entrySet()) {
                Sql.update(c, "INSERT INTO dtp_calendar_exception (calendar_id, calendar_day, business_day,"
                        + " description) VALUES (?, ?, ?, ?)", id, exception.getKey(), exception.getValue(),
                        CHANGED_BY);
            }
            return null;
        }));
        return id;
    }

    /**
     * Saves a collection window; the collector and the paused jobs have to be defined already.
     *
     * @param name           the name
     * @param cron           the schedule (UTC), or {@code null} for occurrences started by operators only
     * @param quiesceTimeout how long to wait for the paused jobs
     * @param collector      the collector
     * @param paused         the paused jobs
     * @return the window id
     */
    public UUID window(String name, String cron, Duration quiesceTimeout, Class<? extends ProcessStep> collector,
                       List<Class<? extends ProcessStep>> paused) {
        UUID id = UUID.randomUUID();
        tx.required(() -> tx.withConnection(c -> {
            Sql.update(c, "INSERT INTO dtp_window (id, namespace, name, schedule_cron, collector_job_id,"
                    + " quiesce_timeout) VALUES (?, ?, ?, ?, ?, ?)", id, namespace, name, cron, jobId(c, collector),
                    quiesceTimeout.toString());
            for (Class<? extends ProcessStep> job : paused) {
                Sql.update(c, "INSERT INTO dtp_window_paused_job (window_id, job_id) VALUES (?, ?)", id,
                        jobId(c, job));
            }
            return null;
        }));
        return id;
    }

    private UUID jobId(Connection c, Class<? extends ProcessStep> step) throws SQLException {
        return Sql.queryOne(c, "SELECT j.id FROM dtp_job j JOIN dtp_job_revision r ON r.job_id = j.id"
                + " AND r.revision = j.current_revision WHERE j.namespace = ? AND r.handler_class = ?",
                rs -> Sql.uuid(rs, "id"), namespace, step.getName())
                  .orElseThrow(() -> new IllegalArgumentException("no job for " + step.getName()));
    }

    /** A job definition under construction. */
    public class Definition {

        private final Class<? extends ProcessStep> step;
        private final List<Object[]> transitions = new ArrayList<>();
        private boolean asap;
        private String cron;
        private boolean external;
        private Integer inboxMinItems;
        private ConcurrencyPolicy concurrency = ConcurrencyPolicy.SKIP;
        private TakeoverPolicy takeover = TakeoverPolicy.RERUN;
        private int retryMax = 3;
        private Duration retryBackoff = Duration.ofSeconds(10);
        private Integer maxItemVisits;
        private Duration leaseDuration;
        private String defaultParameters;
        private Class<? extends ProcessStep> fallbackJob;
        private Duration fallbackDelay;
        private final List<Object[]> joinMembers = new ArrayList<>();
        private final List<Object[]> slots = new ArrayList<>();
        private String calendar;
        private CalendarShift shift = CalendarShift.SKIP;
        private boolean cronFlowInstance;
        private Long itemLimit;
        private Duration timeLimit;
        private boolean ackRequired;
        private Duration ackTimeout;

        Definition(Class<? extends ProcessStep> step) {
            this.step = step;
        }

        public Definition asap() {
            asap = true;
            return this;
        }

        public Definition cron(String expression) {
            cron = expression;
            return this;
        }

        /**
         * The business calendar of the cron trigger (the calendar has to be saved first).
         *
         * @param name  the calendar
         * @param value what happens on other days
         * @return this definition
         */
        public Definition calendar(String name, CalendarShift value) {
            calendar = name;
            shift = value;
            return this;
        }

        public Definition itemLimit(long value) {
            itemLimit = value;
            return this;
        }

        /**
         * Sent work packages wait for the acknowledgement of the receiver.
         *
         * @param timeout the time to wait, or {@code null} without limit
         * @return this definition
         */
        public Definition acknowledged(Duration timeout) {
            ackRequired = true;
            ackTimeout = timeout;
            return this;
        }

        public Definition timeLimit(Duration value) {
            timeLimit = value;
            return this;
        }

        /** Every cron slot starts a flow instance for its business date. */
        public Definition cronFlowInstance() {
            cronFlowInstance = true;
            return this;
        }

        /**
         * A processing slot (UTC without calendar).
         *
         * @param calendarName the calendar, or {@code null} for every day
         * @param from         start time of day
         * @param to           end time of day
         * @return this definition
         */
        public Definition slot(String calendarName, LocalTime from, LocalTime to) {
            slots.add(new Object[] {calendarName, from, to});
            return this;
        }

        public Definition external() {
            external = true;
            return this;
        }

        public Definition inbox(int minItems) {
            inboxMinItems = minItems;
            return this;
        }

        public Definition concurrency(ConcurrencyPolicy value) {
            concurrency = value;
            return this;
        }

        public Definition takeover(TakeoverPolicy value) {
            takeover = value;
            return this;
        }

        public Definition retry(int max, Duration backoff) {
            retryMax = max;
            retryBackoff = backoff;
            return this;
        }

        public Definition maxItemVisits(int value) {
            maxItemVisits = value;
            return this;
        }

        public Definition leaseDuration(Duration value) {
            leaseDuration = value;
            return this;
        }

        public Definition defaultParameters(String json) {
            defaultParameters = json;
            return this;
        }

        /**
         * The fallback of decision requests that stay open (the fallback job has to be saved first).
         *
         * @param job   the fallback job
         * @param delay the delay
         * @return this definition
         */
        public Definition decisionFallback(Class<? extends ProcessStep> job, Duration delay) {
            fallbackJob = job;
            fallbackDelay = delay;
            return this;
        }

        /**
         * Makes this job the target of a run join (the member jobs have to be saved first).
         *
         * @param member   a member job
         * @param accepted the accepted outcomes of the member
         * @return this definition
         */
        public Definition joinOf(Class<? extends ProcessStep> member, RunOutcome... accepted) {
            joinMembers.add(new Object[] {member, String.join(",", java.util.Arrays.stream(accepted)
                                                                                   .map(Enum::name).toList())});
            return this;
        }

        public Definition on(RunOutcome outcome, Class<? extends ProcessStep> target) {
            transitions.add(new Object[] {outcome.name(), null, target});
            return this;
        }

        public Definition onStatus(Enum<?> status, Class<? extends ProcessStep> target) {
            transitions.add(new Object[] {null, status.name(), target});
            return this;
        }

        /**
         * Saves the definition (revision 1). Transition targets have to be saved first.
         *
         * @return the job id
         */
        public UUID save() {
            UUID id = UUID.randomUUID();
            tx.required(() -> tx.withConnection(c -> {
                Instant now = now(c);
                Sql.update(c, "INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at,"
                        + " updated_at) VALUES (?, ?, ?, 1, TRUE, ?, ?)", id, namespace, step.getSimpleName(), now,
                        now);
                Sql.update(c, "INSERT INTO dtp_job_revision (job_id, revision, handler_class, trigger_asap,"
                        + " cron_expression, cron_zone, trigger_external, inbox_min_items, concurrency_policy,"
                        + " takeover_policy, retry_max, retry_backoff, max_item_visits, lease_duration,"
                        + " default_parameters, changed_by, changed_at)"
                        + " VALUES (?, 1, ?, ?, ?, 'UTC', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        id, step.getName(), asap, cron, external, inboxMinItems, concurrency, takeover, retryMax,
                        retryBackoff.toString(), maxItemVisits, leaseDuration == null ? null : leaseDuration.toString(),
                        defaultParameters, CHANGED_BY, now);
                Sql.update(c, "UPDATE dtp_job_revision SET calendar_shift = ?, cron_flow_instance = ?,"
                        + " item_limit = ?, time_limit = ?, ack_required = ?, ack_timeout = ?,"
                        + " cron_calendar_id = (SELECT id FROM dtp_calendar WHERE namespace = ? AND name = ?)"
                        + " WHERE job_id = ?", shift, cronFlowInstance, itemLimit,
                        timeLimit == null ? null : timeLimit.toString(), ackRequired,
                        ackTimeout == null ? null : ackTimeout.toString(), namespace, calendar, id);
                for (Object[] slot : slots) {
                    Sql.update(c, "INSERT INTO dtp_job_slot (id, job_id, revision, calendar_id, from_time, to_time)"
                            + " VALUES (?, ?, 1, (SELECT id FROM dtp_calendar WHERE namespace = ? AND name = ?), ?, ?)",
                            UUID.randomUUID(), id, namespace, slot[0], slot[1], slot[2]);
                }
                if (fallbackJob != null) {
                    Sql.update(c, "UPDATE dtp_job_revision SET fallback_delay = ?, fallback_job_id = (SELECT id"
                            + " FROM dtp_job WHERE namespace = ? AND name = ?) WHERE job_id = ?",
                            fallbackDelay.toString(), namespace, fallbackJob.getSimpleName(), id);
                }
                for (Object[] m : joinMembers) {
                    Sql.update(c, "INSERT INTO dtp_join_member (target_job_id, target_revision, member_job_id,"
                            + " accepted_outcomes) SELECT ?, 1, id, ? FROM dtp_job WHERE namespace = ? AND name = ?",
                            id, m[1], namespace, ((Class<?>) m[0]).getSimpleName());
                }
                for (Object[] t : transitions) {
                    Sql.update(c, "INSERT INTO dtp_transition (id, source_job_id, source_revision, on_outcome,"
                            + " on_result_status, target_job_id) SELECT ?, ?, 1, ?, ?, id FROM dtp_job"
                            + " WHERE namespace = ? AND name = ?", UUID.randomUUID(), id, t[0], t[1], namespace,
                            ((Class<?>) t[2]).getSimpleName());
                }
                return null;
            }));
            return id;
        }
    }
}
