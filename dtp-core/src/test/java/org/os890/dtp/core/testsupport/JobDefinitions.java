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
package org.os890.dtp.core.testsupport;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.core.domain.ConcurrencyPolicy;
import org.os890.dtp.core.domain.TakeoverPolicy;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.spi.Transactions;

/**
 * Test-only builder for job definitions (the design's builder API for tests, D23). Writes the same rows as the SQL
 * definition scripts of an application.
 */
public class JobDefinitions {

    private final Transactions tx;
    private final String namespace;

    public JobDefinitions(Transactions tx, String namespace) {
        this.tx = tx;
        this.namespace = namespace;
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
                Sql.update(c, "INSERT INTO dtp_job (id, namespace, name, current_revision, enabled, created_at,"
                        + " updated_at) VALUES (?, ?, ?, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", id, namespace,
                        step.getSimpleName());
                Sql.update(c, "INSERT INTO dtp_job_revision (job_id, revision, handler_class, trigger_asap,"
                        + " cron_expression, cron_zone, trigger_external, inbox_min_items, concurrency_policy,"
                        + " takeover_policy, retry_max, retry_backoff, max_item_visits, lease_duration,"
                        + " default_parameters, changed_by, changed_at)"
                        + " VALUES (?, 1, ?, ?, ?, 'UTC', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'test', CURRENT_TIMESTAMP)",
                        id, step.getName(), asap, cron, external, inboxMinItems, concurrency, takeover, retryMax,
                        retryBackoff.toString(), maxItemVisits, leaseDuration == null ? null : leaseDuration.toString(),
                        defaultParameters);
                if (fallbackJob != null) {
                    Sql.update(c, "UPDATE dtp_job_revision SET fallback_delay = ?, fallback_job_id = (SELECT id"
                            + " FROM dtp_job WHERE namespace = ? AND name = ?) WHERE job_id = ?",
                            fallbackDelay.toString(), namespace, fallbackJob.getSimpleName(), id);
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
