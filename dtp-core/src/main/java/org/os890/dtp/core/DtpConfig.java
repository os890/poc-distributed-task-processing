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

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Engine settings ({@code dtp.*}, see docs/design/01-architecture.md). All values have defaults.
 *
 * @param namespace           namespace of the application cluster, e.g. the application name
 * @param nodeName            human-readable name of this node
 * @param pollInterval        base interval of the run claimer (jittered, backs off while idle)
 * @param housekeepingInterval interval of the leader's planner and reaper
 * @param heartbeatInterval   interval of node heartbeats and lease renewals
 * @param leaseDuration       default lease duration of a run
 * @param leaderLeaseDuration lease duration of the leadership
 * @param deadAfter           a node without heartbeat for this long is marked dead
 * @param misfireGrace        missed cron slots within this period are still planned
 * @param definitionRefresh   cache period of the job definitions
 * @param drainTimeout        how long a shutdown waits for running runs
 * @param maxConcurrentRuns   maximum number of runs on this node
 * @param migrateSchema       run the Flyway migrations on start
 * @param randomSeed          seed of the jitter (DD-14), or {@code null} for a random one
 * @param flowExport          export of the flows after the start
 * @param flowExportPath      file of the export
 * @param definitionPlaceholders Flyway placeholders of the application's job definitions, e.g. a fallback delay
 * @param monitoring          the monitoring read model ({@code dtp.monitoring.*})
 */
public record DtpConfig(String namespace, String nodeName, Duration pollInterval, Duration housekeepingInterval,
                        Duration heartbeatInterval, Duration leaseDuration, Duration leaderLeaseDuration,
                        Duration deadAfter, Duration misfireGrace, Duration definitionRefresh, Duration drainTimeout,
                        int maxConcurrentRuns, boolean migrateSchema, Long randomSeed, FlowExport flowExport,
                        Path flowExportPath, Map<String, String> definitionPlaceholders, Monitoring monitoring) {

    public DtpConfig {
        Objects.requireNonNull(monitoring, "monitoring");
        definitionPlaceholders = Map.copyOf(definitionPlaceholders);
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(nodeName, "nodeName");
        if (maxConcurrentRuns < 1) {
            throw new IllegalArgumentException("maxConcurrentRuns must be at least 1");
        }
        if (heartbeatInterval.multipliedBy(2).compareTo(leaseDuration) > 0) {
            throw new IllegalArgumentException("leaseDuration must be at least twice the heartbeatInterval");
        }
    }

    /**
     * Settings of the monitoring read model (docs/design/09-monitoring-heatmap.md).
     *
     * @param enabled             project the views on the leader of the role {@code projection}
     * @param interval            interval of the projection cycle
     * @param jamThreshold        jam score (0 to 100) from which a step counts as congested
     * @param failureThreshold    technical failures within the failure window from which a step counts as failing
     * @param failureWindow       window of the technical failures; a failing step recovers after this calm period
     * @param expectedMaxQueueAge default age of the oldest queued item that counts as a jam (per job:
     *                            {@code expected_max_queue_age})
     * @param retention           how long the minute buckets are kept
     * @param feedInterval        how often the live feed checks for a new projection
     */
    public record Monitoring(boolean enabled, Duration interval, int jamThreshold, int failureThreshold,
                             Duration failureWindow, Duration expectedMaxQueueAge, Duration retention,
                             Duration feedInterval) {

        public Monitoring {
            if (jamThreshold < 1 || jamThreshold > 100) {
                throw new IllegalArgumentException("jamThreshold must be between 1 and 100");
            }
            if (failureThreshold < 1) {
                throw new IllegalArgumentException("failureThreshold must be at least 1");
            }
        }

        /**
         * The defaults: a cycle every 10 s, congested from a jam score of 70, failing from 10 technical failures in
         * 5 minutes, a queue age of 10 minutes counts as a jam, minute buckets kept for 48 hours.
         *
         * @return the defaults
         */
        public static Monitoring defaults() {
            return builder().build();
        }

        /**
         * A builder that starts with the defaults.
         *
         * @return the builder
         */
        public static MonitoringBuilder builder() {
            return new MonitoringBuilder();
        }
    }

    /** Builder of {@link Monitoring}, starting with the defaults. */
    public static class MonitoringBuilder {

        private boolean enabled = true;
        private Duration interval = Duration.ofSeconds(10);
        private int jamThreshold = 70;
        private int failureThreshold = 10;
        private Duration failureWindow = Duration.ofMinutes(5);
        private Duration expectedMaxQueueAge = Duration.ofMinutes(10);
        private Duration retention = Duration.ofHours(48);
        private Duration feedInterval = Duration.ofSeconds(2);

        public MonitoringBuilder enabled(boolean value) {
            enabled = value;
            return this;
        }

        public MonitoringBuilder interval(Duration value) {
            interval = Objects.requireNonNull(value, "interval");
            return this;
        }

        public MonitoringBuilder jamThreshold(int value) {
            jamThreshold = value;
            return this;
        }

        public MonitoringBuilder failureThreshold(int value) {
            failureThreshold = value;
            return this;
        }

        public MonitoringBuilder failureWindow(Duration value) {
            failureWindow = Objects.requireNonNull(value, "failureWindow");
            return this;
        }

        public MonitoringBuilder expectedMaxQueueAge(Duration value) {
            expectedMaxQueueAge = Objects.requireNonNull(value, "expectedMaxQueueAge");
            return this;
        }

        public MonitoringBuilder retention(Duration value) {
            retention = Objects.requireNonNull(value, "retention");
            return this;
        }

        public MonitoringBuilder feedInterval(Duration value) {
            feedInterval = Objects.requireNonNull(value, "feedInterval");
            return this;
        }

        public Monitoring build() {
            return new Monitoring(enabled, interval, jamThreshold, failureThreshold, failureWindow,
                    expectedMaxQueueAge, retention, feedInterval);
        }
    }

    /** Export of the flows. */
    public enum FlowExport {
        /** No export. */
        NONE,
        /** Markdown with a Mermaid diagram per flow. */
        MERMAID
    }

    /**
     * A builder with the defaults.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder of {@link DtpConfig}. */
    public static class Builder {

        private String namespace = "default";
        private String nodeName = defaultNodeName();
        private Duration pollInterval = Duration.ofSeconds(1);
        private Duration housekeepingInterval = Duration.ofSeconds(1);
        private Duration heartbeatInterval = Duration.ofSeconds(2);
        private Duration leaseDuration = Duration.ofSeconds(15);
        private Duration leaderLeaseDuration = Duration.ofSeconds(10);
        private Duration deadAfter = Duration.ofSeconds(30);
        private Duration misfireGrace = Duration.ofMinutes(1);
        private Duration definitionRefresh = Duration.ofSeconds(5);
        private Duration drainTimeout = Duration.ofSeconds(30);
        private int maxConcurrentRuns = 8;
        private boolean migrateSchema = true;
        private Long randomSeed;
        private FlowExport flowExport = FlowExport.NONE;
        private Path flowExportPath = Path.of("target", "dtp-flows.md");
        private Map<String, String> definitionPlaceholders = Map.of();
        private Monitoring monitoring = Monitoring.defaults();

        public Builder namespace(String value) {
            namespace = value;
            return this;
        }

        public Builder nodeName(String value) {
            nodeName = value;
            return this;
        }

        public Builder pollInterval(Duration value) {
            pollInterval = value;
            return this;
        }

        public Builder housekeepingInterval(Duration value) {
            housekeepingInterval = value;
            return this;
        }

        public Builder heartbeatInterval(Duration value) {
            heartbeatInterval = value;
            return this;
        }

        public Builder leaseDuration(Duration value) {
            leaseDuration = value;
            return this;
        }

        public Builder leaderLeaseDuration(Duration value) {
            leaderLeaseDuration = value;
            return this;
        }

        public Builder deadAfter(Duration value) {
            deadAfter = value;
            return this;
        }

        public Builder misfireGrace(Duration value) {
            misfireGrace = value;
            return this;
        }

        public Builder definitionRefresh(Duration value) {
            definitionRefresh = value;
            return this;
        }

        public Builder drainTimeout(Duration value) {
            drainTimeout = value;
            return this;
        }

        public Builder maxConcurrentRuns(int value) {
            maxConcurrentRuns = value;
            return this;
        }

        public Builder migrateSchema(boolean value) {
            migrateSchema = value;
            return this;
        }

        public Builder randomSeed(Long value) {
            randomSeed = value;
            return this;
        }

        /**
         * Export of the flows after the start (diagrams generated from the DB).
         *
         * @param value the format
         * @return this builder
         */
        public Builder flowExport(FlowExport value) {
            flowExport = Objects.requireNonNull(value, "flowExport");
            return this;
        }

        /**
         * File of the flow export.
         *
         * @param value the path
         * @return this builder
         */
        public Builder flowExportPath(Path value) {
            flowExportPath = Objects.requireNonNull(value, "flowExportPath");
            return this;
        }

        /**
         * Placeholders of the application's job definitions (DD-15): {@code ${name}} in the SQL scripts, e.g. delays
         * that differ per environment.
         *
         * @param value the placeholders by name
         * @return this builder
         */
        public Builder definitionPlaceholders(Map<String, String> value) {
            definitionPlaceholders = Objects.requireNonNull(value, "definitionPlaceholders");
            return this;
        }

        /**
         * Settings of the monitoring read model.
         *
         * @param value the settings, e.g. {@code Monitoring.defaults()} with other values
         * @return this builder
         */
        public Builder monitoring(Monitoring value) {
            monitoring = Objects.requireNonNull(value, "monitoring");
            return this;
        }

        public DtpConfig build() {
            return new DtpConfig(namespace, nodeName, pollInterval, housekeepingInterval, heartbeatInterval,
                    leaseDuration, leaderLeaseDuration, deadAfter, misfireGrace, definitionRefresh, drainTimeout,
                    maxConcurrentRuns, migrateSchema, randomSeed, flowExport, flowExportPath, definitionPlaceholders,
                    monitoring);
        }

        private static String defaultNodeName() {
            String host = System.getenv().getOrDefault("HOSTNAME", "node");
            return host + "-" + ProcessHandle.current().pid();
        }
    }
}
