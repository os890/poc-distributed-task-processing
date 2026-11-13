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

import java.time.Duration;
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
 */
public record DtpConfig(String namespace, String nodeName, Duration pollInterval, Duration housekeepingInterval,
                        Duration heartbeatInterval, Duration leaseDuration, Duration leaderLeaseDuration,
                        Duration deadAfter, Duration misfireGrace, Duration definitionRefresh, Duration drainTimeout,
                        int maxConcurrentRuns, boolean migrateSchema, Long randomSeed) {

    public DtpConfig {
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

        public DtpConfig build() {
            return new DtpConfig(namespace, nodeName, pollInterval, housekeepingInterval, heartbeatInterval,
                    leaseDuration, leaderLeaseDuration, deadAfter, misfireGrace, definitionRefresh, drainTimeout,
                    maxConcurrentRuns, migrateSchema, randomSeed);
        }

        private static String defaultNodeName() {
            String host = System.getenv().getOrDefault("HOSTNAME", "node");
            return host + "-" + ProcessHandle.current().pid();
        }
    }
}
