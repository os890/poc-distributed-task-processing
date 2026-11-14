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
package org.os890.dtp.cdi;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Consumer;

import org.eclipse.microprofile.config.Config;
import org.os890.dtp.core.DtpConfig;

/** Reads the {@code dtp.*} settings via MicroProfile Config. Durations: ISO-8601 ({@code PT1S}) or {@code 500ms}. */
public class DtpSettings {

    private DtpSettings() {
    }

    /**
     * Builds the engine settings.
     *
     * @param config the MicroProfile config
     * @return the settings
     */
    public static DtpConfig read(Config config) {
        DtpConfig.Builder builder = DtpConfig.builder();
        string(config, "dtp.namespace").or(() -> string(config, "quarkus.application.name"))
                                       .ifPresent(builder::namespace);
        string(config, "dtp.node-name").ifPresent(builder::nodeName);
        duration(config, "dtp.poll-interval", builder::pollInterval);
        duration(config, "dtp.housekeeping-interval", builder::housekeepingInterval);
        duration(config, "dtp.heartbeat-interval", builder::heartbeatInterval);
        duration(config, "dtp.lease-duration", builder::leaseDuration);
        duration(config, "dtp.leader-lease-duration", builder::leaderLeaseDuration);
        duration(config, "dtp.dead-after", builder::deadAfter);
        duration(config, "dtp.misfire-grace", builder::misfireGrace);
        duration(config, "dtp.definition-refresh", builder::definitionRefresh);
        duration(config, "dtp.drain-timeout", builder::drainTimeout);
        string(config, "dtp.max-concurrent-runs").map(Integer::valueOf).ifPresent(builder::maxConcurrentRuns);
        string(config, "dtp.schema.migrate").map(Boolean::valueOf).ifPresent(builder::migrateSchema);
        string(config, "dtp.random.seed").map(Long::valueOf).ifPresent(builder::randomSeed);
        string(config, "dtp.flows.export").map(v -> DtpConfig.FlowExport.valueOf(v.trim().toUpperCase(Locale.ROOT)))
                                          .ifPresent(builder::flowExport);
        string(config, "dtp.flows.export-path").map(Path::of).ifPresent(builder::flowExportPath);
        return builder.build();
    }

    /**
     * Parses a duration.
     *
     * @param value ISO-8601 or a number with the unit ms, s, m or h
     * @return the duration
     */
    public static Duration parse(String value) {
        String v = value.trim().toLowerCase(Locale.ROOT);
        if (v.startsWith("p")) {
            return Duration.parse(v.toUpperCase(Locale.ROOT));
        }
        if (v.endsWith("ms")) {
            return Duration.ofMillis(Long.parseLong(v.substring(0, v.length() - 2)));
        }
        long amount = Long.parseLong(v.substring(0, v.length() - 1));
        return switch (v.charAt(v.length() - 1)) {
            case 's' -> Duration.ofSeconds(amount);
            case 'm' -> Duration.ofMinutes(amount);
            case 'h' -> Duration.ofHours(amount);
            default -> throw new IllegalArgumentException("invalid duration " + value);
        };
    }

    private static Optional<String> string(Config config, String name) {
        return config.getOptionalValue(name, String.class);
    }

    private static void duration(Config config, String name, Consumer<Duration> target) {
        string(config, name).map(DtpSettings::parse).ifPresent(target);
    }
}
