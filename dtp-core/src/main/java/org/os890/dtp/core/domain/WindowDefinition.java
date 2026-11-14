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
package org.os890.dtp.core.domain;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Set;
import java.util.UUID;

/**
 * A collection window (docs/design/02-domain-model.md, D35): at each occurrence, the paused jobs are stopped softly,
 * and once they are idle the collector runs alone with a cut-off, e.g. to export everything changed up to then.
 *
 * @param id             the window
 * @param name           its name, unique per namespace
 * @param scheduleCron   when occurrences start (Quartz syntax with seconds), {@code null}: started by operators only
 * @param zone           the zone of the schedule (the calendar's zone if a calendar is set)
 * @param calendarId     the business calendar: no occurrence on other days, or {@code null}
 * @param collectorJobId the job that collects
 * @param pausedJobIds   the jobs that are paused while an occurrence is active
 * @param quiesceTimeout how long to wait for the paused jobs before a person decides
 * @param enabled        disabled windows start no occurrences
 * @param createdAt      creation: the schedule starts after it
 */
public record WindowDefinition(UUID id, String name, String scheduleCron, ZoneId zone, UUID calendarId,
                               UUID collectorJobId, Set<UUID> pausedJobIds, Duration quiesceTimeout,
                               boolean enabled, Instant createdAt) {

    public WindowDefinition {
        pausedJobIds = Set.copyOf(pausedJobIds);
    }
}
