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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class ProcessingSlotsTest {

    private static final ZoneId VIENNA = ZoneId.of("Europe/Vienna");
    private static final CalendarDefinition BANKING = CalendarDefinitionTest.BANKING;

    private static Instant vienna(int month, int day, int hour, int minute) {
        return ZonedDateTime.of(2026, month, day, hour, minute, 0, 0, VIENNA).toInstant();
    }

    private static ProcessingSlot slot(CalendarDefinition calendar, String from, String to) {
        return new ProcessingSlot(calendar == null ? null : calendar.id(), LocalTime.parse(from), LocalTime.parse(to));
    }

    private static Optional<ProcessingSlots.Window> window(Instant at, ProcessingSlot... slots) {
        return ProcessingSlots.window(List.of(slots), id -> BANKING, ZoneOffset.UTC, at);
    }

    @Test
    void theOpenSlotOrElseTheNextOne() {
        ProcessingSlot evenings = slot(BANKING, "18:00", "22:00");
        assertThat(window(vienna(11, 20, 19, 0), evenings))
                .contains(new ProcessingSlots.Window(vienna(11, 20, 18, 0), vienna(11, 20, 22, 0)));
        // Friday night: the next slot is on Monday
        assertThat(window(vienna(11, 20, 23, 0), evenings))
                .contains(new ProcessingSlots.Window(vienna(11, 23, 18, 0), vienna(11, 23, 22, 0)));
        assertThat(window(vienna(11, 20, 19, 0), evenings).orElseThrow().contains(vienna(11, 20, 19, 0))).isTrue();
        assertThat(window(vienna(11, 20, 23, 0), evenings).orElseThrow().contains(vienna(11, 20, 23, 0))).isFalse();
    }

    @Test
    void holidaysAreSkipped() {
        assertThat(window(vienna(12, 23, 23, 0), slot(BANKING, "18:00", "22:00")))
                .contains(new ProcessingSlots.Window(vienna(12, 28, 18, 0), vienna(12, 28, 22, 0)));
    }

    @Test
    void aSlotCanEndOnTheNextDay() {
        Instant at = Instant.parse("2026-11-21T01:00:00Z");
        assertThat(window(at, slot(null, "22:00", "02:00"))).contains(new ProcessingSlots.Window(
                Instant.parse("2026-11-20T22:00:00Z"), Instant.parse("2026-11-21T02:00:00Z")));
    }

    @Test
    void slotsWithoutGapFormOneWindow() {
        Instant at = Instant.parse("2026-11-21T09:00:00Z");
        ProcessingSlots.Window adjacent = window(at, slot(null, "08:00", "12:00"), slot(null, "12:00", "16:00"))
                .orElseThrow();
        assertThat(adjacent.end()).isEqualTo(Instant.parse("2026-11-21T16:00:00Z"));
        ProcessingSlots.Window overlapping = window(at, slot(null, "12:00", "16:00"), slot(null, "08:00", "13:00"))
                .orElseThrow();
        assertThat(overlapping).isEqualTo(new ProcessingSlots.Window(Instant.parse("2026-11-21T08:00:00Z"),
                Instant.parse("2026-11-21T16:00:00Z")));
        ProcessingSlots.Window gap = window(at, slot(null, "08:00", "12:00"), slot(null, "13:00", "16:00"))
                .orElseThrow();
        assertThat(gap.end()).isEqualTo(Instant.parse("2026-11-21T12:00:00Z"));
    }

    @Test
    void theWholeDayEveryDayIsOneLongWindow() {
        Instant at = Instant.parse("2026-11-21T23:59:59Z");
        ProcessingSlots.Window window = window(at, slot(null, "00:00", "00:00")).orElseThrow();
        assertThat(window.start()).isEqualTo(Instant.parse("2026-11-21T00:00:00Z"));
        // no stop at midnight
        assertThat(window.end()).isAfter(at.plus(Duration.ofDays(13)));
    }

    @Test
    void noSlotWithinTheHorizon() {
        CalendarDefinition never = new CalendarDefinition(UUID.randomUUID(), "NEVER", VIENNA, Set.of(), Map.of());
        assertThat(ProcessingSlots.window(List.of(slot(never, "08:00", "16:00")), id -> never, ZoneOffset.UTC,
                vienna(11, 20, 9, 0))).isEmpty();
    }
}
