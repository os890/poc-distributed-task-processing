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
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

/**
 * Computes when the processing slots of a job are open (docs/design/05-coordination-flows.md §5.1 and §5.7). Slots
 * that overlap or follow each other without a gap form one window, so a run isn't stopped at midnight between two
 * whole-day slots.
 */
public class ProcessingSlots {

    /** How far ahead windows are searched and merged. */
    public static final Duration HORIZON = Duration.ofDays(CalendarDefinition.MAX_SHIFT_DAYS + 1L);

    private ProcessingSlots() {
    }

    /**
     * An open window of the slots.
     *
     * @param start when it opens
     * @param end   when it closes
     */
    public record Window(Instant start, Instant end) {

        /**
         * Whether the window is open at a point in time.
         *
         * @param at the point in time
         * @return {@code true} if {@code start <= at < end}
         */
        public boolean contains(Instant at) {
            return !at.isBefore(start) && at.isBefore(end);
        }
    }

    /**
     * The window that is open at the given point in time, or else the next one.
     *
     * @param slots       the slots of a job, not empty
     * @param calendars   looks up the calendar of a slot
     * @param defaultZone the zone of slots without a calendar
     * @param at          the point in time
     * @return the window, or empty if no slot opens within the {@link #HORIZON}
     */
    public static Optional<Window> window(List<ProcessingSlot> slots, Function<UUID, CalendarDefinition> calendars,
                                          ZoneId defaultZone, Instant at) {
        List<Window> windows = new ArrayList<>();
        Instant until = at.plus(HORIZON);
        for (ProcessingSlot slot : slots) {
            CalendarDefinition calendar = slot.calendarId() == null ? null : calendars.apply(slot.calendarId());
            ZoneId zone = calendar != null ? calendar.zone() : defaultZone;
            LocalDate last = until.atZone(zone).toLocalDate();
            LocalDate day = at.atZone(zone).toLocalDate().minusDays(1);
            while (!day.isAfter(last)) {
                if (calendar == null || calendar.isBusinessDay(day)) {
                    Window window = open(slot, day, zone);
                    if (window.end().isAfter(at) && window.start().isBefore(until)) {
                        windows.add(window);
                    }
                }
                day = day.plusDays(1);
            }
        }
        if (windows.isEmpty()) {
            return Optional.empty();
        }
        windows.sort(Comparator.comparing(Window::start));
        Instant start = windows.getFirst().start();
        Instant end = windows.getFirst().end();
        for (Window window : windows) {
            if (window.start().isAfter(end)) {
                break;
            }
            if (window.end().isAfter(end)) {
                end = window.end();
            }
        }
        return Optional.of(new Window(start, end));
    }

    private static Window open(ProcessingSlot slot, LocalDate day, ZoneId zone) {
        ZonedDateTime start = ZonedDateTime.of(day, slot.from(), zone);
        LocalDate endDay = slot.to().isAfter(slot.from()) ? day : day.plusDays(1);
        return new Window(start.toInstant(), ZonedDateTime.of(endDay, slot.to(), zone).toInstant());
    }
}
