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

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.os890.dtp.api.BusinessCalendar;

/**
 * A business calendar as defined in the DB ({@code dtp_calendar}, {@code dtp_calendar_exception}).
 *
 * @param id         identity of the calendar
 * @param name       unique name within the namespace
 * @param zone       the zone that decides which day it is
 * @param weekdays   the business weekdays
 * @param exceptions days that differ from their weekday: {@code false} for a holiday, {@code true} for an extra
 *                   business day
 */
public record CalendarDefinition(UUID id, String name, ZoneId zone, Set<DayOfWeek> weekdays,
                                 Map<LocalDate, Boolean> exceptions) implements BusinessCalendar {

    /** Search limit for the next or previous business day: no calendar has a longer break. */
    public static final int MAX_SHIFT_DAYS = 14;

    private static final List<String> NAMES = List.of("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN");

    public CalendarDefinition {
        weekdays = weekdays.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(weekdays));
        exceptions = Map.copyOf(exceptions);
    }

    /**
     * Parses business weekdays: names, ranges and lists, e.g. {@code MON-FRI}, {@code MON,WED,FRI} or {@code SUN-THU}.
     *
     * @param value the weekdays
     * @return the parsed weekdays
     * @throws IllegalArgumentException for an unknown name
     */
    public static Set<DayOfWeek> parseWeekdays(String value) {
        Set<DayOfWeek> days = EnumSet.noneOf(DayOfWeek.class);
        for (String part : value.toUpperCase(Locale.ROOT).split(",")) {
            String[] range = part.trim().split("-");
            if (range.length == 1 && range[0].isEmpty()) {
                continue;
            }
            int from = weekday(range[0], value);
            int to = range.length > 1 ? weekday(range[1], value) : from;
            for (int i = from; ; i = (i + 1) % NAMES.size()) {
                days.add(DayOfWeek.of(i + 1));
                if (i == to) {
                    break;
                }
            }
        }
        return days;
    }

    private static int weekday(String name, String value) {
        int index = NAMES.indexOf(name.trim());
        if (index < 0) {
            throw new IllegalArgumentException("unknown weekday " + name + " in " + value);
        }
        return index;
    }

    @Override
    public boolean isBusinessDay(LocalDate day) {
        Boolean exception = exceptions.get(day);
        return exception != null ? exception : weekdays.contains(day.getDayOfWeek());
    }

    @Override
    public LocalDate nextBusinessDay(LocalDate day) {
        return search(day, 1);
    }

    @Override
    public LocalDate previousBusinessDay(LocalDate day) {
        return search(day, -1);
    }

    /**
     * The business day a cron slot of a day belongs to.
     *
     * @param day   the day of the slot
     * @param shift what happens on a day that isn't a business day
     * @return the day itself if it's a business day, otherwise the shifted day, or empty for {@code SKIP}
     */
    public Optional<LocalDate> shift(LocalDate day, CalendarShift shift) {
        if (isBusinessDay(day)) {
            return Optional.of(day);
        }
        return switch (shift) {
            case SKIP -> Optional.empty();
            case NEXT -> Optional.of(nextBusinessDay(day));
            case PREVIOUS -> Optional.of(previousBusinessDay(day));
        };
    }

    private LocalDate search(LocalDate day, int direction) {
        for (int i = 1; i <= MAX_SHIFT_DAYS; i++) {
            LocalDate candidate = day.plusDays((long) i * direction);
            if (isBusinessDay(candidate)) {
                return candidate;
            }
        }
        throw new IllegalStateException("calendar " + name + " has no business day within " + MAX_SHIFT_DAYS
                + " days " + (direction > 0 ? "after " : "before ") + day);
    }
}
