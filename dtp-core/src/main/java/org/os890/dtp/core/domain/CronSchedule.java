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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Which cron slots of a job are due, with its business calendar (docs/design/05-coordination-flows.md §5.1): a slot
 * on a day that isn't a business day is skipped, or moved to the same time on the next or previous business day.
 * Slots moved to the same point in time collapse into one run (the run key is the point in time), e.g. the Saturday
 * and Sunday slots of a daily cron with {@link CalendarShift#NEXT} and the Monday slot.
 */
public class CronSchedule {

    private static final int MAX_ITERATIONS = 10_000;

    private CronSchedule() {
    }

    /**
     * A due slot.
     *
     * @param slot         the slot of the cron expression
     * @param at           when the run is due: the slot, or the slot moved to another business day
     * @param businessDate the business date of the slot, the business key of a flow instance started by the slot
     */
    public record Occurrence(Instant slot, Instant at, LocalDate businessDate) {
    }

    /**
     * The result of a planning step.
     *
     * @param occurrences the due slots, oldest first
     * @param examined    the latest slot that needs no further planning, or {@code null}
     */
    public record Due(List<Occurrence> occurrences, Instant examined) {
    }

    /**
     * The oldest slot the planner looks at: missed slots are planned within the misfire grace period, a slot moved
     * to the next business day is due up to {@link CalendarDefinition#MAX_SHIFT_DAYS} after the slot itself.
     *
     * @param calendar the calendar, or {@code null}
     * @param shift    the shift
     * @param now      the DB time
     * @param grace    the misfire grace period
     * @return the lower bound
     */
    public static Instant lowerBound(CalendarDefinition calendar, CalendarShift shift, Instant now, Duration grace) {
        Instant lower = now.minus(grace);
        return calendar != null && shift == CalendarShift.NEXT
                ? lower.minus(CalendarDefinition.MAX_SHIFT_DAYS, ChronoUnit.DAYS) : lower;
    }

    /**
     * The slots after {@code after} that are due now.
     *
     * @param cron     the cron expression
     * @param zone     the zone of the cron expression
     * @param calendar the calendar, or {@code null} for every day
     * @param shift    what happens on a day that isn't a business day
     * @param after    the latest slot planned before (exclusive)
     * @param now      the DB time
     * @param grace    the misfire grace period: older due points in time are dropped
     * @param max      maximum number of occurrences
     * @return the due slots
     */
    public static Due due(CronExpression cron, ZoneId zone, CalendarDefinition calendar, CalendarShift shift,
                          Instant after, Instant now, Duration grace, int max) {
        // a slot moved to the previous business day is due before the slot itself
        Instant horizon = calendar != null && shift == CalendarShift.PREVIOUS
                ? now.plus(CalendarDefinition.MAX_SHIFT_DAYS, ChronoUnit.DAYS) : now;
        Instant oldest = now.minus(grace);
        List<Occurrence> due = new ArrayList<>();
        Instant examined = null;
        ZonedDateTime slot = cron.next(after.atZone(zone));
        for (int i = 0; i < MAX_ITERATIONS && due.size() < max && !slot.toInstant().isAfter(horizon); i++) {
            Optional<Occurrence> occurrence = occurrence(slot, calendar, shift);
            if (occurrence.isPresent()) {
                Instant at = occurrence.get().at();
                if (at.isAfter(now)) {
                    break;
                }
                if (!at.isBefore(oldest)) {
                    due.add(occurrence.get());
                }
            }
            examined = slot.toInstant();
            slot = cron.next(slot);
        }
        return new Due(due, examined);
    }

    private static Optional<Occurrence> occurrence(ZonedDateTime slot, CalendarDefinition calendar,
                                                   CalendarShift shift) {
        if (calendar == null) {
            return Optional.of(new Occurrence(slot.toInstant(), slot.toInstant(), slot.toLocalDate()));
        }
        LocalDate day = slot.withZoneSameInstant(calendar.zone()).toLocalDate();
        return calendar.shift(day, shift).map(businessDay -> new Occurrence(slot.toInstant(),
                slot.plusDays(ChronoUnit.DAYS.between(day, businessDay)).toInstant(), businessDay));
    }
}
