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
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

class CronScheduleTest {

    private static final ZoneId VIENNA = ZoneId.of("Europe/Vienna");
    private static final CalendarDefinition BANKING = CalendarDefinitionTest.BANKING;
    private static final Duration GRACE = Duration.ofMinutes(1);

    private static Instant vienna(int month, int day, int hour, int minute, int second) {
        return ZonedDateTime.of(2026, month, day, hour, minute, second, 0, VIENNA).toInstant();
    }

    private static CronSchedule.Due due(String cron, CalendarDefinition calendar, CalendarShift shift, Instant after,
                                        Instant now, Duration grace) {
        return CronSchedule.due(CronExpression.parse(cron), VIENNA, calendar, shift, after, now, grace, 10);
    }

    @Test
    void slotsOnTheWeekendAreSkipped() {
        // the weekend isn't missed: the grace period is long enough to plan it if it were a business day
        CronSchedule.Due due = due("0 0 18 * * ?", BANKING, CalendarShift.SKIP, vienna(11, 20, 18, 0, 0),
                vienna(11, 23, 18, 0, 30), Duration.ofDays(7));
        assertThat(due.occurrences()).containsExactly(new CronSchedule.Occurrence(vienna(11, 23, 18, 0, 0),
                vienna(11, 23, 18, 0, 0), LocalDate.of(2026, 11, 23)));
        assertThat(due.examined()).isEqualTo(vienna(11, 23, 18, 0, 0));
    }

    @Test
    void aSkippedSlotIsExaminedOnlyOnce() {
        CronSchedule.Due due = due("0 0 18 * * ?", BANKING, CalendarShift.SKIP, vienna(11, 20, 18, 0, 0),
                vienna(11, 21, 18, 0, 30), GRACE);
        assertThat(due.occurrences()).isEmpty();
        assertThat(due.examined()).isEqualTo(vienna(11, 21, 18, 0, 0));
    }

    @Test
    void aSlotOnAHolidayMovesToTheNextBusinessDay() {
        String monthly = "0 0 18 8 * ?";
        // on the 8th (a holiday) nothing is due yet, and the slot isn't examined, it is due tomorrow
        CronSchedule.Due onTheHoliday = due(monthly, BANKING, CalendarShift.NEXT, vienna(12, 1, 0, 0, 0),
                vienna(12, 8, 18, 0, 30), GRACE);
        assertThat(onTheHoliday.occurrences()).isEmpty();
        assertThat(onTheHoliday.examined()).isNull();

        CronSchedule.Due nextDay = due(monthly, BANKING, CalendarShift.NEXT, vienna(12, 1, 0, 0, 0),
                vienna(12, 9, 18, 0, 10), GRACE);
        assertThat(nextDay.occurrences()).containsExactly(new CronSchedule.Occurrence(vienna(12, 8, 18, 0, 0),
                vienna(12, 9, 18, 0, 0), LocalDate.of(2026, 12, 9)));
        assertThat(nextDay.examined()).isEqualTo(vienna(12, 8, 18, 0, 0));
    }

    @Test
    void weekendSlotsMovedToMondayCollapseWithTheMondaySlot() {
        CronSchedule.Due due = due("0 0 18 * * ?", BANKING, CalendarShift.NEXT, vienna(11, 20, 18, 0, 0),
                vienna(11, 23, 18, 0, 10), GRACE);
        // three slots, one point in time: the planner's run key makes it one run
        assertThat(due.occurrences()).hasSize(3)
                                     .allSatisfy(o -> assertThat(o.at()).isEqualTo(vienna(11, 23, 18, 0, 0)))
                                     .allSatisfy(o -> assertThat(o.businessDate()).isEqualTo(LocalDate.of(2026, 11,
                                             23)));
        assertThat(due.occurrences()).extracting(CronSchedule.Occurrence::slot)
                                     .containsExactly(vienna(11, 21, 18, 0, 0), vienna(11, 22, 18, 0, 0),
                                             vienna(11, 23, 18, 0, 0));
    }

    @Test
    void aSlotMovedToThePreviousBusinessDayIsPlannedAhead() {
        String newYearsEve = "0 0 18 31 12 ?";
        CronSchedule.Due due = due(newYearsEve, BANKING, CalendarShift.PREVIOUS, vienna(12, 30, 0, 0, 0),
                vienna(12, 30, 18, 0, 10), GRACE);
        assertThat(due.occurrences()).containsExactly(new CronSchedule.Occurrence(vienna(12, 31, 18, 0, 0),
                vienna(12, 30, 18, 0, 0), LocalDate.of(2026, 12, 30)));
        // the slot itself lies in the future: examined, so it isn't planned again
        assertThat(due.examined()).isEqualTo(vienna(12, 31, 18, 0, 0));
    }

    @Test
    void aMissedSlotIsDroppedAfterTheGracePeriod() {
        CronSchedule.Due due = due("0 0 18 31 12 ?", BANKING, CalendarShift.PREVIOUS, vienna(12, 1, 0, 0, 0),
                vienna(12, 30, 20, 0, 0), GRACE);
        assertThat(due.occurrences()).isEmpty();
        assertThat(due.examined()).isEqualTo(vienna(12, 31, 18, 0, 0));
    }

    @Test
    void withoutCalendarEverySlotIsDue() {
        CronSchedule.Due due = due("0 0 * * * ?", null, CalendarShift.SKIP, vienna(11, 21, 10, 0, 0),
                vienna(11, 21, 12, 30, 0), Duration.ofHours(3));
        assertThat(due.occurrences()).extracting(CronSchedule.Occurrence::at)
                                     .containsExactly(vienna(11, 21, 11, 0, 0), vienna(11, 21, 12, 0, 0));
        assertThat(due.occurrences().getFirst().businessDate()).isEqualTo(LocalDate.of(2026, 11, 21));

        CronSchedule.Due limited = CronSchedule.due(CronExpression.parse("0 0 * * * ?"), VIENNA, null,
                CalendarShift.SKIP, vienna(11, 21, 10, 0, 0), vienna(11, 21, 12, 30, 0), Duration.ofHours(3), 1);
        assertThat(limited.occurrences()).hasSize(1);
        assertThat(limited.examined()).isEqualTo(vienna(11, 21, 11, 0, 0));
    }

    @Test
    void theCalendarZoneDecidesWhichDayASlotBelongsTo() {
        // 23:30 UTC on Thursday is Friday in Vienna, on Friday it is Saturday
        CronSchedule.Due due = CronSchedule.due(CronExpression.parse("0 30 23 * * ?"), ZoneOffset.UTC, BANKING,
                CalendarShift.SKIP, Instant.parse("2026-11-19T23:00:00Z"), Instant.parse("2026-11-20T23:31:00Z"),
                Duration.ofDays(2), 10);
        assertThat(due.occurrences()).containsExactly(new CronSchedule.Occurrence(
                Instant.parse("2026-11-19T23:30:00Z"), Instant.parse("2026-11-19T23:30:00Z"),
                LocalDate.of(2026, 11, 20)));
        assertThat(due.examined()).isEqualTo(Instant.parse("2026-11-20T23:30:00Z"));
    }

    @Test
    void theLowerBoundReachesBackForSlotsMovedToTheNextBusinessDay() {
        Instant now = vienna(11, 23, 18, 0, 0);
        assertThat(CronSchedule.lowerBound(null, CalendarShift.NEXT, now, GRACE)).isEqualTo(now.minus(GRACE));
        assertThat(CronSchedule.lowerBound(BANKING, CalendarShift.SKIP, now, GRACE)).isEqualTo(now.minus(GRACE));
        assertThat(CronSchedule.lowerBound(BANKING, CalendarShift.NEXT, now, GRACE))
                .isEqualTo(now.minus(GRACE).minus(Duration.ofDays(14)));
    }
}
