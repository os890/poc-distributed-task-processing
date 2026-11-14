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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class CalendarDefinitionTest {

    /** Business days Monday to Friday, with the bank holidays of December 2026 and January 2027 (sample data). */
    static final CalendarDefinition BANKING = new CalendarDefinition(UUID.randomUUID(), "AT-BANKING",
            ZoneId.of("Europe/Vienna"), CalendarDefinition.parseWeekdays("MON-FRI"), Map.of(
                    LocalDate.of(2026, 12, 8), false,
                    LocalDate.of(2026, 12, 24), false,
                    LocalDate.of(2026, 12, 25), false,
                    LocalDate.of(2026, 12, 31), false,
                    LocalDate.of(2027, 1, 1), false));

    @Test
    void weekdaysAsRangesAndLists() {
        assertThat(CalendarDefinition.parseWeekdays("MON-FRI")).containsExactlyInAnyOrder(DayOfWeek.MONDAY,
                DayOfWeek.TUESDAY, DayOfWeek.WEDNESDAY, DayOfWeek.THURSDAY, DayOfWeek.FRIDAY);
        // a range can wrap around the end of the week
        assertThat(CalendarDefinition.parseWeekdays("sun-thu")).containsExactlyInAnyOrder(DayOfWeek.SUNDAY,
                DayOfWeek.MONDAY, DayOfWeek.TUESDAY, DayOfWeek.WEDNESDAY, DayOfWeek.THURSDAY);
        assertThat(CalendarDefinition.parseWeekdays("MON, WED,FRI")).containsExactlyInAnyOrder(DayOfWeek.MONDAY,
                DayOfWeek.WEDNESDAY, DayOfWeek.FRIDAY);
        assertThat(CalendarDefinition.parseWeekdays("")).isEmpty();
        assertThatThrownBy(() -> CalendarDefinition.parseWeekdays("MON-FRY"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("FRY");
    }

    @Test
    void holidaysAndExtraBusinessDays() {
        assertThat(BANKING.isBusinessDay(LocalDate.of(2026, 11, 20))).as("Friday").isTrue();
        assertThat(BANKING.isBusinessDay(LocalDate.of(2026, 11, 21))).as("Saturday").isFalse();
        assertThat(BANKING.isBusinessDay(LocalDate.of(2026, 12, 8))).as("holiday on a Tuesday").isFalse();

        CalendarDefinition migrationWeekend = new CalendarDefinition(BANKING.id(), BANKING.name(), BANKING.zone(),
                BANKING.weekdays(), Map.of(LocalDate.of(2026, 11, 21), true));
        assertThat(migrationWeekend.isBusinessDay(LocalDate.of(2026, 11, 21))).as("extra business day").isTrue();
    }

    @Test
    void nextAndPreviousBusinessDay() {
        // 24th and 25th are holidays, then the weekend
        assertThat(BANKING.nextBusinessDay(LocalDate.of(2026, 12, 23))).isEqualTo(LocalDate.of(2026, 12, 28));
        // 1st of January is a holiday, 31st of December too
        assertThat(BANKING.previousBusinessDay(LocalDate.of(2027, 1, 4))).isEqualTo(LocalDate.of(2026, 12, 30));
        assertThat(BANKING.nextBusinessDay(LocalDate.of(2026, 11, 19))).isEqualTo(LocalDate.of(2026, 11, 20));
    }

    @Test
    void shiftOfADayThatIsNoBusinessDay() {
        LocalDate holiday = LocalDate.of(2026, 12, 8);
        assertThat(BANKING.shift(holiday, CalendarShift.SKIP)).isEmpty();
        assertThat(BANKING.shift(holiday, CalendarShift.NEXT)).contains(LocalDate.of(2026, 12, 9));
        assertThat(BANKING.shift(holiday, CalendarShift.PREVIOUS)).contains(LocalDate.of(2026, 12, 7));
        LocalDate friday = LocalDate.of(2026, 11, 20);
        assertThat(BANKING.shift(friday, CalendarShift.SKIP)).contains(friday);
    }

    @Test
    void aCalendarWithoutBusinessDaysHasNoNextOne() {
        CalendarDefinition never = new CalendarDefinition(UUID.randomUUID(), "NEVER", ZoneId.of("UTC"), Set.of(),
                Map.of());
        assertThatThrownBy(() -> never.nextBusinessDay(LocalDate.of(2026, 11, 20)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("no business day within 14 days");
        assertThatThrownBy(() -> never.shift(LocalDate.of(2026, 11, 20), CalendarShift.PREVIOUS))
                .isInstanceOf(IllegalStateException.class);
    }
}
