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

import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;

class CronExpressionTest {

    private static final ZoneId VIENNA = ZoneId.of("Europe/Vienna");

    @Test
    void nightlyJobRunsAtThreeInTheMorning() {
        CronExpression cron = CronExpression.parse("0 0 3 * * ?");
        ZonedDateTime next = cron.next(ZonedDateTime.of(2026, 11, 13, 22, 10, 0, 0, VIENNA));
        assertThat(next).isEqualTo(ZonedDateTime.of(2026, 11, 14, 3, 0, 0, 0, VIENNA));
    }

    @Test
    void nextIsStrictlyAfterTheReference() {
        CronExpression cron = CronExpression.parse("0 0 3 * * ?");
        ZonedDateTime slot = ZonedDateTime.of(2026, 11, 14, 3, 0, 0, 0, VIENNA);
        assertThat(cron.next(slot)).isEqualTo(slot.plusDays(1));
    }

    @Test
    void weekdaysRangesAndSteps() {
        CronExpression cron = CronExpression.parse("0 */15 9-17 ? * MON-FRI");
        // Friday 17:50 -> Monday 09:00
        ZonedDateTime next = cron.next(ZonedDateTime.of(2026, 11, 13, 17, 50, 0, 0, VIENNA));
        assertThat(next).isEqualTo(ZonedDateTime.of(2026, 11, 16, 9, 0, 0, 0, VIENNA));
        assertThat(cron.next(next)).isEqualTo(next.plusMinutes(15));
    }

    @Test
    void everySecond() {
        CronExpression cron = CronExpression.parse("* * * * * ?");
        ZonedDateTime now = ZonedDateTime.of(2026, 11, 13, 12, 0, 0, 500_000_000, VIENNA);
        assertThat(cron.next(now)).isEqualTo(ZonedDateTime.of(2026, 11, 13, 12, 0, 1, 0, VIENNA));
    }

    @Test
    void listsMonthNamesAndDayOfMonth() {
        CronExpression cron = CronExpression.parse("0 30 6 1,15 JAN,JUL ?");
        ZonedDateTime next = cron.next(ZonedDateTime.of(2026, 11, 13, 0, 0, 0, 0, VIENNA));
        assertThat(next).isEqualTo(ZonedDateTime.of(2027, 1, 1, 6, 30, 0, 0, VIENNA));
        assertThat(cron.next(next)).isEqualTo(ZonedDateTime.of(2027, 1, 15, 6, 30, 0, 0, VIENNA));
        assertThat(cron).hasToString("0 30 6 1,15 JAN,JUL ?");
    }

    @Test
    void sundayIsDayOne() {
        CronExpression cron = CronExpression.parse("0 0 12 ? * 1");
        ZonedDateTime next = cron.next(ZonedDateTime.of(2026, 11, 13, 0, 0, 0, 0, VIENNA));
        assertThat(next.getDayOfWeek()).isEqualTo(java.time.DayOfWeek.SUNDAY);
    }

    @Test
    void invalidExpressionsAreRejected() {
        assertThatThrownBy(() -> CronExpression.parse("0 0 3 * *")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CronExpression.parse("0 0 25 * * ?")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CronExpression.parse("0 0 3 L * ?")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CronExpression.parse("0 0 3 * * ? 2027"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CronExpression.parse("0 */0 3 * * ?")).isInstanceOf(IllegalArgumentException.class);
    }
}
