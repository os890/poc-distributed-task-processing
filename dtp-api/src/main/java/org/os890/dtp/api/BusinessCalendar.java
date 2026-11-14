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
package org.os890.dtp.api;

import java.time.LocalDate;
import java.time.ZoneId;

/**
 * A business calendar of the namespace ({@code dtp_calendar}): business weekdays plus exceptions, i.e. holidays and
 * extra business days. Cron triggers and processing slots of jobs refer to calendars; application code reads them
 * via {@link BusinessCalendars}, e.g. to accept an end of day only for a business date.
 */
public interface BusinessCalendar {

    /** The name, unique within the namespace, e.g. {@code AT-BANKING}. */
    String name();

    /** The zone that decides which day it is. */
    ZoneId zone();

    /**
     * Whether a day is a business day.
     *
     * @param day the day
     * @return {@code true} for a business weekday that isn't a holiday, or an extra business day
     */
    boolean isBusinessDay(LocalDate day);

    /**
     * The first business day after a day.
     *
     * @param day the day
     * @return the next business day
     * @throws IllegalStateException if there is none within the search limit of the engine
     */
    LocalDate nextBusinessDay(LocalDate day);

    /**
     * The last business day before a day.
     *
     * @param day the day
     * @return the previous business day
     * @throws IllegalStateException if there is none within the search limit of the engine
     */
    LocalDate previousBusinessDay(LocalDate day);
}
