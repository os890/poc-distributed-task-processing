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
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;

/**
 * Quartz-style cron expression: {@code second minute hour day-of-month month day-of-week [year]}, e.g.
 * {@code 0 0 3 * * ?}. Supported per field: {@code *}, {@code ?}, values, lists, ranges, steps and month or weekday
 * names. Not supported (slice 1): {@code L}, {@code W}, {@code #} and a restricted year field.
 */
public class CronExpression {

    private static final List<String> MONTHS =
            List.of("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC");

    private static final List<String> WEEKDAYS = List.of("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT");

    private static final int MAX_ITERATIONS = 100_000;

    private final String expression;
    private final BitSet seconds;
    private final BitSet minutes;
    private final BitSet hours;
    private final BitSet daysOfMonth;
    private final BitSet months;
    private final BitSet daysOfWeek;
    private final boolean anyDayOfMonth;
    private final boolean anyDayOfWeek;

    private CronExpression(String expression) {
        this.expression = expression;
        String[] fields = expression.trim().toUpperCase(Locale.ROOT).split("\\s+");
        if (fields.length < 6 || fields.length > 7) {
            throw new IllegalArgumentException("cron expression needs 6 or 7 fields: " + expression);
        }
        if (fields.length == 7 && !"*".equals(fields[6]) && !"?".equals(fields[6])) {
            throw new IllegalArgumentException("a restricted year field isn't supported: " + expression);
        }
        seconds = parse(fields[0], 0, 59, null, expression);
        minutes = parse(fields[1], 0, 59, null, expression);
        hours = parse(fields[2], 0, 23, null, expression);
        daysOfMonth = parse(fields[3], 1, 31, null, expression);
        months = parse(fields[4], 1, 12, MONTHS, expression);
        daysOfWeek = parse(fields[5], 1, 7, WEEKDAYS, expression);
        anyDayOfMonth = isAny(fields[3]);
        anyDayOfWeek = isAny(fields[5]);
    }

    /**
     * Parses an expression.
     *
     * @param expression the cron expression
     * @return the parsed expression
     * @throws IllegalArgumentException if the expression is invalid or uses unsupported features
     */
    public static CronExpression parse(String expression) {
        return new CronExpression(expression);
    }

    /**
     * Next point in time strictly after the given one.
     *
     * @param after the reference time (its zone is the zone of the expression)
     * @return the next matching time
     */
    public ZonedDateTime next(ZonedDateTime after) {
        ZonedDateTime t = after.truncatedTo(ChronoUnit.SECONDS).plusSeconds(1);
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            if (!months.get(t.getMonthValue())) {
                t = t.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS).plusMonths(1);
            } else if (!dayMatches(t)) {
                t = t.truncatedTo(ChronoUnit.DAYS).plusDays(1);
            } else if (!hours.get(t.getHour())) {
                t = t.truncatedTo(ChronoUnit.HOURS).plusHours(1);
            } else if (!minutes.get(t.getMinute())) {
                t = t.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
            } else if (!seconds.get(t.getSecond())) {
                t = t.plusSeconds(1);
            } else {
                return t;
            }
        }
        throw new IllegalStateException("no next time found for cron expression " + expression);
    }

    @Override
    public String toString() {
        return expression;
    }

    private boolean dayMatches(ZonedDateTime t) {
        boolean dom = daysOfMonth.get(t.getDayOfMonth());
        boolean dow = daysOfWeek.get(quartzDayOfWeek(t.getDayOfWeek()));
        if (anyDayOfMonth && anyDayOfWeek) {
            return true;
        }
        if (anyDayOfMonth) {
            return dow;
        }
        if (anyDayOfWeek) {
            return dom;
        }
        return dom || dow;
    }

    private static int quartzDayOfWeek(DayOfWeek day) {
        // Quartz: 1 = Sunday ... 7 = Saturday
        return day == DayOfWeek.SUNDAY ? 1 : day.getValue() + 1;
    }

    private static boolean isAny(String field) {
        return "*".equals(field) || "?".equals(field);
    }

    private static BitSet parse(String field, int min, int max, List<String> names, String expression) {
        BitSet bits = new BitSet(max + 1);
        for (String part : field.split(",")) {
            int step = 1;
            String range = part;
            int slash = part.indexOf('/');
            if (slash >= 0) {
                step = Integer.parseInt(part.substring(slash + 1));
                range = part.substring(0, slash);
                if (step < 1) {
                    throw new IllegalArgumentException("invalid step in cron expression " + expression);
                }
            }
            int from;
            int to;
            if ("*".equals(range) || "?".equals(range)) {
                from = min;
                to = max;
            } else if (range.contains("-")) {
                String[] bounds = range.split("-", 2);
                from = value(bounds[0], names, min);
                to = value(bounds[1], names, min);
            } else {
                from = value(range, names, min);
                to = slash >= 0 ? max : from;
            }
            if (from < min || to > max || from > to) {
                throw new IllegalArgumentException("value out of range in cron expression " + expression);
            }
            for (int v = from; v <= to; v += step) {
                bits.set(v);
            }
        }
        return bits;
    }

    private static int value(String token, List<String> names, int min) {
        if (names != null) {
            int index = names.indexOf(token);
            if (index >= 0) {
                return index + min;
            }
        }
        try {
            return Integer.parseInt(token);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unsupported cron token '" + token + "'", e);
        }
    }
}
