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
package org.os890.dtp.core.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.os890.dtp.core.domain.CalendarDefinition;

/** Reads business calendars ({@code dtp_calendar}, {@code dtp_calendar_exception}). */
public class CalendarStore {

    private record CalendarRow(UUID id, String name, String zone, String weekdays) {
    }

    /**
     * All calendars of a namespace with their exceptions.
     *
     * @param c         the connection
     * @param namespace the namespace
     * @return the calendars
     * @throws SQLException on DB errors
     */
    public List<CalendarDefinition> loadAll(Connection c, String namespace) throws SQLException {
        List<CalendarRow> rows = Sql.query(c, "SELECT id, name, zone, business_weekdays FROM dtp_calendar"
                + " WHERE namespace = ? ORDER BY name", rs -> new CalendarRow(Sql.uuid(rs, "id"), rs.getString("name"),
                        rs.getString("zone"), rs.getString("business_weekdays")), namespace);
        Map<UUID, Map<LocalDate, Boolean>> exceptions = new HashMap<>();
        Sql.query(c, "SELECT e.calendar_id, e.calendar_day, e.business_day FROM dtp_calendar_exception e"
                        + " JOIN dtp_calendar k ON k.id = e.calendar_id WHERE k.namespace = ?",
                rs -> exceptions.computeIfAbsent(Sql.uuid(rs, "calendar_id"), k -> new HashMap<>())
                                .put(rs.getObject("calendar_day", LocalDate.class), rs.getBoolean("business_day")),
                namespace);
        return rows.stream().map(row -> new CalendarDefinition(row.id(), row.name(), ZoneId.of(row.zone()),
                CalendarDefinition.parseWeekdays(row.weekdays()), exceptions.getOrDefault(row.id(), Map.of())))
                   .toList();
    }
}
