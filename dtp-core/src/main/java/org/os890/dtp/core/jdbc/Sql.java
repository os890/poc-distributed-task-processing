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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Minimal JDBC helper: binds parameters by type and maps rows. */
public class Sql {

    /** SQLState of a unique constraint violation (PostgreSQL and H2). */
    public static final String UNIQUE_VIOLATION = "23505";

    private Sql() {
    }

    /**
     * Maps one row.
     *
     * @param <T> result type
     */
    @FunctionalInterface
    public interface RowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    /**
     * Executes an update.
     *
     * @param c      the connection
     * @param sql    the statement
     * @param params the parameters
     * @return the update count
     * @throws SQLException on DB errors
     */
    public static int update(Connection c, String sql, Object... params) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, params);
            return ps.executeUpdate();
        }
    }

    /**
     * Executes a query.
     *
     * @param c      the connection
     * @param sql    the query
     * @param mapper the row mapper
     * @param params the parameters
     * @param <T>    result type
     * @return all rows
     * @throws SQLException on DB errors
     */
    public static <T> List<T> query(Connection c, String sql, RowMapper<T> mapper, Object... params)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                List<T> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(mapper.map(rs));
                }
                return result;
            }
        }
    }

    /**
     * Executes a query that returns at most one row.
     *
     * @param c      the connection
     * @param sql    the query
     * @param mapper the row mapper
     * @param params the parameters
     * @param <T>    result type
     * @return the row, if any
     * @throws SQLException on DB errors
     */
    public static <T> Optional<T> queryOne(Connection c, String sql, RowMapper<T> mapper, Object... params)
            throws SQLException {
        List<T> rows = query(c, sql, mapper, params);
        return rows.isEmpty() ? Optional.empty() : Optional.ofNullable(rows.getFirst());
    }

    /**
     * Reads a UUID column.
     *
     * @param rs     the result set
     * @param column the column
     * @return the value or {@code null}
     * @throws SQLException on DB errors
     */
    public static UUID uuid(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column, UUID.class);
    }

    /**
     * Reads a timestamp column.
     *
     * @param rs     the result set
     * @param column the column
     * @return the value or {@code null}
     * @throws SQLException on DB errors
     */
    public static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    /**
     * Reads a nullable int column.
     *
     * @param rs     the result set
     * @param column the column
     * @return the value or {@code null}
     * @throws SQLException on DB errors
     */
    public static Integer integer(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static void bind(PreparedStatement ps, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            Object p = params[i];
            int index = i + 1;
            switch (p) {
                case null -> ps.setNull(index, Types.NULL);
                case Instant instant -> ps.setObject(index, OffsetDateTime.ofInstant(instant, ZoneOffset.UTC));
                case Enum<?> e -> ps.setString(index, e.name());
                case UUID uuid -> ps.setObject(index, uuid);
                case String s -> ps.setString(index, s);
                case Integer n -> ps.setInt(index, n);
                case Long n -> ps.setLong(index, n);
                case Boolean b -> ps.setBoolean(index, b);
                default -> ps.setObject(index, p);
            }
        }
    }
}
