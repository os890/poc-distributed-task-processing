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
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * The DB is the only clock of the cluster (DD-2): lease and due times are compared with the DB time, never with the
 * clock of a node. Tests can shift it ({@code dtp-test}): all nodes of a test cluster share the shift, so the cluster
 * moves forward in time together (e.g. to the next cron slot or past a retry backoff).
 */
public class DbClock {

    private static final Supplier<Duration> NO_SHIFT = () -> Duration.ZERO;

    private final String query;
    private final Supplier<Duration> shift;

    public DbClock(SqlDialect dialect) {
        this(dialect, NO_SHIFT);
    }

    /**
     * A clock shifted by a duration (test support).
     *
     * @param dialect the dialect
     * @param shift   the shift, added to the DB time
     */
    public DbClock(SqlDialect dialect, Supplier<Duration> shift) {
        this.query = "SELECT " + dialect.now() + " AS now";
        this.shift = shift == null ? NO_SHIFT : shift;
    }

    /**
     * The current DB time, plus the shift of a test.
     *
     * @param c the connection
     * @return the DB time
     * @throws SQLException on DB errors
     */
    public Instant now(Connection c) throws SQLException {
        return Sql.queryOne(c, query, rs -> Sql.instant(rs, "now"))
                  .orElseThrow(() -> new SQLException("no DB time")).plus(shift.get());
    }
}
