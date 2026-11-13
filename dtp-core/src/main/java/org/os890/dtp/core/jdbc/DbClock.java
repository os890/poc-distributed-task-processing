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
import java.time.Instant;

/**
 * The DB is the only clock of the cluster (DD-2): lease and due times are compared with the DB time, never with the
 * clock of a node.
 */
public class DbClock {

    private final String query;

    public DbClock(SqlDialect dialect) {
        this.query = "SELECT " + dialect.now() + " AS now";
    }

    /**
     * The current DB time.
     *
     * @param c the connection
     * @return the DB time
     * @throws SQLException on DB errors
     */
    public Instant now(Connection c) throws SQLException {
        return Sql.queryOne(c, query, rs -> Sql.instant(rs, "now"))
                  .orElseThrow(() -> new SQLException("no DB time"));
    }
}
