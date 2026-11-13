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
import java.util.Locale;

/** The few SQL differences between the supported databases. */
public enum SqlDialect {

    H2("h2", "CURRENT_TIMESTAMP", "NEXT VALUE FOR %s", false),
    POSTGRESQL("postgresql", "clock_timestamp()", "nextval('%s')", true);

    private final String migrationFolder;
    private final String now;
    private final String nextValue;
    private final boolean onConflictDoNothing;

    SqlDialect(String migrationFolder, String now, String nextValue, boolean onConflictDoNothing) {
        this.migrationFolder = migrationFolder;
        this.now = now;
        this.nextValue = nextValue;
        this.onConflictDoNothing = onConflictDoNothing;
    }

    /**
     * Detects the dialect of a connection.
     *
     * @param connection an open connection
     * @return the dialect
     * @throws SQLException if the metadata can't be read
     */
    public static SqlDialect detect(Connection connection) throws SQLException {
        String product = connection.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
        if (product.contains("h2")) {
            return H2;
        }
        if (product.contains("postgres")) {
            return POSTGRESQL;
        }
        throw new SQLException("unsupported database: " + product);
    }

    /** Folder of the schema migrations below {@code db/migration/dtp}. */
    public String migrationFolder() {
        return migrationFolder;
    }

    /** SQL expression of the current DB time. */
    public String now() {
        return now;
    }

    /**
     * SQL expression of the next value of a sequence.
     *
     * @param sequence the sequence name
     * @return the expression
     */
    public String nextValue(String sequence) {
        return String.format(nextValue, sequence);
    }

    /** {@code true} if duplicates can be ignored via {@code ON CONFLICT DO NOTHING}. */
    public boolean supportsOnConflictDoNothing() {
        return onConflictDoNothing;
    }
}
