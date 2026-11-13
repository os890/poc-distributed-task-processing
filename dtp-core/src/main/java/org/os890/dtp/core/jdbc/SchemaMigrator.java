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

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;

/**
 * Applies the dtp schema and then the application's job definitions (DD-15), each with its own Flyway history table,
 * so the application's own Flyway setup stays untouched.
 */
public class SchemaMigrator {

    /** Location of the application's definition scripts. */
    public static final String DEFINITIONS_LOCATION = "classpath:db/dtp-definitions";

    private SchemaMigrator() {
    }

    /**
     * Migrates the schema and applies the definitions.
     *
     * @param dataSource  the data source
     * @param dialect     the SQL dialect
     * @param classLoader the class loader that sees the application's definition scripts
     */
    public static void migrate(DataSource dataSource, SqlDialect dialect, ClassLoader classLoader) {
        Flyway.configure(classLoader)
                .dataSource(dataSource)
                .locations("classpath:db/migration/dtp/" + dialect.migrationFolder())
                .table("dtp_schema_history")
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load()
                .migrate();
        Flyway.configure(classLoader)
                .dataSource(dataSource)
                .locations(DEFINITIONS_LOCATION)
                .table("dtp_definition_history")
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .failOnMissingLocations(false)
                .load()
                .migrate();
    }
}
