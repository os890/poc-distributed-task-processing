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
package org.os890.dtp.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * The database of a test cluster: an in-memory H2 database of its own (the default), or a schema of its own in a
 * PostgreSQL database, created for the cluster and dropped with it, so tests can share one database server.
 *
 * <p>{@link #fromEnvironment()} chooses: the system property {@value #URL} (or the environment variable
 * {@code DTP_TEST_JDBC_URL}) with a PostgreSQL URL selects PostgreSQL, user and password come from {@value #USER} and
 * {@value #PASSWORD} ({@code DTP_TEST_JDBC_USER}, {@code DTP_TEST_JDBC_PASSWORD}). A test that starts its database
 * itself, e.g. with Testcontainers, sets the system properties before its first cluster, or creates the cluster with
 * {@link #postgresql(String, String, String)}. PostgreSQL needs the driver {@code org.postgresql:postgresql} on the
 * test classpath.
 */
public class TestDatabase implements AutoCloseable {

    /** The system property of the JDBC URL; without it, the cluster uses H2. */
    public static final String URL = "dtp.test.jdbc-url";
    /** The system property of the user. */
    public static final String USER = "dtp.test.jdbc-user";
    /** The system property of the password. */
    public static final String PASSWORD = "dtp.test.jdbc-password";

    private static final AtomicInteger DATABASES = new AtomicInteger();
    private static final String DEADLOCK = "40P01";

    private final DataSource dataSource;
    private final String description;
    private final String schema;

    private TestDatabase(DataSource dataSource, String description, String schema) {
        this.dataSource = dataSource;
        this.description = description;
        this.schema = schema;
    }

    /**
     * An in-memory H2 database of its own.
     *
     * @return the database
     */
    public static TestDatabase h2() {
        JdbcDataSource h2 = new JdbcDataSource();
        String name = "dtp-test-" + DATABASES.incrementAndGet() + "-" + UUID.randomUUID();
        h2.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=5000");
        return new TestDatabase(h2, "H2 " + name, null);
    }

    /**
     * A schema of its own in a PostgreSQL database, dropped on {@link #close()}.
     *
     * @param url      the JDBC URL, {@code jdbc:postgresql://…}
     * @param user     the user, needs the right to create schemas
     * @param password the password
     * @return the database
     */
    public static TestDatabase postgresql(String url, String user, String password) {
        if (!url.startsWith("jdbc:postgresql:")) {
            throw new IllegalArgumentException("not a PostgreSQL URL: " + url);
        }
        String schema = "dtp_cluster_" + DATABASES.incrementAndGet() + "_"
                + UUID.randomUUID().toString().substring(0, 8);
        try (Connection c = DriverManager.getConnection(url, user, password); Statement s = c.createStatement()) {
            s.execute("CREATE SCHEMA " + schema);
        } catch (SQLException e) {
            throw new IllegalStateException("can't create the schema of the test cluster in " + url
                    + " (is org.postgresql:postgresql on the test classpath?)", e);
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(40);
        // like LOCK_TIMEOUT of the H2 URL: a lock held by a hanging node is a conflict, not an endless wait
        config.setConnectionInitSql("SET search_path TO " + schema + "; SET lock_timeout = '5s'");
        config.setPoolName(schema);
        return new TestDatabase(new HikariDataSource(config), "PostgreSQL " + url + ", schema " + schema, schema);
    }

    /**
     * The database the environment configures: PostgreSQL if {@value #URL} (or {@code DTP_TEST_JDBC_URL}) is set,
     * else H2.
     *
     * @return the database
     */
    public static TestDatabase fromEnvironment() {
        String url = setting(URL);
        if (url == null) {
            return h2();
        }
        return postgresql(url, setting(USER), setting(PASSWORD));
    }

    private static String setting(String property) {
        String value = System.getProperty(property);
        if (value == null || value.isBlank()) {
            value = System.getenv(property.toUpperCase(Locale.ROOT).replace('.', '_').replace('-', '_'));
        }
        return value == null || value.isBlank() ? null : value;
    }

    /** The data source, shared by all nodes of the cluster. */
    public DataSource dataSource() {
        return dataSource;
    }

    /** {@code true} for PostgreSQL. */
    public boolean isPostgresql() {
        return schema != null;
    }

    @Override
    public String toString() {
        return description;
    }

    /**
     * Drops the database: shuts the H2 database down, or drops the schema and closes the pool. The nodes are killed
     * first; a zombie that still queries the schema may make the drop a deadlock victim, then it's tried again.
     */
    @Override
    public void close() {
        if (schema == null) {
            execute("SHUTDOWN");
            return;
        }
        try {
            for (int attempt = 1; ; attempt++) {
                try {
                    execute("DROP SCHEMA " + schema + " CASCADE");
                    return;
                } catch (IllegalStateException e) {
                    if (attempt == 5 || !(e.getCause() instanceof SQLException sql)
                            || !DEADLOCK.equals(sql.getSQLState())) {
                        throw e;
                    }
                    pause(200L * attempt, e);
                }
            }
        } finally {
            ((HikariDataSource) dataSource).close();
        }
    }

    private void execute(String sql) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(sql + " failed in " + description, e);
        }
    }

    private static void pause(long millis, RuntimeException cause) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw cause;
        }
    }
}
