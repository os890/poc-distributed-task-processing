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
package org.os890.dtp.core.testsupport;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.core.DtpConfig;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.jdbc.DbClock;
import org.os890.dtp.core.jdbc.JdbcTransactions;
import org.os890.dtp.core.jdbc.SchemaMigrator;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.jdbc.SqlDialect;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.spi.Telemetry;
import org.os890.dtp.core.spi.Transactions;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Several dtp nodes in one JVM on one shared in-memory H2 database, with short intervals. Each node is a separate
 * engine with its own loops, exactly like separate deployments.
 */
public class TestCluster implements AutoCloseable {

    public static final String NAMESPACE = "test";

    private static final AtomicInteger DATABASES = new AtomicInteger();

    private final DataSource dataSource;
    private final Transactions tx;
    /** Runs the cluster tests on PostgreSQL instead of H2: {@code -Ddtp.test.db=postgresql}. */
    public static final boolean POSTGRESQL = "postgresql".equals(System.getProperty("dtp.test.db"));

    private final String schema;
    private final List<DtpEngine> nodes = new ArrayList<>();
    private final JobDefinitions definitions;
    private final List<Published> events = new CopyOnWriteArrayList<>();

    /**
     * A domain event published by a node.
     *
     * @param node  the node name
     * @param event the event
     */
    public record Published(String node, Object event) {
    }

    public TestCluster() {
        if (POSTGRESQL) {
            // one schema per cluster, dropped at the end (tools/postgresql/run.sh starts a local PostgreSQL)
            schema = "dtp_test_" + DATABASES.incrementAndGet() + "_" + UUID.randomUUID().toString().substring(0, 8);
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(System.getProperty("dtp.test.db.url", "jdbc:postgresql://127.0.0.1:5433/dtp"));
            config.setUsername(System.getProperty("dtp.test.db.user", "dtp"));
            config.setPassword(System.getProperty("dtp.test.db.password", "dtp"));
            config.setMaximumPoolSize(40);
            // like LOCK_TIMEOUT of the H2 URL: a lock held by a hanging node is a conflict, not an endless wait
            config.setConnectionInitSql("SET search_path TO " + schema + "; SET lock_timeout = '5s'");
            config.setPoolName(schema);
            HikariDataSource pool = new HikariDataSource(config);
            try (var c = pool.getConnection(); var s = c.createStatement()) {
                s.execute("CREATE SCHEMA " + schema);
            } catch (java.sql.SQLException e) {
                pool.close();
                throw new IllegalStateException(e);
            }
            pool.close();
            // the schema exists now: a new pool whose connections start in it
            this.dataSource = new HikariDataSource(config);
        } else {
            schema = null;
            JdbcDataSource h2 = new JdbcDataSource();
            h2.setURL("jdbc:h2:mem:dtp" + DATABASES.incrementAndGet() + "-" + UUID.randomUUID()
                    + ";DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=5000");
            this.dataSource = h2;
        }
        // one instance for all nodes: the transaction is bound to the thread, so sharing it is safe, and handlers can
        // join the unit of work of their run via withConnection
        this.tx = new JdbcTransactions(dataSource);
        this.definitions = new JobDefinitions(tx, NAMESPACE);
    }

    /** Fast settings for tests. */
    public static DtpConfig.Builder config(String nodeName) {
        return DtpConfig.builder()
                .namespace(NAMESPACE)
                .nodeName(nodeName)
                .pollInterval(Duration.ofMillis(50))
                .housekeepingInterval(Duration.ofMillis(100))
                .heartbeatInterval(Duration.ofMillis(200))
                .leaseDuration(Duration.ofSeconds(1))
                .leaderLeaseDuration(Duration.ofMillis(900))
                .deadAfter(Duration.ofSeconds(2))
                .definitionRefresh(Duration.ofMillis(100))
                .drainTimeout(Duration.ofSeconds(5))
                .randomSeed(42L);
    }

    /**
     * Starts a node.
     *
     * @param name  the node name
     * @param steps the process steps of the node
     * @return the started engine
     */
    public DtpEngine start(String name, ProcessStep... steps) {
        return start(config(name).build(), steps);
    }

    /**
     * Starts a node.
     *
     * @param config the settings
     * @param steps  the process steps of the node
     * @return the started engine
     */
    public DtpEngine start(DtpConfig config, ProcessStep... steps) {
        return start(config, new MapStepRegistry(steps));
    }

    /**
     * Starts a node with flow definitions.
     *
     * @param name  the node name
     * @param flows the flow definitions
     * @param steps the process steps
     * @return the started node
     */
    public DtpEngine start(String name, List<FlowDefinition> flows, ProcessStep... steps) {
        return start(config(name).build(), new MapStepRegistry(steps).flows(flows.toArray(FlowDefinition[]::new)));
    }

    /**
     * Starts a node.
     *
     * @param config   the settings
     * @param registry the steps and flows
     * @return the started node
     */
    public DtpEngine start(DtpConfig config, MapStepRegistry registry) {
        return start(config, registry, Telemetry.NONE);
    }

    /**
     * Starts a node with telemetry.
     *
     * @param config    the settings
     * @param registry  the steps and flows
     * @param telemetry the telemetry
     * @return the started node
     */
    public DtpEngine start(DtpConfig config, MapStepRegistry registry, Telemetry telemetry) {
        DtpEngine engine = DtpEngine.builder()
                .telemetry(telemetry)
                .dataSource(dataSource)
                .transactions(tx)
                .steps(registry)
                .config(config)
                .events(event -> events.add(new Published(config.nodeName(), event)))
                .build()
                .start();
        nodes.add(engine);
        return engine;
    }

    /**
     * The domain events published by all nodes, in the order of publishing.
     *
     * @param type the event type
     * @param <E>  the event type
     * @return the events
     */
    public <E> List<E> events(Class<E> type) {
        return events.stream().map(Published::event).filter(type::isInstance).map(type::cast).toList();
    }

    /**
     * The domain events published by all nodes.
     *
     * @return the events
     */
    public List<Published> published() {
        return List.copyOf(events);
    }

    /** Creates the schema without starting a node, e.g. to define jobs before the first node starts. */
    public void migrate() {
        SchemaMigrator.migrate(dataSource, tx.withConnection(SqlDialect::detect), getClass().getClassLoader(),
                Map.of());
    }

    public Transactions tx() {
        return tx;
    }

    public DataSource dataSource() {
        return dataSource;
    }

    public JobDefinitions definitions() {
        return definitions;
    }

    public DtpQueries queries() {
        return new DtpQueries(tx, NAMESPACE);
    }

    /**
     * Executes SQL in its own transaction (or the current one).
     *
     * @param sql    the statement
     * @param params the parameters
     */
    public void execute(String sql, Object... params) {
        tx.required(() -> tx.withConnection(c -> Sql.update(c, sql, params)));
    }

    /**
     * Counts rows.
     *
     * @param sql    a {@code SELECT COUNT(*)} query
     * @param params the parameters
     * @return the count
     */
    public int count(String sql, Object... params) {
        return tx.withConnection(c -> Sql.queryOne(c, sql, rs -> rs.getInt(1), params).orElse(0));
    }

    /**
     * The time of the database, the clock of the engine (a database in a VM, e.g. in Podman, may differ from the
     * clock of the test).
     *
     * @return the DB time
     */
    public Instant dbNow() {
        return tx.withConnection(c -> new DbClock(SqlDialect.detect(c)).now(c));
    }

    /**
     * Waits until the cluster is idle.
     *
     * @param timeout the timeout
     */
    public void awaitIdle(Duration timeout) {
        await().atMost(timeout).pollInterval(Duration.ofMillis(100)).until(() -> queries().isIdle());
    }

    @Override
    public void close() {
        nodes.forEach(DtpEngine::kill);
        if (schema != null) {
            // zombies of killed nodes may still hold connections: the schema goes with the pool; a zombie that still
            // queries the schema may make the drop a deadlock victim, then it's tried again
            try {
                dropSchema();
            } finally {
                ((HikariDataSource) dataSource).close();
            }
        } else {
            tx.withConnection(c -> c.createStatement().execute("SHUTDOWN"));
        }
    }

    private void dropSchema() {
        for (int attempt = 1; ; attempt++) {
            try {
                tx.withConnection(c -> c.createStatement().execute("DROP SCHEMA " + schema + " CASCADE"));
                return;
            } catch (RuntimeException e) {
                if (attempt == 5 || !(e.getCause() instanceof java.sql.SQLException sql)
                        || !"40P01".equals(sql.getSQLState())) {
                    throw e;
                }
                try {
                    Thread.sleep(200L * attempt);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }
}
