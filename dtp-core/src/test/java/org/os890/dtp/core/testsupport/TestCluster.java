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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.core.DtpConfig;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.jdbc.JdbcTransactions;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.spi.Transactions;

/**
 * Several dtp nodes in one JVM on one shared in-memory H2 database, with short intervals. Each node is a separate
 * engine with its own loops, exactly like separate deployments.
 */
public class TestCluster implements AutoCloseable {

    public static final String NAMESPACE = "test";

    private static final AtomicInteger DATABASES = new AtomicInteger();

    private final DataSource dataSource;
    private final Transactions tx;
    private final List<DtpEngine> nodes = new ArrayList<>();
    private final JobDefinitions definitions;

    public TestCluster() {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:dtp" + DATABASES.incrementAndGet() + "-" + UUID.randomUUID()
                + ";DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=5000");
        this.dataSource = h2;
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
        DtpEngine engine = DtpEngine.builder()
                .dataSource(dataSource)
                .transactions(tx)
                .steps(new MapStepRegistry(steps))
                .config(config)
                .build()
                .start();
        nodes.add(engine);
        return engine;
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
     * Waits until no run is pending or active and no inbox with an inbox trigger has due items.
     *
     * @param timeout maximum wait
     */
    public void awaitIdle(Duration timeout) {
        await().atMost(timeout).pollInterval(Duration.ofMillis(100)).until(() -> queries().isIdle());
    }

    @Override
    public void close() {
        nodes.forEach(DtpEngine::kill);
        tx.withConnection(c -> c.createStatement().execute("SHUTDOWN"));
    }
}
