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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.os890.dtp.api.FlowEvent;
import org.os890.dtp.api.ItemType;
import org.os890.dtp.api.JobEngine;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpConfig;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.JobDefinitions;
import org.os890.dtp.core.engine.RunContext;
import org.os890.dtp.core.jdbc.DbClock;
import org.os890.dtp.core.jdbc.JdbcTransactions;
import org.os890.dtp.core.jdbc.SchemaMigrator;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.jdbc.SqlDialect;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.spi.DomainEventPublisher;
import org.os890.dtp.core.spi.StepRegistry;
import org.os890.dtp.core.spi.Transactions;

/**
 * A cluster of simulated nodes in one JVM on a database of its own (design 06 §6.4): the nodes coordinate only via
 * the database, as in production. The database is an in-memory H2 database, or a schema of its own on PostgreSQL
 * ({@link TestDatabase#fromEnvironment()}). The test defines jobs ({@link #definitions()}), starts nodes with its
 * process steps and flows, moves the clock forward ({@link #advance(Duration)}), kills nodes ({@link #kill(String)})
 * and checks the journeys of items ({@link #journey(ItemType, String)}, {@link DtpAssertions}).
 *
 * <p>The nodes have fast settings (polling every 50 ms, leases of 1 s, a node is dead after 2 s), see
 * {@link #config(String)}. The definition scripts of the application ({@code db/dtp-definitions}) are applied like at
 * the start of a node.
 *
 * <p>The process steps are instances given to {@link #start(String, Object...)}, or the beans of a CDI container
 * ({@link #start(String, StepRegistry)}, {@code org.os890.dtp.test.cdi.DtpTestCdi}).
 */
public class DtpTestCluster implements AutoCloseable {

    /** The namespace of the test cluster. */
    public static final String NAMESPACE = "dtp-test";

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL = Duration.ofMillis(25);
    private final TestDatabase database;
    private final DataSource dataSource;
    private final SqlDialect dialect;
    private final Transactions jdbc;
    private final PausingTransactions tx;
    private final AtomicReference<Duration> shift = new AtomicReference<>(Duration.ZERO);
    private final DbClock clock;
    private final Map<String, DtpEngine> nodes = new LinkedHashMap<>();
    private final List<Object> events = new CopyOnWriteArrayList<>();
    private final List<DomainEventPublisher> forwards = new CopyOnWriteArrayList<>();
    private final Map<String, String> placeholders = new HashMap<>();
    private boolean migrated;

    /** A new cluster without nodes on the database the environment configures, see {@link TestDatabase}. */
    public DtpTestCluster() {
        this(TestDatabase.fromEnvironment());
    }

    /**
     * A new cluster without nodes on a database of its own; the cluster drops it on {@link #close()}.
     *
     * @param database the database, e.g. {@link TestDatabase#postgresql(String, String, String)}
     */
    public DtpTestCluster(TestDatabase database) {
        this.database = database;
        this.dataSource = database.dataSource();
        jdbc = new JdbcTransactions(dataSource);
        tx = new PausingTransactions(jdbc);
        dialect = jdbc.withConnection(SqlDialect::detect);
        clock = new DbClock(dialect, shift::get);
    }

    /**
     * Sets a placeholder of the definition scripts, before the first node starts or {@link #definitions()} is used.
     *
     * @param name  the name
     * @param value the value
     * @return this cluster
     */
    public DtpTestCluster placeholder(String name, String value) {
        if (migrated) {
            throw new IllegalStateException("the definition scripts were applied already");
        }
        placeholders.put(name, value);
        return this;
    }

    /**
     * The builder of job definitions; creates the schema on first use.
     *
     * @return the builder
     */
    public JobDefinitions definitions() {
        migrate();
        return new JobDefinitions(tx, NAMESPACE, clock);
    }

    private synchronized void migrate() {
        if (!migrated) {
            SchemaMigrator.migrate(dataSource, dialect, classLoader(), Map.copyOf(placeholders));
            migrated = true;
        }
    }

    private static ClassLoader classLoader() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        return loader != null ? loader : DtpTestCluster.class.getClassLoader();
    }

    /**
     * The fast settings of a node of the test cluster, to change before {@link #start(DtpConfig, Object...)}.
     *
     * @param nodeName the node name
     * @return the settings
     */
    public DtpConfig.Builder config(String nodeName) {
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
                        .definitionPlaceholders(Map.copyOf(placeholders));
    }

    /**
     * Starts a node.
     *
     * @param nodeName       the node name
     * @param stepsAndFlows  the process steps and flow definitions of the node (instances)
     * @return the node
     */
    public DtpEngine start(String nodeName, Object... stepsAndFlows) {
        return start(config(nodeName).build(), stepsAndFlows);
    }

    /**
     * Starts nodes with the same process steps and flows ({@code node-1}, {@code node-2}, ...). The nodes share the
     * instances, as the deployments of one release share their code.
     *
     * @param count          the number of nodes
     * @param stepsAndFlows  the process steps and flow definitions (instances)
     * @return the nodes
     */
    public List<DtpEngine> start(int count, Object... stepsAndFlows) {
        List<DtpEngine> started = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            started.add(start("node-" + (nodes.size() + 1), stepsAndFlows));
        }
        return started;
    }

    /**
     * Starts a node with own settings, e.g. {@code config("green-1").color("green").build()}.
     *
     * @param config         the settings, from {@link #config(String)}
     * @param stepsAndFlows  the process steps and flow definitions (instances)
     * @return the node
     */
    public DtpEngine start(DtpConfig config, Object... stepsAndFlows) {
        return start(config, new InstanceRegistry(stepsAndFlows));
    }

    /**
     * Starts a node whose process steps and flows come from a registry, e.g. the beans of a CDI container
     * ({@code org.os890.dtp.test.cdi.DtpTestCdi}).
     *
     * @param nodeName the node name
     * @param steps    the process steps and flow definitions
     * @return the node
     */
    public DtpEngine start(String nodeName, StepRegistry steps) {
        return start(config(nodeName).build(), steps);
    }

    /**
     * Starts nodes ({@code node-1}, {@code node-2}, ...) whose process steps and flows come from a registry. The nodes
     * share the instances.
     *
     * @param count the number of nodes
     * @param steps the process steps and flow definitions
     * @return the nodes
     */
    public List<DtpEngine> start(int count, StepRegistry steps) {
        List<DtpEngine> started = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            started.add(start("node-" + (nodes.size() + 1), steps));
        }
        return started;
    }

    /**
     * Starts a node with own settings whose process steps and flows come from a registry.
     *
     * @param config the settings, from {@link #config(String)}
     * @param steps  the process steps and flow definitions
     * @return the node
     */
    public synchronized DtpEngine start(DtpConfig config, StepRegistry steps) {
        if (!NAMESPACE.equals(config.namespace())) {
            throw new IllegalArgumentException("the nodes of a test cluster use the namespace " + NAMESPACE);
        }
        if (nodes.containsKey(config.nodeName())) {
            throw new IllegalArgumentException("node " + config.nodeName() + " exists already");
        }
        migrate();
        DtpEngine node = DtpEngine.builder()
                                  .dataSource(dataSource)
                                  .transactions(tx)
                                  .steps(steps)
                                  .config(config)
                                  .events(this::publish)
                                  .clockShift(shift::get)
                                  .build()
                                  .start();
        nodes.put(config.nodeName(), node);
        return node;
    }

    private void publish(Object event) {
        events.add(event);
        forwards.forEach(forward -> forward.publish(event));
    }

    /**
     * Forwards the domain events of all nodes, e.g. as CDI events to the observers of the application
     * ({@code org.os890.dtp.test.cdi.DtpTestCdi}). {@link #events(Class)} records them anyway.
     *
     * @param publisher the publisher
     * @return this cluster
     */
    public DtpTestCluster forwardEvents(DomainEventPublisher publisher) {
        forwards.add(publisher);
        return this;
    }

    /**
     * Starts a flow or routes an item by a signal, like a {@code FlowEvent} of CDI: on the node whose run fires it
     * (joining the unit of work of the run), outside of a run on the first running node.
     *
     * @param event the event
     * @return the item
     */
    public TrackedItem<?, ?> fire(FlowEvent event) {
        RunContext run = RunContext.current();
        DtpEngine node = null;
        if (run != null) {
            synchronized (this) {
                node = nodes.values().stream().filter(n -> n.nodeId().equals(run.engine().nodeId())).findFirst()
                            .orElse(null);
            }
        }
        if (node == null) {
            node = runningNodes().stream().findFirst().orElseThrow(() -> new IllegalStateException("no node runs"));
        }
        return node.fire(event);
    }

    /**
     * A node.
     *
     * @param nodeName the node name
     * @return the node
     */
    public synchronized DtpEngine node(String nodeName) {
        DtpEngine node = nodes.get(nodeName);
        if (node == null) {
            throw new IllegalArgumentException("no node " + nodeName);
        }
        return node;
    }

    /** The nodes that run (not stopped or killed). */
    public synchronized List<DtpEngine> runningNodes() {
        return nodes.values().stream().filter(DtpEngine::isRunning).toList();
    }

    /**
     * The entry point for triggers, of the first running node.
     *
     * @return the job engine
     */
    public JobEngine jobEngine() {
        return runningNodes().stream().findFirst()
                             .orElseThrow(() -> new IllegalStateException("no node runs")).jobEngine();
    }

    /**
     * Simulates a crashed or partitioned node: its loops stop at once, nothing is released; its leases and the
     * leadership expire and the other nodes take over. A handler that runs continues as a zombie, its commits are
     * fenced once its lease was taken over.
     *
     * @param nodeName the node name
     */
    public void kill(String nodeName) {
        node(nodeName).kill();
    }

    /**
     * Stops a node gracefully: its runs stop after their current unit of work and the other nodes continue them.
     *
     * @param nodeName the node name
     */
    public void stop(String nodeName) {
        node(nodeName).stop();
    }

    /**
     * The time of the cluster: the DB time, plus how far the test moved the clock.
     *
     * @return the time
     */
    public Instant now() {
        return tx.withConnection(clock::now);
    }

    /**
     * Moves the clock of all nodes forward, e.g. past a retry backoff, to the next cron slot or past the delay of a
     * decision fallback. The nodes stay alive: waits until no transaction is open, then moves their heartbeats and
     * leases forward with the clock, so the jump doesn't look like a crash. Due work (runs, retries, fallbacks)
     * follows with the next cycles of the nodes, {@link #awaitIdle()} waits for it.
     *
     * @param duration how far, positive
     */
    public void advance(Duration duration) {
        if (duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException("the clock only moves forward: " + duration);
        }
        PausingTransactions.Pause pause = tx.pause(DEFAULT_TIMEOUT);
        try {
            long millis = duration.toMillis();
            jdbc.required(() -> jdbc.withConnection(c -> {
                Sql.update(c, "UPDATE dtp_node SET heartbeat_at = " + dialect.plusMillis("heartbeat_at")
                        + " WHERE namespace = ?", millis, NAMESPACE);
                Sql.update(c, "UPDATE dtp_job_lease SET lease_until = " + dialect.plusMillis("lease_until")
                        + " WHERE namespace = ? AND lease_until IS NOT NULL", millis, NAMESPACE);
                Sql.update(c, "UPDATE dtp_leader SET lease_until = " + dialect.plusMillis("lease_until")
                        + " WHERE namespace = ? AND lease_until IS NOT NULL", millis, NAMESPACE);
                return null;
            }));
            shift.accumulateAndGet(duration, Duration::plus);
        } finally {
            pause.close();
        }
    }

    /**
     * Moves the clock of all nodes forward to a point in time, see {@link #advance(Duration)}.
     *
     * @param time the time, after {@link #now()}
     */
    public void advanceTo(Instant time) {
        advance(Duration.between(now(), time));
    }

    /** Waits up to 30 s until the cluster is idle, see {@link #awaitIdle(Duration)}. */
    public void awaitIdle() {
        awaitIdle(DEFAULT_TIMEOUT);
    }

    /**
     * Waits until the cluster is idle: no run is pending or active, no inbox with an inbox trigger has due items, no
     * fallback or work package is due and no collection window is in progress, for a quiet period of two
     * housekeeping cycles (so work that was just due, e.g. after {@link #advance(Duration)}, is planned first).
     *
     * @param timeout the timeout
     * @throws AssertionError if the cluster isn't idle in time
     */
    public void awaitIdle(Duration timeout) {
        long quietNanos = quietPeriod().toNanos();
        long deadline = System.nanoTime() + timeout.toNanos();
        long idleSince = -1;
        DtpQueries queries = queries();
        while (true) {
            long now = System.nanoTime();
            if (queries.isIdle()) {
                if (idleSince < 0) {
                    idleSince = now;
                } else if (now - idleSince >= quietNanos) {
                    return;
                }
            } else {
                idleSince = -1;
            }
            if (now - deadline > 0) {
                throw new AssertionError("the cluster isn't idle after " + timeout + ", runs: " + queries.runs(10)
                        + ", items: " + queries.itemCounts());
            }
            sleep(POLL);
        }
    }

    private synchronized Duration quietPeriod() {
        Duration longest = Duration.ofMillis(100);
        for (DtpEngine node : nodes.values()) {
            Duration cycle = node.config().housekeepingInterval().plus(node.config().pollInterval());
            if (cycle.compareTo(longest) > 0) {
                longest = cycle;
            }
        }
        return longest.multipliedBy(2);
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }

    /**
     * The journey of an item: its journal entries and where it is now.
     *
     * @param type        the item type
     * @param businessKey the business key
     * @return the journey (without entries if there is no such item)
     */
    public Journey journey(ItemType<?, ?> type, String businessKey) {
        DtpQueries queries = queries();
        List<JourneyEntry> entries = queries.journey(type.name(), businessKey);
        Map<UUID, String> jobsByRun = new HashMap<>();
        entries.stream().map(JourneyEntry::runId).filter(Objects::nonNull).distinct().forEach(run ->
                queries.run(run).ifPresent(view -> jobsByRun.put(run, view.job())));
        return new Journey(type.name(), businessKey, queries.item(type.name(), businessKey).orElse(null), entries,
                jobsByHandler(), jobsByRun);
    }

    private Map<String, String> jobsByHandler() {
        Map<String, String> jobs = new HashMap<>();
        tx.withConnection(c -> Sql.query(c, "SELECT j.name, r.handler_class FROM dtp_job j JOIN dtp_job_revision r"
                + " ON r.job_id = j.id AND r.revision = j.current_revision WHERE j.namespace = ?",
                rs -> jobs.put(rs.getString("handler_class"), rs.getString("name")), NAMESPACE));
        return jobs;
    }

    /**
     * The read-only views of the cluster (runs, packages, nodes, item counts, journal).
     *
     * @return the queries
     */
    public DtpQueries queries() {
        return new DtpQueries(tx, NAMESPACE, clock);
    }

    /**
     * The domain events published by all nodes, in the order of publishing.
     *
     * @param type the event type
     * @param <E>  the event type
     * @return the events
     */
    public <E> List<E> events(Class<E> type) {
        return events.stream().filter(type::isInstance).map(type::cast).toList();
    }

    /**
     * Executes SQL in its own transaction, e.g. to create the tables of the test's entities.
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
     * The transactions of the cluster, e.g. for process steps that write their own tables.
     *
     * @return the transactions
     */
    public Transactions tx() {
        return tx;
    }

    /** The data source of the cluster's database. */
    public DataSource dataSource() {
        return dataSource;
    }

    /** The database of the cluster. */
    public TestDatabase database() {
        return database;
    }

    /** Kills all nodes and drops the database (H2) or its schema (PostgreSQL). */
    @Override
    public synchronized void close() {
        nodes.values().forEach(DtpEngine::kill);
        database.close();
    }
}
