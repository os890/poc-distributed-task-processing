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
package org.os890.dtp.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.os890.dtp.api.BusinessCalendars;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.FlowEvent;
import org.os890.dtp.api.Items;
import org.os890.dtp.api.JobEngine;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.api.StopReason;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.domain.FlowModel;
import org.os890.dtp.core.engine.BusinessCalendarsImpl;
import org.os890.dtp.core.engine.ClaimedRun;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.engine.EngineContext;
import org.os890.dtp.core.engine.FlowAdmin;
import org.os890.dtp.core.engine.FlowInstances;
import org.os890.dtp.core.engine.FlowRegistrar;
import org.os890.dtp.core.engine.FlowRouting;
import org.os890.dtp.core.engine.Housekeeping;
import org.os890.dtp.core.engine.ItemRouting;
import org.os890.dtp.core.engine.ItemsImpl;
import org.os890.dtp.core.engine.JobCatalog;
import org.os890.dtp.core.engine.JobEngineImpl;
import org.os890.dtp.core.engine.JsonCodec;
import org.os890.dtp.core.engine.LeaderElector;
import org.os890.dtp.core.engine.RunClaimer;
import org.os890.dtp.core.engine.RunContext;
import org.os890.dtp.core.engine.RunExecutor;
import org.os890.dtp.core.engine.RunPlanner;
import org.os890.dtp.core.engine.StepInfo;
import org.os890.dtp.core.jdbc.DbClock;
import org.os890.dtp.core.jdbc.FlowStore;
import org.os890.dtp.core.jdbc.ItemStore;
import org.os890.dtp.core.jdbc.JdbcTransactions;
import org.os890.dtp.core.jdbc.JobStore;
import org.os890.dtp.core.jdbc.LeaderStore;
import org.os890.dtp.core.jdbc.LeaseStore;
import org.os890.dtp.core.jdbc.NodeStore;
import org.os890.dtp.core.jdbc.RunStore;
import org.os890.dtp.core.jdbc.SchemaMigrator;
import org.os890.dtp.core.jdbc.SqlDialect;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.FlowQueries;
import org.os890.dtp.core.spi.DomainEventPublisher;
import org.os890.dtp.core.spi.StepRegistry;
import org.os890.dtp.core.spi.Transactions;

/**
 * The dtp engine of one node. Every deployment that contains the library starts one engine and so joins the cluster;
 * the database is the only coordination point (D1).
 *
 * <p>Loops of a node: heartbeat and lease renewal, leader election, housekeeping (leader only: planner, lease
 * takeover, dead nodes) and the run claimer (adaptive polling with jitter, DD-11 and DD-14).
 */
public class DtpEngine {

    private static final System.Logger LOG = System.getLogger(DtpEngine.class.getName());

    private static final int MAX_IDLE_BACKOFF = 2;

    private final DataSource dataSource;
    private final Transactions tx;
    private final StepRegistry registry;
    private final DtpConfig config;
    private final DomainEventPublisher events;
    private final UUID nodeId = UUID.randomUUID();
    private final Random random;
    private final ReentrantLock claimLock = new ReentrantLock();

    private EngineContext ctx;
    private LeaderElector leader;
    private Housekeeping housekeeping;
    private RunClaimer claimer;
    private RunExecutor executor;
    private JobEngine jobEngine;
    private Items items;
    private DtpQueries queries;
    private FlowQueries flowQueries;
    private FlowRouting flowRouting;
    private FlowAdmin flowAdmin;
    private Decisions decisions;
    private FlowInstances flowInstances;
    private ScheduledExecutorService scheduler;
    private int idleRounds;

    private volatile boolean running;
    private volatile boolean draining;

    private DtpEngine(Builder builder) {
        this.dataSource = Objects.requireNonNull(builder.dataSource, "dataSource");
        this.registry = Objects.requireNonNull(builder.registry, "steps");
        this.config = builder.config != null ? builder.config : DtpConfig.builder().build();
        this.tx = builder.transactions != null ? builder.transactions : new JdbcTransactions(dataSource);
        this.events = builder.events != null ? builder.events : DomainEventPublisher.NONE;
        long seed = config.randomSeed() != null ? config.randomSeed() ^ nodeId.getLeastSignificantBits()
                                                 : new Random().nextLong();
        this.random = new Random(seed);
    }

    /**
     * A builder.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder of {@link DtpEngine}. */
    public static class Builder {

        private DataSource dataSource;
        private Transactions transactions;
        private StepRegistry registry;
        private DtpConfig config;
        private DomainEventPublisher events;

        public Builder dataSource(DataSource value) {
            dataSource = value;
            return this;
        }

        public Builder transactions(Transactions value) {
            transactions = value;
            return this;
        }

        public Builder steps(StepRegistry value) {
            registry = value;
            return this;
        }

        public Builder config(DtpConfig value) {
            config = value;
            return this;
        }

        /**
         * The publisher of the domain events, e.g. CDI events. Without one, the events are dropped.
         *
         * @param value the publisher
         * @return this builder
         */
        public Builder events(DomainEventPublisher value) {
            events = value;
            return this;
        }

        public DtpEngine build() {
            return new DtpEngine(this);
        }
    }

    /**
     * Starts the node: migrates the schema (optional), validates the process steps, registers the node and starts
     * the loops.
     *
     * @return this engine
     */
    public synchronized DtpEngine start() {
        if (running) {
            return this;
        }
        SqlDialect dialect = tx.withConnection(SqlDialect::detect);
        if (config.migrateSchema()) {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            SchemaMigrator.migrate(dataSource, dialect, loader != null ? loader : getClass().getClassLoader(),
                    config.definitionPlaceholders());
        }
        Map<Class<? extends ProcessStep>, StepInfo> steps = new LinkedHashMap<>();
        registry.stepClasses().forEach(step -> steps.put(step, StepInfo.of(step)));
        JobStore jobStore = new JobStore();
        ctx = new EngineContext(config, nodeId, tx, dialect, new DbClock(dialect), jobStore, new LeaseStore(dialect),
                new RunStore(dialect), new NodeStore(), new LeaderStore(dialect), new ItemStore(dialect),
                new FlowStore(dialect),
                new JobCatalog(tx, jobStore, config.namespace(), config.definitionRefresh().toNanos()),
                new JsonCodec(), registry, Map.copyOf(steps), events);
        RunPlanner planner = new RunPlanner(ctx);
        ItemRouting routing = new ItemRouting(ctx);
        List<FlowModel> flowModels = FlowRegistrar.compile(registry.flowDefinitions());
        Map<Class<? extends FlowDefinition>, String> flowNames = new LinkedHashMap<>();
        for (FlowModel model : flowModels) {
            model.steps().stream().filter(step -> !steps.containsKey(step)).findFirst().ifPresent(step -> {
                throw new IllegalStateException("flow " + model.name() + ": the step " + step.getName()
                        + " isn't a process step of this node");
            });
            flowNames.put(model.definitionClass().asSubclass(FlowDefinition.class), model.name());
        }
        flowRouting = new FlowRouting(ctx, routing, planner, flowNames);
        routing.attach(flowRouting);
        flowAdmin = new FlowAdmin(ctx);
        decisions = routing.decisions();
        flowInstances = new FlowInstances(ctx);
        Set<String> capabilities = steps.keySet().stream().map(Class::getName).collect(Collectors.toSet());
        leader = new LeaderElector(ctx);
        housekeeping = new Housekeeping(ctx, planner);
        claimer = new RunClaimer(ctx, capabilities, random);
        executor = new RunExecutor(ctx, planner, routing, flowRouting, this::claimSoon);
        jobEngine = new JobEngineImpl(ctx, planner);
        items = new ItemsImpl(ctx, routing, null);
        queries = new DtpQueries(tx, config.namespace());
        flowQueries = new FlowQueries(tx, config.namespace());

        tx.required(() -> tx.withConnection(c -> {
            ctx.nodes().register(c, nodeId, config.namespace(), config.nodeName(), hostName(),
                    ctx.json().write(capabilities.stream().sorted().toList()), ctx.clock().now(c));
            return null;
        }));
        new FlowRegistrar(ctx).register(flowModels);
        exportFlows();
        leader.init();

        scheduler = Executors.newScheduledThreadPool(2, Thread.ofPlatform().daemon()
                .name("dtp-" + config.nodeName() + "-", 0).factory());
        long heartbeat = config.heartbeatInterval().toMillis();
        long leaderTick = config.leaderLeaseDuration().toMillis() / 3;
        scheduler.scheduleWithFixedDelay(this::heartbeat, heartbeat, heartbeat, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(leader::tick, jitter(leaderTick / 4), leaderTick, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::housekeeping, jitter(config.housekeepingInterval().toMillis()),
                config.housekeepingInterval().toMillis(), TimeUnit.MILLISECONDS);
        running = true;
        scheduler.schedule(this::claimLoop, jitter(config.pollInterval().toMillis()), TimeUnit.MILLISECONDS);
        LOG.log(System.Logger.Level.INFO, "dtp node {0} ({1}) started in namespace {2} with {3} process steps",
                config.nodeName(), nodeId, config.namespace(), steps.size());
        return this;
    }

    /**
     * Graceful shutdown (drain): no new runs, running runs are asked to stop softly and get time to finish, the
     * leadership is handed over.
     */
    public synchronized void stop() {
        if (!running) {
            return;
        }
        draining = true;
        executor.stopAll(StopReason.DRAIN, config.drainTimeout().toMillis());
        running = false;
        scheduler.shutdownNow();
        leader.resign();
        try {
            tx.required(() -> tx.withConnection(c -> {
                ctx.nodes().setState(c, nodeId, "LEFT");
                return null;
            }));
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "deregistering the node failed", e);
        }
        LOG.log(System.Logger.Level.INFO, "dtp node {0} stopped", config.nodeName());
    }

    /**
     * Test support: simulates a crashed or partitioned node. All loops stop immediately, nothing is released, leases
     * and the leadership expire. Running handlers continue as zombies, their commits are fenced once their leases
     * were taken over.
     */
    public synchronized void kill() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (executor != null) {
            executor.abandon();
        }
    }

    /** Injectable entry point for external triggers. */
    public JobEngine jobEngine() {
        return requireStarted(jobEngine);
    }

    /** Tracks items outside of runs, joining the caller's transaction. */
    public Items items() {
        return requireStarted(items);
    }

    /** Read-only views. */
    public DtpQueries queries() {
        return requireStarted(queries);
    }

    /**
     * Read model of the flows (versions, activations, overrides, diagrams, validation).
     *
     * @return the queries
     */
    public FlowQueries flowQueries() {
        return requireStarted(flowQueries);
    }

    /**
     * Operator actions on flows (activation, overrides, migration).
     *
     * @return the admin
     */
    public FlowAdmin flowAdmin() {
        return requireStarted(flowAdmin);
    }

    /**
     * Decision requests and approvals: open requests per user, decide, vote.
     *
     * @return the decisions
     */
    public Decisions decisions() {
        return requireStarted(decisions);
    }

    /**
     * The business calendars of the namespace.
     *
     * @return the calendars
     */
    public BusinessCalendars calendars() {
        return new BusinessCalendarsImpl(requireStarted(ctx).catalog());
    }

    /**
     * The flow instances and run joins, e.g. to show an end of day and to rerun a failed member.
     *
     * @return the flow instances
     */
    public FlowInstances flowInstances() {
        return requireStarted(flowInstances);
    }

    /**
     * Handles a flow event: starts a flow or routes an item by a signal. Joins the caller's transaction (or the
     * unit of work of the current run).
     *
     * @param event the event
     * @return the item
     */
    public TrackedItem<?, ?> fire(FlowEvent event) {
        return requireStarted(flowRouting).handle(Objects.requireNonNull(event, "event"));
    }

    /** The id of this node. */
    public UUID nodeId() {
        return nodeId;
    }

    /** The settings. */
    public DtpConfig config() {
        return config;
    }

    /** {@code true} while the node runs. */
    public boolean isRunning() {
        return running;
    }

    /** {@code true} while this node leads the housekeeping. */
    public boolean isLeader() {
        return running && leader.isLeader();
    }

    /** Number of runs executed by this node right now. */
    public int activeRuns() {
        return executor == null ? 0 : executor.active().size();
    }

    /**
     * Cancels a run: a pending run is finished as CANCELLED, a running one is asked to stop softly.
     *
     * @param runId the run
     * @return {@code true} if the run was pending or running
     */
    public boolean cancel(UUID runId) {
        return requireStarted(ctx).tx().required(() -> tx.withConnection(c -> {
            var now = ctx.clock().now(c);
            boolean cancelled = ctx.runs().cancel(c, runId, now);
            flowInstances.runEnded(c, runId, now);
            return cancelled;
        }));
    }

    private void heartbeat() {
        try {
            tx.required(() -> tx.withConnection(c -> {
                ctx.nodes().heartbeat(c, nodeId, draining ? "DRAINING" : "ACTIVE", ctx.clock().now(c));
                return null;
            }));
            for (RunContext run : executor.active()) {
                ClaimedRun claimed = run.claimed();
                boolean renewed = tx.required(() -> tx.withConnection(c -> ctx.leases().renew(c, claimed.jobId(),
                        claimed.runId(), claimed.token(), ctx.clock().now(c).plus(claimed.leaseDuration()))));
                // a run that finished between the snapshot and the renewal released its lease itself
                if (!renewed && !tx.withConnection(c -> ctx.runs().finishedWith(c, claimed.runId(),
                        claimed.token()))) {
                    LOG.log(System.Logger.Level.WARNING, "run {0} lost its lease", claimed.runId());
                    run.requestStop(StopReason.LEASE_LOST);
                }
            }
            // processing slots and time limits: a run is asked to stop at the end of its slot or of its time limit,
            // the running unit of work is finished first
            var limited = executor.active().stream().filter(run -> run.claimed().slotEnd() != null
                    || run.claimed().limitAt() != null).toList();
            var now = limited.isEmpty() ? null : tx.withConnection(c -> ctx.clock().now(c));
            for (RunContext run : limited) {
                var slotEnd = run.claimed().slotEnd();
                var limitAt = run.claimed().limitAt();
                if (slotEnd != null && !now.isBefore(slotEnd) && run.shouldContinue()) {
                    LOG.log(System.Logger.Level.INFO, "run {0} of job {1}: its processing slot ended",
                            run.claimed().runId(), run.job().name());
                    run.requestStop(StopReason.SLOT_END);
                } else if (limitAt != null && !now.isBefore(limitAt) && run.shouldContinue()) {
                    LOG.log(System.Logger.Level.INFO, "run {0} of job {1}: its time limit is reached",
                            run.claimed().runId(), run.job().name());
                    run.requestStop(StopReason.LIMIT_REACHED);
                }
            }
            var ids = executor.active().stream().map(run -> run.claimed().runId()).toList();
            if (!ids.isEmpty()) {
                tx.withConnection(c -> ctx.runs().stopRequests(c, ids)).forEach(requested ->
                        executor.active().stream()
                                .filter(run -> run.claimed().runId().equals(requested.id()))
                                .forEach(run -> run.requestStop(requested.stopReason())));
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "heartbeat failed", e);
        }
    }

    private void housekeeping() {
        if (running && leader.isLeader()) {
            housekeeping.tick();
        }
    }

    private void claimLoop() {
        if (!running || draining) {
            return;
        }
        int claimed = claimOnce();
        idleRounds = claimed > 0 ? 0 : Math.min(idleRounds + 1, MAX_IDLE_BACKOFF);
        long delay = jitter(config.pollInterval().toMillis() << idleRounds);
        if (running && !scheduler.isShutdown()) {
            scheduler.schedule(this::claimLoop, delay, TimeUnit.MILLISECONDS);
        }
    }

    private void claimSoon() {
        if (running && !draining && !scheduler.isShutdown()) {
            scheduler.execute(this::claimOnce);
        }
    }

    private int claimOnce() {
        if (!claimLock.tryLock()) {
            return 0;
        }
        try {
            int free = executor.freeSlots();
            if (free <= 0 || !running || draining) {
                return 0;
            }
            var runs = claimer.claim(free);
            runs.forEach(executor::submit);
            return runs.size();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "claiming runs failed", e);
            return 0;
        } finally {
            claimLock.unlock();
        }
    }

    private void exportFlows() {
        if (config.flowExport() == DtpConfig.FlowExport.NONE) {
            return;
        }
        Path path = config.flowExportPath();
        try {
            if (path.toAbsolutePath().getParent() != null) {
                Files.createDirectories(path.toAbsolutePath().getParent());
            }
            Files.writeString(path, flowQueries.exportMarkdown());
            LOG.log(System.Logger.Level.INFO, "dtp flows exported to {0}", path.toAbsolutePath());
        } catch (IOException | RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "exporting the flows to " + path + " failed", e);
        }
    }

    private long jitter(long base) {
        // +-25 % (DD-14)
        return Math.max(1, Math.round(base * (0.75 + random.nextDouble() * 0.5)));
    }

    private <T> T requireStarted(T value) {
        if (value == null) {
            throw new IllegalStateException("the engine isn't started");
        }
        return value;
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }
}
