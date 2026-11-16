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
package org.os890.dtp.core.query;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.os890.dtp.core.jdbc.DbClock;
import org.os890.dtp.core.jdbc.MonitoringStore;
import org.os890.dtp.core.jdbc.MonitoringStore.StatusRow;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.jdbc.SqlDialect;
import org.os890.dtp.core.query.FlowQueries.StepDecoration;
import org.os890.dtp.core.spi.Transactions;

/**
 * Reads the monitoring read model (docs/design/09-monitoring-heatmap.md §9.7): the heat map, the minute series and
 * the failures of a step, and the heat map as a Mermaid diagram of a flow. Reads only the views, never the processing
 * tables, so clients can poll it as often as they like. The views lag one projection cycle behind.
 */
public class MonitoringQueries {

    /** Styles of the heat map diagram, light enough for both themes. */
    static final List<String> CLASS_DEFS = List.of(
            "classDef ok fill:#d8f0dc,stroke:#2e7d32,color:#1b1b1b",
            "classDef busy fill:#fff3c4,stroke:#b28704,color:#1b1b1b",
            "classDef jam fill:#ffe2b8,stroke:#e65100,color:#1b1b1b",
            "classDef fail fill:#f8d0d0,stroke:#c62828,color:#1b1b1b",
            "classDef idle fill:#e4e4e4,stroke:#757575,color:#1b1b1b");

    private static final String TOTALS = "SUM(arrived) AS arrived, SUM(departed) AS departed,"
            + " SUM(routed_back) AS routed_back, SUM(completed) AS completed, SUM(discarded) AS discarded,"
            + " SUM(escalated) AS escalated, SUM(failed_business) AS failed_business,"
            + " SUM(failed_technical) AS failed_technical, MAX(max_wait_ms) AS max_wait_ms,"
            + " SUM(sum_wait_ms) AS sum_wait_ms";

    private final Transactions tx;
    private final String namespace;
    private final DbClock clock;
    private final FlowQueries flows;

    /**
     * Creates the queries.
     *
     * @param tx        the transactions
     * @param namespace the namespace
     * @param flows     the flow queries (for the diagram)
     */
    public MonitoringQueries(Transactions tx, String namespace, FlowQueries flows) {
        this(tx, namespace, flows, null);
    }

    /**
     * Creates the queries with the clock of an engine.
     *
     * @param tx        the transactions
     * @param namespace the namespace
     * @param flows     the flow queries (for the diagram)
     * @param clock     the clock, or {@code null} for the DB time
     */
    public MonitoringQueries(Transactions tx, String namespace, FlowQueries flows, DbClock clock) {
        this.tx = tx;
        this.namespace = namespace;
        this.clock = clock != null ? clock : new DbClock(tx.withConnection(SqlDialect::detect));
        this.flows = flows;
    }

    /**
     * Counters of a step over a range.
     *
     * @param arrived         items that arrived
     * @param departed        items that left
     * @param routedBack      of these, routed back
     * @param completed       completed by the step
     * @param discarded       discarded by the step
     * @param escalated       decision requests created
     * @param failedBusiness  business failures
     * @param failedTechnical technical failures
     * @param avgWaitMs       average time from arrival to departure
     * @param maxWaitMs       longest time from arrival to departure
     */
    public record Totals(int arrived, int departed, int routedBack, int completed, int discarded, int escalated,
                         int failedBusiness, int failedTechnical, long avgWaitMs, long maxWaitMs) {

        static final Totals NONE = new Totals(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    /**
     * A step in the heat map.
     *
     * @param step               the job name
     * @param statusReason       PROCESSING, WAITING_FOR_RUN, WAITING_FOR_TRIGGER, IDLE, SLOT_CLOSED, PAUSED_BY_WINDOW,
     *                           NO_CAPABLE_NODE or DISABLED
     * @param queued             due items
     * @param queuedByLane       due items per lane ({@code ""} = no lane)
     * @param oldestQueued       age of the oldest due item, or {@code null}
     * @param waitingForDecision items waiting for a person
     * @param deferred           items waiting for their next attempt
     * @param forked             items waiting for their branches
     * @param packaged           items in pending work packages
     * @param activeRunId        the run holding the lease, or {@code null}
     * @param activeNode         its node
     * @param jamScore           0 to 100
     * @param alertState         OK, CONGESTED or FAILING
     * @param alertSince         since when
     * @param version            the projection cycle that changed the step last
     * @param totals             the counters over the range
     */
    public record StepView(String step, String statusReason, int queued, Map<String, Integer> queuedByLane,
                           Duration oldestQueued, int waitingForDecision, int deferred, int forked, int packaged,
                           UUID activeRunId, String activeNode, int jamScore, String alertState, Instant alertSince,
                           long version, Totals totals) {
    }

    /**
     * Items that moved from one step to another over the range.
     *
     * @param from      the source step
     * @param to        the target step
     * @param backRoute routed back
     * @param items     the number of items
     * @param perMinute the average per minute
     */
    public record EdgeView(String from, String to, boolean backRoute, int items, double perMinute) {
    }

    /**
     * A snapshot of the heat map.
     *
     * @param version        the projection version
     * @param projectedUntil the DB time of the last cycle
     * @param range          the range of the counters
     * @param steps          the steps
     * @param edges          the edges
     */
    public record HeatMap(long version, Instant projectedUntil, Duration range, List<StepView> steps,
                          List<EdgeView> edges) {
    }

    /**
     * A minute of a step.
     *
     * @param minute the start of the minute
     * @param totals the counters
     */
    public record MinuteView(Instant minute, Totals totals) {
    }

    /**
     * An error type of a step.
     *
     * @param errorType     the exception class
     * @param errorHash     the hash of the top stack frames
     * @param occurrences   the number within the range
     * @param sampleMessage one of the messages
     * @param sampleItemId  one of the items (its journey)
     * @param lastSeen      the minute it occurred last
     */
    public record FailureView(String errorType, String errorHash, int occurrences, String sampleMessage,
                              UUID sampleItemId, Instant lastSeen) {
    }

    /**
     * The state of the projection.
     *
     * @param version        incremented every cycle
     * @param projectedUntil the DB time of the last cycle, {@code null} before the first
     */
    public record Projection(long version, Instant projectedUntil) {
    }

    /**
     * The state of the projection: the live feed polls it.
     *
     * @return the version and the time of the last cycle
     */
    public Projection projection() {
        return tx.withConnection(c -> Sql.queryOne(c, "SELECT version, projected_until FROM dtp_projection_state"
                + " WHERE namespace = ? AND projection = ?", rs -> new Projection(rs.getLong("version"),
                Sql.instant(rs, "projected_until")), namespace, MonitoringStore.PROJECTION)
                .orElse(new Projection(0, null)));
    }

    /**
     * The heat map: all steps with their status and counters, and the edges between them.
     *
     * @param range the range of the counters, e.g. the last 15 minutes
     * @return the snapshot
     */
    public HeatMap heatmap(Duration range) {
        return tx.withConnection(c -> {
            Projection projection = projection();
            Instant from = from(c, range);
            Map<UUID, Totals> totals = totals(c, from);
            List<StepView> steps = steps(c, totals, -1);
            Map<UUID, String> names = jobNames(c);
            double minutes = Math.max(1, range.toMinutes());
            List<EdgeView> edges = Sql.query(c, "SELECT from_job_id, to_job_id, back_route, SUM(items) AS items"
                    + " FROM dtp_view_edge_minute WHERE namespace = ? AND minute_at >= ?"
                    + " GROUP BY from_job_id, to_job_id, back_route ORDER BY items DESC",
                    rs -> new EdgeView(names.get(Sql.uuid(rs, "from_job_id")), names.get(Sql.uuid(rs, "to_job_id")),
                            rs.getBoolean("back_route"), rs.getInt("items"),
                            Math.round(rs.getInt("items") / minutes * 10) / 10.0), namespace, from);
            return new HeatMap(projection.version(), projection.projectedUntil(), range, steps, edges);
        });
    }

    /**
     * The steps that changed after a projection version (the live feed), with the counters of the last 15 minutes.
     *
     * @param version the last version a client saw
     * @return the changed steps
     */
    public List<StepView> changedSince(long version) {
        return tx.withConnection(c -> steps(c, totals(c, from(c, Duration.ofMinutes(15))), version));
    }

    /**
     * The minute series of a step.
     *
     * @param step  the job name
     * @param range e.g. the last 24 hours
     * @return the minutes with activity, oldest first
     * @throws IllegalArgumentException for an unknown step
     */
    public List<MinuteView> series(String step, Duration range) {
        return tx.withConnection(c -> {
            UUID jobId = jobId(c, step);
            return Sql.query(c, "SELECT minute_at, " + TOTALS + " FROM dtp_view_step_minute WHERE namespace = ?"
                    + " AND job_id = ? AND minute_at >= ? GROUP BY minute_at ORDER BY minute_at",
                    rs -> new MinuteView(Sql.instant(rs, "minute_at"), totals(rs)), namespace, jobId, from(c, range));
        });
    }

    /**
     * The technical failures of a step per error type, most frequent first. Messages and items may contain
     * business data: only for operators.
     *
     * @param step  the job name
     * @param range e.g. the last hour
     * @return the error types
     * @throws IllegalArgumentException for an unknown step
     */
    public List<FailureView> failures(String step, Duration range) {
        return tx.withConnection(c -> {
            UUID jobId = jobId(c, step);
            Map<String, FailureView> byType = new LinkedHashMap<>();
            Sql.query(c, "SELECT error_type, error_hash, minute_at, occurrences, sample_message, sample_item_id"
                    + " FROM dtp_view_failure_minute WHERE namespace = ? AND job_id = ? AND minute_at >= ?"
                    + " ORDER BY minute_at", rs -> {
                        String key = rs.getString("error_type") + "|" + rs.getString("error_hash");
                        FailureView before = byType.get(key);
                        byType.put(key, new FailureView(rs.getString("error_type"), rs.getString("error_hash"),
                                (before == null ? 0 : before.occurrences()) + rs.getInt("occurrences"),
                                rs.getString("sample_message"), Sql.uuid(rs, "sample_item_id"),
                                Sql.instant(rs, "minute_at")));
                        return null;
                    }, namespace, jobId, from(c, range));
            List<FailureView> failures = new ArrayList<>(byType.values());
            failures.sort((a, b) -> Integer.compare(b.occurrences(), a.occurrences()));
            return failures;
        });
    }

    /**
     * The heat map as a Mermaid diagram of a flow: every step with its queue, status and counters, colored by its
     * alert state (FAILING red, CONGESTED orange, processing yellow, idle or waiting for a slot gray, else green).
     *
     * @param flow  the flow name
     * @param range the range of the counters
     * @return the diagram source
     */
    public String diagram(String flow, Duration range) {
        HeatMap heatmap = heatmap(range);
        Map<String, StepView> byName = new HashMap<>();
        heatmap.steps().forEach(step -> byName.put(step.step(), step));
        Map<UUID, StepDecoration> decorations = new HashMap<>();
        tx.withConnection(c -> jobNames(c)).forEach((id, name) -> {
            StepView step = byName.get(name);
            if (step != null) {
                decorations.put(id, new StepDecoration(lines(step, range), cssClass(step)));
            }
        });
        return flows.mermaid(flow, null, decorations, CLASS_DEFS);
    }

    static List<String> lines(StepView step, Duration range) {
        List<String> lines = new ArrayList<>();
        String queue = step.queued() + " queued";
        if (step.oldestQueued() != null) {
            queue += ", oldest " + human(step.oldestQueued());
        }
        lines.add(queue);
        switch (step.alertState()) {
            case "FAILING" -> lines.add("FAILING: " + step.totals().failedTechnical() + " technical failures");
            case "CONGESTED" -> lines.add("CONGESTED, jam score " + step.jamScore());
            default -> lines.add(step.statusReason() + (step.activeNode() == null ? "" : " on " + step.activeNode()));
        }
        lines.add(step.totals().arrived() + " in / " + step.totals().departed() + " out in " + human(range));
        if (step.waitingForDecision() > 0) {
            lines.add(step.waitingForDecision() + " waiting for a decision");
        }
        return lines;
    }

    static String cssClass(StepView step) {
        return switch (step.alertState()) {
            case "FAILING" -> "fail";
            case "CONGESTED" -> "jam";
            default -> switch (step.statusReason()) {
                case "PROCESSING", "WAITING_FOR_RUN" -> "busy";
                case "NO_CAPABLE_NODE" -> "jam";
                case "IDLE" -> "ok";
                default -> "idle";
            };
        };
    }

    static String human(Duration duration) {
        long seconds = duration.toSeconds();
        if (seconds < 120) {
            return seconds + " s";
        }
        return seconds < 7200 ? seconds / 60 + " min" : seconds / 3600 + " h";
    }

    private Instant from(Connection c, Duration range) throws SQLException {
        return clock.now(c).minus(range).truncatedTo(ChronoUnit.MINUTES);
    }

    private Map<UUID, Totals> totals(Connection c, Instant from) throws SQLException {
        Map<UUID, Totals> totals = new HashMap<>();
        Sql.query(c, "SELECT job_id, " + TOTALS + " FROM dtp_view_step_minute WHERE namespace = ? AND minute_at >= ?"
                + " GROUP BY job_id", rs -> totals.put(Sql.uuid(rs, "job_id"), totals(rs)), namespace, from);
        return totals;
    }

    private static Totals totals(ResultSet rs) throws SQLException {
        int departed = rs.getInt("departed");
        return new Totals(rs.getInt("arrived"), departed, rs.getInt("routed_back"), rs.getInt("completed"),
                rs.getInt("discarded"), rs.getInt("escalated"), rs.getInt("failed_business"),
                rs.getInt("failed_technical"), departed == 0 ? 0 : rs.getLong("sum_wait_ms") / departed,
                rs.getLong("max_wait_ms"));
    }

    private List<StepView> steps(Connection c, Map<UUID, Totals> totals, long afterVersion) throws SQLException {
        Map<UUID, String> nodes = new HashMap<>();
        Sql.query(c, "SELECT id, name FROM dtp_node WHERE namespace = ?", rs -> nodes.put(Sql.uuid(rs, "id"),
                rs.getString("name")), namespace);
        return Sql.query(c, "SELECT * FROM dtp_view_step_status WHERE namespace = ? AND projection_version > ?"
                + " ORDER BY step_name", rs -> view(MonitoringStore.status(rs), totals, nodes), namespace,
                afterVersion);
    }

    private static StepView view(StatusRow row, Map<UUID, Totals> totals, Map<UUID, String> nodes) {
        Map<String, Integer> lanes = new LinkedHashMap<>();
        if (row.queuedByLane() != null) {
            // {"":12,"VIP":3}: written by the projector with Jackson, read here without a JSON library
            for (String pair : row.queuedByLane().replaceAll("[{}\"]", "").split(",")) {
                int colon = pair.lastIndexOf(':');
                if (colon >= 0) {
                    lanes.put(pair.substring(0, colon), Integer.valueOf(pair.substring(colon + 1).trim()));
                }
            }
        }
        return new StepView(row.stepName(), row.statusReason(), row.queued(), lanes,
                row.oldestQueuedMs() == null ? null : Duration.ofMillis(row.oldestQueuedMs()),
                row.waitingForDecision(), row.deferred(), row.forked(), row.packaged(), row.activeRunId(),
                row.activeNodeId() == null ? null : nodes.get(row.activeNodeId()), row.jamScore(), row.alertState(),
                row.alertSince(), row.projectionVersion(), totals.getOrDefault(row.jobId(), Totals.NONE));
    }

    private Map<UUID, String> jobNames(Connection c) throws SQLException {
        Map<UUID, String> names = new HashMap<>();
        Sql.query(c, "SELECT id, name FROM dtp_job WHERE namespace = ?", rs -> names.put(Sql.uuid(rs, "id"),
                rs.getString("name")), namespace);
        return names;
    }

    private UUID jobId(Connection c, String step) throws SQLException {
        return Sql.queryOne(c, "SELECT id FROM dtp_job WHERE namespace = ? AND name = ?", rs -> Sql.uuid(rs, "id"),
                namespace, step).orElseThrow(() -> new IllegalArgumentException("unknown step " + step));
    }
}
