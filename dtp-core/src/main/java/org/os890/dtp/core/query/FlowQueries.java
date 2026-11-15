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
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.RouteKind;
import org.os890.dtp.core.jdbc.DbClock;
import org.os890.dtp.core.jdbc.FlowStore;
import org.os890.dtp.core.jdbc.FlowStore.FlowRow;
import org.os890.dtp.core.jdbc.FlowStore.RouteRow;
import org.os890.dtp.core.jdbc.FlowStore.VersionRow;
import org.os890.dtp.core.jdbc.JobStore;
import org.os890.dtp.core.jdbc.NodeStore;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.jdbc.SqlDialect;
import org.os890.dtp.core.spi.Transactions;

/**
 * Read model of the flows: versions, activations, overrides, Mermaid diagrams generated from the DB (i.e. what is
 * actually executed, including operator changes) and validation.
 */
public class FlowQueries {

    private static final Pattern JSON_STRING = Pattern.compile("\"([^\"]+)\"");

    private final Transactions tx;
    private final String namespace;
    private final FlowStore flows;
    private final DbClock clock;
    private final JobStore jobs = new JobStore();
    private final NodeStore nodes = new NodeStore();

    /**
     * Creates the queries.
     *
     * @param tx        the transactions
     * @param namespace the namespace
     */
    public FlowQueries(Transactions tx, String namespace) {
        this.tx = tx;
        this.namespace = namespace;
        SqlDialect dialect = tx.withConnection(SqlDialect::detect);
        this.flows = new FlowStore(dialect);
        this.clock = new DbClock(dialect);
    }

    /**
     * A flow.
     *
     * @param name            the name
     * @param definitionClass the class of the Java definition
     * @param activeVersion   hash of the active version, or {@code null}
     * @param versions        the versions, newest first
     * @param laneVersions    hashes of the versions active for single lanes, by lane
     */
    public record FlowView(String name, String definitionClass, String activeVersion, List<VersionView> versions,
                           Map<String, String> laneVersions) {
    }

    /**
     * A version.
     *
     * @param hash      the content hash
     * @param origin    CODE or MANUAL
     * @param createdBy the node (or operator) that created it
     * @param createdAt when
     * @param active    {@code true} if it is the active version
     */
    public record VersionView(String hash, String origin, String createdBy, Instant createdAt, boolean active) {
    }

    /**
     * An activation.
     *
     * @param version     the activated version
     * @param activeFrom  from when
     * @param generated   {@code true} if the framework activated a newly deployed version
     * @param activatedBy who
     * @param reason      why
     * @param createdAt   when it was recorded
     * @param lane        the lane it is for, {@code null} for all lanes
     */
    public record ActivationView(String version, Instant activeFrom, boolean generated, String activatedBy,
                                 String reason, Instant createdAt, String lane) {
    }

    /**
     * An override that is in effect or planned.
     *
     * @param id         the id
     * @param fromJob    the job that reports the signal
     * @param signal     the signal
     * @param kind       what happens instead
     * @param target     the target job or flow
     * @param activeFrom from when
     * @param until      until when
     * @param createdBy  who
     * @param reason     why
     */
    public record OverrideView(UUID id, String fromJob, String signal, String kind, String target,
                               Instant activeFrom, Instant until, String createdBy, String reason) {
    }

    /** Severity of a validation issue. */
    public enum Severity {
        ERROR, WARNING, INFO
    }

    /**
     * A validation issue.
     *
     * @param severity how bad
     * @param flow     the flow
     * @param version  the version
     * @param message  what
     */
    public record Issue(Severity severity, String flow, String version, String message) {
    }

    /**
     * The flows of the namespace.
     *
     * @return the flows
     */
    public List<FlowView> flows() {
        return tx.withConnection(c -> {
            Instant now = clock.now(c);
            List<FlowView> result = new ArrayList<>();
            for (FlowRow flow : flows.flows(c, namespace)) {
                Optional<UUID> active = flows.activeVersion(c, flow.id(), now);
                List<VersionView> versions = flows.versions(c, flow.id()).stream()
                        .map(v -> new VersionView(v.hash(), v.origin(), v.createdBy(), v.createdAt(),
                                active.isPresent() && active.get().equals(v.id())))
                        .toList();
                String activeHash = versions.stream().filter(VersionView::active).map(VersionView::hash)
                                            .findFirst().orElse(null);
                Map<UUID, String> hashes = flows.versions(c, flow.id()).stream()
                        .collect(Collectors.toMap(VersionRow::id, VersionRow::hash));
                Map<String, String> lanes = new TreeMap<>();
                flows.laneVersions(c, flow.id(), now).forEach((lane, id) -> lanes.put(lane, hashes.get(id)));
                result.add(new FlowView(flow.name(), flow.definitionClass(), activeHash, versions, lanes));
            }
            return result;
        });
    }

    /**
     * The activations of a flow, newest first.
     *
     * @param flow the flow name
     * @return the activations
     */
    public List<ActivationView> activations(String flow) {
        return tx.withConnection(c -> {
            FlowRow row = flow(c, flow);
            Map<UUID, String> hashes = flows.versions(c, row.id()).stream()
                    .collect(Collectors.toMap(VersionRow::id, VersionRow::hash));
            return flows.activations(c, row.id()).stream()
                    .map(a -> new ActivationView(a.versionId() == null ? null : hashes.get(a.versionId()),
                            a.activeFrom(), a.generated(), a.activatedBy(), a.reason(), a.createdAt(), a.lane()))
                    .toList();
        });
    }

    /**
     * The overrides of a flow that are in effect or planned.
     *
     * @param flow the flow name
     * @return the overrides
     */
    public List<OverrideView> overrides(String flow) {
        return tx.withConnection(c -> Sql.query(c, "SELECT o.id, fj.name AS from_job, o.signal, o.kind,"
                + " COALESCE(tj.name, tf.name) AS target, o.active_from, o.active_until, o.created_by, o.reason"
                + " FROM dtp_route_override o JOIN dtp_flow f ON f.id = o.flow_id"
                + " JOIN dtp_job fj ON fj.id = o.from_job_id LEFT JOIN dtp_job tj ON tj.id = o.target_job_id"
                + " LEFT JOIN dtp_flow tf ON tf.id = o.target_flow_id WHERE f.namespace = ? AND f.name = ?"
                + " AND (o.active_until IS NULL OR o.active_until > ?) ORDER BY o.created_at",
                rs -> new OverrideView(Sql.uuid(rs, "id"), rs.getString("from_job"), rs.getString("signal"),
                        rs.getString("kind"), rs.getString("target"), Sql.instant(rs, "active_from"),
                        Sql.instant(rs, "active_until"), rs.getString("created_by"), rs.getString("reason")),
                namespace, flow, clock.now(c)));
    }

    /**
     * Mermaid diagram of a flow version, generated from the DB: the triggers of the entry step, item routes
     * ({@code -->}, back routes {@code -.->}), successors of run results ({@code ==>}) and the overrides in effect
     * (red; the overruled route is dashed).
     *
     * @param flow the flow name
     * @param hash the version, or {@code null} for the active one
     * @return the diagram source
     */
    public String mermaid(String flow, String hash) {
        return mermaid(flow, hash, Map.of(), List.of());
    }

    /**
     * Extra lines and a style of a step in a diagram, e.g. its state in the heat map.
     *
     * @param lines    lines below the name of the step (escaped here)
     * @param cssClass a class defined by the caller ({@code classDef}), or {@code null}
     */
    public record StepDecoration(List<String> lines, String cssClass) {

        public StepDecoration {
            lines = List.copyOf(lines);
        }
    }

    /**
     * The diagram of a flow version with decorated steps.
     *
     * @param flow        the flow name
     * @param hash        the version, or {@code null} for the active one
     * @param decorations the decorations by job id
     * @param extraLines  lines appended to the diagram, e.g. {@code classDef}s
     * @return the diagram source
     */
    public String mermaid(String flow, String hash, Map<UUID, StepDecoration> decorations, List<String> extraLines) {
        return tx.withConnection(c -> {
            Instant now = clock.now(c);
            FlowRow row = flow(c, flow);
            VersionRow version = version(c, row, hash, now);
            StringBuilder out = new StringBuilder(mermaid(c, row, version, now, decorations));
            extraLines.forEach(line -> out.append("  ").append(line).append("\n"));
            return out.toString();
        });
    }

    /**
     * Validates the active versions of all flows.
     *
     * @return the issues
     */
    public List<Issue> validate() {
        return tx.withConnection(c -> {
            Instant now = clock.now(c);
            List<Issue> issues = new ArrayList<>();
            for (FlowRow flow : flows.flows(c, namespace)) {
                Optional<UUID> active = flows.activeVersion(c, flow.id(), now);
                if (active.isEmpty()) {
                    issues.add(new Issue(Severity.WARNING, flow.name(), null, "no active version"));
                } else {
                    issues.addAll(validate(c, flow, flows.version(c, active.get())));
                }
                // a version active for a single lane runs too
                for (UUID laneVersion : new LinkedHashSet<>(flows.laneVersions(c, flow.id(), now).values())) {
                    if (active.isEmpty() || !laneVersion.equals(active.get())) {
                        issues.addAll(validate(c, flow, flows.version(c, laneVersion)));
                    }
                }
            }
            return issues;
        });
    }

    /**
     * Validates a version of a flow.
     *
     * @param flow the flow name
     * @param hash the version, or {@code null} for the active one
     * @return the issues
     */
    public List<Issue> validate(String flow, String hash) {
        return tx.withConnection(c -> {
            FlowRow row = flow(c, flow);
            return validate(c, row, version(c, row, hash, clock.now(c)));
        });
    }

    /**
     * Markdown document with the diagram and the validation of the active version of every flow.
     *
     * @return the document
     */
    public String exportMarkdown() {
        StringBuilder md = new StringBuilder("# dtp flows (namespace `").append(namespace).append("`)\n\n")
                .append("Generated from the DB, i.e. including activations and overrides of operators.\n");
        for (FlowView flow : flows()) {
            md.append("\n## ").append(flow.name()).append("\n\n");
            if (flow.activeVersion() == null) {
                md.append("No active version.\n");
                continue;
            }
            md.append("Active version `").append(flow.activeVersion()).append("`, ").append(flow.versions().size())
              .append(flow.versions().size() == 1 ? " version" : " versions").append(", defined by `")
              .append(flow.definitionClass()).append("`.\n\n```mermaid\n").append(mermaid(flow.name(), null))
              .append("```\n");
            List<Issue> issues = validate(flow.name(), null);
            if (!issues.isEmpty()) {
                md.append("\n");
                issues.forEach(i -> md.append("- **").append(i.severity()).append("** ").append(i.message())
                                      .append("\n"));
            }
        }
        return md.toString();
    }

    private String mermaid(Connection c, FlowRow flow, VersionRow version, Instant now) throws SQLException {
        return mermaid(c, flow, version, now, Map.of());
    }

    private String mermaid(Connection c, FlowRow flow, VersionRow version, Instant now,
                           Map<UUID, StepDecoration> decorations) throws SQLException {
        Map<UUID, JobDefinition> jobById = jobs(c);
        List<UUID> steps = orderedSteps(c, version);
        Map<UUID, String> ids = new LinkedHashMap<>();
        steps.forEach(step -> ids.put(step, "s" + ids.size()));
        StringBuilder out = new StringBuilder("flowchart LR\n");
        JobDefinition entry = jobById.get(version.entryJobId());
        out.append("  start([\"").append(escape(String.join("<br/>", triggers(entry)))).append("\"])\n");
        for (UUID step : steps) {
            JobDefinition job = jobById.get(step);
            out.append("  ").append(ids.get(step)).append("[\"").append(escape(simpleName(job.handlerClass())))
               .append("<br/><small>").append(escape(job.name())).append("</small>");
            StepDecoration decoration = decorations.get(step);
            if (decoration != null) {
                decoration.lines().forEach(line -> out.append("<br/>").append(escape(line)));
            }
            out.append("\"]\n");
        }
        List<String> links = new ArrayList<>();
        List<Integer> overruled = new ArrayList<>();
        List<Integer> overriding = new ArrayList<>();
        links.add("start --> " + ids.get(version.entryJobId()));
        Set<String> ends = new TreeSet<>();
        Map<String, RouteRow> overrides = activeOverrides(c, flow, now);
        for (RouteRow route : flows.routes(c, version.id())) {
            if (route.kind().isItemRoute() && overrides.containsKey(route.fromJobId() + "|" + route.signal())) {
                overruled.add(links.size());
            }
            links.addAll(links(route, ids, jobById, ends, c));
        }
        for (RouteRow override : overrides.values()) {
            if (ids.containsKey(override.fromJobId())) {
                overriding.add(links.size());
                links.addAll(links(new RouteRow(override.id(), override.fromJobId(), null, override.signal() + " ⚠",
                        override.kind(), override.targetJobId(), override.targetFlowId(), true), ids, jobById, ends,
                        c));
            }
        }
        ends.forEach(end -> out.append("  ").append(end).append("\n"));
        links.forEach(link -> out.append("  ").append(link).append("\n"));
        overruled.forEach(i -> out.append("  linkStyle ").append(i).append(" stroke:#999,stroke-dasharray:4\n"));
        overriding.forEach(i -> out.append("  linkStyle ").append(i).append(" stroke:#d9480f,stroke-width:2px\n"));
        for (UUID step : steps) {
            StepDecoration decoration = decorations.get(step);
            if (decoration != null && decoration.cssClass() != null) {
                out.append("  class ").append(ids.get(step)).append(' ').append(decoration.cssClass()).append('\n');
            }
        }
        return out.toString();
    }

    private Map<String, RouteRow> activeOverrides(Connection c, FlowRow flow, Instant now) throws SQLException {
        Map<String, RouteRow> active = new LinkedHashMap<>();
        for (RouteRow override : flows.currentOverrides(c, flow.id(), now)) {
            String hop = override.fromJobId() + "|" + override.signal();
            if (!active.containsKey(hop)) {
                Optional<RouteRow> winner = flows.activeOverride(c, flow.id(), override.fromJobId(),
                        override.signal(), now);
                winner.ifPresent(route -> active.put(hop, route));
            }
        }
        return active;
    }

    private List<String> links(RouteRow route, Map<UUID, String> ids, Map<UUID, JobDefinition> jobById,
                               Set<String> ends, Connection c) throws SQLException {
        String from = ids.get(route.fromJobId());
        String label = escape(route.signal());
        return switch (route.kind()) {
            case TO -> List.of(from + " -->|" + label + "| " + node(route.targetJobId(), ids, jobById, ends));
            case BACK -> List.of(from + " -.->|" + label + "| " + node(route.targetJobId(), ids, jobById, ends));
            case RUN -> List.of(from + " ==>|" + label + "| " + node(route.targetJobId(), ids, jobById, ends));
            case COMPLETE -> {
                ends.add("done(((complete)))");
                yield List.of(from + " -->|" + label + "| done");
            }
            case DISCARD -> {
                ends.add("discarded[/discarded/]");
                yield List.of(from + " -->|" + label + "| discarded");
            }
            case FLOW -> {
                String name = flows.flow(c, route.targetFlowId()).name();
                String id = "f_" + name.replaceAll("[^A-Za-z0-9]", "_");
                ends.add(id + "[[\"flow " + escape(name) + "\"]]");
                yield List.of(from + " -->|" + label + "| " + id);
            }
            case FORK -> {
                // the branches run in parallel, the join step gets the item when the last one has finished
                String fork = "fork_" + from + "_" + label.replaceAll("[^A-Za-z0-9]", "_");
                String join = "join_" + from + "_" + label.replaceAll("[^A-Za-z0-9]", "_");
                ends.add(fork + "{{\"fork\"}}");
                ends.add(join + "{{\"join\"}}");
                List<String> edges = new ArrayList<>();
                edges.add(from + " -->|" + label + "| " + fork);
                List<UUID> branches = flows.branches(c, route.id());
                for (UUID branch : branches) {
                    edges.add(fork + " --> " + node(branch, ids, jobById, ends));
                }
                for (UUID branch : branches) {
                    edges.add(node(branch, ids, jobById, ends) + " --> " + join);
                }
                edges.add(join + " --> " + node(route.targetJobId(), ids, jobById, ends));
                yield edges;
            }
        };
    }

    private static String node(UUID jobId, Map<UUID, String> ids, Map<UUID, JobDefinition> jobById,
                               Set<String> ends) {
        String id = ids.get(jobId);
        if (id != null) {
            return id;
        }
        // the target of an override that isn't a step of the version
        JobDefinition job = jobById.get(jobId);
        String extra = "x_" + job.name().replaceAll("[^A-Za-z0-9]", "_");
        ends.add(extra + "[\"" + escape(simpleName(job.handlerClass())) + "<br/><small>" + escape(job.name())
                + "</small>\"]");
        return extra;
    }

    private List<Issue> validate(Connection c, FlowRow flow, VersionRow version) throws SQLException {
        Map<UUID, JobDefinition> jobById = jobs(c);
        List<UUID> steps = flows.steps(c, version.id());
        List<RouteRow> routes = flows.routes(c, version.id());
        List<Issue> issues = new ArrayList<>();
        Function<String, Issue> warning = m -> new Issue(Severity.WARNING, flow.name(), version.hash(), m);

        JobDefinition entry = jobById.get(version.entryJobId());
        if (triggers(entry).equals(List.of("FlowEvent.start"))) {
            issues.add(warning.apply("the entry step " + entry.name() + " has no trigger, configure it in the DB"));
        }

        Set<String> capable = capabilities(c);
        for (UUID step : steps) {
            JobDefinition job = jobById.get(step);
            if (!capable.contains(job.handlerClass())) {
                issues.add(new Issue(Severity.ERROR, flow.name(), version.hash(), "no active node can run "
                        + job.name() + " (" + job.handlerClass() + ")"));
            }
        }

        Map<UUID, Set<UUID>> successors = new HashMap<>();
        for (RouteRow route : routes) {
            if (route.kind() == RouteKind.FORK) {
                for (UUID branch : flows.branches(c, route.id())) {
                    successors.computeIfAbsent(route.fromJobId(), k -> new HashSet<>()).add(branch);
                    successors.computeIfAbsent(branch, k -> new HashSet<>()).add(route.targetJobId());
                }
            } else if (route.targetJobId() != null) {
                successors.computeIfAbsent(route.fromJobId(), k -> new HashSet<>()).add(route.targetJobId());
            }
        }
        Set<UUID> reachable = reachable(version.entryJobId(), successors);
        for (UUID step : steps) {
            if (!reachable.contains(step)) {
                issues.add(warning.apply(jobById.get(step).name() + " is unreachable from the entry step"));
            }
        }
        for (UUID step : steps) {
            boolean inLoop = successors.getOrDefault(step, Set.of()).stream()
                                       .anyMatch(next -> reachable(next, successors).contains(step));
            JobDefinition job = jobById.get(step);
            if (inLoop && job.maxItemVisits() == null) {
                issues.add(warning.apply(job.name() + " is part of a loop but has no max_item_visits"));
            }
        }

        Map<String, Set<String>> routed = new LinkedHashMap<>();
        for (RouteRow route : routes) {
            if (route.kind().isItemRoute()) {
                String key = route.fromJobId() + "|" + route.signalType();
                routed.computeIfAbsent(key, k -> new HashSet<>()).add(route.signal());
            }
        }
        for (Map.Entry<String, Set<String>> e : routed.entrySet()) {
            String[] key = e.getKey().split("\\|", 2);
            for (String constant : enumConstants(key[1])) {
                if (!e.getValue().contains(constant)) {
                    issues.add(warning.apply(jobById.get(UUID.fromString(key[0])).name() + ": "
                            + simpleName(key[1]) + "." + constant + " has no route"));
                }
            }
        }
        return issues;
    }

    private static Set<UUID> reachable(UUID start, Map<UUID, Set<UUID>> successors) {
        Set<UUID> seen = new LinkedHashSet<>();
        Deque<UUID> todo = new ArrayDeque<>(List.of(start));
        while (!todo.isEmpty()) {
            UUID next = todo.pop();
            if (seen.add(next)) {
                todo.addAll(successors.getOrDefault(next, Set.of()));
            }
        }
        return seen;
    }

    private static List<String> enumConstants(String type) {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            Class<?> signalType = Class.forName(type, false, loader != null ? loader
                                                                         : FlowQueries.class.getClassLoader());
            Object[] constants = signalType.getEnumConstants();
            if (constants == null) {
                return List.of();
            }
            List<String> names = new ArrayList<>();
            for (Object constant : constants) {
                names.add(((Enum<?>) constant).name());
            }
            return names;
        } catch (ClassNotFoundException | LinkageError e) {
            return List.of();
        }
    }

    private Set<String> capabilities(Connection c) throws SQLException {
        Set<String> capable = new HashSet<>();
        for (String json : nodes.activeCapabilities(c, namespace)) {
            if (json != null) {
                Matcher m = JSON_STRING.matcher(json);
                while (m.find()) {
                    capable.add(m.group(1));
                }
            }
        }
        return capable;
    }

    private List<UUID> orderedSteps(Connection c, VersionRow version) throws SQLException {
        Map<UUID, Set<UUID>> successors = new HashMap<>();
        for (RouteRow route : flows.routes(c, version.id())) {
            if (route.targetJobId() != null) {
                successors.computeIfAbsent(route.fromJobId(), k -> new TreeSet<>()).add(route.targetJobId());
            }
        }
        List<UUID> ordered = new ArrayList<>(reachable(version.entryJobId(), successors));
        Set<UUID> all = new HashSet<>(flows.steps(c, version.id()));
        ordered.retainAll(all);
        all.stream().filter(step -> !ordered.contains(step)).sorted().forEach(ordered::add);
        return ordered;
    }

    private static List<String> triggers(JobDefinition job) {
        List<String> triggers = new ArrayList<>();
        if (job.asap()) {
            triggers.add("asap");
        }
        if (job.cronExpression() != null) {
            triggers.add("cron " + job.cronExpression() + (job.cronZone() != null ? " " + job.cronZone() : ""));
        }
        if (job.external()) {
            triggers.add("external");
        }
        if (job.inboxMinItems() != null) {
            triggers.add("inbox ≥ " + job.inboxMinItems());
        }
        triggers.add("FlowEvent.start");
        return triggers;
    }

    private Map<UUID, JobDefinition> jobs(Connection c) throws SQLException {
        return jobs.loadCurrent(c, namespace).stream()
                   .collect(Collectors.toMap(JobDefinition::id, Function.identity()));
    }

    private FlowRow flow(Connection c, String name) throws SQLException {
        return flows.flow(c, namespace, name).orElseThrow(() -> new IllegalArgumentException("unknown flow " + name));
    }

    private VersionRow version(Connection c, FlowRow flow, String hash, Instant now) throws SQLException {
        if (hash != null) {
            return flows.version(c, flow.id(), hash).orElseThrow(() -> new IllegalArgumentException("flow "
                    + flow.name() + " has no (unique) version " + hash));
        }
        UUID active = flows.activeVersion(c, flow.id(), now)
                           .orElseThrow(() -> new IllegalArgumentException("flow " + flow.name()
                                   + " has no active version"));
        return flows.version(c, active);
    }

    private static String simpleName(String className) {
        return className.substring(className.lastIndexOf('.') + 1).replace('$', '.');
    }

    private static String escape(String text) {
        return text.replace("\"", "#quot;").replace("|", "#124;");
    }
}
