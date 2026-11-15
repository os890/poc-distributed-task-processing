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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.query.JourneyGraph.Edge;
import org.os890.dtp.core.query.JourneyGraph.EdgeKind;
import org.os890.dtp.core.query.JourneyGraph.Node;
import org.os890.dtp.core.query.JourneyGraph.NodeKind;
import org.os890.dtp.core.query.JourneyGraph.Style;
import org.os890.dtp.core.query.JourneyQueries.BranchInfo;
import org.os890.dtp.core.query.JourneyQueries.Context;
import org.os890.dtp.core.query.JourneyQueries.DecisionInfo;
import org.os890.dtp.core.query.JourneyQueries.Detail;
import org.os890.dtp.core.query.JourneyQueries.Options;
import org.os890.dtp.core.query.JourneyQueries.PackageInfo;

/**
 * Folds the journal entries of an item into a {@link JourneyGraph} (docs/design/09-monitoring-heatmap.md §9.9,
 * folding rules): every step once, with its visits; repeated edges between the same nodes collapse into one edge with
 * all entry numbers; failed attempts with a retry become a self-loop; decisions and packages are nodes of their own;
 * an unfinished journey ends at the current place of the item, annotated with the reason why it waits.
 */
class JourneyFolder {

    static final int MAX_TEXT = 60;

    private static final int MAX_ENTRIES = 4;
    private static final String START = "start";
    // "end" is a keyword of Mermaid
    private static final String END = "finish";
    private static final String FORK_INTO = "fork into ";
    private static final String JOIN_AT = ", join at ";

    private final Context ctx;
    private final Options options;
    private final DateTimeFormatter seconds;
    private final DateTimeFormatter minutes;
    private final Map<String, NodeBuilder> nodes = new LinkedHashMap<>();
    private final Map<String, EdgeBuilder> edges = new LinkedHashMap<>();
    private final Map<String, String> ids = new HashMap<>();
    private int steps;
    private int decisions;
    private int packages;
    private String position;
    private JourneyEntry trackedAt;

    JourneyFolder(Context ctx, Options options) {
        this.ctx = ctx;
        this.options = options;
        this.seconds = DateTimeFormatter.ofPattern("EEE HH:mm:ss", Locale.ENGLISH).withZone(options.zone());
        this.minutes = DateTimeFormatter.ofPattern("EEE HH:mm", Locale.ENGLISH).withZone(options.zone());
    }

    static JourneyGraph fold(Context ctx, Options options) {
        return new JourneyFolder(ctx, options).fold();
    }

    private JourneyGraph fold() {
        List<JourneyEntry> entries = ctx.entries();
        for (int i = 0; i < entries.size(); i++) {
            apply(entries.get(i), i + 1 < entries.size() ? entries.get(i + 1) : null);
        }
        markCurrent();
        List<Node> folded = nodes.values().stream().map(NodeBuilder::build).toList();
        List<Edge> labeled = edges.values().stream().map(EdgeBuilder::build).toList();
        return new JourneyGraph(ctx.item().id(), ctx.item().itemType(), ctx.item().businessKey(),
                ctx.item().version(), ctx.item().state(), folded, labeled);
    }

    private void apply(JourneyEntry e, JourneyEntry next) {
        switch (e.type()) {
            case "TRACKED" -> {
                trackedAt = e;
                NodeBuilder start = node(START, NodeKind.START, "tracked", null);
                start.fixed.add(seconds.format(e.occurredAt()));
                position = START;
            }
            case "ROUTED", "ROUTED_BACK", "JOINED" -> {
                String from = from(e);
                String to = step(e.toJob());
                leave(from, e);
                if ("JOINED".equals(e.type())) {
                    EdgeBuilder edge = edge(from, to, EdgeKind.BRANCH, e, null);
                    edge.join = true;
                    edge.traversals--; // the join belongs to the branch that finished last
                } else {
                    edge(from, to, "ROUTED".equals(e.type()) ? EdgeKind.ROUTE : EdgeKind.BACK, e, e.reason());
                }
                arrive(to);
            }
            case "FORKED" -> {
                String from = from(e);
                leave(from, e);
                for (String branch : branches(e.reason())) {
                    String to = step(branch);
                    edge(from, to, EdgeKind.FORK, e, null);
                    arrive(to);
                }
                position = from;
            }
            case "BRANCH_FINISHED" -> {
                String branch = step(e.fromJob());
                leave(branch, e);
                nodes.get(branch).result = signal(e.reason());
                edge(branch, step(e.toJob()), EdgeKind.BRANCH, e, null);
            }
            case "FAILED" -> {
                NodeBuilder step = nodes.get(from(e));
                track(step, e);
                step.failures++;
                step.lastFailure = e;
            }
            case "RETRY_SCHEDULED" -> retry(e);
            case "ESCALATED", "LOOP_LIMIT_EXCEEDED" -> escalate(e);
            case "VOTED" -> {
                NodeBuilder decision = decisionOf(e);
                if (decision != null) {
                    decision.fixed.add(text(e.reason()) + ", " + minutes.format(e.occurredAt()));
                }
            }
            case "DECIDED" -> decided(e, next);
            case "DEFERRED" -> nodes.get(from(e)).deferred++;
            case "REPROCESSED" -> nodes.get(from(e)).reprocessed++;
            case "MIGRATED" -> nodes.get(from(e)).annotations
                    .add("migrated: " + text(e.reason()));
            case "PACKAGED" -> packaged(e);
            case "SENT", "ACKNOWLEDGED", "REJECTED" -> answer(e);
            case "COMPLETED", "DISCARDED" -> end(e);
            default -> {
                // an event type of a later version: not shown
            }
        }
    }

    private void retry(JourneyEntry e) {
        String id = from(e);
        NodeBuilder step = nodes.get(id);
        JourneyEntry failure = step.lastFailure != null ? step.lastFailure : e;
        EdgeBuilder loop = edge(id, id, EdgeKind.RETRY, failure, null);
        loop.causes.add(cause(failure));
        step.retried = true;
    }

    private void escalate(JourneyEntry e) {
        boolean loop = "LOOP_LIMIT_EXCEEDED".equals(e.type());
        String job = loop ? e.toJob() : e.fromJob();
        String from = job != null ? step(job) : here();
        track(nodes.get(from), e);
        if (options.detail() == Detail.STEPS) {
            position = from;
            return;
        }
        NodeBuilder decision = decisionNode(e);
        decision.fixed.add(text(e.reason()));
        edge(from, decision.id, EdgeKind.DECISION, e, loop ? "loop limit" : null);
        position = decision.id;
    }

    private void decided(JourneyEntry e, JourneyEntry next) {
        String option = option(e.reason());
        String job = e.toJob() != null ? e.toJob() : e.fromJob();
        String to = job != null ? step(job) : here();
        boolean discarded = next != null && "DISCARDED".equals(next.type()) && next.decisionId() != null
                && next.decisionId().equals(e.decisionId());
        if ("RETRY".equals(option)) {
            nodes.get(to).failures = 0; // the retry counter starts again
        }
        NodeBuilder decision = options.detail() == Detail.FULL ? decisionOf(e) : null;
        if (decision != null) {
            decision.fixed.add("decided " + text(e.reason()) + ", " + minutes.format(e.occurredAt()));
            if (discarded) {
                position = decision.id;
                return;
            }
            edge(decision.id, to, EdgeKind.DECISION, e, option);
        }
        if (!discarded) {
            arrive(to);
        }
    }

    private void packaged(JourneyEntry e) {
        String from = from(e);
        leave(from, e);
        if (options.detail() == Detail.STEPS) {
            position = from;
            return;
        }
        String key = e.packageId() != null ? "p:" + e.packageId() : "p#" + e.no();
        NodeBuilder pkg = nodes.get(ids.get(key));
        if (pkg == null) {
            PackageInfo info = e.packageId() != null ? ctx.packages().get(e.packageId()) : null;
            String title = info != null ? "work package " + info.seq()
                    + (info.groupKey() == null ? "" : " (" + info.groupKey() + ")") : e.reason();
            pkg = node("p" + ++packages, NodeKind.PACKAGE, text(title), null);
            ids.put(key, pkg.id);
            pkg.packageId = e.packageId();
            if (info != null) {
                pkg.fixed.addAll(describe(info));
            }
        }
        if (e.toJob() != null) {
            pkg.fixed.add("handed over to " + title(e.toJob()));
        }
        edge(from, pkg.id, EdgeKind.PACKAGE, e, null);
        position = pkg.id;
    }

    /** Sending, acknowledgement and rejections are shown at the package (slice 14). */
    private void answer(JourneyEntry e) {
        String id = e.packageId() == null ? null : ids.get("p:" + e.packageId());
        if (id == null) {
            return;
        }
        NodeBuilder pkg = nodes.get(id);
        switch (e.type()) {
            case "SENT" -> pkg.sends++;
            case "REJECTED" -> pkg.annotations.add("attempt " + ++pkg.rejections + " rejected: " + text(e.reason())
                    + ", " + minutes.format(e.occurredAt()));
            default -> pkg.annotations.add("acknowledged " + minutes.format(e.occurredAt()));
        }
    }

    private List<String> describe(PackageInfo info) {
        List<String> lines = new ArrayList<>();
        lines.add(info.state() + ("DONE".equals(info.state()) && info.attempts() > 0
                ? " in attempt " + (info.attempts() + 1) : ""));
        if (info.info() != null) {
            lines.add(text(info.info().replaceAll("[{}\"]", "").replace(",", ", ")));
        }
        // a sent package shows its rejections from the journal instead
        if (!"DONE".equals(info.state()) && !"SENT".equals(info.state()) && info.attempts() > 0) {
            lines.add(info.attempts() + (info.attempts() == 1 ? " failed attempt" : " failed attempts")
                    + (options.technical() && info.lastError() != null ? ": " + info.lastError() : ""));
        }
        return lines;
    }

    private void end(JourneyEntry e) {
        boolean completed = "COMPLETED".equals(e.type());
        String from;
        NodeBuilder decision = e.decisionId() != null && options.detail() == Detail.FULL ? decisionOf(e) : null;
        if (decision != null) {
            from = decision.id;
        } else {
            from = from(e);
            leave(from, e);
        }
        NodeBuilder end = node(END, NodeKind.END, e.type(), null);
        end.fixed.add(minutes.format(e.occurredAt()));
        if (!completed && e.reason() != null) {
            end.fixed.add(text(e.reason()));
        }
        end.style = completed ? Style.DONE : Style.DISCARDED;
        edge(from, END, EdgeKind.END, e, null);
        position = END;
    }

    /** Marks the current place of an unfinished item, with the reason why it waits. */
    private void markCurrent() {
        JourneyQueries.ItemInfo item = ctx.item();
        switch (item.state()) {
            case "QUEUED" -> {
                if (item.currentJob() == null) {
                    current(here(), "not routed yet");
                    return;
                }
                current(step(item.currentJob()), waiting(item.currentJob(), item.updatedAt(),
                        item.nextAttemptAt()));
            }
            case "WAITING_FOR_DECISION" -> waitingForDecision(item.currentJob());
            case "FORKED" -> {
                for (BranchInfo branch : ctx.openBranches()) {
                    if ("WAITING_FOR_DECISION".equals(branch.state())) {
                        waitingForDecision(branch.job());
                    } else {
                        current(step(branch.job()), waiting(branch.job(), branch.updatedAt(),
                                branch.nextAttemptAt()));
                    }
                }
            }
            case "PACKAGED" -> {
                if (item.packageId() == null) {
                    return;
                }
                String id = ids.get("p:" + item.packageId());
                PackageInfo info = ctx.packages().get(item.packageId());
                if (id != null && info != null) {
                    if ("DONE".equals(info.state())) {
                        nodes.get(id).style = Style.DONE;
                    } else if ("SENT".equals(info.state()) && info.sentAt() != null) {
                        current(id, "waiting for the acknowledgement since " + minutes.format(info.sentAt())
                                + (info.ackDueAt() == null ? "" : ", until " + minutes.format(info.ackDueAt())));
                    } else {
                        current(id, "waiting since " + minutes.format(item.updatedAt()));
                    }
                }
            }
            default -> {
                // COMPLETED or DISCARDED: the end node shows it
            }
        }
    }

    /** The open decision request of the step (the item links a request only once it is decided). */
    private void waitingForDecision(String job) {
        DecisionInfo info = ctx.decisions().values().stream().filter(d -> "OPEN".equals(d.state()))
                               .filter(d -> job == null || job.equals(d.job())).findFirst().orElse(null);
        UUID decisionId = info == null ? null : info.id();
        String waiting = info == null ? "waiting for a decision"
                : "APPROVAL".equals(info.kind()) && info.requiredApprovals() != null
                ? "waiting: " + info.approvals() + " of " + info.requiredApprovals() + " approvals"
                : "waiting for " + (info.group() != null ? info.group() : "a decision");
        String id = decisionId != null && options.detail() == Detail.FULL ? ids.get("d:" + decisionId) : null;
        if (id == null) {
            id = job != null ? step(job) : here();
            waiting += decisionId != null ? " " + shortId(decisionId) : "";
        }
        current(id, waiting);
    }

    private String waiting(String job, Instant updatedAt, Instant nextAttemptAt) {
        if (nextAttemptAt != null && nextAttemptAt.isAfter(ctx.now())) {
            return "next attempt " + minutes.format(nextAttemptAt);
        }
        String reason = ctx.statusReasons().get(job);
        return "waiting since " + minutes.format(updatedAt) + (reason == null ? "" : ": " + reason);
    }

    private void current(String id, String line) {
        NodeBuilder node = nodes.get(id);
        node.style = Style.CURRENT;
        node.current = line;
    }

    private void arrive(String id) {
        NodeBuilder node = nodes.get(id);
        node.visits++;
        node.failures = 0;
        position = id;
    }

    /** The item leaves a step: an attempt succeeded, after the failed ones since its arrival. */
    private void leave(String id, JourneyEntry e) {
        NodeBuilder node = nodes.get(id);
        track(node, e);
        if (node.failures > 0 && node.retried) {
            node.succeededOn = node.failures + 1;
        }
        node.failures = 0;
    }

    private void track(NodeBuilder node, JourneyEntry e) {
        if (node.kind != NodeKind.STEP || e.runId() == null) {
            return;
        }
        node.runIds.add(e.runId());
        if (e.traceId() != null) {
            node.traceIds.add(e.traceId());
        }
        if (e.node() != null) {
            node.workers.add(e.node());
        }
        if (e.runAttempt() != null && e.runAttempt() > node.runAttempt) {
            node.runAttempt = e.runAttempt();
        }
    }

    /**
     * The step an entry comes from. A step the item never arrived at took it over from where it was: it tracked
     * the item (an edge from the start with the number of the tracking entry), or it processed the package the item
     * is in (an edge without number).
     */
    private String from(JourneyEntry e) {
        if (e.fromJob() == null) {
            return here();
        }
        String id = step(e.fromJob());
        NodeBuilder node = nodes.get(id);
        if (node.visits == 0 && position != null && !position.equals(id)) {
            NodeBuilder at = nodes.get(position);
            if (at.kind == NodeKind.START && trackedAt != null) {
                edge(position, id, EdgeKind.ROUTE, trackedAt, null);
            } else {
                edges.computeIfAbsent(position + ">" + id + ">" + EdgeKind.PACKAGE,
                        key -> new EdgeBuilder(at.id, id, at.kind == NodeKind.PACKAGE ? EdgeKind.PACKAGE
                                : EdgeKind.ROUTE));
            }
            node.visits = 1;
        }
        return id;
    }

    private String here() {
        if (position == null) {
            node(START, NodeKind.START, "tracked", null);
            position = START;
        }
        return position;
    }

    private String step(String job) {
        String id = ids.get("s:" + job);
        if (id == null) {
            id = "s" + ++steps;
            ids.put("s:" + job, id);
            node(id, NodeKind.STEP, title(job), job);
        }
        return id;
    }

    private NodeBuilder decisionNode(JourneyEntry e) {
        String key = e.decisionId() != null ? "d:" + e.decisionId() : "d#" + e.no();
        String id = ids.get(key);
        if (id != null) {
            return nodes.get(id);
        }
        DecisionInfo info = e.decisionId() != null ? ctx.decisions().get(e.decisionId()) : null;
        String title = info == null ? "Decision"
                : ("APPROVAL".equals(info.kind()) ? "Approval " : "Decision ") + shortId(info.id())
                + (info.requiredApprovals() != null ? ", quorum " + info.requiredApprovals() : "");
        NodeBuilder node = node("d" + ++decisions, NodeKind.DECISION, title, null);
        ids.put(key, node.id);
        node.decisionId = e.decisionId();
        node.style = Style.DECISION;
        if (info != null && info.group() != null) {
            node.fixed.add("team " + info.group());
        }
        return node;
    }

    /** The decision node of a vote or a decision: by its link, else the last decision of the journey. */
    private NodeBuilder decisionOf(JourneyEntry e) {
        if (options.detail() == Detail.STEPS) {
            return null;
        }
        String id = e.decisionId() != null ? ids.get("d:" + e.decisionId()) : null;
        if (id == null) {
            id = nodes.values().stream().filter(n -> n.kind == NodeKind.DECISION).map(n -> n.id)
                      .reduce((first, second) -> second).orElse(null);
        }
        return id == null ? null : nodes.get(id);
    }

    private NodeBuilder node(String id, NodeKind kind, String title, String job) {
        return nodes.computeIfAbsent(id, key -> new NodeBuilder(key, kind, title, job));
    }

    private EdgeBuilder edge(String from, String to, EdgeKind kind, JourneyEntry e, String text) {
        EdgeBuilder edge = edges.computeIfAbsent(from + ">" + to + ">" + kind, key -> new EdgeBuilder(from, to, kind));
        edge.entries.add(e.no());
        edge.traversals++;
        if (edge.text == null && text != null && options.detail() == Detail.FULL) {
            edge.text = text(text);
        }
        return edge;
    }

    private String title(String job) {
        String handler = ctx.handlers().get(job);
        return handler == null ? job : simpleName(handler);
    }

    private static String simpleName(String className) {
        return className.substring(className.lastIndexOf('.') + 1).replace('$', '.');
    }

    private String cause(JourneyEntry failure) {
        String kind = failure.failureKind() == null ? "failed" : failure.failureKind().toLowerCase(Locale.ROOT);
        if (failure.errorType() != null) {
            String type = simpleName(failure.errorType());
            return kind + ": " + type + (options.technical() && failure.reason() != null
                    ? ": " + text(failure.reason()) : "");
        }
        return kind + (failure.reason() == null ? "" : ": " + text(failure.reason()));
    }

    /** The branch steps of a fork, from the reason written by the routing ("fork into A, B, join at C"). */
    static List<String> branches(String reason) {
        if (reason == null || !reason.startsWith(FORK_INTO) || !reason.contains(JOIN_AT)) {
            return List.of();
        }
        return List.of(reason.substring(FORK_INTO.length(), reason.indexOf(JOIN_AT)).split(", "));
    }

    /** The signal of a finished branch ("PASSED: reason"). */
    private static String signal(String reason) {
        if (reason == null) {
            return null;
        }
        int colon = reason.indexOf(": ");
        return text(colon < 0 ? reason : reason.substring(0, colon));
    }

    /** The option of a decision ("APPROVED by bob: comment"). */
    private static String option(String reason) {
        if (reason == null) {
            return null;
        }
        int by = reason.indexOf(" by ");
        return text(by < 0 ? reason : reason.substring(0, by));
    }

    static String shortId(UUID id) {
        return "D-" + id.toString().substring(0, 8);
    }

    /** A business text for a label: one line, truncated. */
    static String text(String text) {
        if (text == null) {
            return "";
        }
        String line = text.replaceAll("\\s+", " ").trim();
        return line.length() <= MAX_TEXT ? line : line.substring(0, MAX_TEXT - 1) + "…";
    }

    private class NodeBuilder {

        private final String id;
        private final NodeKind kind;
        private final String title;
        private final String job;
        private final List<String> fixed = new ArrayList<>();
        private final List<String> annotations = new ArrayList<>();
        private final Set<UUID> runIds = new LinkedHashSet<>();
        private final Set<String> traceIds = new LinkedHashSet<>();
        private final Set<String> workers = new LinkedHashSet<>();
        private int visits;
        private int failures;
        private boolean retried;
        private int succeededOn;
        private int deferred;
        private int reprocessed;
        private int sends;
        private int rejections;
        private int runAttempt;
        private String result;
        private String current;
        private JourneyEntry lastFailure;
        private Style style = Style.NONE;
        private UUID decisionId;
        private UUID packageId;

        NodeBuilder(String id, NodeKind kind, String title, String job) {
            this.id = id;
            this.kind = kind;
            this.title = title;
            this.job = job;
        }

        Node build() {
            List<String> lines = new ArrayList<>(fixed);
            if (kind == NodeKind.STEP) {
                List<String> first = new ArrayList<>();
                if (result != null) {
                    first.add(result);
                }
                if (visits > 1) {
                    first.add(visits + " visits");
                }
                if (options.technical() && !workers.isEmpty()) {
                    first.add("on " + String.join(", ", workers));
                }
                if (!first.isEmpty()) {
                    lines.add(String.join(", ", first));
                }
                if (options.detail() == Detail.FULL) {
                    lines.addAll(stepAnnotations());
                }
                if (retried && style == Style.NONE) {
                    style = Style.RETRIED;
                }
            }
            if (kind == NodeKind.PACKAGE) {
                if (sends > 0) {
                    lines.add(sends == 1 ? "sent" : "sent " + sends + "×");
                }
                lines.addAll(annotations);
            }
            if (current != null) {
                lines.add(current);
            }
            return new Node(id, kind, title, job, List.copyOf(lines), visits, style, List.copyOf(runIds),
                    List.copyOf(traceIds), decisionId, packageId);
        }

        private List<String> stepAnnotations() {
            List<String> lines = new ArrayList<>();
            if (succeededOn > 0) {
                lines.add("succeeded on attempt " + succeededOn);
            }
            if (deferred > 0) {
                lines.add("deferred " + times(deferred));
            }
            if (reprocessed > 0) {
                lines.add("reprocessed " + times(reprocessed) + " after a batch rollback");
            }
            if (runAttempt > 1) {
                lines.add("processed in run attempt " + runAttempt + ", after a takeover");
            }
            lines.addAll(annotations);
            return lines;
        }

        private String times(int count) {
            return count == 1 ? "once" : count + "×";
        }
    }

    private class EdgeBuilder {

        private final String from;
        private final String to;
        private final EdgeKind kind;
        private final List<Integer> entries = new ArrayList<>();
        private final Set<String> causes = new LinkedHashSet<>();
        private String text;
        private boolean join;
        private int traversals;

        EdgeBuilder(String from, String to, EdgeKind kind) {
            this.from = from;
            this.to = to;
            this.kind = kind;
        }

        Edge build() {
            StringBuilder label = new StringBuilder(numbers());
            switch (kind) {
                case BACK -> label.append(" back-route").append(text == null ? "" : ": " + text);
                case FORK -> label.append(" fork");
                case RETRY -> label.append(" retry ").append(entries.size()).append('×')
                                   .append(options.detail() == Detail.FULL && !causes.isEmpty()
                                           ? "\n" + String.join("\n", causes) : "");
                default -> label.append(join ? " join" : "").append(text == null ? "" : " " + text);
            }
            return new Edge(from, to, kind, List.copyOf(entries), label.toString());
        }

        private String numbers() {
            List<String> shown = entries.size() <= MAX_ENTRIES
                    ? entries.stream().map(String::valueOf).toList()
                    : List.of(String.valueOf(entries.getFirst()), String.valueOf(entries.get(1)), "…",
                              String.valueOf(entries.getLast()));
            String joined = shown.stream().collect(Collectors.joining(", "));
            return traversals > 2 && kind != EdgeKind.RETRY ? joined + " (" + traversals + "×)" : joined;
        }
    }
}
