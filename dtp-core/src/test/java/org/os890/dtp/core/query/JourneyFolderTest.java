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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
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
import org.os890.dtp.core.query.JourneyQueries.ItemInfo;
import org.os890.dtp.core.query.JourneyQueries.Options;
import org.os890.dtp.core.query.JourneyQueries.PackageInfo;

/** The folding rules of the journey diagram, on journal entries written by hand. */
class JourneyFolderTest {

    private static final Instant MONDAY = Instant.parse("2026-11-16T10:00:00Z");
    private static final Options FULL = new Options(Detail.FULL, false, ZoneOffset.UTC);
    private static final Options STEPS = new Options(Detail.STEPS, false, ZoneOffset.UTC);
    private static final Map<String, String> HANDLERS = Map.of("import", "com.acme.TradeImport",
            "check", "com.acme.TradeCheck", "collector", "com.acme.Settlement$Collector", "sender", "com.acme.Sender");

    private final List<JourneyEntry> entries = new ArrayList<>();
    private final UUID run = UUID.randomUUID();

    @Test
    void packagesHandoversTakeoversAndRollbacksAreShownAtTheirSteps() {
        UUID pkg = UUID.randomUUID();
        add("TRACKED", null, null, null);
        add("ROUTED", null, "import", "imported");
        add("ROUTED", "import", "check", null);
        add("REPROCESSED", "check", null, "batch rollback");
        // written by the second attempt of the run of the check step, after a takeover
        entries.add(new JourneyEntry(5, 5, MONDAY, "ROUTED", "check", "collector", run, 2, "n2", null, null, null,
                null, null));
        entries.add(new JourneyEntry(6, 6, MONDAY, "PACKAGED", "collector", "sender", run, 1, "n1",
                "work package 7 (OEKB-CSD)", null, null, null, pkg));
        Context ctx = context(item("PACKAGED", null, pkg), Map.of(), Map.of(pkg, new PackageInfo(pkg, 7,
                "OEKB-CSD", "PENDING", "sender", "{\"file\":\"settle-7.xml\",\"size\":2}", 1, "gateway down")),
                List.of(), Map.of());

        JourneyGraph graph = JourneyFolder.fold(ctx, FULL);
        assertThat(graph.nodes()).extracting(Node::kind).containsExactly(NodeKind.START, NodeKind.STEP,
                NodeKind.STEP, NodeKind.STEP, NodeKind.PACKAGE);
        assertThat(node(graph, "TradeCheck").lines()).containsExactly("reprocessed once after a batch rollback",
                "processed in run attempt 2, after a takeover");
        assertThat(node(graph, "Settlement.Collector").job()).isEqualTo("collector");
        Node pkgNode = graph.nodes().getLast();
        assertThat(pkgNode.title()).isEqualTo("work package 7 (OEKB-CSD)");
        assertThat(pkgNode.packageId()).isEqualTo(pkg);
        assertThat(pkgNode.lines()).containsExactly("PENDING", "file:settle-7.xml, size:2", "1 failed attempts",
                "handed over to Sender", "waiting since Mon 10:00");
        assertThat(pkgNode.style()).isEqualTo(Style.CURRENT);
        assertThat(graph.mermaid()).contains("p1[/\"work package 7 (OEKB-CSD)<br/>PENDING<br/>",
                "s3 -- \"6\" --> p1", "class p1 current");
        // operators see the error of the package, steps only hides the package
        assertThat(JourneyFolder.fold(ctx, new Options(Detail.FULL, true, ZoneOffset.UTC)).nodes().getLast().lines())
                .contains("1 failed attempts: gateway down");
        JourneyGraph steps = JourneyFolder.fold(ctx, STEPS);
        assertThat(steps.nodes()).extracting(Node::kind).doesNotContain(NodeKind.PACKAGE);
        assertThat(node(steps, "TradeCheck").lines()).isEmpty();

        // a finished package
        Context done = context(item("PACKAGED", null, pkg), Map.of(), Map.of(pkg, new PackageInfo(pkg, 7,
                "OEKB-CSD", "DONE", "sender", null, 1, null)), List.of(), Map.of());
        Node finished = JourneyFolder.fold(done, FULL).nodes().getLast();
        assertThat(finished.style()).isEqualTo(Style.DONE);
        assertThat(finished.lines()).containsExactly("DONE in attempt 2", "handed over to Sender");
        // a packaged item without its package (written before slice 12)
        assertThat(JourneyFolder.fold(context(item("PACKAGED", null, null), Map.of(), Map.of(), List.of(), Map.of()),
                FULL).nodes().getLast().style()).isEqualTo(Style.NONE);
    }

    @Test
    void anItemTrackedByACollectorIsSentWithItsPackage() {
        UUID pkg = UUID.randomUUID();
        add("TRACKED", null, null, null);
        entries.add(new JourneyEntry(2, 2, MONDAY, "PACKAGED", "collector", "sender", run, 1, "n1",
                "work package 3 (EUROCLEAR)", null, null, null, pkg));
        add("COMPLETED", "sender", null, null);
        Context ctx = context(item("COMPLETED", null, pkg), Map.of(), Map.of(pkg, new PackageInfo(pkg, 3,
                "EUROCLEAR", "DONE", "sender", null, 0, null)), List.of(), Map.of());

        JourneyGraph graph = JourneyFolder.fold(ctx, FULL);
        assertThat(graph.edges()).extracting(Edge::from, Edge::to, Edge::label).containsExactly(
                tuple("start", "s1", "1"),
                tuple("s1", "p1", "2"),
                tuple("p1", "s2", ""),
                tuple("s2", "finish", "3"));
        assertThat(graph.mermaid()).contains("  p1 --> s2\n", "  s2 -- \"3\" --> finish\n");
        // steps only: from the collector to the sender
        assertThat(JourneyFolder.fold(ctx, STEPS).edges()).extracting(Edge::from, Edge::to, Edge::kind)
                .contains(tuple("s1", "s2", EdgeKind.ROUTE));
    }

    @Test
    void loopsCollapseIntoOneEdgeAndTheLoopLimitEndsInADecision() {
        UUID request = UUID.randomUUID();
        add("TRACKED", null, null, null);
        add("ROUTED", null, "import", null);
        for (int i = 0; i < 5; i++) {
            add("ROUTED", "import", "check", "<b>\"x\"</b> #1 | y");
            add("ROUTED_BACK", "check", "import", "missing ISIN");
        }
        add("MIGRATED", "import", "import", "flow trades a1 -> b2 by ops");
        add("FUTURE_EVENT", "import", null, null);
        entries.add(entry("LOOP_LIMIT_EXCEEDED", "check", "import", "visit 6 of import exceeds max_item_visits 5",
                request));
        entries.add(entry("DECIDED", "import", "import", "DISCARD by otto: enough", request));
        entries.add(entry("DISCARDED", "import", null, "DISCARD by otto: enough", request));
        Context ctx = context(item("DISCARDED", null, null), Map.of(request, new DecisionInfo(request, "CHOICE",
                "import", null, "dtp-operator", "DECIDED", 0)), Map.of(), List.of(), Map.of());

        JourneyGraph graph = JourneyFolder.fold(ctx, FULL);
        Edge forward = edges(graph, EdgeKind.ROUTE).get(1);
        assertThat(forward.entries()).containsExactly(3, 5, 7, 9, 11);
        assertThat(forward.label()).isEqualTo("3, 5, …, 11 (5×) <b>\"x\"</b> #1 | y");
        assertThat(edges(graph, EdgeKind.BACK)).singleElement().extracting(Edge::label)
                .isEqualTo("4, 6, …, 12 (5×) back-route: missing ISIN");
        assertThat(node(graph, "TradeImport").lines()).containsExactly("6 visits",
                "migrated: flow trades a1 -> b2 by ops");
        Node decision = graph.nodes().stream().filter(n -> n.kind() == NodeKind.DECISION).findFirst().orElseThrow();
        assertThat(decision.title()).isEqualTo("Decision " + JourneyFolder.shortId(request));
        assertThat(decision.lines()).containsExactly("team dtp-operator", "visit 6 of import exceeds max_item_visits 5",
                "decided DISCARD by otto: enough, Mon 10:00");
        // the decision ends the journey: no edge back to the step
        assertThat(edges(graph, EdgeKind.DECISION)).singleElement().satisfies(e -> {
            assertThat(e.to()).isEqualTo(decision.id());
            assertThat(e.label()).isEqualTo("15 loop limit");
        });
        assertThat(edges(graph, EdgeKind.END)).singleElement().extracting(Edge::from).isEqualTo(decision.id());
        Node end = graph.nodes().getLast();
        assertThat(end.title()).isEqualTo("DISCARDED");
        assertThat(end.style()).isEqualTo(Style.DISCARDED);
        assertThat(end.lines()).containsExactly("Mon 10:00", "DISCARD by otto: enough");
        String mermaid = graph.mermaid();
        assertThat(mermaid).contains("#lt;b#gt;#quot;x#quot;#lt;/b#gt; #35;1 #124; y", "finish((\"DISCARDED<br/>",
                "classDef discarded").doesNotContain("<b>", "\"x\"", " end");
    }

    @Test
    void entriesWithoutLinksAndItemsThatWaitInAStep() {
        add("TRACKED", null, null, null);
        add("ROUTED", null, "check", null);
        entries.add(new JourneyEntry(3, 3, MONDAY, "FAILED", "check", null, run, 1, "n1", "missing price",
                "BUSINESS", null, null, null));
        add("ESCALATED", "check", null, "retries exhausted (0): missing price");
        add("DECIDED", "check", "check", "RETRY by otto");
        String text = "a very long reason that is much longer than sixty characters, so it is truncated";
        Context ctx = context(item("QUEUED", "check", null), Map.of(), Map.of(), List.of(),
                Map.of("check", "SLOT_CLOSED"));

        JourneyGraph graph = JourneyFolder.fold(ctx, FULL);
        Node decision = graph.nodes().stream().filter(n -> n.kind() == NodeKind.DECISION).findFirst().orElseThrow();
        assertThat(decision.title()).isEqualTo("Decision");
        assertThat(decision.decisionId()).isNull();
        assertThat(edges(graph, EdgeKind.DECISION)).extracting(Edge::label).containsExactly("4", "5 RETRY");
        assertThat(edges(graph, EdgeKind.RETRY)).isEmpty();
        Node check = node(graph, "TradeCheck");
        assertThat(check.style()).isEqualTo(Style.CURRENT);
        assertThat(check.lines()).containsExactly("2 visits", "waiting since Mon 10:00: SLOT_CLOSED");
        assertThat(JourneyFolder.fold(ctx, STEPS).nodes()).extracting(Node::kind)
                .containsExactly(NodeKind.START, NodeKind.STEP);
        assertThat(JourneyFolder.text(text)).hasSize(JourneyFolder.MAX_TEXT).endsWith("…");
        assertThat(JourneyFolder.text(null)).isEmpty();

        // an item that wasn't routed yet
        entries.subList(1, entries.size()).clear();
        Node start = JourneyFolder.fold(context(item("QUEUED", null, null), Map.of(), Map.of(), List.of(),
                Map.of()), FULL).nodes().getFirst();
        assertThat(start.style()).isEqualTo(Style.CURRENT);
        assertThat(start.lines()).containsExactly("Mon 10:00:00", "not routed yet");
    }

    @Test
    void openBranchesAreWhereAForkedItemIsNow() {
        UUID request = UUID.randomUUID();
        add("TRACKED", null, null, null);
        add("ROUTED", null, "import", null);
        add("FORKED", "import", "sender", "fork into check, collector, join at sender (VALID)");
        add("BRANCH_FINISHED", "check", "sender", "PASSED: limit ok");
        entries.add(entry("ESCALATED", "collector", null, "large trade", request));
        Context ctx = context(item("FORKED", null, null), Map.of(request, new DecisionInfo(request, "APPROVAL",
                "collector", 2, "risk", "OPEN", 1)), Map.of(), List.of(new BranchInfo("collector",
                "WAITING_FOR_DECISION", MONDAY, null)), Map.of());

        JourneyGraph graph = JourneyFolder.fold(ctx, FULL);
        assertThat(edges(graph, EdgeKind.FORK)).extracting(Edge::label).containsExactly("3 fork", "3 fork");
        assertThat(node(graph, "TradeCheck").lines()).containsExactly("PASSED");
        Node decision = graph.nodes().stream().filter(n -> n.kind() == NodeKind.DECISION).findFirst().orElseThrow();
        assertThat(decision.style()).isEqualTo(Style.CURRENT);
        assertThat(decision.lines()).last().isEqualTo("waiting: 1 of 2 approvals");
        // steps only: the step of the branch waits
        Node branch = node(JourneyFolder.fold(ctx, STEPS), "Settlement.Collector");
        assertThat(branch.lines()).containsExactly("waiting: 1 of 2 approvals " + JourneyFolder.shortId(request));

        // a queued branch whose retry is due later
        Context queued = context(item("FORKED", null, null), Map.of(), Map.of(), List.of(new BranchInfo("collector",
                "QUEUED", MONDAY, MONDAY.plusSeconds(3600))), Map.of());
        entries.removeLast();
        assertThat(node(JourneyFolder.fold(queued, FULL), "Settlement.Collector").lines())
                .containsExactly("next attempt Mon 11:00");
        assertThat(JourneyFolder.branches("something else")).isEmpty();
    }

    private void add(String type, String from, String to, String reason) {
        entries.add(entry(type, from, to, reason, null));
    }

    private JourneyEntry entry(String type, String from, String to, String reason, UUID decision) {
        int no = entries.size() + 1;
        return new JourneyEntry(no, no, MONDAY, type, from, to, null, null, null, reason, null, null, decision, null);
    }

    private ItemInfo item(String state, String job, UUID pkg) {
        return new ItemInfo(UUID.randomUUID(), "trade", "T-1", 0, state, job, MONDAY, null, pkg);
    }

    private Context context(ItemInfo item, Map<UUID, DecisionInfo> decisions, Map<UUID, PackageInfo> packages,
                            List<BranchInfo> branches, Map<String, String> reasons) {
        return new Context(item, List.copyOf(entries), HANDLERS, decisions, packages, branches, reasons, MONDAY);
    }

    private static Node node(JourneyGraph graph, String title) {
        return graph.nodes().stream().filter(n -> title.equals(n.title())).findFirst()
                    .orElseThrow(() -> new AssertionError("no node " + title + " in " + graph.nodes()));
    }

    private static List<Edge> edges(JourneyGraph graph, EdgeKind kind) {
        return graph.edges().stream().filter(e -> e.kind() == kind).toList();
    }
}
