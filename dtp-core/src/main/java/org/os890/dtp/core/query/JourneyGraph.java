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

import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/**
 * The journey of one item folded into a graph (docs/design/09-monitoring-heatmap.md §9.9): the steps the item visited,
 * once each, its decisions and work packages, and edges labeled with the numbers of the journal entries. Texts are
 * plain here, {@link #mermaid()} escapes them.
 *
 * @param itemId      the item
 * @param itemType    name of the item type
 * @param businessKey the business key
 * @param version     the version of the item reference
 * @param state       the state of the item
 * @param nodes       the nodes, in the order of the journey
 * @param edges       the edges, in the order of their first journal entry
 */
public record JourneyGraph(UUID itemId, String itemType, String businessKey, long version, String state,
                           List<Node> nodes, List<Edge> edges) {

    /** Styles of the diagram, light enough for both themes. */
    static final List<String> CLASS_DEFS = List.of(
            "classDef done fill:#d8f0dc,stroke:#2e7d32,color:#1b1b1b",
            "classDef discarded fill:#e4e4e4,stroke:#757575,color:#1b1b1b",
            "classDef decision fill:#fff3c4,stroke:#b28704,color:#1b1b1b",
            "classDef retried fill:#ffe2b8,stroke:#e65100,color:#1b1b1b",
            "classDef current fill:#d6e6ff,stroke:#1565c0,stroke-width:3px,color:#1b1b1b");

    /** Kinds of nodes. */
    public enum NodeKind {
        /** Where the item was tracked. */
        START,
        /** A step the item visited. */
        STEP,
        /** A decision request or an approval. */
        DECISION,
        /** A work package. */
        PACKAGE,
        /** The end of the journey: completed or discarded. */
        END
    }

    /** Kinds of edges. */
    public enum EdgeKind {
        /** Routed to the next step. */
        ROUTE,
        /** Routed back (dashed). */
        BACK,
        /** A branch of a fork. */
        FORK,
        /** A finished branch, towards the join step. */
        BRANCH,
        /** Failed attempts with a retry (a dashed self-loop). */
        RETRY,
        /** Handed over to people, or back from them. */
        DECISION,
        /** Put into a work package. */
        PACKAGE,
        /** Completed or discarded. */
        END
    }

    /** Highlighting of a node. */
    public enum Style {
        /** Not highlighted. */
        NONE,
        /** Completed. */
        DONE,
        /** Discarded. */
        DISCARDED,
        /** A decision. */
        DECISION,
        /** A step with failed attempts. */
        RETRIED,
        /** Where the item is now. */
        CURRENT
    }

    /**
     * A node.
     *
     * @param id         the Mermaid id
     * @param kind       the kind
     * @param title      the title: the handler class of a step, the decision, the package
     * @param job        the job name of a step, or {@code null}
     * @param lines      the lines below the title
     * @param visits     the visits of a step (0 for other nodes)
     * @param style      the highlighting
     * @param runIds     the runs that processed the item in this step
     * @param decisionId the decision request of a decision node, or {@code null}
     * @param packageId  the work package of a package node, or {@code null}
     */
    public record Node(String id, NodeKind kind, String title, String job, List<String> lines, int visits,
                       Style style, List<UUID> runIds, UUID decisionId, UUID packageId) {
    }

    /**
     * An edge.
     *
     * @param from    id of the source node
     * @param to      id of the target node
     * @param kind    the kind
     * @param entries the numbers of the journal entries (see {@code DtpQueries.JourneyEntry#no()})
     * @param label   the label, with the entry numbers; a new line separates its lines
     */
    public record Edge(String from, String to, EdgeKind kind, List<Integer> entries, String label) {
    }

    /**
     * The graph as Mermaid source. Business texts are escaped and truncated already, so clients can render it with
     * Mermaid's strict security level.
     *
     * @return the Mermaid flowchart
     */
    public String mermaid() {
        StringBuilder out = new StringBuilder("flowchart LR\n");
        Set<Style> used = EnumSet.noneOf(Style.class);
        for (Node node : nodes) {
            String text = escape(node.title()) + node.lines().stream().map(line -> "<br/>" + escape(line))
                                                     .reduce("", String::concat);
            String[] shape = switch (node.kind()) {
                case START -> new String[] {"([\"", "\"])"};
                case STEP -> new String[] {"[\"", "\"]"};
                case DECISION -> new String[] {"{{\"", "\"}}"};
                case PACKAGE -> new String[] {"[/\"", "\"/]"};
                case END -> new String[] {"((\"", "\"))"};
            };
            out.append("  ").append(node.id()).append(shape[0]).append(text).append(shape[1]).append('\n');
            if (node.style() != Style.NONE) {
                used.add(node.style());
            }
        }
        for (Edge edge : edges) {
            String label = escape(edge.label()).replace("\n", "<br/>");
            boolean dashed = edge.kind() == EdgeKind.BACK || edge.kind() == EdgeKind.RETRY;
            if (label.isEmpty()) {
                out.append("  ").append(edge.from()).append(dashed ? " -.-> " : " --> ").append(edge.to()).append('\n');
            } else {
                out.append("  ").append(edge.from()).append(dashed ? " -. \"" : " -- \"").append(label)
                   .append(dashed ? "\" .-> " : "\" --> ").append(edge.to()).append('\n');
            }
        }
        for (Style style : used) {
            String name = style.name().toLowerCase(Locale.ROOT);
            CLASS_DEFS.stream().filter(def -> def.startsWith("classDef " + name + " "))
                      .forEach(def -> out.append("  ").append(def).append('\n'));
            out.append("  class ").append(String.join(",", nodes.stream().filter(n -> n.style() == style)
                                                               .map(Node::id).toList())).append(' ').append(name)
               .append('\n');
        }
        return out.toString();
    }

    /** Escapes a text for a quoted Mermaid label: entity codes instead of characters with a meaning. */
    static String escape(String text) {
        StringBuilder out = new StringBuilder(text.length());
        for (char ch : text.toCharArray()) {
            switch (ch) {
                case '#' -> out.append("#35;");
                case '"' -> out.append("#quot;");
                case '<' -> out.append("#lt;");
                case '>' -> out.append("#gt;");
                case '&' -> out.append("#amp;");
                case '|' -> out.append("#124;");
                case '\r', '\t' -> out.append(' ');
                default -> out.append(ch);
            }
        }
        return out.toString();
    }
}
