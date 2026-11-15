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
package org.os890.dtp.core.spi;

import java.util.List;

/**
 * The cluster-wide state that the leader of the projection reports as gauges (docs/design/09-monitoring-heatmap.md
 * §9.8, cluster gauges), read from the monitoring read model after a projection cycle.
 *
 * @param steps     one entry per step
 * @param queues    queued items per step and lane
 * @param waiting   items waiting for people, per step and responsible group
 * @param decisions the oldest open decision request per responsible group
 * @param nodes     the nodes per color and state
 */
public record ClusterState(List<Step> steps, List<Queue> queues, List<Waiting> waiting, List<OpenDecisions> decisions,
                           List<Nodes> nodes) {

    /**
     * Validates the state.
     *
     * @param steps     one entry per step
     * @param queues    queued items per step and lane
     * @param waiting   items waiting for people, per step and responsible group
     * @param decisions the oldest open decision request per responsible group
     * @param nodes     the nodes per color and state
     */
    public ClusterState {
        steps = List.copyOf(steps);
        queues = List.copyOf(queues);
        waiting = List.copyOf(waiting);
        decisions = List.copyOf(decisions);
        nodes = List.copyOf(nodes);
    }

    /**
     * A step.
     *
     * @param step             the job name
     * @param statusReason     why it processes or doesn't
     * @param oldestQueuedSecs the age of its oldest due item in seconds (0 if none)
     * @param jamScore         0 to 100
     * @param alertState       0 = OK, 1 = CONGESTED, 2 = FAILING
     */
    public record Step(String step, String statusReason, double oldestQueuedSecs, int jamScore, int alertState) {
    }

    /**
     * Queued items.
     *
     * @param step  the job name
     * @param lane  the lane ({@code ""} without lane)
     * @param depth the number of queued items
     */
    public record Queue(String step, String lane, int depth) {
    }

    /**
     * Items waiting for people.
     *
     * @param step  the requesting job
     * @param group the responsible group ({@code ""} without group)
     * @param items the number of items
     */
    public record Waiting(String step, String group, int items) {
    }

    /**
     * The oldest open decision request of a group.
     *
     * @param group          the responsible group ({@code ""} without group)
     * @param oldestOpenSecs its age in seconds
     */
    public record OpenDecisions(String group, double oldestOpenSecs) {
    }

    /**
     * Nodes.
     *
     * @param color the color ({@code ""} without color)
     * @param state {@code ACTIVE}, {@code DRAINING}, ...
     * @param count the number of nodes
     */
    public record Nodes(String color, String state, int count) {
    }
}
