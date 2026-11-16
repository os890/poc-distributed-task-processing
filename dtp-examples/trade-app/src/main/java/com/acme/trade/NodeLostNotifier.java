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
package com.acme.trade;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

import org.os890.dtp.api.event.NodeLost;
import org.os890.dtp.core.engine.Decisions;

/**
 * Informs operations when a node of the cluster stopped sending heartbeats: its runs are taken over by the other
 * nodes, but the node itself (a crashed pod, a stalled VM) needs a look. The housekeeping leader fires the event once
 * for the whole cluster.
 */
@ApplicationScoped
public class NodeLostNotifier {

    @Inject
    TeamNotifications team;

    void onLost(@ObservesAsync NodeLost event) {
        team.send(Decisions.OPERATORS, "Node lost: " + event.node(), "No heartbeat since " + event.lastHeartbeat()
                + (event.color() == null ? "" : " (color " + event.color() + ")")
                + ". Its runs are taken over by the other nodes.\n\nNodes: /dtp/nodes");
    }
}
