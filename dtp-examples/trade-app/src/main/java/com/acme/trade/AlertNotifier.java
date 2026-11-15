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

import org.os890.dtp.api.event.StepCongested;
import org.os890.dtp.api.event.StepRecovered;
import org.os890.dtp.api.event.TechnicalFailureSpike;
import org.os890.dtp.core.engine.Decisions;

/**
 * Informs operations about jams and technical failure spikes of the heat map (the framework never notifies people
 * itself). The projector leader fires each alert transition once for the whole cluster.
 */
@ApplicationScoped
public class AlertNotifier {

    @Inject
    TeamNotifications team;

    void onCongested(@ObservesAsync StepCongested event) {
        team.send(Decisions.OPERATORS, "Step congested: " + event.step(),
                "%d queued, the oldest for %s, jam score %d (%s)".formatted(event.queued(), event.oldestQueued(),
                        event.jamScore(), event.statusReason()) + "\n\nHeat map: /dtp/monitor/heatmap/diagram");
    }

    void onFailureSpike(@ObservesAsync TechnicalFailureSpike event) {
        team.send(Decisions.OPERATORS, "Technical failures: " + event.step(), "%d within %s: %s".formatted(
                event.failures(), event.window(), String.join(", ", event.errorTypes()))
                + "\n\nDetails: /dtp/monitor/steps/" + event.step() + "/failures");
    }

    void onRecovered(@ObservesAsync StepRecovered event) {
        team.send(Decisions.OPERATORS, "Step recovered: " + event.step(), "no longer " + event.previousState());
    }
}
