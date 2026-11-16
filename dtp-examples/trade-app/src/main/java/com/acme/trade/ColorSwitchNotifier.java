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

import org.os890.dtp.api.event.ActiveColorSwitched;

/**
 * Informs operations when the active color of a red/green deployment was switched: the nodes of the old release
 * stand by and can be shut down once their runs ended, the flows follow the new release.
 */
@ApplicationScoped
public class ColorSwitchNotifier {

    @Inject
    TeamNotifications team;

    void onSwitched(@ObservesAsync ActiveColorSwitched event) {
        String active = event.activeColor() == null ? "all colors" : event.activeColor();
        String previous = event.previousColor() == null ? "all colors" : event.previousColor();
        String flows = event.activatedFlows().isEmpty() ? "none" : String.join(", ", event.activatedFlows());
        team.send("operations", "Active color: " + active, "Switched from " + previous + " by " + event.by()
                + (event.reason() == null ? "" : ": " + event.reason()) + ". Flow versions activated: " + flows
                + ".");
    }
}
