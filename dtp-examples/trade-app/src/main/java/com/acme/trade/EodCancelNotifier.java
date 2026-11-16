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

import org.os890.dtp.api.event.FlowInstanceCancelled;

/**
 * Informs operations and product control when an operator cancelled the end of day of a business date, e.g. one
 * started by mistake: its open runs were cancelled, a pending sign-off was withdrawn.
 */
@ApplicationScoped
public class EodCancelNotifier {

    @Inject
    TeamNotifications team;

    void onCancelled(@ObservesAsync FlowInstanceCancelled event) {
        if (!EodResource.FLOW.equals(event.instance().name())) {
            return;
        }
        String subject = "EOD " + event.instance().businessKey() + " cancelled";
        String body = "Cancelled by " + event.by() + ": " + event.reason();
        team.send("operations", subject, body);
        team.send(Desk.PRODUCT_CONTROL.group(), subject, body + ". A pending sign-off isn't needed any more.");
    }
}
