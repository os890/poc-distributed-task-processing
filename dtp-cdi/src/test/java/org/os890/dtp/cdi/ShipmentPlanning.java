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
package org.os890.dtp.cdi;

import jakarta.enterprise.context.ApplicationScoped;

import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;

/** Tracks a number of shipments and routes them to the dispatch. */
@ApplicationScoped
public class ShipmentPlanning implements JobHandler<PlanningParams, DefaultOutcome> {

    @Override
    public JobResult<DefaultOutcome> execute(JobContext<PlanningParams> ctx) {
        for (long id = 1; id <= ctx.parameters().shipments(); id++) {
            ctx.items().track(ItemRef.of(Shipment.ITEM, id), "S-" + id).routeTo(ShipmentDispatch.class);
        }
        return JobResult.success();
    }
}
