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

import org.os890.dtp.api.FlowBuilder;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.cdi.ShipmentDelivery.DeliverySignal;
import org.os890.dtp.cdi.ShipmentSorting.SortSignal;

/** A flow defined as CDI bean. */
@ApplicationScoped
public class ShipmentFlow implements FlowDefinition {

    @Override
    public void define(FlowBuilder flow) {
        flow.entry(ShipmentSorting.class)
                .on(SortSignal.SORTED).to(ShipmentDelivery.class);
        flow.step(ShipmentDelivery.class)
                .on(DeliverySignal.DELIVERED).complete();
    }
}
