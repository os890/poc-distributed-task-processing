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

import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.TrackedItem;

/**
 * A parallel branch of the trade flow: checks the quantity against the limit. Runs at the same time as the
 * {@link ComplianceCheck}, possibly on another node. The branches only read the trade, the join step
 * ({@link TradeEnrichment}) writes it, so they never conflict on its version.
 */
@ApplicationScoped
public class LimitCheck implements ItemProcessor<Trade, UUID> {

    @Inject
    EntityManager em;

    @ConfigProperty(name = "trade.approval.quantity-limit", defaultValue = "1000000")
    long quantityLimit;

    @Override
    public void process(TrackedItem<Trade, UUID> item) {
        Trade trade = em.find(Trade.class, item.ref().id());
        if (trade.getQuantity() > quantityLimit) {
            item.signal(LimitSignal.ABOVE_LIMIT, "quantity " + trade.getQuantity() + " above the limit of "
                    + quantityLimit);
        } else {
            item.signal(LimitSignal.WITHIN_LIMIT);
        }
    }
}
