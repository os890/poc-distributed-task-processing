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

import java.math.BigDecimal;
import java.time.Duration;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Simulated market data service. The optional delay makes the cluster behavior visible in the demo. */
@ApplicationScoped
public class MarketData {

    @ConfigProperty(name = "trade.enrichment.delay", defaultValue = "PT0S")
    Duration delay;

    /**
     * Sets the price of a trade.
     *
     * @param trade the trade
     */
    public void enrich(Trade trade) {
        if (!delay.isZero()) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        trade.setPrice(BigDecimal.valueOf(100 + Math.abs(trade.getIsin().hashCode() % 900), 2));
    }
}
