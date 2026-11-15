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

import org.os890.dtp.api.ItemLane;

/**
 * The lanes of the trades: tickets of institutional clients are served first, with three of four batches while both
 * lanes have trades waiting, so retail trades still move on (DD-23). Kept for the whole journey of a trade.
 */
public enum TradeLane implements ItemLane {

    /** Large tickets (from 250,000). */
    INSTITUTIONAL(10, 3),

    /** All other trades. */
    RETAIL(0, 1);

    /** The quantity from which a trade counts as institutional. */
    static final long INSTITUTIONAL_QUANTITY = 250_000;

    private final int priority;
    private final int weight;

    TradeLane(int priority, int weight) {
        this.priority = priority;
        this.weight = weight;
    }

    @Override
    public int priority() {
        return priority;
    }

    @Override
    public int weight() {
        return weight;
    }

    /**
     * The lane of a trade.
     *
     * @param trade the trade
     * @return its lane
     */
    static TradeLane of(Trade trade) {
        return trade.getQuantity() >= INSTITUTIONAL_QUANTITY ? INSTITUTIONAL : RETAIL;
    }
}
