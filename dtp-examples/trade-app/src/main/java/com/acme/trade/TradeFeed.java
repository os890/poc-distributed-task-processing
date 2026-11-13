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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

/** Simulated trade feed: every fourth trade arrives without ISIN, a re-import delivers it. */
@ApplicationScoped
public class TradeFeed {

    @Inject
    EntityManager em;

    /**
     * Imports the trades of a business date.
     *
     * @param businessDate the business date
     * @param count        number of trades
     * @return the persisted trades
     */
    public List<Trade> importFor(LocalDate businessDate, int count) {
        long offset = em.createQuery("SELECT COUNT(t) FROM Trade t WHERE t.businessDate = :date", Long.class)
                        .setParameter("date", businessDate).getSingleResult();
        List<Trade> trades = new ArrayList<>();
        for (long n = offset + 1; n <= offset + count; n++) {
            String isin = n % 4 == 0 ? null : String.format("AT%010d", n);
            Trade trade = new Trade("T-" + businessDate + "-" + n, isin, businessDate);
            em.persist(trade);
            trades.add(trade);
        }
        em.flush();
        return trades;
    }

    /**
     * Fetches a trade again from the feed, now with its ISIN.
     *
     * @param id the trade
     */
    public void reimport(UUID id) {
        Trade trade = em.find(Trade.class, id);
        trade.setIsin(String.format("AT%010d", Math.abs(trade.getTradeNumber().hashCode() % 1_000_000_000)));
    }
}
