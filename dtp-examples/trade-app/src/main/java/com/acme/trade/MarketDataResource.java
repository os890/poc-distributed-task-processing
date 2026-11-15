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

import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

/** Demo controls of the simulated market data service of this node: an outage fails the enrichment technically. */
@Path("/market-data/outage")
public class MarketDataResource {

    @Inject
    MarketData marketData;

    /** The market data service of this node is unavailable from now on. */
    @POST
    public void outage() {
        marketData.setOutage(true);
    }

    /** The market data service is back. */
    @DELETE
    public void recovered() {
        marketData.setOutage(false);
    }
}
