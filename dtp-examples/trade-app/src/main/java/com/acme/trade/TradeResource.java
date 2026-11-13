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

import java.util.List;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.os890.dtp.api.JobEngine;
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;

/** Triggers imports and shows the journey of a trade. */
@Path("/trades")
@Produces(MediaType.APPLICATION_JSON)
public class TradeResource {

    @Inject
    JobEngine jobEngine;

    @Inject
    DtpQueries queries;

    /**
     * Triggers an import. The compiler checks that the parameters match the handler's type.
     *
     * @param params the import parameters
     * @return the planned run
     */
    @POST
    @Path("/imports")
    public RunHandle triggerImport(ImportParams params) {
        return jobEngine.trigger(TradeImport.class, params);
    }

    /**
     * The journey of a trade.
     *
     * @param tradeNumber the trade number (business key)
     * @return the journal entries
     */
    @GET
    @Path("/{tradeNumber}/journey")
    public List<JourneyEntry> journey(@PathParam("tradeNumber") String tradeNumber) {
        List<JourneyEntry> journey = queries.journey(Trade.ITEM.name(), tradeNumber);
        if (journey.isEmpty()) {
            throw new NotFoundException("unknown trade " + tradeNumber);
        }
        return journey;
    }
}
