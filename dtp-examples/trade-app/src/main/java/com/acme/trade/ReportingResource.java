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
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.os890.dtp.core.engine.CollectionWindows;
import org.os890.dtp.core.engine.CollectionWindows.OccurrenceView;

/** The regulatory trade report: start the collection window now, and the report files of this node. */
@Path("/reporting")
@Produces(MediaType.APPLICATION_JSON)
public class ReportingResource {

    /** The collection window of the trade report (definition {@code V7__trade_reporting.sql}). */
    static final String WINDOW = "trade-reporting";

    @Inject
    CollectionWindows windows;

    @Inject
    TradeReports reports;

    /**
     * Starts the window now, e.g. a report wanted before the scheduled one.
     *
     * @param user the operator (demo: a header instead of a JWT)
     * @return the active occurrence: the new one, or the one that was active already
     */
    @POST
    @Path("/occurrences")
    public OccurrenceView startNow(@HeaderParam("X-User") String user) {
        if (user == null || user.isBlank()) {
            throw new NotAuthorizedException("X-User");
        }
        return windows.startNow(WINDOW, user);
    }

    /**
     * The report files this node wrote.
     *
     * @return the files
     */
    @GET
    @Path("/files")
    public List<String> files() {
        return reports.files();
    }
}
