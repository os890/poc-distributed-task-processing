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
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.DtpQueries.ItemCount;
import org.os890.dtp.core.query.DtpQueries.NodeView;
import org.os890.dtp.core.query.DtpQueries.RunView;

/** Read-only operator views of the cluster (the REST adapter of the design follows in a later slice). */
@Path("/dtp")
@Produces(MediaType.APPLICATION_JSON)
public class DtpResource {

    @Inject
    DtpQueries queries;

    /**
     * Nodes of the cluster.
     *
     * @return the nodes
     */
    @GET
    @Path("/nodes")
    public List<NodeView> nodes() {
        return queries.nodes();
    }

    /**
     * Most recent runs.
     *
     * @param limit maximum number of runs
     * @return the runs
     */
    @GET
    @Path("/runs")
    public List<RunView> runs(@QueryParam("limit") @DefaultValue("20") int limit) {
        return queries.runs(limit);
    }

    /**
     * Items per job and state.
     *
     * @return the counts
     */
    @GET
    @Path("/items")
    public List<ItemCount> items() {
        return queries.itemCounts();
    }
}
