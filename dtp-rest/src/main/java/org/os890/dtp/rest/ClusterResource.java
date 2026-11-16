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
package org.os890.dtp.rest;

import java.util.List;
import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.SecurityContext;

import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.os890.dtp.api.BusinessCalendars;
import org.os890.dtp.core.engine.CollectionWindows;
import org.os890.dtp.core.engine.CollectionWindows.OccurrenceView;
import org.os890.dtp.core.engine.FlowInstances;
import org.os890.dtp.core.engine.FlowInstances.Cancelled;
import org.os890.dtp.core.engine.FlowInstances.InstanceView;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.DtpQueries.ItemCount;
import org.os890.dtp.core.query.DtpQueries.NodeView;

/**
 * The cluster: nodes, items per job, flow instances (and cancelling one), collection windows and business calendars.
 */
@Path("/dtp")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "dtp cluster", description = "Nodes, items, flow instances, collection windows, business calendars")
public class ClusterResource {

    @Inject
    DtpAccess access;

    @Inject
    DtpQueries queries;

    @Inject
    FlowInstances instances;

    @Inject
    CollectionWindows windows;

    @Inject
    BusinessCalendars calendars;

    /**
     * The nodes of the cluster.
     *
     * @param security the security context
     * @param headers  the headers
     * @return the nodes, incl. which one leads
     */
    @GET
    @Path("/nodes")
    @Operation(summary = "The nodes of the cluster and which one leads")
    public List<NodeView> nodes(@Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return queries.nodes();
    }

    /**
     * Items per job and state.
     *
     * @param security the security context
     * @param headers  the headers
     * @return the counts
     */
    @GET
    @Path("/items")
    @Operation(summary = "Items per job and state")
    public List<ItemCount> items(@Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return queries.itemCounts();
    }

    /**
     * The most recent flow instances.
     *
     * @param limit    maximum number
     * @param security the security context
     * @param headers  the headers
     * @return the instances
     */
    @GET
    @Path("/flow-instances")
    @Operation(summary = "The most recent flow instances (e.g. an end of day per business date)")
    public List<InstanceView> flowInstances(@QueryParam("limit") @DefaultValue("20") int limit,
                                            @Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return instances.latest(limit);
    }

    /**
     * Cancels an open flow instance, e.g. an end of day started by mistake: its pending and waiting runs are
     * cancelled, its running ones stop softly, the open decision requests about its runs are withdrawn.
     *
     * @param id       the instance
     * @param cancel   the reason
     * @param security the security context
     * @param headers  the headers
     * @return the runs cancelled or asked to stop and the withdrawn decision requests
     */
    @POST
    @Path("/flow-instances/{id}/cancel")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Cancels an open flow instance with a reason (operators)")
    public Cancelled cancelFlowInstance(@PathParam("id") UUID id, CancelRequest cancel,
                                        @Context SecurityContext security, @Context HttpHeaders headers) {
        String user = access.caller(security, headers).require(DtpAccess.OPERATOR).requireUser();
        if (cancel == null || cancel.reason() == null || cancel.reason().isBlank()) {
            throw new BadRequestException("a cancel needs a reason");
        }
        return DtpErrors.call(() -> instances.cancel(id, user, cancel.reason()));
    }

    /**
     * The body of a cancel.
     *
     * @param reason why
     */
    public record CancelRequest(String reason) {
    }

    /**
     * The most recent occurrences of the collection windows.
     *
     * @param limit    maximum number
     * @param security the security context
     * @param headers  the headers
     * @return the occurrences
     */
    @GET
    @Path("/windows")
    @Operation(summary = "The most recent occurrences of the collection windows, with cut-off")
    public List<OccurrenceView> windows(@QueryParam("limit") @DefaultValue("20") int limit,
                                        @Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return windows.latest(limit);
    }

    /**
     * Starts an occurrence of a collection window now.
     *
     * @param name     the window
     * @param security the security context
     * @param headers  the headers
     * @return the occurrence
     */
    @POST
    @Path("/windows/{name}/occurrences")
    @Operation(summary = "Starts an occurrence of a collection window now (operators)")
    public OccurrenceView startWindow(@PathParam("name") String name, @Context SecurityContext security,
                                      @Context HttpHeaders headers) {
        String user = access.caller(security, headers).require(DtpAccess.OPERATOR).requireUser();
        return DtpErrors.call(() -> windows.startNow(name, user));
    }

    /**
     * The names of the business calendars.
     *
     * @param security the security context
     * @param headers  the headers
     * @return the names
     */
    @GET
    @Path("/calendars")
    @Operation(summary = "The business calendars")
    public List<String> calendars(@Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return List.copyOf(calendars.names());
    }
}
