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

import java.time.Duration;
import java.util.List;

import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.sse.OutboundSseEvent;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseEventSink;

import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.os890.dtp.cdi.DtpSettings;
import org.os890.dtp.core.query.FlowQueries;
import org.os890.dtp.core.query.MonitoringFeed;
import org.os890.dtp.core.query.MonitoringFeed.Subscription;
import org.os890.dtp.core.query.MonitoringQueries;
import org.os890.dtp.core.query.MonitoringQueries.FailureView;
import org.os890.dtp.core.query.MonitoringQueries.HeatMap;
import org.os890.dtp.core.query.MonitoringQueries.MinuteView;

/**
 * The monitoring read model (design 09 §9.7): heat map, diagram, minute series, failures and the live feed. Reads
 * only the views, never the processing tables. Failures carry business data: operators only.
 */
@Path("/dtp/monitor")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "dtp monitoring", description = "Heat map, minute series, failures, live feed")
public class MonitorResource {

    @Inject
    DtpAccess access;

    @Inject
    MonitoringQueries monitoring;

    @Inject
    MonitoringFeed feed;

    @Inject
    FlowQueries flows;

    /**
     * The heat map.
     *
     * @param range    e.g. {@code 15m}
     * @param security the security context
     * @param headers  the headers
     * @return status, queue, jam score and alert per step, edges
     */
    @GET
    @Path("/heatmap")
    @Operation(summary = "The heat map: status, queue, jam score and alert per step, edges")
    public HeatMap heatmap(@QueryParam("range") @DefaultValue("15m") String range, @Context SecurityContext security,
                           @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return monitoring.heatmap(duration(range));
    }

    /**
     * A flow colored by its state.
     *
     * @param flow     the flow, optional if there is only one
     * @param range    e.g. {@code 15m}
     * @param security the security context
     * @param headers  the headers
     * @return Mermaid source
     */
    @GET
    @Path("/heatmap/diagram")
    @Produces(MediaType.TEXT_PLAIN)
    @Operation(summary = "A flow colored by its state (Mermaid)")
    public String diagram(@QueryParam("flow") String flow, @QueryParam("range") @DefaultValue("15m") String range,
                          @Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        String name = flow;
        if (name == null) {
            List<FlowQueries.FlowView> all = flows.flows();
            if (all.size() != 1) {
                throw new BadRequestException("flow is required, there are " + all.size() + " flows");
            }
            name = all.getFirst().name();
        }
        String chosen = name;
        return DtpErrors.call(() -> monitoring.diagram(chosen, duration(range)));
    }

    /**
     * The minute series of a step.
     *
     * @param step     the job name
     * @param range    e.g. {@code 1h}
     * @param security the security context
     * @param headers  the headers
     * @return the minutes
     */
    @GET
    @Path("/steps/{step}")
    @Operation(summary = "The minute series of a step")
    public List<MinuteView> series(@PathParam("step") String step,
                                   @QueryParam("range") @DefaultValue("1h") String range,
                                   @Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return DtpErrors.call(() -> monitoring.series(step, duration(range)));
    }

    /**
     * The technical failures of a step per error type, with a sample message and item.
     *
     * @param step     the job name
     * @param range    e.g. {@code 1h}
     * @param security the security context
     * @param headers  the headers
     * @return the error types, most frequent first
     */
    @GET
    @Path("/steps/{step}/failures")
    @Operation(summary = "The technical failures of a step per error type (operators)")
    public List<FailureView> failures(@PathParam("step") String step,
                                      @QueryParam("range") @DefaultValue("1h") String range,
                                      @Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.OPERATOR);
        return DtpErrors.call(() -> monitoring.failures(step, duration(range)));
    }

    /**
     * The live feed: {@code heatmap-update} with the changed steps, {@code alert} with changed alert states.
     *
     * @param sink     the client
     * @param sse      the event factory
     * @param security the security context
     * @param headers  the headers
     */
    @GET
    @Path("/stream")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @Operation(summary = "The live feed of the heat map (Server-Sent Events)")
    public void stream(@Context SseEventSink sink, @Context Sse sse, @Context SecurityContext security,
                       @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        Subscription[] subscription = new Subscription[1];
        subscription[0] = feed.subscribe(update -> {
            if (sink.isClosed()) {
                throw new IllegalStateException("client disconnected");
            }
            OutboundSseEvent event = sse.newEventBuilder().name(update.type()).id(Long.toString(update.version()))
                    .mediaType(MediaType.APPLICATION_JSON_TYPE).data(MonitoringFeed.Update.class, update).build();
            sink.send(event).whenComplete((ok, error) -> {
                if (error != null) {
                    subscription[0].close();
                    sink.close();
                }
            });
        });
        sink.send(sse.newEventBuilder().name("connected").data(String.class, "dtp heat map").build());
    }

    private static Duration duration(String range) {
        try {
            return DtpSettings.parse(range);
        } catch (RuntimeException e) {
            throw new BadRequestException("invalid range " + range);
        }
    }
}
