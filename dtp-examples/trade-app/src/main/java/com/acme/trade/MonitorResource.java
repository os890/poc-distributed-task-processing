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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.sse.OutboundSseEvent;
import jakarta.ws.rs.sse.Sse;
import jakarta.ws.rs.sse.SseEventSink;

import org.os890.dtp.cdi.DtpSettings;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.query.MonitoringFeed;
import org.os890.dtp.core.query.MonitoringFeed.Subscription;
import org.os890.dtp.core.query.MonitoringQueries;
import org.os890.dtp.core.query.MonitoringQueries.FailureView;
import org.os890.dtp.core.query.MonitoringQueries.HeatMap;
import org.os890.dtp.core.query.MonitoringQueries.MinuteView;

/**
 * The heat map of the cluster (docs/design/09-monitoring-heatmap.md §9.7): snapshots, minute series and failures read
 * from the monitoring views, the heat map as a Mermaid diagram of a flow, and the live feed as Server-Sent Events.
 * Any node answers, the views are cluster-wide.
 */
@Path("/dtp/monitor")
@Produces(MediaType.APPLICATION_JSON)
public class MonitorResource {

    @Inject
    MonitoringQueries monitoring;

    @Inject
    MonitoringFeed feed;

    /**
     * The heat map: status, queue, jam score and alert state of every step, edges with items per minute.
     *
     * @param range the range of the counters, e.g. {@code 15m}
     * @return the snapshot
     */
    @GET
    @Path("/heatmap")
    public HeatMap heatmap(@QueryParam("range") @DefaultValue("15m") String range) {
        return monitoring.heatmap(duration(range));
    }

    /**
     * The heat map as a Mermaid diagram of a flow, e.g. for a wiki page or a ticket.
     *
     * @param flow  the flow
     * @param range the range of the counters
     * @return the diagram source
     */
    @GET
    @Path("/heatmap/diagram")
    @Produces(MediaType.TEXT_PLAIN)
    public String diagram(@QueryParam("flow") @DefaultValue("trade-flow") String flow,
                          @QueryParam("range") @DefaultValue("15m") String range) {
        return call(() -> monitoring.diagram(flow, duration(range)));
    }

    /**
     * The minute series of a step.
     *
     * @param step  the job name
     * @param range e.g. {@code 24h}
     * @return the minutes with activity
     */
    @GET
    @Path("/steps/{step}")
    public List<MinuteView> series(@PathParam("step") String step,
                                   @QueryParam("range") @DefaultValue("1h") String range) {
        return call(() -> monitoring.series(step, duration(range)));
    }

    /**
     * The technical failures of a step per error type. Messages and sample items may contain business data, so only
     * operators see them (demo: a header instead of a JWT).
     *
     * @param step   the job name
     * @param range  e.g. {@code 1h}
     * @param groups the groups of the caller
     * @return the error types, most frequent first
     */
    @GET
    @Path("/steps/{step}/failures")
    public List<FailureView> failures(@PathParam("step") String step,
                                      @QueryParam("range") @DefaultValue("1h") String range,
                                      @HeaderParam("X-Groups") String groups) {
        if (groups == null || Arrays.stream(groups.split(",")).map(String::trim)
                                    .noneMatch(Decisions.OPERATORS::equals)) {
            throw new ForbiddenException("only " + Decisions.OPERATORS);
        }
        return call(() -> monitoring.failures(step, duration(range)));
    }

    /**
     * The live feed: {@code heatmap-update} with the changed steps after every projection cycle, {@code alert} with
     * the steps whose alert state changed. The node polls the projection only while clients are connected.
     *
     * @param sink the client
     * @param sse  the event factory
     */
    @GET
    @Path("/stream")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public void stream(@Context SseEventSink sink, @Context Sse sse) {
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

    private static <T> T call(Supplier<T> action) {
        try {
            return action.get();
        } catch (IllegalArgumentException e) {
            throw new NotFoundException(e.getMessage());
        }
    }
}
