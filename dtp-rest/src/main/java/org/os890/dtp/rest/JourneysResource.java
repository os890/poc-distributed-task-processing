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

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.query.JourneyGraph;
import org.os890.dtp.core.query.JourneyQueries;
import org.os890.dtp.core.query.JourneyQueries.Detail;
import org.os890.dtp.core.query.JourneyQueries.Options;

/**
 * The journey of an item (design 09 §9.9): the journal as a table, and the journey diagram as Mermaid source or as the
 * folded graph. Node names and error messages only for operators. The root path is {@code /dtp} like the one of
 * {@link ClusterResource}, so that {@code GET /dtp/items} stays there.
 */
@Path("/dtp")
@Tag(name = "dtp journeys", description = "The journey of an item as a table and as a diagram")
public class JourneysResource {

    @Inject
    DtpAccess access;

    @Inject
    JourneyQueries journeys;

    @Inject
    DtpQueries queries;

    /**
     * The journal entries of an item, numbered as in the diagram.
     *
     * @param itemType    the item type
     * @param businessKey the business key
     * @param security    the security context
     * @param headers     the headers
     * @return the entries
     */
    @GET
    @Path("/items/{itemType}/{businessKey}/journey")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "The journal of an item, numbered as in its diagram")
    public List<JourneyEntry> journey(@PathParam("itemType") String itemType,
                                      @PathParam("businessKey") String businessKey,
                                      @Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        List<JourneyEntry> journey = queries.journey(itemType, businessKey);
        if (journey.isEmpty()) {
            throw new NotFoundException("unknown item " + itemType + " " + businessKey);
        }
        return journey;
    }

    /**
     * The journey diagram of an item.
     *
     * @param itemType    the item type
     * @param businessKey the business key
     * @param format      {@code mermaid} or {@code json}
     * @param detail      {@code full} or {@code steps}
     * @param zone        the time zone of the labels
     * @param security    the security context
     * @param headers     the headers
     * @return the diagram
     */
    @GET
    @Path("/items/{itemType}/{businessKey}/journey/diagram")
    @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Operation(summary = "The journey diagram of an item: Mermaid source, or the folded graph as JSON")
    public Response diagram(@PathParam("itemType") String itemType, @PathParam("businessKey") String businessKey,
                            @QueryParam("format") @DefaultValue("mermaid") String format,
                            @QueryParam("detail") @DefaultValue("full") String detail,
                            @QueryParam("zone") String zone, @Context SecurityContext security,
                            @Context HttpHeaders headers) {
        Options options = options(detail, zone, security, headers);
        return render(journeys.graph(itemType, businessKey, options), format, itemType + " " + businessKey);
    }

    /**
     * The journey diagram of an item, by its internal id.
     *
     * @param itemId   the item
     * @param format   {@code mermaid} or {@code json}
     * @param detail   {@code full} or {@code steps}
     * @param zone     the time zone of the labels
     * @param security the security context
     * @param headers  the headers
     * @return the diagram
     */
    @GET
    @Path("/items/by-id/{itemId}/journey/diagram")
    @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Operation(summary = "The journey diagram of an item by its internal id")
    public Response diagramById(@PathParam("itemId") UUID itemId,
                                @QueryParam("format") @DefaultValue("mermaid") String format,
                                @QueryParam("detail") @DefaultValue("full") String detail,
                                @QueryParam("zone") String zone, @Context SecurityContext security,
                                @Context HttpHeaders headers) {
        Options options = options(detail, zone, security, headers);
        return render(journeys.graph(itemId, options), format, itemId.toString());
    }

    private static Response render(Optional<JourneyGraph> graph, String format, String item) {
        JourneyGraph found = graph.orElseThrow(() -> new NotFoundException("unknown item " + item));
        return switch (format) {
            case "mermaid" -> Response.ok(found.mermaid(), MediaType.TEXT_PLAIN_TYPE.withCharset("UTF-8")).build();
            case "json" -> Response.ok(found, MediaType.APPLICATION_JSON_TYPE).build();
            default -> throw new BadRequestException("format is mermaid or json, not " + format);
        };
    }

    private Options options(String detail, String zone, SecurityContext security, HttpHeaders headers) {
        DtpAccess.Caller caller = access.caller(security, headers).require(DtpAccess.VIEWER, DtpAccess.OPERATOR);
        try {
            return new Options(Detail.valueOf(detail.toUpperCase(Locale.ROOT)), caller.isInRole(DtpAccess.OPERATOR),
                    zone == null ? ZoneId.systemDefault() : ZoneId.of(zone));
        } catch (IllegalArgumentException | DateTimeException e) {
            throw new BadRequestException(e.getMessage());
        }
    }
}
