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

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.query.JourneyGraph;
import org.os890.dtp.core.query.JourneyQueries;
import org.os890.dtp.core.query.JourneyQueries.Detail;
import org.os890.dtp.core.query.JourneyQueries.Options;

/**
 * The journey of any item (docs/design/09-monitoring-heatmap.md §9.9): the journal as a table, and the journey diagram
 * as Mermaid source or as the folded graph. Viewers and operators only; node names and error messages only for
 * operators (demo: a header instead of a JWT). The class shares its root path with {@link DtpResource}, so that
 * {@code GET /dtp/items} stays there.
 */
@Path("/dtp")
public class JourneyResource {

    /** The group of people who may read journeys. */
    public static final String VIEWERS = "dtp-viewer";

    @Inject
    JourneyQueries journeys;

    @Inject
    DtpQueries queries;

    /**
     * The journal entries of an item, numbered as in the diagram.
     *
     * @param itemType    the item type, e.g. {@code trade}
     * @param businessKey the business key
     * @param groups      the groups of the caller
     * @return the entries
     */
    @GET
    @Path("/items/{itemType}/{businessKey}/journey")
    @Produces(MediaType.APPLICATION_JSON)
    public List<JourneyEntry> journey(@PathParam("itemType") String itemType,
                                      @PathParam("businessKey") String businessKey,
                                      @HeaderParam("X-Groups") String groups) {
        requireViewer(groups);
        List<JourneyEntry> journey = queries.journey(itemType, businessKey);
        if (journey.isEmpty()) {
            throw new NotFoundException("unknown item " + itemType + " " + businessKey);
        }
        return journey;
    }

    /**
     * The journey diagram of an item.
     *
     * @param itemType    the item type, e.g. {@code trade}
     * @param businessKey the business key
     * @param format      {@code mermaid} (text/plain) or {@code json} (the folded graph)
     * @param detail      {@code full} or {@code steps}
     * @param zone        the time zone of the labels, e.g. {@code Europe/Vienna}
     * @param groups      the groups of the caller
     * @return the diagram
     */
    @GET
    @Path("/items/{itemType}/{businessKey}/journey/diagram")
    public Response diagram(@PathParam("itemType") String itemType, @PathParam("businessKey") String businessKey,
                            @QueryParam("format") @DefaultValue("mermaid") String format,
                            @QueryParam("detail") @DefaultValue("full") String detail,
                            @QueryParam("zone") String zone, @HeaderParam("X-Groups") String groups) {
        Options options = options(detail, zone, groups);
        return render(journeys.graph(itemType, businessKey, options), format, itemType + " " + businessKey);
    }

    /**
     * The journey diagram of an item, by its internal id.
     *
     * @param itemId the item
     * @param format {@code mermaid} (text/plain) or {@code json} (the folded graph)
     * @param detail {@code full} or {@code steps}
     * @param zone   the time zone of the labels, e.g. {@code Europe/Vienna}
     * @param groups the groups of the caller
     * @return the diagram
     */
    @GET
    @Path("/items/by-id/{itemId}/journey/diagram")
    public Response diagramById(@PathParam("itemId") UUID itemId,
                                @QueryParam("format") @DefaultValue("mermaid") String format,
                                @QueryParam("detail") @DefaultValue("full") String detail,
                                @QueryParam("zone") String zone, @HeaderParam("X-Groups") String groups) {
        Options options = options(detail, zone, groups);
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

    private static Options options(String detail, String zone, String groups) {
        Set<String> member = requireViewer(groups);
        Detail level;
        ZoneId zoneId;
        try {
            level = Detail.valueOf(detail.toUpperCase(Locale.ROOT));
            zoneId = zone == null ? ZoneId.systemDefault() : ZoneId.of(zone);
        } catch (IllegalArgumentException | DateTimeException e) {
            throw new BadRequestException(e.getMessage());
        }
        return new Options(level, member.contains(Decisions.OPERATORS), zoneId);
    }

    private static Set<String> requireViewer(String groups) {
        Set<String> member = groups == null ? Set.of()
                : Arrays.stream(groups.split(",")).map(String::trim).collect(Collectors.toSet());
        if (!member.contains(VIEWERS) && !member.contains(Decisions.OPERATORS)) {
            throw new ForbiddenException("only " + VIEWERS + " and " + Decisions.OPERATORS);
        }
        return member;
    }
}
