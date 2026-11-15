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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
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
import org.os890.dtp.core.domain.RouteKind;
import org.os890.dtp.core.engine.FlowAdmin;
import org.os890.dtp.core.engine.FlowAdmin.Migration;
import org.os890.dtp.core.engine.FlowAdmin.Redirect;
import org.os890.dtp.core.query.FlowQueries;
import org.os890.dtp.core.query.FlowQueries.ActivationView;
import org.os890.dtp.core.query.FlowQueries.FlowView;
import org.os890.dtp.core.query.FlowQueries.Issue;
import org.os890.dtp.core.query.FlowQueries.OverrideView;

/** Flows: versions, diagrams generated from the DB, validation, activations (also per lane), overrides, migrations. */
@Path("/dtp/flows")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "dtp flows", description = "Flow versions, diagrams, activations, overrides, migrations")
public class FlowsResource {

    @Inject
    DtpAccess access;

    @Inject
    FlowQueries queries;

    @Inject
    FlowAdmin admin;

    /**
     * Activation request.
     *
     * @param version    the version hash (or a unique prefix)
     * @param activeFrom from when, or {@code null} for now
     * @param by         who, default: the caller
     * @param reason     why
     * @param lane       the lane the version is active for, or {@code null} for all lanes
     */
    public record Activation(String version, Instant activeFrom, String by, String reason, String lane) {
    }

    /**
     * Override request.
     *
     * @param fromJob    the job that reports the signal
     * @param signal     the signal
     * @param kind       TO, BACK, FLOW, COMPLETE or DISCARD
     * @param target     the target job or flow
     * @param activeFrom from when, or {@code null} for now
     * @param until      until when, or {@code null}
     * @param by         who, default: the caller
     * @param reason     why
     */
    public record OverrideRequest(String fromJob, String signal, RouteKind kind, String target, Instant activeFrom,
                                  Instant until, String by, String reason) {
    }

    /**
     * Migration request.
     *
     * @param from the old version
     * @param to   the new version
     * @param by   who, default: the caller
     */
    public record MigrationRequest(String from, String to, String by) {
    }

    /**
     * The flows with their versions.
     *
     * @param security the security context
     * @param headers  the headers
     * @return the flows
     */
    @GET
    @Operation(summary = "The flows with their versions, the active one and the ones active per lane")
    public List<FlowView> flows(@Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return queries.flows();
    }

    /**
     * Issues of the active versions.
     *
     * @param security the security context
     * @param headers  the headers
     * @return the issues
     */
    @GET
    @Path("/validation")
    @Operation(summary = "Issues of the active versions (unrouted signals, unreachable steps, ...)")
    public List<Issue> validation(@Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return queries.validate();
    }

    /**
     * All flows as Markdown with diagrams.
     *
     * @param security the security context
     * @param headers  the headers
     * @return the Markdown
     */
    @GET
    @Path("/export")
    @Produces("text/markdown")
    @Operation(summary = "All flows as Markdown with Mermaid diagrams")
    public String export(@Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return queries.exportMarkdown();
    }

    /**
     * The diagram of a flow version, generated from the DB.
     *
     * @param flow     the flow
     * @param version  the version, default: the active one
     * @param security the security context
     * @param headers  the headers
     * @return Mermaid source
     */
    @GET
    @Path("/{flow}/diagram")
    @Produces(MediaType.TEXT_PLAIN)
    @Operation(summary = "The diagram of a flow version, generated from the DB (Mermaid)")
    public String diagram(@PathParam("flow") String flow, @QueryParam("version") String version,
                          @Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return DtpErrors.call(() -> queries.mermaid(flow, version));
    }

    /**
     * The activations of a flow.
     *
     * @param flow     the flow
     * @param security the security context
     * @param headers  the headers
     * @return the activations, newest first
     */
    @GET
    @Path("/{flow}/activations")
    @Operation(summary = "The activations of a flow, newest first")
    public List<ActivationView> activations(@PathParam("flow") String flow, @Context SecurityContext security,
                                            @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return DtpErrors.call(() -> queries.activations(flow));
    }

    /**
     * Activates a version, for all lanes or for one lane.
     *
     * @param flow       the flow
     * @param activation the activation
     * @param security   the security context
     * @param headers    the headers
     * @return 204
     */
    @POST
    @Path("/{flow}/activations")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Activates a version, for all lanes or for one lane (administrators)")
    public Response activate(@PathParam("flow") String flow, Activation activation,
                             @Context SecurityContext security, @Context HttpHeaders headers) {
        DtpAccess.Caller caller = access.caller(security, headers).require(DtpAccess.ADMIN);
        String by = activation.by() != null ? activation.by() : caller.requireUser();
        DtpErrors.run(() -> {
            if (activation.lane() == null) {
                admin.activate(flow, activation.version(), activation.activeFrom(), by, activation.reason());
            } else {
                admin.activateForLane(flow, activation.lane(), activation.version(), activation.activeFrom(), by,
                        activation.reason());
            }
        });
        return Response.noContent().build();
    }

    /**
     * Ends the activation of a lane.
     *
     * @param flow     the flow
     * @param lane     the lane
     * @param by       who, default: the caller
     * @param reason   why
     * @param security the security context
     * @param headers  the headers
     * @return 204
     */
    @DELETE
    @Path("/{flow}/activations/lanes/{lane}")
    @Operation(summary = "Ends the activation of a lane: it follows the version for all lanes again (administrators)")
    public Response endLaneActivation(@PathParam("flow") String flow, @PathParam("lane") String lane,
                                      @QueryParam("by") String by, @QueryParam("reason") String reason,
                                      @Context SecurityContext security, @Context HttpHeaders headers) {
        DtpAccess.Caller caller = access.caller(security, headers).require(DtpAccess.ADMIN);
        String who = by != null ? by : caller.requireUser();
        DtpErrors.run(() -> admin.endLaneActivation(flow, lane, who, reason));
        return Response.noContent().build();
    }

    /**
     * The overrides of a flow.
     *
     * @param flow     the flow
     * @param security the security context
     * @param headers  the headers
     * @return the overrides in effect or planned
     */
    @GET
    @Path("/{flow}/overrides")
    @Operation(summary = "The overrides of a flow that are in effect or planned")
    public List<OverrideView> overrides(@PathParam("flow") String flow, @Context SecurityContext security,
                                        @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return DtpErrors.call(() -> queries.overrides(flow));
    }

    /**
     * Overrules one hop of a flow.
     *
     * @param flow     the flow
     * @param override the override
     * @param security the security context
     * @param headers  the headers
     * @return the id of the override
     */
    @POST
    @Path("/{flow}/overrides")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Overrules one hop of a flow, optionally time-boxed (administrators)")
    public Map<String, UUID> override(@PathParam("flow") String flow, OverrideRequest override,
                                      @Context SecurityContext security, @Context HttpHeaders headers) {
        DtpAccess.Caller caller = access.caller(security, headers).require(DtpAccess.ADMIN);
        String by = override.by() != null ? override.by() : caller.requireUser();
        return Map.of("id", DtpErrors.call(() -> admin.override(new Redirect(flow, override.fromJob(),
                override.signal(), override.kind(), override.target()), override.activeFrom(), override.until(), by,
                override.reason())));
    }

    /**
     * Ends an override.
     *
     * @param id       the override
     * @param security the security context
     * @param headers  the headers
     * @return 204
     */
    @DELETE
    @Path("/overrides/{id}")
    @Operation(summary = "Ends an override (administrators)")
    public Response endOverride(@PathParam("id") UUID id, @Context SecurityContext security,
                                @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.ADMIN);
        if (!admin.endOverride(id)) {
            throw new NotFoundException("no override " + id + " in effect");
        }
        return Response.noContent().build();
    }

    /**
     * Pins the queued items of a version to another version.
     *
     * @param flow     the flow
     * @param request  the migration
     * @param security the security context
     * @param headers  the headers
     * @return migrated and skipped items
     */
    @POST
    @Path("/{flow}/migrations")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Pins the queued items of a version to another version (administrators)")
    public Migration migrate(@PathParam("flow") String flow, MigrationRequest request,
                             @Context SecurityContext security, @Context HttpHeaders headers) {
        DtpAccess.Caller caller = access.caller(security, headers).require(DtpAccess.ADMIN);
        String by = request.by() != null ? request.by() : caller.requireUser();
        return DtpErrors.call(() -> admin.migrateQueued(flow, request.from(), request.to(), by));
    }
}
