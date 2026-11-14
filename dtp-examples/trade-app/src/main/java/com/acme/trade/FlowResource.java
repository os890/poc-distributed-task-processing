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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import jakarta.inject.Inject;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.domain.RouteKind;
import org.os890.dtp.core.engine.FlowAdmin;
import org.os890.dtp.core.engine.FlowAdmin.Migration;
import org.os890.dtp.core.engine.FlowAdmin.Redirect;
import org.os890.dtp.core.query.FlowQueries;
import org.os890.dtp.core.query.FlowQueries.ActivationView;
import org.os890.dtp.core.query.FlowQueries.FlowView;
import org.os890.dtp.core.query.FlowQueries.Issue;
import org.os890.dtp.core.query.FlowQueries.OverrideView;

/**
 * Flows for operators: versions, diagrams generated from the DB, validation, activation, overrides and migration.
 * A demo without security, protect these endpoints in a real application.
 */
@Path("/dtp/flows")
@Produces(MediaType.APPLICATION_JSON)
public class FlowResource {

    @Inject
    FlowQueries queries;

    @Inject
    FlowAdmin admin;

    /**
     * Activation request.
     *
     * @param version    the version hash (or a unique prefix)
     * @param activeFrom from when, or {@code null} for now
     * @param by         who
     * @param reason     why
     */
    public record Activation(String version, Instant activeFrom, String by, String reason) {
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
     * @param by         who
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
     * @param by   who
     */
    public record MigrationRequest(String from, String to, String by) {
    }

    @GET
    public List<FlowView> flows() {
        return queries.flows();
    }

    @GET
    @Path("/validation")
    public List<Issue> validation() {
        return queries.validate();
    }

    @GET
    @Path("/export")
    @Produces("text/markdown")
    public String export() {
        return queries.exportMarkdown();
    }

    @GET
    @Path("/{flow}/diagram")
    @Produces(MediaType.TEXT_PLAIN)
    public String diagram(@PathParam("flow") String flow, @QueryParam("version") String version) {
        return call(() -> queries.mermaid(flow, version));
    }

    @GET
    @Path("/{flow}/activations")
    public List<ActivationView> activations(@PathParam("flow") String flow) {
        return call(() -> queries.activations(flow));
    }

    @POST
    @Path("/{flow}/activations")
    public Response activate(@PathParam("flow") String flow, Activation activation) {
        call(() -> {
            admin.activate(flow, activation.version(), activation.activeFrom(), activation.by(),
                    activation.reason());
            return null;
        });
        return Response.noContent().build();
    }

    @GET
    @Path("/{flow}/overrides")
    public List<OverrideView> overrides(@PathParam("flow") String flow) {
        return call(() -> queries.overrides(flow));
    }

    @POST
    @Path("/{flow}/overrides")
    public Map<String, UUID> override(@PathParam("flow") String flow, OverrideRequest override) {
        return Map.of("id", call(() -> admin.override(new Redirect(flow, override.fromJob(), override.signal(),
                override.kind(), override.target()), override.activeFrom(), override.until(), override.by(),
                override.reason())));
    }

    @DELETE
    @Path("/overrides/{id}")
    public Response endOverride(@PathParam("id") UUID id) {
        if (!admin.endOverride(id)) {
            throw new NotFoundException("no override " + id + " in effect");
        }
        return Response.noContent().build();
    }

    @POST
    @Path("/{flow}/migrations")
    public Migration migrate(@PathParam("flow") String flow, MigrationRequest request) {
        return call(() -> admin.migrateQueued(flow, request.from(), request.to(), request.by()));
    }

    private static <T> T call(Supplier<T> action) {
        try {
            return action.get();
        } catch (IllegalArgumentException e) {
            throw new NotFoundException(e.getMessage());
        } catch (DtpException | NullPointerException e) {
            throw new ClientErrorException(e.getMessage(), Response.Status.CONFLICT);
        }
    }
}
