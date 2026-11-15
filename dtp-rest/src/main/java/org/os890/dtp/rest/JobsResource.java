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
import java.util.Map;
import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
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
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.core.engine.FlowInstances;
import org.os890.dtp.core.engine.JobAdmin;
import org.os890.dtp.core.engine.JobAdmin.JobView;
import org.os890.dtp.core.engine.JobAdmin.RevisionView;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.DtpQueries.RunView;

/**
 * Job definitions and runs: the definitions with their triggers and revisions (they are written by SQL scripts, D1),
 * enabling and disabling, triggering a run with JSON parameters, cancelling and rerunning runs.
 */
@Path("/dtp")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "dtp jobs and runs", description = "Job definitions, triggers, runs, cancel, rerun")
public class JobsResource {

    @Inject
    DtpAccess access;

    @Inject
    JobAdmin jobs;

    @Inject
    DtpQueries queries;

    @Inject
    FlowInstances instances;

    /**
     * The jobs with their current revision.
     *
     * @param security the security context
     * @param headers  the headers
     * @return the jobs
     */
    @GET
    @Path("/jobs")
    @Operation(summary = "The jobs with their current revision and triggers")
    public List<JobView> jobs(@Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return jobs.jobs();
    }

    /**
     * The revisions of a job.
     *
     * @param name     the job
     * @param security the security context
     * @param headers  the headers
     * @return the revisions, newest first
     */
    @GET
    @Path("/jobs/{name}/revisions")
    @Operation(summary = "The revisions of a job, newest first")
    public List<RevisionView> revisions(@PathParam("name") String name, @Context SecurityContext security,
                                        @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return DtpErrors.call(() -> jobs.revisions(name));
    }

    /**
     * Enables a job.
     *
     * @param name     the job
     * @param security the security context
     * @param headers  the headers
     * @return whether the state changed
     */
    @POST
    @Path("/jobs/{name}/enable")
    @Operation(summary = "Enables a job (administrators)")
    public Map<String, Boolean> enable(@PathParam("name") String name, @Context SecurityContext security,
                                       @Context HttpHeaders headers) {
        return setEnabled(name, true, security, headers);
    }

    /**
     * Disables a job: it isn't planned any more, running runs finish.
     *
     * @param name     the job
     * @param security the security context
     * @param headers  the headers
     * @return whether the state changed
     */
    @POST
    @Path("/jobs/{name}/disable")
    @Operation(summary = "Disables a job: no new runs, running runs finish (administrators)")
    public Map<String, Boolean> disable(@PathParam("name") String name, @Context SecurityContext security,
                                        @Context HttpHeaders headers) {
        return setEnabled(name, false, security, headers);
    }

    private Map<String, Boolean> setEnabled(String name, boolean enabled, SecurityContext security,
                                            HttpHeaders headers) {
        String user = access.caller(security, headers).require(DtpAccess.ADMIN).requireUser();
        return Map.of("changed", DtpErrors.call(() -> jobs.setEnabled(name, enabled, user)));
    }

    /**
     * Triggers a run of a job that allows external triggers.
     *
     * @param name           the job
     * @param idempotencyKey a second request with the same key plans no second run, optional
     * @param parameters     the run parameters as JSON, optional
     * @param security       the security context
     * @param headers        the headers
     * @return the run, and whether it was newly created
     */
    @POST
    @Path("/jobs/{name}/runs")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Triggers a run with JSON parameters, checked against the parameter type (operators)")
    public RunHandle trigger(@PathParam("name") String name, @HeaderParam("Idempotency-Key") String idempotencyKey,
                             String parameters, @Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.OPERATOR);
        return DtpErrors.call(() -> jobs.trigger(name, parameters, idempotencyKey));
    }

    /**
     * The most recent runs.
     *
     * @param limit    maximum number
     * @param security the security context
     * @param headers  the headers
     * @return the runs, newest first
     */
    @GET
    @Path("/runs")
    @Operation(summary = "The most recent runs with node, outcome, typed result and run info")
    public List<RunView> runs(@QueryParam("limit") @DefaultValue("20") int limit, @Context SecurityContext security,
                              @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return queries.runs(limit);
    }

    /**
     * A run.
     *
     * @param id       the run
     * @param security the security context
     * @param headers  the headers
     * @return the run
     */
    @GET
    @Path("/runs/{id}")
    @Operation(summary = "A run")
    public RunView run(@PathParam("id") UUID id, @Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return queries.run(id).orElseThrow(() -> new NotFoundException("unknown run " + id));
    }

    /**
     * Cancels a run: a pending one is finished as cancelled, a running one stops softly.
     *
     * @param id       the run
     * @param security the security context
     * @param headers  the headers
     * @return whether the run was pending or running
     */
    @POST
    @Path("/runs/{id}/cancel")
    @Operation(summary = "Cancels a run: pending ones are cancelled, running ones stop softly (operators)")
    public Map<String, Boolean> cancel(@PathParam("id") UUID id, @Context SecurityContext security,
                                       @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.OPERATOR);
        return Map.of("cancelled", jobs.cancel(id));
    }

    /**
     * Runs a finished run of an open flow instance again, e.g. a failed member of a join.
     *
     * @param id       the run
     * @param security the security context
     * @param headers  the headers
     * @return the new run
     */
    @POST
    @Path("/runs/{id}/reruns")
    @Operation(summary = "Runs a finished run of an open flow instance again (operators)")
    public Map<String, UUID> rerun(@PathParam("id") UUID id, @Context SecurityContext security,
                                   @Context HttpHeaders headers) {
        String user = access.caller(security, headers).require(DtpAccess.OPERATOR).requireUser();
        return Map.of("runId", DtpErrors.call(() -> instances.rerun(id, user)));
    }
}
