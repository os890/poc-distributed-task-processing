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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.engine.Decisions.DecisionView;
import org.os890.dtp.core.engine.Decisions.Vote;

/**
 * Decision requests: the open ones of the caller's teams, deciding and voting (mission §6). Only members of the
 * responsible team see and decide a request; technical ones go to {@code dtp-operator}.
 */
@Path("/dtp/decisions")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "dtp decisions", description = "Decision requests and approvals of the caller's teams")
public class DecisionsResource {

    @Inject
    DtpAccess access;

    @Inject
    Decisions decisions;

    /**
     * A decision.
     *
     * @param option  the chosen option
     * @param comment the comment, optional
     */
    public record DecisionRequest(String option, String comment) {
    }

    /**
     * A vote of an approval.
     *
     * @param vote    APPROVE or REJECT
     * @param comment the comment, optional
     */
    public record VoteRequest(Vote vote, String comment) {
    }

    /**
     * The open requests the caller may decide.
     *
     * @param security the security context
     * @param headers  the headers
     * @return the requests
     */
    @GET
    @Operation(summary = "The open decision requests of the caller's teams")
    public List<DecisionView> open(@Context SecurityContext security, @Context HttpHeaders headers) {
        DtpAccess.Caller caller = access.caller(security, headers);
        caller.requireUser();
        return decisions.open(caller.decider(decisions.openTeams()));
    }

    /**
     * A request with its context.
     *
     * @param id       the request
     * @param security the security context
     * @param headers  the headers
     * @return the request
     */
    @GET
    @Path("/{id}")
    @Operation(summary = "A decision request with its options and votes")
    public DecisionView get(@PathParam("id") UUID id, @Context SecurityContext security,
                            @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER, DtpAccess.BUSINESS, DtpAccess.OPERATOR);
        return DtpErrors.call(() -> decisions.get(id));
    }

    /**
     * Decides a request.
     *
     * @param id       the request
     * @param decision the option and comment
     * @param security the security context
     * @param headers  the headers
     * @return 204
     */
    @POST
    @Path("/{id}/decision")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Decides a request (members of the responsible team)")
    public Response decide(@PathParam("id") UUID id, DecisionRequest decision, @Context SecurityContext security,
                           @Context HttpHeaders headers) {
        DtpAccess.Caller caller = access.caller(security, headers);
        caller.requireUser();
        DtpErrors.run(() -> decisions.decide(id, decision.option(), caller.decider(teamOf(id)),
                decision.comment()));
        return Response.noContent().build();
    }

    /**
     * Votes on an approval.
     *
     * @param id       the request
     * @param vote     the vote and comment
     * @param security the security context
     * @param headers  the headers
     * @return whether the vote decided the approval
     */
    @POST
    @Path("/{id}/votes")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Votes on an approval (members of the responsible team)")
    public Map<String, Boolean> vote(@PathParam("id") UUID id, VoteRequest vote, @Context SecurityContext security,
                                     @Context HttpHeaders headers) {
        DtpAccess.Caller caller = access.caller(security, headers);
        caller.requireUser();
        return Map.of("decided", DtpErrors.call(() -> decisions.vote(id, vote.vote(), caller.decider(teamOf(id)),
                vote.comment())));
    }

    private List<String> teamOf(UUID id) {
        String team = DtpErrors.call(() -> decisions.get(id)).responsibleGroup();
        return team == null ? List.of() : List.of(team);
    }
}
