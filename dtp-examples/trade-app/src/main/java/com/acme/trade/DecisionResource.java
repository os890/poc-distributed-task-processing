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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import jakarta.inject.Inject;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.engine.Decisions;
import org.os890.dtp.core.engine.Decisions.Decider;
import org.os890.dtp.core.engine.Decisions.DecisionView;
import org.os890.dtp.core.engine.Decisions.Vote;

/**
 * Decision requests and approvals for business users and operators. Demo only: the user and the groups come from the
 * headers {@code X-User} and {@code X-Groups}, a real application takes them from the MP JWT ({@code upn},
 * {@code groups}).
 */
@Path("/dtp/decisions")
@Produces(MediaType.APPLICATION_JSON)
public class DecisionResource {

    @Inject
    Decisions decisions;

    /**
     * A decision.
     *
     * @param option  the chosen option
     * @param comment the comment
     */
    public record DecisionRequest(String option, String comment) {
    }

    /**
     * A vote.
     *
     * @param vote    APPROVE or REJECT
     * @param comment the comment
     */
    public record VoteRequest(Vote vote, String comment) {
    }

    @GET
    public List<DecisionView> open(@HeaderParam("X-User") String user, @HeaderParam("X-Groups") String groups) {
        return decisions.open(decider(user, groups));
    }

    @GET
    @Path("/{id}")
    public DecisionView get(@PathParam("id") UUID id) {
        return call(() -> decisions.get(id));
    }

    @POST
    @Path("/{id}/decision")
    public Response decide(@PathParam("id") UUID id, @HeaderParam("X-User") String user,
                           @HeaderParam("X-Groups") String groups, DecisionRequest decision) {
        call(() -> {
            decisions.decide(id, decision.option(), decider(user, groups), decision.comment());
            return null;
        });
        return Response.noContent().build();
    }

    @POST
    @Path("/{id}/votes")
    public Map<String, Boolean> vote(@PathParam("id") UUID id, @HeaderParam("X-User") String user,
                                     @HeaderParam("X-Groups") String groups, VoteRequest vote) {
        return Map.of("decided", call(() -> decisions.vote(id, vote.vote(), decider(user, groups), vote.comment())));
    }

    private static Decider decider(String user, String groups) {
        if (user == null || user.isBlank()) {
            throw new NotAuthorizedException("X-User missing", "demo");
        }
        return new Decider(user, groups == null ? Set.of()
                : Arrays.stream(groups.split(",")).map(String::trim).filter(g -> !g.isEmpty())
                        .collect(Collectors.toSet()));
    }

    private static <T> T call(Supplier<T> action) {
        try {
            return action.get();
        } catch (IllegalArgumentException e) {
            throw new NotFoundException(e.getMessage());
        } catch (DtpException e) {
            throw new ClientErrorException(e.getMessage(), Response.Status.CONFLICT);
        }
    }
}
