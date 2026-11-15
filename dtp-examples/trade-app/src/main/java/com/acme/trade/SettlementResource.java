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

import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.os890.dtp.core.engine.Acknowledgements;
import org.os890.dtp.core.engine.Acknowledgements.Outcome;

/** Demo controls of the simulated settlement gateway of this node, and the callback of the custodians. */
@Path("/settlement")
@Produces(MediaType.APPLICATION_JSON)
public class SettlementResource {

    @Inject
    SettlementGateway gateway;

    @Inject
    Acknowledgements acknowledgements;

    /**
     * The answer of a custodian to a file.
     *
     * @param file      the file name
     * @param accepted  {@code true} for an acknowledgement
     * @param custodian the custodian
     * @param reason    the reason of a rejection
     */
    public record Answer(String file, boolean accepted, String custodian, String reason) {
    }

    /**
     * The files sent by this node.
     *
     * @return the files
     */
    @GET
    @Path("/files")
    public List<String> files() {
        return gateway.files();
    }

    /**
     * The answers of the simulated custodians to the files of this node.
     *
     * @return the answers with their outcome
     */
    @GET
    @Path("/answers")
    public List<String> answers() {
        return gateway.answers();
    }

    /**
     * The callback of a custodian: acknowledges or rejects a file, on any node.
     *
     * @param answer the answer
     * @return the outcome; 404 for an unknown file, 409 if the file doesn't wait for an answer
     */
    @POST
    @Path("/acknowledgements")
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, Outcome> acknowledge(Answer answer) {
        if (answer == null || answer.file() == null) {
            throw new BadRequestException("file is required");
        }
        Outcome outcome = answer.accepted()
                ? acknowledgements.acknowledge(SettlementSender.class, answer.file(), answer.custodian())
                : acknowledgements.reject(SettlementSender.class, answer.file(), answer.reason() == null
                        ? answer.custodian() + " rejected " + answer.file() : answer.reason());
        return switch (outcome) {
            case UNKNOWN -> throw new NotFoundException("unknown file " + answer.file());
            case NOT_WAITING -> throw new WebApplicationException(answer.file() + " doesn't wait for an answer",
                    Response.Status.CONFLICT);
            default -> Map.of("outcome", outcome);
        };
    }

    @POST
    @Path("/custodians/{custodian}/outage")
    public void outage(@PathParam("custodian") String custodian) {
        gateway.setAvailable(custodian, false);
    }

    @DELETE
    @Path("/custodians/{custodian}/outage")
    public void recovered(@PathParam("custodian") String custodian) {
        gateway.setAvailable(custodian, true);
    }

    /**
     * Sets how a simulated custodian of this node answers.
     *
     * @param custodian the custodian
     * @param mode      {@code ACCEPTING}, {@code REJECTING} or {@code SILENT}
     */
    @PUT
    @Path("/custodians/{custodian}/mode")
    @Consumes(MediaType.TEXT_PLAIN)
    public void mode(@PathParam("custodian") String custodian, String mode) {
        try {
            gateway.setMode(custodian, SettlementGateway.Mode.valueOf(mode.trim()));
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new BadRequestException("mode is ACCEPTING, REJECTING or SILENT");
        }
    }
}
