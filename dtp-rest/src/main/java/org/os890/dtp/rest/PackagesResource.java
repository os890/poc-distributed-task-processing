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

import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.os890.dtp.core.engine.Acknowledgements;
import org.os890.dtp.core.engine.Acknowledgements.Outcome;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.DtpQueries.PackageView;

/** Work packages and the acknowledgements of sent packages. */
@Path("/dtp/packages")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "dtp work packages", description = "Work packages and acknowledgements")
public class PackagesResource {

    @Inject
    DtpAccess access;

    @Inject
    DtpQueries queries;

    @Inject
    Acknowledgements acknowledgements;

    /**
     * The answer of a receiver.
     *
     * @param job       the job that sent the package
     * @param reference the reference it was sent with
     * @param accepted  {@code true} for an acknowledgement, {@code false} for a rejection
     * @param reason    who acknowledged it, or the reason of a rejection
     */
    public record Answer(String job, String reference, boolean accepted, String reason) {
    }

    /**
     * The most recent work packages.
     *
     * @param limit    maximum number
     * @param security the security context
     * @param headers  the headers
     * @return the packages, newest first
     */
    @GET
    @Operation(summary = "The most recent work packages")
    public List<PackageView> packages(@QueryParam("limit") @DefaultValue("20") int limit,
                                      @Context SecurityContext security, @Context HttpHeaders headers) {
        access.caller(security, headers).require(DtpAccess.VIEWER);
        return queries.packages(limit);
    }

    /**
     * Acknowledges or rejects a sent package.
     *
     * @param answer   the answer
     * @param security the security context
     * @param headers  the headers
     * @return the outcome; 404 for an unknown reference, 409 if the package doesn't wait for an answer
     */
    @POST
    @Path("/acknowledgements")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Acknowledges or rejects a sent package by job and reference (operators)")
    public Map<String, Outcome> answer(Answer answer, @Context SecurityContext security,
                                       @Context HttpHeaders headers) {
        DtpAccess.Caller caller = access.caller(security, headers).require(DtpAccess.OPERATOR);
        if (answer == null || answer.job() == null || answer.reference() == null) {
            throw new BadRequestException("job and reference are required");
        }
        String text = answer.reason() != null ? answer.reason() : caller.user().orElse(null);
        Outcome outcome = DtpErrors.call(() -> acknowledgements.answer(answer.job(), answer.reference(),
                answer.accepted(), text));
        return switch (outcome) {
            case UNKNOWN -> throw new NotFoundException("unknown reference " + answer.reference());
            case NOT_WAITING -> throw new ClientErrorException(answer.reference() + " doesn't wait for an answer",
                    Response.Status.CONFLICT);
            default -> Map.of("outcome", outcome);
        };
    }
}
