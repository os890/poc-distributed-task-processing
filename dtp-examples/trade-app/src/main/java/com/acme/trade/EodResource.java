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

import java.time.LocalDate;

import jakarta.inject.Inject;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.os890.dtp.api.BusinessCalendars;
import org.os890.dtp.api.JobEngine;
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.core.engine.FlowInstances;
import org.os890.dtp.core.engine.FlowInstances.InstanceView;

/**
 * The end of day per business date: a flow instance of the job {@code end-of-day}, started at 18:00 on each business
 * day or via REST.
 */
@Path("/eod")
@Produces(MediaType.APPLICATION_JSON)
public class EodResource {

    static final String FLOW = "end-of-day";

    @Inject
    JobEngine jobs;

    @Inject
    FlowInstances instances;

    @Inject
    BusinessCalendars calendars;

    @ConfigProperty(name = "trade.eod.calendar")
    String calendar;

    /**
     * Starts the end of day of a business date (idempotent), e.g. after a failure of the scheduled start.
     *
     * @param date the business date
     * @param user who starts it (demo: a header instead of a JWT)
     * @return the root run
     * @throws ClientErrorException 409 if the date isn't a business day
     */
    @POST
    @Path("/{date}")
    public RunHandle start(@PathParam("date") LocalDate date, @HeaderParam("X-User") @DefaultValue("ops") String user) {
        if (!calendars.get(calendar).isBusinessDay(date)) {
            throw new ClientErrorException(date + " is no business day of " + calendar, Response.Status.CONFLICT);
        }
        return jobs.startFlowInstance(EndOfDay.class, new EodParams(user), date.toString());
    }

    /**
     * The end of day of a business date: its runs and the sign-off join.
     *
     * @param date the business date
     * @return the flow instance
     */
    @GET
    @Path("/{date}")
    public InstanceView get(@PathParam("date") LocalDate date) {
        return instances.find(FLOW, date.toString()).orElseThrow(NotFoundException::new);
    }
}
