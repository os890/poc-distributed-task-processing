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

import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/** Demo controls of the simulated settlement gateway of this node. */
@Path("/settlement")
@Produces(MediaType.APPLICATION_JSON)
public class SettlementResource {

    @Inject
    SettlementGateway gateway;

    /**
     * The files this node sent.
     *
     * @return the files
     */
    @GET
    @Path("/files")
    public List<String> files() {
        return gateway.files();
    }

    /**
     * A custodian of this node's gateway rejects files from now on.
     *
     * @param custodian the custodian
     */
    @POST
    @Path("/custodians/{custodian}/outage")
    public void outage(@PathParam("custodian") String custodian) {
        gateway.setAvailable(custodian, false);
    }

    /**
     * A custodian accepts files again.
     *
     * @param custodian the custodian
     */
    @DELETE
    @Path("/custodians/{custodian}/outage")
    public void recovered(@PathParam("custodian") String custodian) {
        gateway.setAvailable(custodian, true);
    }
}
