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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;
import org.os890.dtp.cdi.DtpBootstrap;
import org.os890.dtp.core.DtpEngine;

/** Ready once this node runs the dtp engine (MP Health). */
@Readiness
@ApplicationScoped
public class DtpHealthCheck implements HealthCheck {

    @Inject
    DtpBootstrap bootstrap;

    @Override
    public HealthCheckResponse call() {
        DtpEngine engine = bootstrap.engine();
        return HealthCheckResponse.named("dtp")
                                  .status(engine.isRunning())
                                  .withData("node", engine.config().nodeName())
                                  .withData("namespace", engine.config().namespace())
                                  .withData("leader", engine.isLeader())
                                  .withData("activeRuns", engine.activeRuns())
                                  .build();
    }
}
