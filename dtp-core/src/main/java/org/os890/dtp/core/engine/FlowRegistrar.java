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
package org.os890.dtp.core.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.domain.FlowModel;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.RouteModel;
import org.os890.dtp.core.jdbc.FlowStore;
import org.os890.dtp.core.jdbc.FlowStore.ActivationRow;
import org.os890.dtp.core.jdbc.FlowStore.RouteRow;
import org.os890.dtp.core.jdbc.FlowStore.VersionRow;

/**
 * Registers the flows defined in Java at startup: generates the missing jobs of their steps, stores each flow as an
 * immutable version (identified by its content hash) and activates a new version unless an operator activated a
 * version manually. Registering a known version never activates it, so nodes of different releases (red/green) and
 * restarts don't switch the active version back and forth. A node on standby (another color is active) activates
 * nothing: the switch to its color does.
 */
public class FlowRegistrar {

    private static final System.Logger LOG = System.getLogger(FlowRegistrar.class.getName());

    private final EngineContext ctx;

    public FlowRegistrar(EngineContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Compiles the definitions to models.
     *
     * @param definitions the definitions by (bean) class
     * @return the models
     */
    public static List<FlowModel> compile(Map<Class<? extends FlowDefinition>, FlowDefinition> definitions) {
        Map<Class<? extends FlowDefinition>, String> names = new LinkedHashMap<>();
        definitions.forEach((type, definition) -> names.put(type,
                definition.name() != null ? definition.name() : FlowModel.defaultName(type)));
        List<FlowModel> models = new ArrayList<>();
        definitions.forEach((type, definition) -> models.add(FlowBuilderImpl.compile(definition, type,
                target -> names.getOrDefault(target, FlowModel.defaultName(target)))));
        return models;
    }

    /**
     * Registers the flows. Each flow in its own transaction, retried once if another node registered it meanwhile.
     *
     * @param models the flows
     */
    public void register(List<FlowModel> models) {
        for (FlowModel model : models) {
            try {
                ctx.tx().requiresNew(() -> ctx.tx().withConnection(c -> register(c, model)));
            } catch (DtpException e) {
                LOG.log(System.Logger.Level.DEBUG, "registering flow " + model.name() + " again", e);
                ctx.tx().requiresNew(() -> ctx.tx().withConnection(c -> register(c, model)));
            }
        }
        ctx.catalog().invalidate();
    }

    private Object register(Connection c, FlowModel model) throws SQLException {
        Instant now = ctx.clock().now(c);
        FlowStore flows = ctx.flows();
        Map<Class<? extends ProcessStep>, UUID> jobs = new LinkedHashMap<>();
        for (Class<? extends ProcessStep> step : model.steps()) {
            jobs.put(step, jobOf(c, model, step, now));
        }
        UUID flowId = flows.ensureFlow(c, ctx.namespace(), model.name(), model.definitionClass().getName(), now);
        String hash = model.hash();
        Optional<VersionRow> existing = flows.version(c, flowId, hash);
        UUID versionId;
        boolean created = existing.isEmpty();
        if (created) {
            versionId = UUID.randomUUID();
            flows.insertVersion(c, new VersionRow(versionId, flowId, hash, "CODE", jobs.get(model.entry()),
                    ctx.config().nodeName(), now), List.copyOf(jobs.values()));
            for (RouteModel route : model.routes()) {
                UUID targetFlow = route.targetFlow() == null ? null
                        : flows.ensureFlow(c, ctx.namespace(), route.targetFlow(), null, now);
                flows.insertRoute(c, versionId, new RouteRow(null, jobs.get(route.from()), route.signalType(),
                        route.signal(), route.kind(), route.targetStep() == null ? null : jobs.get(route.targetStep()),
                        targetFlow, false), route.branches().stream().map(jobs::get).toList());
            }
        } else {
            versionId = existing.get().id();
        }
        List<ActivationRow> activations = flows.activations(c, flowId);
        boolean manual = !activations.isEmpty() && !activations.getFirst().generated();
        if (ctx.color().standby()) {
            // red/green: the release on standby doesn't change the flows of the active one, the switch to its color
            // activates the versions its nodes run (slice 21)
            if (!versionId.equals(flows.activeVersion(c, flowId, now).orElse(null))) {
                LOG.log(System.Logger.Level.INFO, "flow {0}: version {1} of color {2} is a candidate until {2} is"
                        + " the active color", model.name(), hash, ctx.color().color());
            }
        } else if (activations.isEmpty() || (created && !manual)) {
            flows.activate(c, flowId, new ActivationRow(versionId, now, true, ctx.config().nodeName(),
                    created ? "new version deployed" : "first activation", now, null));
            LOG.log(System.Logger.Level.INFO, "flow {0}: version {1} is active", model.name(), hash);
        } else if (!versionId.equals(flows.activeVersion(c, flowId, now).orElse(null))) {
            LOG.log(System.Logger.Level.WARNING, "flow {0}: the deployed version {1} is a candidate, the active"
                    + " version was {2}", model.name(), hash, manual ? "activated manually" : "deployed later");
        }
        return null;
    }

    private UUID jobOf(Connection c, FlowModel model, Class<? extends ProcessStep> step, Instant now)
            throws SQLException {
        Optional<JobDefinition> job = ctx.jobs().loadCurrent(c, ctx.namespace()).stream()
                .filter(j -> j.handlerClass().equals(step.getName())).findFirst();
        if (job.isPresent()) {
            return job.get().id();
        }
        boolean processor = ItemProcessor.class.isAssignableFrom(step);
        if (step == model.entry() && !processor) {
            LOG.log(System.Logger.Level.WARNING, "flow {0}: the entry step {1} has no job definition, generated one"
                    + " without trigger: configure its trigger in the DB", model.name(), step.getName());
        }
        String name = FlowModel.defaultName(step);
        boolean taken = ctx.jobs().loadCurrent(c, ctx.namespace()).stream().anyMatch(j -> j.name().equals(name));
        if (taken) {
            throw new IllegalStateException("flow " + model.name() + ": a job named " + name
                    + " exists for another handler class, define the job of " + step.getName() + " in the DB");
        }
        return ctx.jobs().insertGenerated(c, ctx.namespace(), name, step.getName(), processor ? 1 : null,
                "dtp-flow:" + model.name(), now);
    }
}
