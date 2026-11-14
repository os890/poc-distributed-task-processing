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

import java.util.UUID;

import org.os890.dtp.api.JobEngine;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.TriggerType;

/** External triggers from application code. Joins the caller's transaction, the run starts after the commit. */
public class JobEngineImpl implements JobEngine {

    private final EngineContext ctx;
    private final RunPlanner planner;
    private final FlowInstances instances;

    public JobEngineImpl(EngineContext ctx, RunPlanner planner) {
        this.ctx = ctx;
        this.planner = planner;
        this.instances = new FlowInstances(ctx);
    }

    @Override
    public <P> RunHandle trigger(Class<? extends JobHandler<P, ?>> job, P parameters) {
        return trigger(job, parameters, null);
    }

    @Override
    public <P> RunHandle trigger(Class<? extends JobHandler<P, ?>> job, P parameters, String idempotencyKey) {
        JobDefinition definition = external(job);
        String key = idempotencyKey != null ? idempotencyKey : UUID.randomUUID().toString();
        RunPlanner.Planned planned = ctx.tx().required(() -> ctx.tx().withConnection(c -> planner.plan(c, definition,
                new RunPlanner.Trigger(TriggerType.EXTERNAL, "external:" + definition.id() + ":" + key, key,
                        ctx.json().write(parameters), null), ctx.clock().now(c))));
        return new RunHandle(planned.runId() == null ? null : planned.runId().toString(), planned.created());
    }

    @Override
    public <P> RunHandle startFlowInstance(Class<? extends JobHandler<P, ?>> root, P parameters, String businessKey) {
        JobDefinition definition = external(root);
        if (businessKey == null || businessKey.isBlank()) {
            throw new IllegalArgumentException("a flow instance needs a business key");
        }
        RunPlanner.Planned planned = ctx.tx().required(() -> ctx.tx().withConnection(c -> instances.start(c,
                definition, businessKey, ctx.json().write(parameters), ctx.clock().now(c))));
        return new RunHandle(planned.runId() == null ? null : planned.runId().toString(), planned.created());
    }

    private JobDefinition external(Class<? extends JobHandler<?, ?>> job) {
        JobDefinition definition = ctx.catalog().byStep(job);
        if (!definition.external()) {
            throw new IllegalStateException("job " + definition.name() + " doesn't allow external triggers"
                    + " (trigger_external)");
        }
        return definition;
    }
}
