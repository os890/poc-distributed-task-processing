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

import java.util.Optional;

import org.os890.dtp.api.FlowInstance;
import org.os890.dtp.api.Inbox;
import org.os890.dtp.api.ItemType;
import org.os890.dtp.api.Items;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.RunInfo;

/**
 * The context of a {@code JobHandler} run.
 *
 * @param <P> the parameter type
 */
public class JobContextImpl<P> implements JobContext<P> {

    private final RunContext run;
    private final ItemRouting routing;
    private final P parameters;

    public JobContextImpl(RunContext run, ItemRouting routing, P parameters) {
        this.run = run;
        this.routing = routing;
        this.parameters = parameters;
    }

    @Override
    public P parameters() {
        return parameters;
    }

    @Override
    public RunInfo runInfo() {
        return run.runInfo();
    }

    @Override
    public boolean shouldContinue() {
        return run.shouldContinue();
    }

    @Override
    public Optional<FlowInstance> flowInstance() {
        return new FlowInstances(run.engine()).ofRun(run.claimed().runId());
    }

    @Override
    public Items items() {
        return new ItemsImpl(run.engine(), routing, run);
    }

    @Override
    public <E, ID> Inbox<E, ID> inbox(ItemType<E, ID> type) {
        return new InboxImpl<>(run, routing, type, false);
    }
}
