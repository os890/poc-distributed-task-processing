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
package org.os890.dtp.test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.core.spi.StepRegistry;

/** The process steps and flow definitions of a simulated node, given as instances. */
class InstanceRegistry implements StepRegistry {

    private final Map<Class<? extends ProcessStep>, ProcessStep> steps = new LinkedHashMap<>();
    private final Map<Class<? extends FlowDefinition>, FlowDefinition> flows = new LinkedHashMap<>();

    InstanceRegistry(Object... instances) {
        for (Object instance : instances) {
            if (instance instanceof ProcessStep step) {
                steps.put(step.getClass(), step);
            } else if (instance instanceof FlowDefinition flow) {
                flows.put(flow.getClass(), flow);
            } else {
                throw new IllegalArgumentException("neither a process step nor a flow definition: " + instance);
            }
        }
    }

    @Override
    public Map<Class<? extends FlowDefinition>, FlowDefinition> flowDefinitions() {
        return flows;
    }

    @Override
    public Set<Class<? extends ProcessStep>> stepClasses() {
        return steps.keySet();
    }

    @Override
    public ProcessStep instance(Class<? extends ProcessStep> stepClass) {
        return steps.get(stepClass);
    }
}
