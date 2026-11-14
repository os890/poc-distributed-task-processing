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
package org.os890.dtp.cdi;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.inject.Instance;

import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.core.spi.StepRegistry;

/**
 * The process steps of the deployment: all beans of type {@link ProcessStep}, referenced by their bean class. The
 * flows are all beans of type {@link FlowDefinition}.
 */
public class CdiStepRegistry implements StepRegistry {

    private final Map<Class<? extends ProcessStep>, Instance.Handle<ProcessStep>> steps = new LinkedHashMap<>();
    private final Map<Class<? extends FlowDefinition>, FlowDefinition> flows = new LinkedHashMap<>();

    public CdiStepRegistry(Instance<ProcessStep> beans) {
        beans.handles().forEach(handle -> {
            Class<?> beanClass = handle.getBean().getBeanClass();
            if (!ProcessStep.class.isAssignableFrom(beanClass)) {
                throw new IllegalStateException("producer of process steps aren't supported: " + beanClass.getName());
            }
            steps.put(beanClass.asSubclass(ProcessStep.class), handle);
        });
    }

    /**
     * Creates the registry with flows.
     *
     * @param beans           the process steps
     * @param flowDefinitions the flow definitions
     */
    public CdiStepRegistry(Instance<ProcessStep> beans, Instance<FlowDefinition> flowDefinitions) {
        this(beans);
        flowDefinitions.handles().forEach(handle -> {
            Class<?> beanClass = handle.getBean().getBeanClass();
            if (!FlowDefinition.class.isAssignableFrom(beanClass)) {
                throw new IllegalStateException("producer of flow definitions aren't supported: "
                        + beanClass.getName());
            }
            flows.put(beanClass.asSubclass(FlowDefinition.class), handle.get());
        });
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
        return steps.get(stepClass).get();
    }
}
