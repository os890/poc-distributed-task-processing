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
package org.os890.dtp.test.cdi;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.BeanManager;

import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.cdi.CdiEventPublisher;
import org.os890.dtp.cdi.CdiStepRegistry;
import org.os890.dtp.core.spi.StepRegistry;
import org.os890.dtp.test.DtpTestCluster;

/**
 * Connects a test cluster to a CDI container, e.g. Weld SE in a test: the process steps and flows are the beans of
 * the container (with their injected dependencies), the domain events of all nodes are fired as CDI events (sync, then
 * async), and the {@code FlowEvent}s that steps fire go to the node whose run fired them ({@link FlowEventBridge}).
 *
 * <pre>{@code
 * try (WeldContainer container = new Weld().disableDiscovery()
 *         .addBeanClasses(FlowEventBridge.class, OrderFlow.class, OrderIntake.class, OrderCheck.class, Carrier.class)
 *         .initialize()) {
 *     cluster.start(2, DtpTestCdi.connect(cluster, container.getBeanManager()));
 *     ...
 * }
 * }</pre>
 *
 * <p>The container must not contain {@code DtpBootstrap}: it would start a node of its own. Needs {@code dtp-cdi} and
 * a CDI implementation on the test classpath.
 */
public class DtpTestCdi {

    private DtpTestCdi() {
    }

    /**
     * Connects the cluster to the container.
     *
     * @param cluster     the test cluster
     * @param beanManager the bean manager of the container, with the bean {@link FlowEventBridge}
     * @return the process steps and flow definitions of the container, for {@code cluster.start(…)}
     * @throws IllegalStateException if the container has no {@link FlowEventBridge}
     */
    public static StepRegistry connect(DtpTestCluster cluster, BeanManager beanManager) {
        Instance<Object> beans = beanManager.createInstance();
        Instance<FlowEventBridge> bridge = beans.select(FlowEventBridge.class);
        if (!bridge.isResolvable()) {
            throw new IllegalStateException("the container has no bean " + FlowEventBridge.class.getName()
                    + ": add its class to the container");
        }
        bridge.get().connect(cluster);
        cluster.forwardEvents(new CdiEventPublisher(beanManager));
        return new CdiStepRegistry(beans.select(ProcessStep.class), beans.select(FlowDefinition.class));
    }
}
