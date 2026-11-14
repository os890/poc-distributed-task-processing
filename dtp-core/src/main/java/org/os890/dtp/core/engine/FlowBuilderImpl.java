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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.os890.dtp.api.FlowBuilder;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.api.ResultStatus;
import org.os890.dtp.api.Signal;
import org.os890.dtp.core.domain.FlowModel;
import org.os890.dtp.core.domain.RouteKind;
import org.os890.dtp.core.domain.RouteModel;

/** Collects the steps and routes of a {@link FlowDefinition} and validates them. */
public class FlowBuilderImpl implements FlowBuilder {

    private final String name;
    private final Class<?> definitionClass;
    private final Function<Class<? extends FlowDefinition>, String> flowNames;
    private final Set<Class<? extends ProcessStep>> steps = new LinkedHashSet<>();
    private final List<RouteModel> routes = new ArrayList<>();
    private Class<? extends ProcessStep> entry;

    /**
     * Creates a builder.
     *
     * @param name            the flow name
     * @param definitionClass the class of the definition
     * @param flowNames       resolves the names of target flows
     */
    public FlowBuilderImpl(String name, Class<?> definitionClass,
                           Function<Class<? extends FlowDefinition>, String> flowNames) {
        this.name = name;
        this.definitionClass = definitionClass;
        this.flowNames = flowNames;
    }

    /**
     * Compiles a definition.
     *
     * @param definition      the definition
     * @param definitionClass its class (the bean class, not a proxy)
     * @param flowNames       resolves the names of target flows
     * @return the model
     */
    public static FlowModel compile(FlowDefinition definition, Class<? extends FlowDefinition> definitionClass,
                                    Function<Class<? extends FlowDefinition>, String> flowNames) {
        FlowBuilderImpl builder = new FlowBuilderImpl(flowNames.apply(definitionClass), definitionClass, flowNames);
        definition.define(builder);
        return builder.build();
    }

    @Override
    public StepRoutes entry(Class<? extends ProcessStep> step) {
        Objects.requireNonNull(step, "step");
        if (entry != null && entry != step) {
            throw new IllegalStateException("flow " + name + " has two entry steps: " + entry.getName() + " and "
                    + step.getName());
        }
        entry = step;
        steps.add(step);
        return new Routes(step);
    }

    @Override
    public StepRoutes step(Class<? extends ProcessStep> step) {
        steps.add(Objects.requireNonNull(step, "step"));
        return new Routes(step);
    }

    /**
     * The validated model.
     *
     * @return the model
     */
    public FlowModel build() {
        if (entry == null) {
            throw new IllegalStateException("flow " + name + " has no entry step");
        }
        for (RouteModel route : routes) {
            steps.addAll(route.branches());
            if (route.targetStep() != null) {
                steps.add(route.targetStep());
            }
        }
        return new FlowModel(name, definitionClass, entry, new ArrayList<>(steps), routes);
    }

    private void add(RouteModel route) {
        for (RouteModel existing : routes) {
            boolean sameSource = existing.from() == route.from() && existing.signalType().equals(route.signalType())
                    && existing.signal().equals(route.signal());
            if (sameSource && existing.kind().isItemRoute() == route.kind().isItemRoute()) {
                throw new IllegalStateException("flow " + name + ": " + route.from().getSimpleName() + " routes "
                        + route.signal() + " twice");
            }
        }
        routes.add(route);
    }

    private class Routes implements StepRoutes {

        private final Class<? extends ProcessStep> from;

        Routes(Class<? extends ProcessStep> from) {
            this.from = from;
        }

        @Override
        public Target on(Signal signal) {
            Objects.requireNonNull(signal, "signal");
            return new SignalTarget(this, signal);
        }

        @Override
        public StepRoutes onResult(ResultStatus result, Class<? extends ProcessStep> next) {
            add(new RouteModel(from, RouteModel.typeOf(result), result.name(), RouteKind.RUN,
                    Objects.requireNonNull(next, "next"), null));
            return this;
        }
    }

    private class SignalTarget implements Target {

        private final Routes routes;
        private final Signal signal;

        SignalTarget(Routes routes, Signal signal) {
            this.routes = routes;
            this.signal = signal;
        }

        @Override
        public StepRoutes to(Class<? extends ProcessStep> step) {
            return route(RouteKind.TO, Objects.requireNonNull(step, "step"), null);
        }

        @Override
        public StepRoutes back(Class<? extends ProcessStep> step) {
            return route(RouteKind.BACK, Objects.requireNonNull(step, "step"), null);
        }

        @Override
        public StepRoutes toFlow(Class<? extends FlowDefinition> flow) {
            return route(RouteKind.FLOW, null, flowNames.apply(Objects.requireNonNull(flow, "flow")));
        }

        @Override
        public StepRoutes complete() {
            return route(RouteKind.COMPLETE, null, null);
        }

        @Override
        public StepRoutes discard() {
            return route(RouteKind.DISCARD, null, null);
        }

        @Override
        public Fork fork(Class<? extends ProcessStep> branch) {
            return new ForkTarget(this).and(branch);
        }

        private StepRoutes route(RouteKind kind, Class<? extends ProcessStep> step, String flow) {
            add(new RouteModel(routes.from, RouteModel.typeOf(signal), signal.name(), kind, step, flow));
            return routes;
        }
    }

    private class ForkTarget implements Fork {

        private final SignalTarget target;
        private final List<Class<? extends ProcessStep>> branches = new ArrayList<>();

        ForkTarget(SignalTarget target) {
            this.target = target;
        }

        @Override
        public Fork and(Class<? extends ProcessStep> branch) {
            Objects.requireNonNull(branch, "branch");
            if (branches.contains(branch) || branch == target.routes.from) {
                throw new IllegalStateException("flow " + name + ": " + branch.getSimpleName() + " is twice in the"
                        + " fork of " + target.signal.name());
            }
            branches.add(branch);
            return this;
        }

        @Override
        public StepRoutes joinAt(Class<? extends ProcessStep> step) {
            Objects.requireNonNull(step, "step");
            if (branches.size() < 2) {
                throw new IllegalStateException("flow " + name + ": the fork of " + target.signal.name()
                        + " needs at least two branches");
            }
            if (branches.contains(step)) {
                throw new IllegalStateException("flow " + name + ": the join step " + step.getSimpleName()
                        + " can't be a branch of its fork");
            }
            add(new RouteModel(target.routes.from, RouteModel.typeOf(target.signal), target.signal.name(),
                    RouteKind.FORK, step, null, branches));
            return target.routes;
        }
    }
}
