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
package org.os890.dtp.api;

/** Typed builder of a flow: steps, the routes of their signals and the successors of their results. */
public interface FlowBuilder {

    /**
     * The entry step. Its job carries the trigger (cron, external, …), configured in the DB. Items started via
     * {@link FlowEvent#start} enter its inbox.
     *
     * @param step the process step
     * @return the routes of the step
     */
    StepRoutes entry(Class<? extends ProcessStep> step);

    /**
     * Another step of the flow. Its job is generated if it doesn't exist (with an inbox trigger for item processors).
     *
     * @param step the process step
     * @return the routes of the step
     */
    StepRoutes step(Class<? extends ProcessStep> step);

    /** The routes of one step. */
    interface StepRoutes {

        /**
         * Where an item goes when the step reports this signal.
         *
         * @param signal the signal
         * @return the target
         */
        Target on(Signal signal);

        /**
         * Starts a run of the next step when a run of this step finishes with the given typed result.
         *
         * @param result the result status
         * @param next   the next step
         * @return these routes
         */
        StepRoutes onResult(ResultStatus result, Class<? extends ProcessStep> next);
    }

    /** Target of a signal. */
    interface Target {

        /**
         * Into the inbox of a step.
         *
         * @param step the step
         * @return the routes of the current step
         */
        StepRoutes to(Class<? extends ProcessStep> step);

        /**
         * Back to an earlier step (journey entry {@code ROUTED_BACK}).
         *
         * @param step the step
         * @return the routes of the current step
         */
        StepRoutes back(Class<? extends ProcessStep> step);

        /**
         * Into the entry step of the active version of another flow (sub-flow or hand-over to a new flow).
         *
         * @param flow the other flow
         * @return the routes of the current step
         */
        StepRoutes toFlow(Class<? extends FlowDefinition> flow);

        /** Completes the item. */
        StepRoutes complete();

        /** Discards the item, the signal name is the reason. */
        StepRoutes discard();
    }
}
