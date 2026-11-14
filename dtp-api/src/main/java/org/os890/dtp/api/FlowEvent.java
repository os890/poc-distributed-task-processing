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

import java.util.Objects;

/**
 * Events provided by the framework to drive flows: application code only fires them (synchronously, within its
 * transaction), the framework observes them and routes the items according to the flow definition in the DB.
 * Application code never observes them itself.
 */
public sealed interface FlowEvent permits FlowEvent.StartFlow, FlowEvent.ItemSignal {

    /**
     * Starts a flow for an entity: tracks it and puts it into the inbox of the entry step of the flow's active
     * version.
     *
     * @param flow        the flow
     * @param ref         the entity
     * @param businessKey the business key
     * @param <E>         the entity type
     * @param <ID>        the id type
     * @return the event
     */
    static <E, ID> FlowEvent start(Class<? extends FlowDefinition> flow, ItemRef<E, ID> ref, String businessKey) {
        return new StartFlow(flow, ref, businessKey, null);
    }

    /**
     * Same as {@link #start(Class, ItemRef, String)} in a lane.
     *
     * @param flow        the flow
     * @param ref         the entity
     * @param businessKey the business key
     * @param lane        the lane
     * @param <E>         the entity type
     * @param <ID>        the id type
     * @return the event
     */
    static <E, ID> FlowEvent start(Class<? extends FlowDefinition> flow, ItemRef<E, ID> ref, String businessKey,
                                   ItemLane lane) {
        return new StartFlow(flow, ref, businessKey, lane);
    }

    /**
     * Reports a signal for an item, the alternative to {@link TrackedItem#signal(Signal)}.
     *
     * @param item   the item
     * @param signal the signal
     * @return the event
     */
    static FlowEvent signal(TrackedItem<?, ?> item, Signal signal) {
        return new ItemSignal(item, signal, null);
    }

    /**
     * Reports a signal for an item, with a reason the next step can read.
     *
     * @param item   the item
     * @param signal the signal
     * @param reason the reason
     * @return the event
     */
    static FlowEvent signal(TrackedItem<?, ?> item, Signal signal, String reason) {
        return new ItemSignal(item, signal, reason);
    }

    /**
     * Start of a flow.
     *
     * @param flow        the flow
     * @param ref         the entity
     * @param businessKey the business key
     * @param lane        the lane or {@code null}
     */
    record StartFlow(Class<? extends FlowDefinition> flow, ItemRef<?, ?> ref, String businessKey, ItemLane lane)
            implements FlowEvent {

        public StartFlow {
            Objects.requireNonNull(flow, "flow");
            Objects.requireNonNull(ref, "ref");
        }
    }

    /**
     * Signal of an item.
     *
     * @param item   the item
     * @param signal the signal
     * @param reason the reason or {@code null}
     */
    record ItemSignal(TrackedItem<?, ?> item, Signal signal, String reason) implements FlowEvent {

        public ItemSignal {
            Objects.requireNonNull(item, "item");
            Objects.requireNonNull(signal, "signal");
        }
    }
}
