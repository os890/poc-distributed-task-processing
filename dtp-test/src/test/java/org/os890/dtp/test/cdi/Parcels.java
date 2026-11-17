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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.os890.dtp.api.FlowBuilder;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.FlowEvent;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemType;
import org.os890.dtp.api.Signal;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.api.event.NodeJoined;

/**
 * A parcel flow of an application whose process steps are CDI beans: they get their dependencies injected and route
 * parcels by firing {@code FlowEvent}s. A heavy parcel takes a detour through the heavy goods handling.
 */
public class Parcels {

    /** The item type. */
    public static final ItemType<Parcels, Long> ITEM = ItemType.of("parcel", Parcels.class, Long.class);

    private Parcels() {
    }

    /** Signals of the steps. */
    public enum ParcelSignal implements Signal {
        LIGHT, HEAVY, HANDLED, SHIPPED
    }

    /** A service the check depends on. */
    @ApplicationScoped
    public static class Scale {

        /**
         * The weight of a parcel.
         *
         * @param id the parcel
         * @return the weight in kg
         */
        public double weigh(long id) {
            return id * 1.5;
        }
    }

    /** Weighs a parcel. */
    @ApplicationScoped
    public static class ParcelCheck implements ItemProcessor<Parcels, Long> {

        @Inject
        Scale scale;

        @Inject
        Event<FlowEvent> flowEvents;

        @Override
        public void process(TrackedItem<Parcels, Long> item) {
            double weight = scale.weigh(item.ref().id());
            flowEvents.fire(FlowEvent.signal(item, weight > 10 ? ParcelSignal.HEAVY : ParcelSignal.LIGHT,
                    weight + " kg"));
        }
    }

    /** Heavy goods. */
    @ApplicationScoped
    public static class HeavyHandling implements ItemProcessor<Parcels, Long> {

        @Inject
        Event<FlowEvent> flowEvents;

        @Override
        public void process(TrackedItem<Parcels, Long> item) {
            flowEvents.fire(FlowEvent.signal(item, ParcelSignal.HANDLED));
        }
    }

    /** Ships a parcel. */
    @ApplicationScoped
    public static class ParcelShipping implements ItemProcessor<Parcels, Long> {

        @Inject
        Event<FlowEvent> flowEvents;

        @Override
        public void process(TrackedItem<Parcels, Long> item) {
            flowEvents.fire(FlowEvent.signal(item, ParcelSignal.SHIPPED));
        }
    }

    /** The flow. */
    @ApplicationScoped
    public static class ParcelFlow implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(ParcelCheck.class)
                .on(ParcelSignal.LIGHT).to(ParcelShipping.class)
                .on(ParcelSignal.HEAVY).to(HeavyHandling.class);
            flow.step(HeavyHandling.class)
                .on(ParcelSignal.HANDLED).to(ParcelShipping.class);
            flow.step(ParcelShipping.class)
                .on(ParcelSignal.SHIPPED).complete();
        }
    }

    /** An observer of the domain events, as operations would have one. */
    @ApplicationScoped
    public static class NodeLog {

        final List<String> joined = new CopyOnWriteArrayList<>();

        void onJoined(@Observes NodeJoined event) {
            joined.add(event.node());
        }

        /** The nodes that joined. */
        public List<String> joined() {
            return joined;
        }
    }
}
