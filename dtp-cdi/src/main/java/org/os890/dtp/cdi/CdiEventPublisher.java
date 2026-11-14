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

import jakarta.enterprise.event.Event;
import jakarta.enterprise.inject.spi.BeanManager;

import org.os890.dtp.core.spi.DomainEventPublisher;

/**
 * Fires the domain events of dtp as CDI events, called by the engine after the commit: first synchronously
 * ({@code @Observes}), then asynchronously ({@code @ObservesAsync}). Failing observers are logged, they never affect
 * the engine. As always in CDI, a failing synchronous observer stops the remaining synchronous observers of the
 * event; the asynchronous observers are notified anyway.
 */
class CdiEventPublisher implements DomainEventPublisher {

    private static final System.Logger LOG = System.getLogger(CdiEventPublisher.class.getName());

    private final BeanManager beanManager;

    CdiEventPublisher(BeanManager beanManager) {
        this.beanManager = beanManager;
    }

    @Override
    public void publish(Object event) {
        // observers are resolved by the runtime type of the event
        Event<Object> cdiEvent = beanManager.getEvent();
        try {
            cdiEvent.fire(event);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "an observer of " + event.getClass().getSimpleName() + " failed", e);
        }
        cdiEvent.fireAsync(event).exceptionally(e -> {
            LOG.log(System.Logger.Level.WARNING, "an asynchronous observer of " + event.getClass().getSimpleName()
                    + " failed", e);
            return null;
        });
    }
}
