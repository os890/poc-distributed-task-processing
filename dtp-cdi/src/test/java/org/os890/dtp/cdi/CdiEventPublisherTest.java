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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.ObservesAsync;

import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DecisionRequest;
import org.os890.dtp.api.event.DecisionMade;
import org.os890.dtp.api.event.DecisionRequested;

/** Domain events as CDI events: synchronous and asynchronous observers, failing observers don't matter. */
class CdiEventPublisherTest {

    @ApplicationScoped
    static class Observer {

        private final List<String> notified = new CopyOnWriteArrayList<>();

        void sync(@Observes DecisionMade event) {
            notified.add("sync " + event.option());
        }

        void failing(@Observes DecisionMade event) {
            throw new IllegalStateException("a failing observer");
        }

        void async(@ObservesAsync DecisionMade event) {
            notified.add("async " + event.option());
        }

        List<String> notified() {
            return notified;
        }

        void failingAsync(@ObservesAsync DecisionRequested event) {
            throw new IllegalStateException("a failing asynchronous observer");
        }
    }

    @Test
    void eventsReachSynchronousAndAsynchronousObservers() {
        try (WeldContainer container = new Weld().disableDiscovery().addBeanClasses(Observer.class).initialize()) {
            CdiEventPublisher publisher = new CdiEventPublisher(container.getBeanManager());
            DecisionRequest request = new DecisionRequest(UUID.randomUUID(), DecisionRequest.Kind.CHOICE, "ITEM",
                    UUID.randomUUID(), "shipment", "S-1", "shipment-sorting", "unknown hub", null, 0, null,
                    Instant.now());
            publisher.publish(new DecisionMade(request, "RETRY", "otto", null));
            publisher.publish(new DecisionRequested(request));

            Observer observer = container.select(Observer.class).get();
            await().atMost(Duration.ofSeconds(5)).until(() -> observer.notified().size() == 2);
            assertThat(observer.notified()).containsExactlyInAnyOrder("sync RETRY", "async RETRY");
        }
    }
}
