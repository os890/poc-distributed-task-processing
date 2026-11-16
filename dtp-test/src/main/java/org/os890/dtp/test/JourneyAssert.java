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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Assertions on the {@link Journey} of an item (design 06 §6.4). A failed assertion shows the whole journey.
 *
 * <pre>{@code
 * assertThat(cluster.journey(Trade.ITEM, "T-2"))
 *         .hasPath(TradeImport.class, TradeValidation.class, TradeImport.class)
 *         .isQueuedIn(TradeImport.class);
 * }</pre>
 */
public class JourneyAssert {

    private final Journey journey;

    JourneyAssert(Journey journey) {
        this.journey = Objects.requireNonNull(journey, "journey");
        if (journey.item().isEmpty()) {
            throw failure("expected an item, but there is none");
        }
    }

    /**
     * The item passed exactly these steps, in this order (see {@link Journey#path()}).
     *
     * @param steps the process step classes
     * @return this
     */
    public JourneyAssert hasPath(Class<?>... steps) {
        List<String> expected = names(steps);
        if (!journey.path().equals(expected)) {
            throw failure("expected the path " + expected + ", but it was " + journey.path());
        }
        return this;
    }

    /**
     * The item passed these steps, in any order, maybe others too.
     *
     * @param steps the process step classes
     * @return this
     */
    public JourneyAssert hasVisited(Class<?>... steps) {
        List<String> missing = names(steps).stream().filter(step -> !journey.path().contains(step)).toList();
        if (!missing.isEmpty()) {
            throw failure("expected a visit of " + missing + ", but the path was " + journey.path());
        }
        return this;
    }

    /**
     * The item passed none of these steps.
     *
     * @param steps the process step classes
     * @return this
     */
    public JourneyAssert hasNotVisited(Class<?>... steps) {
        List<String> visited = names(steps).stream().filter(journey.path()::contains).toList();
        if (!visited.isEmpty()) {
            throw failure("expected no visit of " + visited + ", but the path was " + journey.path());
        }
        return this;
    }

    /**
     * The journal has exactly these entry types, in this order, e.g. {@code "TRACKED", "ROUTED", "COMPLETED"}.
     *
     * @param types the entry types
     * @return this
     */
    public JourneyAssert hasEntries(String... types) {
        if (!journey.types().equals(List.of(types))) {
            throw failure("expected the entries " + List.of(types) + ", but they were " + journey.types());
        }
        return this;
    }

    /**
     * A step scheduled this number of retries of the item.
     *
     * @param step  the process step class
     * @param count the number of retries
     * @return this
     */
    public JourneyAssert hasRetries(Class<?> step, int count) {
        String job = journey.jobName(step);
        long retries = journey.entries().stream()
                              .filter(e -> "RETRY_SCHEDULED".equals(e.type()) && job.equals(e.fromJob())).count();
        if (retries != count) {
            throw failure("expected " + count + " retries in " + job + ", but there were " + retries);
        }
        return this;
    }

    /** The item completed its flow. */
    public JourneyAssert isCompleted() {
        return hasState("COMPLETED");
    }

    /** The item was discarded. */
    public JourneyAssert isDiscarded() {
        return hasState("DISCARDED");
    }

    /** The item waits for a decision of a person. */
    public JourneyAssert isWaitingForDecision() {
        return hasState("WAITING_FOR_DECISION");
    }

    /**
     * The item waits in the inbox of a step.
     *
     * @param step the process step class
     * @return this
     */
    public JourneyAssert isQueuedIn(Class<?> step) {
        hasState("QUEUED");
        String job = journey.jobName(step);
        String current = journey.item().orElseThrow().job();
        if (!job.equals(current)) {
            throw failure("expected the item in the inbox of " + job + ", but it is in " + current);
        }
        return this;
    }

    private JourneyAssert hasState(String state) {
        String actual = journey.item().orElseThrow().state();
        if (!state.equals(actual)) {
            throw failure("expected the state " + state + ", but it is " + actual);
        }
        return this;
    }

    private List<String> names(Class<?>... steps) {
        return Arrays.stream(steps).map(journey::jobName).toList();
    }

    private AssertionError failure(String message) {
        return new AssertionError(message + "\n" + journey.describe());
    }
}
