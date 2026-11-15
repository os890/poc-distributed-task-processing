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
package org.os890.dtp.core.query;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.os890.dtp.core.query.MonitoringQueries.StepView;

/**
 * The live feed of the heat map (docs/design/09-monitoring-heatmap.md §9.2): while at least one listener is
 * subscribed, this node reads the projection version (one tiny row) every feed interval; after a new cycle it reads
 * the changed steps and sends them as {@code heatmap-update}, and steps whose alert state changed once more as
 * {@code alert}. Without listeners nothing is polled.
 */
public class MonitoringFeed implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(MonitoringFeed.class.getName());

    /** Type of an update with the steps that changed. */
    public static final String HEATMAP_UPDATE = "heatmap-update";

    /** Type of an update with the steps whose alert state changed. */
    public static final String ALERT = "alert";

    /**
     * An update of the feed.
     *
     * @param type    {@link #HEATMAP_UPDATE} or {@link #ALERT}
     * @param version the projection version
     * @param steps   the steps
     */
    public record Update(String type, long version, List<StepView> steps) {

        public Update {
            steps = List.copyOf(steps);
        }
    }

    /** A subscription of the feed. */
    @FunctionalInterface
    public interface Subscription extends AutoCloseable {

        /** Unsubscribes the listener. */
        @Override
        void close();
    }

    private final MonitoringQueries queries;
    private final Duration interval;
    private final List<Consumer<Update>> listeners = new CopyOnWriteArrayList<>();
    private final Map<String, String> alertStates = new HashMap<>();
    private ScheduledExecutorService poller;
    private long version = -1;

    /**
     * Creates the feed.
     *
     * @param queries  the monitoring queries
     * @param interval how often the projection version is read
     */
    public MonitoringFeed(MonitoringQueries queries, Duration interval) {
        this.queries = queries;
        this.interval = interval;
    }

    /**
     * Subscribes a listener, e.g. a Server-Sent Events sink. It is called in the polling thread and should return
     * quickly; a listener that throws is unsubscribed.
     *
     * @param listener the listener
     * @return closing it unsubscribes the listener
     */
    public synchronized Subscription subscribe(Consumer<Update> listener) {
        listeners.add(listener);
        if (poller == null) {
            version = queries.projection().version();
            alertStates.clear();
            queries.changedSince(-1).forEach(step -> alertStates.put(step.step(), step.alertState()));
            poller = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("dtp-feed")
                    .factory());
            poller.scheduleWithFixedDelay(this::poll, interval.toMillis(), interval.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
        return () -> unsubscribe(listener);
    }

    /**
     * The number of subscribed listeners.
     *
     * @return the listeners
     */
    public int listeners() {
        return listeners.size();
    }

    private synchronized void unsubscribe(Consumer<Update> listener) {
        listeners.remove(listener);
        if (listeners.isEmpty() && poller != null) {
            poller.shutdownNow();
            poller = null;
        }
    }

    private void poll() {
        try {
            long current = queries.projection().version();
            if (current == version) {
                return;
            }
            List<StepView> changed = queries.changedSince(version);
            version = current;
            if (changed.isEmpty()) {
                return;
            }
            List<StepView> alerts = changed.stream().filter(step -> !step.alertState().equals(
                    Objects.requireNonNullElse(alertStates.put(step.step(), step.alertState()), "OK")))
                    .toList();
            send(new Update(HEATMAP_UPDATE, current, changed));
            if (!alerts.isEmpty()) {
                send(new Update(ALERT, current, alerts));
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "the live feed couldn't read the projection", e);
        }
    }

    private void send(Update update) {
        for (Consumer<Update> listener : listeners) {
            try {
                listener.accept(update);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.DEBUG, "a listener of the live feed failed and was removed", e);
                unsubscribe(listener);
            }
        }
    }

    @Override
    public synchronized void close() {
        listeners.clear();
        if (poller != null) {
            poller.shutdownNow();
            poller = null;
        }
    }
}
