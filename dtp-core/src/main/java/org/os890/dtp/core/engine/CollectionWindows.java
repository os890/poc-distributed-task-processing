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

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.core.domain.WindowDefinition;
import org.os890.dtp.core.jdbc.WindowStore;
import org.os890.dtp.core.jdbc.WindowStore.OccurrenceRow;

/**
 * Collection windows for operators (docs/design/04-lifecycles.md §4.6): start an occurrence now, e.g. an export
 * that is wanted before the scheduled one. The window coordinator of the leader moves it on from there.
 */
public class CollectionWindows {

    private static final System.Logger LOG = System.getLogger(CollectionWindows.class.getName());

    private final EngineContext ctx;
    private final WindowStore store = new WindowStore();

    public CollectionWindows(EngineContext ctx) {
        this.ctx = ctx;
    }

    /**
     * An occurrence of a window.
     *
     * @param id          the occurrence
     * @param window      the window
     * @param sequence    its number within the window
     * @param state       QUIESCING, OPEN, WAITING_FOR_DECISION, CLOSED or SKIPPED
     * @param scheduledAt the slot of the schedule, or when an operator started it
     * @param triggeredBy the operator, {@code null} for the schedule
     * @param from        the cut-off of the last closed occurrence, {@code null} for the first
     * @param cutOff      its cut-off, once open
     * @param collectorRunId the current collector run, or {@code null}
     */
    public record OccurrenceView(UUID id, String window, long sequence, String state, Instant scheduledAt,
                                 String triggeredBy, Instant from, Instant cutOff, UUID collectorRunId) {

        static OccurrenceView of(OccurrenceRow row) {
            return new OccurrenceView(row.id(), row.window(), row.seq(), row.state().name(), row.scheduledAt(),
                    row.triggeredBy(), row.from(), row.cutOff(), row.collectorRunId());
        }
    }

    /**
     * Starts an occurrence now, unless one is active already.
     *
     * @param name the window
     * @param user the operator
     * @return the active occurrence: the new one, or the one that was active already
     * @throws IllegalArgumentException for an unknown window
     */
    public OccurrenceView startNow(String name, String user) {
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            WindowDefinition window = store.loadAll(c, ctx.namespace()).stream().filter(w -> w.name().equals(name))
                    .findFirst().orElseThrow(() -> new IllegalArgumentException("unknown collection window " + name));
            Instant now = ctx.clock().now(c);
            store.lockWindow(c, window.id());
            Optional<OccurrenceRow> active = store.active(c, window.id());
            if (active.isPresent()) {
                return OccurrenceView.of(active.get());
            }
            UUID id = store.startQuiescing(c, window.id(), now, user, now).orElseThrow();
            LOG.log(System.Logger.Level.INFO, "window {0}: occurrence started by {1}", name, user);
            return OccurrenceView.of(store.load(c, id).orElseThrow());
        }));
    }

    /**
     * The latest occurrences of all windows.
     *
     * @param limit at most this many
     * @return the occurrences, newest first
     */
    public List<OccurrenceView> latest(int limit) {
        return ctx.tx().withConnection(c -> store.latest(c, ctx.namespace(), limit)).stream()
                  .map(OccurrenceView::of).toList();
    }
}
