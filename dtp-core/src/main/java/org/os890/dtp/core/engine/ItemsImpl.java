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

import java.util.Objects;
import java.util.UUID;

import org.os890.dtp.api.ItemLane;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.Items;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.domain.ItemState;
import org.os890.dtp.core.jdbc.ItemStore.ItemRow;

/** Tracks items, within a run or in the caller's transaction (e.g. a REST resource). */
public class ItemsImpl implements Items {

    private final EngineContext ctx;
    private final ItemRouting routing;
    private final RunContext run;

    /**
     * Creates the entry point.
     *
     * @param ctx     the engine
     * @param routing the item routing
     * @param run     the run, or {@code null} for the run of the current thread (if any)
     */
    public ItemsImpl(EngineContext ctx, ItemRouting routing, RunContext run) {
        this.ctx = ctx;
        this.routing = routing;
        this.run = run;
    }

    @Override
    public <E, ID> TrackedItem<E, ID> track(ItemRef<E, ID> ref, String businessKey) {
        return track(ref, businessKey, null);
    }

    @Override
    public <E, ID> TrackedItem<E, ID> track(ItemRef<E, ID> ref, String businessKey, ItemLane lane) {
        Objects.requireNonNull(ref, "ref");
        RunContext current = currentRun();
        ItemRow row = ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            ItemRow inserted = new ItemRow(UUID.randomUUID(), ctx.namespace(), ref.type().name(),
                    IdCodec.encode(ref.id()), ref.optionalVersion().orElse(0L), businessKey,
                    lane == null ? null : lane.name(), null, null, ItemState.QUEUED, null, 0, null);
            ctx.items().insert(c, inserted, lane == null ? 0 : lane.priority(),
                    current != null ? current.now() : ctx.clock().now(c));
            routing.tracked(inserted, current);
            return inserted;
        }));
        return new TrackedItemImpl<>(ctx, routing, row, ref.type(), current);
    }

    private RunContext currentRun() {
        return run != null ? run : RunContext.current();
    }
}
