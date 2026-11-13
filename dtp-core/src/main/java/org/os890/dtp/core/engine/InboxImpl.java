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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.os890.dtp.api.Inbox;
import org.os890.dtp.api.ItemType;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.jdbc.ItemStore.InboxQuery;
import org.os890.dtp.core.jdbc.ItemStore.ItemRow;

/**
 * The inbox of the current job within a {@code JobHandler}. Every batch is its own fenced unit of work. Each item is
 * handed over at most once per iteration; items the handler doesn't route stay queued.
 *
 * @param <E>  the entity type
 * @param <ID> the id type
 */
public class InboxImpl<E, ID> implements Inbox<E, ID> {

    private final RunContext run;
    private final ItemRouting routing;
    private final ItemType<E, ID> type;
    private final boolean includeDeferred;

    public InboxImpl(RunContext run, ItemRouting routing, ItemType<E, ID> type, boolean includeDeferred) {
        this.run = run;
        this.routing = routing;
        this.type = type;
        this.includeDeferred = includeDeferred;
    }

    @Override
    public void forEachBatch(int batchSize, Consumer<List<TrackedItem<E, ID>>> batch) {
        AtomicLong cursor = new AtomicLong();
        while (run.shouldContinue() && nextBatch(batchSize, cursor, batch)) {
            // next batch
        }
    }

    @Override
    public void forEachBatchInParallel(int batchSize, int parallelism, Consumer<List<TrackedItem<E, ID>>> batch) {
        AtomicLong cursor = new AtomicLong();
        try (ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < parallelism; i++) {
                futures.add(workers.submit(() -> {
                    while (run.shouldContinue() && nextBatch(batchSize, cursor, batch)) {
                        // next batch
                    }
                }));
            }
            for (Future<?> future : futures) {
                await(future);
            }
        }
    }

    @Override
    public Inbox<E, ID> includingDeferred() {
        return new InboxImpl<>(run, routing, type, true);
    }

    private boolean nextBatch(int batchSize, AtomicLong cursor, Consumer<List<TrackedItem<E, ID>>> batch) {
        return run.unitOfWork(true, () -> {
            EngineContext ctx = run.engine();
            InboxQuery query = new InboxQuery(ctx.namespace(), run.claimed().jobId(), type.name(), cursor.get(),
                    includeDeferred, false, batchSize);
            List<ItemRow> rows = ctx.tx().withConnection(c -> ctx.items().lockInboxBatch(c, query, run.now()));
            if (rows.isEmpty()) {
                return false;
            }
            cursor.accumulateAndGet(rows.getLast().inboxSeq(), Math::max);
            List<TrackedItem<E, ID>> handles = new ArrayList<>();
            for (ItemRow row : rows) {
                handles.add(new TrackedItemImpl<>(ctx, routing, row, type, run));
            }
            batch.accept(handles);
            return true;
        });
    }

    private static void await(Future<?> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DtpException(e);
        } catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new DtpException(e.getCause());
        }
    }
}
