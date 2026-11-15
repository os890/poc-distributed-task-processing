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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemType;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.domain.FailureKind;
import org.os890.dtp.core.domain.LeaseLostException;
import org.os890.dtp.core.jdbc.ItemStore.InboxEntry;
import org.os890.dtp.core.jdbc.ItemStore.InboxQuery;
import org.os890.dtp.core.jdbc.ItemStore.InboxTrace;
import org.os890.dtp.core.spi.Telemetry;

/**
 * Runs an {@link ItemProcessor}: iterates the inbox in batches (each batch is one fenced unit of work), up to
 * {@code parallelism} batches at the same time, and isolates poison items (DD-29): if a batch fails, it is rolled back
 * and its items are processed one by one, so only the failing item records a technical failure.
 */
public class ItemProcessorDriver {

    private static final System.Logger LOG = System.getLogger(ItemProcessorDriver.class.getName());

    private final RunContext run;
    private final ItemRouting routing;
    private final ItemProcessor<Object, Object> processor;
    private final ItemType<Object, Object> type;
    private final ItemProcessing settings;
    private final AtomicLong cursor;

    @SuppressWarnings("unchecked")
    public ItemProcessorDriver(RunContext run, ItemRouting routing, ItemProcessor<?, ?> processor) {
        this.run = run;
        this.routing = routing;
        this.processor = (ItemProcessor<Object, Object>) processor;
        this.type = (ItemType<Object, Object>) run.step().itemType();
        this.settings = run.step().processing();
        // with includeDeferred, deferred items stay due: hand each one over at most once per run
        this.cursor = settings.includeDeferred() ? new AtomicLong() : null;
    }

    /**
     * Processes the inbox until it has no due items left or a stop is requested.
     *
     * @return {@code true} if due items are left (the run stopped early)
     */
    public boolean run() {
        if (settings.parallelism() == 1) {
            loop();
        } else {
            try (ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < settings.parallelism(); i++) {
                    futures.add(workers.submit(this::loop));
                }
                RuntimeException first = null;
                for (Future<?> future : futures) {
                    RuntimeException e = await(future);
                    if (first == null || e instanceof LeaseLostException) {
                        first = e != null ? e : first;
                    }
                }
                if (first != null) {
                    throw first;
                }
            }
        }
        EngineContext ctx = run.engine();
        return ctx.tx().withConnection(c -> ctx.items().countDue(c, run.claimed().jobId(), ctx.clock().now(c))) > 0;
    }

    private void loop() {
        while (run.shouldContinue() && nextBatch()) {
            // next batch
        }
    }

    private boolean nextBatch() {
        List<UUID> ids = new ArrayList<>();
        try {
            boolean found = run.unitOfWork(false, () -> {
                List<InboxEntry> entries = lock();
                if (entries.isEmpty()) {
                    return false;
                }
                entries.forEach(entry -> ids.add(entry.item().id()));
                processInSpan(entries);
                return true;
            });
            run.countItems(ids.size());
            return found;
        } catch (LeaseLostException e) {
            throw e;
        } catch (RuntimeException e) {
            if (ids.isEmpty()) {
                throw e;
            }
            isolate(ids, e);
            run.countItems(ids.size());
            return true;
        }
    }

    private void isolate(List<UUID> ids, RuntimeException batchError) {
        if (ids.size() == 1) {
            technicalFailure(ids.getFirst(), batchError);
            return;
        }
        LOG.log(System.Logger.Level.DEBUG, "batch of {0} items rolled back, processing them one by one",
                ids.size());
        for (UUID id : ids) {
            if (!run.shouldContinue()) {
                return;
            }
            try {
                run.unitOfWork(false, () -> {
                    EngineContext ctx = run.engine();
                    InboxEntry entry = ctx.tx().withConnection(c -> ctx.items().lockQueued(c, id,
                            run.claimed().jobId(), run.now())).orElse(null);
                    if (entry != null) {
                        routing.reprocessed(entry.item(), entry.branch(), run);
                        processInSpan(List.of(entry));
                    }
                    return null;
                });
            } catch (LeaseLostException e) {
                throw e;
            } catch (RuntimeException e) {
                technicalFailure(id, e);
            }
        }
    }

    private void technicalFailure(UUID id, RuntimeException error) {
        run.unitOfWork(false, () -> {
            EngineContext ctx = run.engine();
            ctx.tx().withConnection(c -> ctx.items().lockQueued(c, id, run.claimed().jobId(), run.now()))
                    .ifPresent(entry -> inSpan(List.of(entry), error, () -> routing.fail(entry.item(), entry.branch(),
                            run.job(), FailureKind.TECHNICAL, String.valueOf(error.getMessage()), error, run)));
            return null;
        });
    }

    private List<InboxEntry> lock() {
        EngineContext ctx = run.engine();
        Long after = cursor == null ? null : cursor.get();
        InboxQuery query = new InboxQuery(ctx.namespace(), run.claimed().jobId(), type.name(), after,
                settings.includeDeferred(), true, settings.batchSize());
        List<InboxEntry> entries = ctx.tx().withConnection(c -> ctx.items().lockInboxBatch(c, query, run.now()));
        if (cursor != null) {
            entries.forEach(entry -> cursor.accumulateAndGet(entry.seq(), Math::max));
        }
        return entries;
    }

    private void processInSpan(List<InboxEntry> entries) {
        inSpan(entries, null, () -> entries.forEach(this::process));
    }

    /**
     * Works on entries in the span of their unit of work, linked to the traces of their items, and reports how long
     * they waited (telemetry, after the commit).
     *
     * @param entries the entries
     * @param failure the failure the work records, or {@code null} for their processing
     * @param work    the work
     */
    private void inSpan(List<InboxEntry> entries, Throwable failure, Runnable work) {
        EngineContext ctx = run.engine();
        Telemetry telemetry = ctx.telemetry();
        List<String> links = List.of();
        if (telemetry != Telemetry.NONE) {
            Map<UUID, InboxTrace> traces = ctx.tx().withConnection(c -> ctx.items().traces(c,
                    run.claimed().jobId(), entries));
            Instant now = run.now();
            String step = run.job().name();
            for (InboxEntry entry : entries) {
                InboxTrace trace = traces.get(entry.item().id());
                if (failure == null && trace != null && trace.dueAt() != null) {
                    Duration wait = trace.dueAt().isBefore(now) ? Duration.between(trace.dueAt(), now) : Duration.ZERO;
                    String lane = entry.item().lane();
                    ctx.tx().afterCommit(() -> telemetry.itemWaited(step, lane, wait));
                }
            }
            if (telemetry.tracing()) {
                links = traces.values().stream().map(InboxTrace::traceParent).filter(Objects::nonNull).distinct()
                              .toList();
            }
        }
        try (Telemetry.Span span = telemetry.startUnitOfWork(run.job().name(), run.claimed().runId(),
                run.claimed().attempt(), links)) {
            span.items(entries.size());
            if (failure != null) {
                span.failed(failure);
            }
            try {
                work.run();
            } catch (RuntimeException | Error e) {
                span.failed(e);
                throw e;
            }
        }
    }

    private void process(InboxEntry entry) {
        TrackedItemImpl<Object, Object> item = new TrackedItemImpl<>(run.engine(), routing, entry, type, run);
        processor.process(item);
        if (!item.isHandled()) {
            throw new IllegalStateException(processor.getClass().getName() + " returned without routing " + item
                    + " (signal, routeTo, routeBack, defer, fail, complete or discard)");
        }
    }

    private static RuntimeException await(Future<?> future) {
        try {
            future.get();
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new DtpException(e);
        } catch (ExecutionException e) {
            return e.getCause() instanceof RuntimeException runtime ? runtime : new DtpException(e.getCause());
        }
    }
}
