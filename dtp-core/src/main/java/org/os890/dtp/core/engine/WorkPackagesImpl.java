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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.ItemType;
import org.os890.dtp.api.RunInfo;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.api.WorkPackage;
import org.os890.dtp.api.WorkPackages;
import org.os890.dtp.core.domain.BuiltInOption;
import org.os890.dtp.core.domain.DecisionKind;
import org.os890.dtp.core.domain.FailureKind;
import org.os890.dtp.core.domain.ItemEventType;
import org.os890.dtp.core.domain.ItemState;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.LeaseLostException;
import org.os890.dtp.core.domain.SubjectType;
import org.os890.dtp.core.jdbc.ItemStore.InboxEntry;
import org.os890.dtp.core.jdbc.ItemStore.InboxQuery;
import org.os890.dtp.core.jdbc.ItemStore.ItemEvent;
import org.os890.dtp.core.jdbc.ItemStore.ItemRow;
import org.os890.dtp.core.jdbc.PackageStore;
import org.os890.dtp.core.jdbc.PackageStore.PackageRow;

/**
 * The work packages of the current job for one item type (docs/design/04-lifecycles.md §4.4). Creating packages and
 * processing a package are fenced units of work of the run; a package is processed in one transaction, so it is
 * never left in progress: a crash rolls it back to {@code PENDING} and the next run (after a takeover: the same run)
 * processes it again.
 *
 * @param <E>  the entity type
 * @param <ID> the id type
 */
public class WorkPackagesImpl<E, ID> implements WorkPackages<E, ID> {

    private static final System.Logger LOG = System.getLogger(WorkPackagesImpl.class.getName());

    /** Items per unit of work when packages are created. */
    static final int CHUNK = 500;

    private final RunContext run;
    private final ItemRouting routing;
    private final ItemType<E, ID> type;
    private final EngineContext ctx;
    private final PackageStore store;

    public WorkPackagesImpl(RunContext run, ItemRouting routing, ItemType<E, ID> type) {
        this.run = run;
        this.routing = routing;
        this.type = type;
        this.ctx = run.engine();
        this.store = new PackageStore(ctx.dialect());
    }

    /** A package that is being filled. */
    private static class Filling {

        private final UUID id;
        private final long seq;
        private int count;
        private int added;

        Filling(UUID id, long seq) {
            this.id = id;
            this.seq = seq;
        }
    }

    @Override
    public int createFromInbox(Function<TrackedItem<E, ID>, String> groupKey, int maxSize) {
        requirePositive(maxSize);
        Map<String, Filling> filling = new HashMap<>();
        int[] created = {0};
        AtomicLong cursor = new AtomicLong();
        boolean more = true;
        while (more && run.shouldContinue()) {
            more = run.unitOfWork(true, () -> ctx.tx().withConnection(c -> {
                Instant now = run.now();
                List<InboxEntry> entries = ctx.items().lockInboxBatch(c, new InboxQuery(ctx.namespace(), jobId(),
                        type.name(), cursor.get(), false, false, CHUNK), now);
                if (entries.isEmpty()) {
                    return false;
                }
                cursor.accumulateAndGet(entries.getLast().seq(), Math::max);
                for (InboxEntry entry : entries) {
                    if (entry.branch() != null) {
                        continue; // a branch of a forked item belongs to its fork, it isn't packaged
                    }
                    String key = groupKey.apply(new TrackedItemImpl<>(ctx, routing, entry, type, run));
                    Filling pkg = filling(c, filling, key, maxSize, created, now);
                    ctx.items().assignToPackage(c, entry.item().id(), pkg.id, now);
                    journal(c, entry.item().id(), ItemEventType.PACKAGED, describe(key, pkg.seq), now);
                }
                flush(c, filling);
                return true;
            }));
        }
        return created[0];
    }

    @Override
    public int createFrom(Stream<ItemRef<E, ID>> refs, Function<ItemRef<E, ID>, String> groupKey, int maxSize) {
        requirePositive(maxSize);
        Map<String, Filling> filling = new HashMap<>();
        int[] created = {0};
        Iterator<ItemRef<E, ID>> iterator = refs.iterator();
        while (iterator.hasNext() && run.shouldContinue()) {
            List<ItemRef<E, ID>> chunk = new ArrayList<>();
            while (iterator.hasNext() && chunk.size() < CHUNK) {
                chunk.add(Objects.requireNonNull(iterator.next(), "ref"));
            }
            run.unitOfWork(true, () -> ctx.tx().withConnection(c -> {
                Instant now = run.now();
                for (ItemRef<E, ID> ref : chunk) {
                    String refId = IdCodec.encode(ref.id());
                    long version = ref.optionalVersion().orElse(0L);
                    if (ctx.items().isTracked(c, ctx.namespace(), type.name(), refId, version)) {
                        continue;
                    }
                    String key = groupKey.apply(ref);
                    Filling pkg = filling(c, filling, key, maxSize, created, now);
                    ItemRow item = new ItemRow(UUID.randomUUID(), ctx.namespace(), type.name(), refId, version,
                            refId, null, jobId(), null, ItemState.PACKAGED, null, 0, null, null, 0);
                    ctx.items().insertPackaged(c, item, jobId(), pkg.id, now);
                    journal(c, item.id(), ItemEventType.TRACKED, null, now);
                    journal(c, item.id(), ItemEventType.PACKAGED, describe(key, pkg.seq), now);
                }
                flush(c, filling);
                return null;
            }));
        }
        return created[0];
    }

    @Override
    public int forEachPending(Consumer<WorkPackage<E, ID>> processing) {
        int handed = 0;
        while (run.shouldContinue()) {
            AtomicReference<PackageRow> current = new AtomicReference<>();
            try {
                Integer items = run.unitOfWork(true, () -> ctx.tx().withConnection(c -> {
                    Instant now = run.now();
                    PackageRow row = store.lockNextPending(c, jobId(), type.name(), now).orElse(null);
                    if (row == null) {
                        return null;
                    }
                    current.set(row);
                    List<TrackedItemImpl<E, ID>> handles = new ArrayList<>();
                    for (ItemRow item : ctx.items().lockPackaged(c, row.id())) {
                        handles.add(new TrackedItemImpl<>(ctx, routing, item, type, run));
                    }
                    PackageHandle pkg = new PackageHandle(row, handles);
                    processing.accept(pkg);
                    pkg.apply(c, now);
                    return handles.size();
                }));
                if (items == null) {
                    break;
                }
                handed++;
                run.countItems(items);
            } catch (LeaseLostException e) {
                throw e;
            } catch (RuntimeException e) {
                PackageRow row = current.get();
                if (row == null) {
                    throw e;
                }
                handed++;
                technicalFailure(row, e);
            }
        }
        return handed;
    }

    private void technicalFailure(PackageRow row, RuntimeException error) {
        LOG.log(System.Logger.Level.WARNING, "work package " + row.seq() + " (" + row.groupKey() + ") of job "
                + run.job().name() + " failed", error);
        String reason = error.getClass().getSimpleName() + ": " + error.getMessage();
        run.unitOfWork(true, () -> ctx.tx().withConnection(c -> {
            PackageRow locked = store.lock(c, row.id()).orElseThrow();
            if ("PENDING".equals(locked.state())) {
                failed(c, locked, FailureKind.TECHNICAL, reason, run.now());
            }
            return null;
        }));
    }

    /**
     * Records a failed attempt with the retry policy of the job: pending again after the backoff, or waiting for a
     * decision once the retries are exhausted (technical failures go to operations, DD-29).
     */
    private void failed(Connection c, PackageRow row, FailureKind kind, String reason, Instant now)
            throws SQLException {
        JobDefinition job = run.job();
        int attempts = row.attempts() + 1;
        UUID runId = run.claimed().runId();
        if (attempts <= job.retryMax()) {
            store.failed(c, row.id(), runId, attempts, now.plus(job.retryBackoff().multipliedBy(attempts)), reason,
                    now);
            return;
        }
        store.failed(c, row.id(), runId, attempts, null, reason, now);
        String why = "work package " + row.seq() + " (" + row.groupKey() + "): retries exhausted (" + job.retryMax()
                + ")" + (reason == null ? "" : ": " + reason);
        routing.decisions().request(c, new Decisions.NewRequest(DecisionKind.CHOICE, SubjectType.WORK_PACKAGE,
                row.id(), job.id(), runId, why, Decisions.builtIn(BuiltInOption.PACKAGE), null, null,
                kind == FailureKind.TECHNICAL ? Decisions.OPERATORS : null), now);
    }

    private Filling filling(Connection c, Map<String, Filling> filling, String key, int maxSize, int[] created,
                            Instant now) throws SQLException {
        Filling pkg = filling.get(key);
        if (pkg == null || pkg.count >= maxSize) {
            if (pkg != null && pkg.added > 0) {
                // the full package leaves the map: its count is written now
                store.addItems(c, pkg.id, pkg.added);
            }
            UUID id = UUID.randomUUID();
            store.insert(c, id, ctx.namespace(), jobId(), type.name(), key, run.claimed().runId(), now);
            pkg = new Filling(id, store.load(c, id).seq());
            filling.put(key, pkg);
            created[0]++;
        }
        pkg.count++;
        pkg.added++;
        return pkg;
    }

    private void flush(Connection c, Map<String, Filling> filling) throws SQLException {
        for (Filling pkg : filling.values()) {
            if (pkg.added > 0) {
                store.addItems(c, pkg.id, pkg.added);
                pkg.added = 0;
            }
        }
    }

    private void journal(Connection c, UUID itemId, ItemEventType eventType, String reason, Instant now)
            throws SQLException {
        ctx.items().append(c, new ItemEvent(itemId, ctx.namespace(), eventType, jobId(), null, run.claimed().runId(),
                run.claimed().attempt(), ctx.nodeId(), reason, null, null, null), now);
    }

    private UUID jobId() {
        return run.claimed().jobId();
    }

    private static String describe(String key, long seq) {
        return "work package " + seq + (key == null ? "" : " (" + key + ")");
    }

    private static void requirePositive(int maxSize) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("the maximum package size has to be positive: " + maxSize);
        }
    }

    /** A package handed over to the handler: exactly one of {@code done} or {@code fail}. */
    private class PackageHandle implements WorkPackage<E, ID> {

        private final PackageRow row;
        private final List<TrackedItemImpl<E, ID>> items;
        private final RunInfoImpl info;
        private String failure;
        private boolean done;

        PackageHandle(PackageRow row, List<TrackedItemImpl<E, ID>> items) {
            this.row = row;
            this.items = items;
            this.info = new RunInfoImpl(ctx.json());
        }

        @Override
        public UUID id() {
            return row.id();
        }

        @Override
        public long sequence() {
            return row.seq();
        }

        @Override
        public String groupKey() {
            return row.groupKey();
        }

        @Override
        public int attempts() {
            return row.attempts();
        }

        @Override
        public List<TrackedItem<E, ID>> items() {
            return List.copyOf(items);
        }

        @Override
        public RunInfo info() {
            return info;
        }

        @Override
        public void done() {
            ended();
            done = true;
        }

        @Override
        public void fail(String reason) {
            ended();
            if (items.stream().anyMatch(TrackedItemImpl::isHandled)) {
                throw new IllegalStateException("work package " + row.seq() + ": items were routed already, it can't"
                        + " fail as a whole");
            }
            failure = reason == null ? "failed" : reason;
        }

        private void ended() {
            if (done || failure != null) {
                throw new IllegalStateException("work package " + row.seq() + " ended already");
            }
        }

        void apply(Connection c, Instant now) throws SQLException {
            if (failure != null) {
                failed(c, row, FailureKind.BUSINESS, failure, now);
                return;
            }
            if (!done) {
                throw new IllegalStateException("work package " + row.seq() + " of job " + run.job().name()
                        + " ended without done() or fail()");
            }
            for (TrackedItemImpl<E, ID> item : items) {
                if (!item.isHandled()) {
                    item.complete();
                }
            }
            store.done(c, row.id(), run.claimed().runId(), info.toJson(), now);
        }
    }
}
