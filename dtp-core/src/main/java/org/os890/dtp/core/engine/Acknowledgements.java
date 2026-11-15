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
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.api.JobHandler;
import org.os890.dtp.core.domain.FailureKind;
import org.os890.dtp.core.domain.ItemEventType;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.jdbc.ItemStore.ItemEvent;
import org.os890.dtp.core.jdbc.ItemStore.ItemRow;
import org.os890.dtp.core.jdbc.PackageStore;
import org.os890.dtp.core.jdbc.PackageStore.PackageRow;

/**
 * Acknowledgements of sent work packages (mission D37, docs/design/04-lifecycles.md §4.4): the receiver answers on
 * any node, usually through a callback of the application, with the reference the package was sent with. An
 * acknowledgement completes the package and its items; a rejection, or no answer within the job's {@code ack_timeout}
 * (checked by the housekeeping leader), is a failed attempt: the package is sent again after the retry backoff, or
 * waits for a decision once the retries are exhausted.
 */
public class Acknowledgements {

    private static final System.Logger LOG = System.getLogger(Acknowledgements.class.getName());
    private static final int TIMEOUTS_PER_TICK = 100;

    /** The result of an answer. */
    public enum Outcome {
        /** The package is done, its items are completed. */
        ACKNOWLEDGED,
        /** A failed attempt was recorded: the package is sent again, or waits for a decision. */
        REJECTED,
        /** The job has no package with this reference. */
        UNKNOWN,
        /** The package doesn't wait for an answer (any more): answered already, timed out, or never sent. */
        NOT_WAITING
    }

    private final EngineContext ctx;
    private final ItemRouting routing;
    private final PackageStore store;

    public Acknowledgements(EngineContext ctx, ItemRouting routing) {
        this.ctx = ctx;
        this.routing = routing;
        this.store = new PackageStore(ctx.dialect());
    }

    /**
     * The receiver acknowledged a package.
     *
     * @param job       the job that sent it
     * @param reference the reference it was sent with
     * @param by        who acknowledged it (e.g. the receiver), or {@code null}
     * @return the outcome
     */
    public Outcome acknowledge(Class<? extends JobHandler<?, ?>> job, String reference, String by) {
        return answer(job, reference, true, by);
    }

    /**
     * The receiver rejected a package: a failed attempt (business failure).
     *
     * @param job       the job that sent it
     * @param reference the reference it was sent with
     * @param reason    the reason of the receiver
     * @return the outcome
     */
    public Outcome reject(Class<? extends JobHandler<?, ?>> job, String reference, String reason) {
        return answer(job, reference, false, reason);
    }

    /**
     * The receiver acknowledged a package.
     *
     * @param packageId the package
     * @param by        who acknowledged it, or {@code null}
     * @return the outcome
     */
    public Outcome acknowledge(UUID packageId, String by) {
        return answer(packageId, true, by);
    }

    /**
     * The receiver rejected a package.
     *
     * @param packageId the package
     * @param reason    the reason of the receiver
     * @return the outcome
     */
    public Outcome reject(UUID packageId, String reason) {
        return answer(packageId, false, reason);
    }

    private Outcome answer(Class<? extends JobHandler<?, ?>> job, String reference, boolean acknowledged,
                           String text) {
        Objects.requireNonNull(reference, "reference");
        UUID jobId = ctx.catalog().byStep(Objects.requireNonNull(job, "job")).id();
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            Optional<PackageRow> row = store.lockSent(c, jobId, reference);
            if (row.isEmpty()) {
                return store.exists(c, jobId, reference) ? Outcome.NOT_WAITING : Outcome.UNKNOWN;
            }
            return apply(c, row.get(), acknowledged, text, FailureKind.BUSINESS, ctx.clock().now(c));
        }));
    }

    private Outcome answer(UUID packageId, boolean acknowledged, String text) {
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            Optional<PackageRow> row = store.lock(c, Objects.requireNonNull(packageId, "packageId"));
            if (row.isEmpty()) {
                return Outcome.UNKNOWN;
            }
            if (!"SENT".equals(row.get().state())) {
                return Outcome.NOT_WAITING;
            }
            return apply(c, row.get(), acknowledged, text, FailureKind.BUSINESS, ctx.clock().now(c));
        }));
    }

    /**
     * Rejects the sent packages whose acknowledgement is overdue (housekeeping leader): a technical failure, each
     * package in its own transaction.
     *
     * @return the number of rejected packages
     */
    public int timeOut() {
        List<UUID> overdue = ctx.tx().withConnection(c -> store.overdue(c, ctx.namespace(), ctx.clock().now(c),
                TIMEOUTS_PER_TICK));
        int rejected = 0;
        for (UUID id : overdue) {
            Outcome outcome = ctx.tx().required(() -> ctx.tx().withConnection(c -> {
                Instant now = ctx.clock().now(c);
                Optional<PackageRow> row = store.lock(c, id);
                if (row.isEmpty() || !"SENT".equals(row.get().state()) || row.get().ackDueAt() == null
                        || row.get().ackDueAt().isAfter(now)) {
                    return Outcome.NOT_WAITING; // answered meanwhile
                }
                JobDefinition job = ctx.catalog().byId(row.get().jobId());
                // the reference is in the SENT entry and the package: keep the reason short for the journey
                String reason = "no acknowledgement within " + (job.ackTimeout() != null ? job.ackTimeout()
                        : Duration.between(row.get().sentAt(), row.get().ackDueAt()));
                return apply(c, row.get(), false, reason, FailureKind.TECHNICAL, now);
            }));
            if (outcome == Outcome.REJECTED) {
                rejected++;
            }
        }
        if (rejected > 0) {
            LOG.log(System.Logger.Level.INFO, "{0} sent work packages without acknowledgement in time", rejected);
        }
        return rejected;
    }

    private Outcome apply(Connection c, PackageRow row, boolean acknowledged, String text, FailureKind kind,
                          Instant now) throws SQLException {
        JobDefinition job = ctx.catalog().byId(row.jobId());
        List<ItemRow> items = ctx.items().lockPackaged(c, row.id());
        String step = job.name();
        if (acknowledged) {
            for (ItemRow item : items) {
                journal(c, item, ItemEventType.ACKNOWLEDGED, text == null ? null : "acknowledged by " + text, row,
                        now);
                routing.complete(item, null);
            }
            store.acknowledged(c, row.id(), text, now);
            Duration latency = row.sentAt() == null || row.sentAt().isAfter(now) ? Duration.ZERO
                    : Duration.between(row.sentAt(), now);
            ctx.tx().afterCommit(() -> ctx.telemetry().packageAcknowledged(step, latency));
            return Outcome.ACKNOWLEDGED;
        }
        String reason = text == null ? "rejected" : text;
        for (ItemRow item : items) {
            journal(c, item, ItemEventType.REJECTED, reason, row, now);
        }
        new PackageFailures(routing.decisions(), store).failed(c, row, job, null, kind, reason, now);
        String cause = kind == FailureKind.TECHNICAL ? "timeout" : "rejected";
        ctx.tx().afterCommit(() -> ctx.telemetry().packageRejected(step, cause));
        return Outcome.REJECTED;
    }

    private void journal(Connection c, ItemRow item, ItemEventType type, String reason, PackageRow row, Instant now)
            throws SQLException {
        ctx.items().append(c, new ItemEvent(item.id(), ctx.namespace(), type, row.jobId(), null, null, null,
                ctx.nodeId(), reason, null, null, null, null, row.id()), now);
    }
}
