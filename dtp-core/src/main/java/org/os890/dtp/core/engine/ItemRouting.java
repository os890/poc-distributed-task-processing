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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;
import java.util.stream.Collectors;

import org.os890.dtp.core.domain.FailureKind;
import org.os890.dtp.core.domain.ItemEventType;
import org.os890.dtp.core.domain.ItemState;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.jdbc.ItemStore;
import org.os890.dtp.core.jdbc.ItemStore.ItemEvent;
import org.os890.dtp.core.jdbc.ItemStore.ItemRow;

/**
 * Item routing and failure handling. Every change is recorded in the item's journey and runs in the current
 * transaction, i.e. within the fenced unit of work of the run (or the caller's transaction outside of runs).
 */
public class ItemRouting {

    private static final int STACK_FRAMES_FOR_HASH = 5;

    private final EngineContext ctx;

    public ItemRouting(EngineContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Moves an item into a job's inbox, honoring the job's loop limit.
     *
     * @param item   the item
     * @param target the target job
     * @param reason the reason, or {@code null}
     * @param back   {@code true} for a back-routing
     * @param run    the current run, or {@code null}
     */
    public void routeTo(ItemRow item, JobDefinition target, String reason, boolean back, RunContext run) {
        inTx(run, (c, now) -> {
            ItemStore items = ctx.items();
            int visits = items.incrementVisits(c, item.id(), target.id());
            items.route(c, item.id(), target.id(), reason, now);
            items.append(c, event(item, back ? ItemEventType.ROUTED_BACK : ItemEventType.ROUTED, target.id(), reason,
                    run), now);
            if (target.maxItemVisits() != null && visits > target.maxItemVisits()) {
                items.setState(c, item.id(), ItemState.WAITING_FOR_DECISION, 0, now);
                items.append(c, event(item, ItemEventType.LOOP_LIMIT_EXCEEDED, target.id(), "visit " + visits
                        + " of " + target.name() + " exceeds max_item_visits " + target.maxItemVisits(), run), now);
            }
        });
    }

    /**
     * Completes the journey of an item.
     *
     * @param item the item
     * @param run  the current run, or {@code null}
     */
    public void complete(ItemRow item, RunContext run) {
        inTx(run, (c, now) -> {
            ctx.items().setState(c, item.id(), ItemState.COMPLETED, item.attempts(), now);
            ctx.items().append(c, event(item, ItemEventType.COMPLETED, null, null, run), now);
        });
    }

    /**
     * Ends the journey of an item without completing it.
     *
     * @param item   the item
     * @param reason the reason
     * @param run    the current run, or {@code null}
     */
    public void discard(ItemRow item, String reason, RunContext run) {
        inTx(run, (c, now) -> {
            ctx.items().setState(c, item.id(), ItemState.DISCARDED, item.attempts(), now);
            ctx.items().append(c, event(item, ItemEventType.DISCARDED, null, reason, run), now);
        });
    }

    /**
     * Keeps an item in its inbox until the delay has passed.
     *
     * @param item   the item
     * @param delay  the delay
     * @param reason the reason
     * @param run    the current run, or {@code null}
     */
    public void defer(ItemRow item, Duration delay, String reason, RunContext run) {
        inTx(run, (c, now) -> {
            ctx.items().postpone(c, item.id(), item.attempts(), now.plus(delay), now);
            ctx.items().append(c, event(item, ItemEventType.DEFERRED, null, reason, run), now);
        });
    }

    /**
     * Records a failed attempt and schedules a retry, or lets the item wait for a decision once the retries of the
     * job are exhausted.
     *
     * @param item   the item
     * @param job    the job whose retry policy applies
     * @param kind   business or technical failure
     * @param reason the reason (the message of a technical failure)
     * @param error  the exception of a technical failure, or {@code null}
     * @param run    the current run, or {@code null}
     */
    public void fail(ItemRow item, JobDefinition job, FailureKind kind, String reason, Throwable error,
                     RunContext run) {
        inTx(run, (c, now) -> {
            ItemStore items = ctx.items();
            int attempts = item.attempts() + 1;
            items.append(c, new ItemEvent(item.id(), item.namespace(), ItemEventType.FAILED, item.currentJobId(),
                    null, runId(run), runAttempt(run), ctx.nodeId(), reason, kind,
                    error == null ? null : error.getClass().getName(), error == null ? null : hash(error)), now);
            if (attempts <= job.retryMax()) {
                Instant next = now.plus(job.retryBackoff().multipliedBy(attempts));
                items.postpone(c, item.id(), attempts, next, now);
                items.append(c, event(item, ItemEventType.RETRY_SCHEDULED, null, "retry " + attempts + " of "
                        + job.retryMax() + " at " + next, run), now);
            } else {
                items.setState(c, item.id(), ItemState.WAITING_FOR_DECISION, attempts, now);
                items.append(c, event(item, ItemEventType.ESCALATED, null, "retries exhausted ("
                        + job.retryMax() + ")", run), now);
            }
        });
    }

    /**
     * Records that an item is processed again because its batch was rolled back (another item failed).
     *
     * @param item the item
     * @param run  the current run
     */
    public void reprocessed(ItemRow item, RunContext run) {
        inTx(run, (c, now) -> ctx.items().append(c, event(item, ItemEventType.REPROCESSED, null,
                "batch rollback", run), now));
    }

    /**
     * Records that an item was tracked.
     *
     * @param item the new item
     * @param run  the current run, or {@code null}
     */
    public void tracked(ItemRow item, RunContext run) {
        inTx(run, (c, now) -> ctx.items().append(c, event(item, ItemEventType.TRACKED, null, null, run), now));
    }

    /**
     * Visits of an item in a job's inbox.
     *
     * @param item  the item
     * @param jobId the job
     * @return the visits
     */
    public int visits(ItemRow item, UUID jobId) {
        return ctx.tx().withConnection(c -> ctx.items().visits(c, item.id(), jobId));
    }

    /**
     * Hash of the exception class and the top stack frames, to group technical failures (DD-29).
     *
     * @param error the exception
     * @return a short hex hash
     */
    public static String hash(Throwable error) {
        String frames = Arrays.stream(error.getStackTrace())
                              .limit(STACK_FRAMES_FOR_HASH)
                              .map(StackTraceElement::toString)
                              .collect(Collectors.joining("|", error.getClass().getName() + "|", ""));
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(frames.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @FunctionalInterface
    private interface ItemWork {
        void run(Connection c, Instant now) throws SQLException;
    }

    private void inTx(RunContext run, ItemWork work) {
        ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            work.run(c, run != null ? run.now() : ctx.clock().now(c));
            return null;
        }));
    }

    private ItemEvent event(ItemRow item, ItemEventType type, UUID toJobId, String reason, RunContext run) {
        return new ItemEvent(item.id(), item.namespace(), type, item.currentJobId(), toJobId, runId(run),
                runAttempt(run), ctx.nodeId(), reason, null, null, null);
    }

    private static UUID runId(RunContext run) {
        return run == null ? null : run.claimed().runId();
    }

    private static Integer runAttempt(RunContext run) {
        return run == null ? null : run.claimed().attempt();
    }
}
