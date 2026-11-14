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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.os890.dtp.core.domain.BuiltInOption;
import org.os890.dtp.core.domain.CronExpression;
import org.os890.dtp.core.domain.DecisionKind;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.domain.SubjectType;
import org.os890.dtp.core.domain.TakeoverPolicy;
import org.os890.dtp.core.domain.TriggerType;
import org.os890.dtp.core.jdbc.LeaseStore.ExpiredLease;

/**
 * The tasks of the housekeeping leader (DD-3): creating lease rows, planning time and inbox triggers and decision
 * fallbacks, taking over expired leases and marking dead nodes. Every action is idempotent (run keys) or conditional
 * (tokens), so two leaders for a short moment do no harm.
 */
public class Housekeeping {

    private static final System.Logger LOG = System.getLogger(Housekeeping.class.getName());

    private static final int MAX_CRON_SLOTS_PER_TICK = 10;

    private final EngineContext ctx;
    private final RunPlanner planner;
    private final Decisions decisions;
    private final Map<String, CronExpression> crons = new ConcurrentHashMap<>();

    public Housekeeping(EngineContext ctx, RunPlanner planner) {
        this.ctx = ctx;
        this.planner = planner;
        this.decisions = new Decisions(ctx);
    }

    /** One housekeeping cycle. Each step runs in its own transaction, so one failure doesn't block the others. */
    public void tick() {
        guarded("lease rows", () -> ctx.tx().required(() -> ctx.tx().withConnection(
                c -> ctx.leases().createMissing(c, ctx.namespace()))));
        for (JobDefinition job : ctx.catalog().all()) {
            if (!job.enabled()) {
                continue;
            }
            if (job.cronExpression() != null) {
                guarded("cron of " + job.name(), () -> planCron(job));
            }
            if (job.asap()) {
                guarded("asap of " + job.name(), () -> planContinuous(job, TriggerType.ASAP, 0));
            }
            if (job.inboxMinItems() != null) {
                guarded("inbox of " + job.name(), () -> planContinuous(job, TriggerType.INBOX, job.inboxMinItems()));
            }
        }
        guarded("decision fallbacks", () -> decisions.planFallbacks(planner));
        guarded("lease takeover", this::reapLeases);
        guarded("dead nodes", () -> ctx.tx().required(() -> ctx.tx().withConnection(c -> ctx.nodes().markDead(c,
                ctx.namespace(), ctx.clock().now(c).minus(ctx.config().deadAfter())))));
    }

    private Object planCron(JobDefinition job) {
        CronExpression cron = crons.computeIfAbsent(job.cronExpression(), CronExpression::parse);
        ZoneId zone = job.cronZone() == null ? ZoneOffset.UTC : ZoneId.of(job.cronZone());
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            Instant now = ctx.clock().now(c);
            // a new job starts with its first slot after its creation, missed slots are planned within the grace period
            Instant last = ctx.runs().lastCronSlot(c, job.id()).orElseGet(() -> createdAt(job));
            Instant grace = now.minus(ctx.config().misfireGrace());
            Instant from = last.isAfter(grace) ? last : grace;
            ZonedDateTime next = cron.next(from.atZone(zone));
            for (int i = 0; i < MAX_CRON_SLOTS_PER_TICK && !next.toInstant().isAfter(now); i++) {
                planner.plan(c, job, new RunPlanner.Trigger(TriggerType.CRON,
                        "cron:" + job.id() + ":" + next.toEpochSecond(), "cron slot " + next, null,
                        next.toInstant()), now);
                next = cron.next(next);
            }
            return null;
        }));
    }

    private Instant createdAt(JobDefinition job) {
        return ctx.tx().withConnection(c -> ctx.jobs().createdAt(c, job.id()));
    }

    private Object planContinuous(JobDefinition job, TriggerType type, int minItems) {
        return ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            if (!ctx.runs().openRunStates(c, job.id()).isEmpty()) {
                return null;
            }
            Instant now = ctx.clock().now(c);
            if (minItems > 0 && ctx.items().countDue(c, job.id(), now) < minItems) {
                return null;
            }
            String previous = ctx.runs().latestRunId(c, job.id()).map(UUID::toString).orElse("first");
            planner.plan(c, job, new RunPlanner.Trigger(type, type.name().toLowerCase(java.util.Locale.ROOT) + ":"
                    + job.id() + ":" + previous, null, null, null), now);
            return null;
        }));
    }

    private Object reapLeases() {
        var expired = ctx.tx().withConnection(c -> ctx.leases().findExpired(c, ctx.namespace(), ctx.clock().now(c)));
        for (ExpiredLease lease : expired) {
            boolean rerun = ctx.catalog().byId(lease.jobId()).takeover() == TakeoverPolicy.RERUN;
            boolean taken = ctx.tx().required(() -> ctx.tx().withConnection(c -> {
                Instant now = ctx.clock().now(c);
                if (!ctx.leases().expire(c, lease, now)) {
                    return false;
                }
                if (ctx.runs().afterLeaseExpired(c, lease.runId(), rerun, now) && !rerun) {
                    // takeover policy ABANDON: an operator decides (DD-5)
                    decisions.request(c, new Decisions.NewRequest(DecisionKind.CHOICE, SubjectType.RUN,
                            lease.runId(), lease.jobId(), lease.runId(), "lease of node " + lease.owner()
                            + " expired, takeover policy ABANDON", Decisions.builtIn(BuiltInOption.RUN), null, null,
                            Decisions.OPERATORS), now);
                }
                return true;
            }));
            if (taken) {
                LOG.log(System.Logger.Level.INFO, "took over the expired lease of run {0} (node {1}): {2}",
                        lease.runId(), lease.owner(), rerun ? "RERUN" : "ABANDON");
            }
        }
        return null;
    }

    @FunctionalInterface
    private interface Step {
        Object run();
    }

    private static void guarded(String name, Step step) {
        try {
            step.run();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "housekeeping step '" + name + "' failed", e);
        }
    }
}
