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
import java.util.OptionalLong;

/**
 * JDBC leader election of a role (DD-3): {@code housekeeping} (planner, reaper, windows) or {@code projection} (the
 * monitoring read model, DD-30), each with its own leader. The leader only acts within a local safety deadline,
 * shorter than its lease, so it stops before another node can be elected. Correctness never depends on the leader:
 * all its actions are idempotent or conditional.
 */
public class LeaderElector {

    /** The role of the planner and reaper. */
    public static final String ROLE = "housekeeping";

    /** The role of the monitoring projector. */
    public static final String PROJECTION = "projection";

    private static final System.Logger LOG = System.getLogger(LeaderElector.class.getName());

    private final EngineContext ctx;
    private final String role;
    private final String title;

    private volatile long term = -1;
    private volatile long validUntilNanos;

    public LeaderElector(EngineContext ctx) {
        this(ctx, ROLE);
    }

    public LeaderElector(EngineContext ctx, String role) {
        this.ctx = ctx;
        this.role = role;
        this.title = ROLE.equals(role) ? "the leader" : "the leader of " + role;
    }

    /** Creates the row of the role if needed. */
    public void init() {
        ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            ctx.leaders().ensureRole(c, ctx.namespace(), role);
            return null;
        }));
    }

    /** Acquires or renews the leadership. */
    public void tick() {
        long duration = ctx.config().leaderLeaseDuration().toNanos();
        long started = System.nanoTime();
        try {
            if (term >= 0) {
                long current = term;
                boolean renewed = ctx.tx().required(() -> ctx.tx().withConnection(c -> {
                    Instant now = ctx.clock().now(c);
                    return ctx.leaders().renew(c, ctx.namespace(), role, ctx.nodeId(), current,
                            now.plus(ctx.config().leaderLeaseDuration()));
                }));
                if (renewed) {
                    validUntilNanos = started + duration * 2 / 3;
                } else {
                    term = -1;
                    LOG.log(System.Logger.Level.INFO, "node {0} lost the leadership of {1} (term {2})",
                            ctx.config().nodeName(), role, current);
                }
                return;
            }
            OptionalLong elected = ctx.tx().required(() -> ctx.tx().withConnection(c -> {
                Instant now = ctx.clock().now(c);
                return ctx.leaders().tryAcquire(c, ctx.namespace(), role, ctx.nodeId(),
                        now.plus(ctx.config().leaderLeaseDuration()), now);
            }));
            if (elected.isPresent()) {
                validUntilNanos = started + duration * 2 / 3;
                term = elected.getAsLong();
                ctx.telemetry().leaderElected(role);
                LOG.log(System.Logger.Level.INFO, "node {0} is {1} (term {2})", ctx.config().nodeName(), title,
                        term);
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "leader election failed", e);
        }
    }

    /**
     * The term of the current leadership.
     *
     * @return the term, or -1 if this node doesn't lead
     */
    public long term() {
        return term;
    }

    /** {@code true} while this node leads and its local safety deadline isn't over. */
    public boolean isLeader() {
        return term >= 0 && System.nanoTime() - validUntilNanos < 0;
    }

    /** Resigns the leadership, e.g. on shutdown. */
    public void resign() {
        long current = term;
        if (current < 0) {
            return;
        }
        term = -1;
        try {
            ctx.tx().required(() -> ctx.tx().withConnection(c -> {
                ctx.leaders().resign(c, ctx.namespace(), role, ctx.nodeId(), current);
                return null;
            }));
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "resigning failed, the leadership expires", e);
        }
    }
}
