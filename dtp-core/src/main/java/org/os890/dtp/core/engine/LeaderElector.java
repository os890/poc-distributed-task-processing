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
 * JDBC leader election of the role {@code housekeeping} (DD-3). The leader only acts within a local safety deadline,
 * shorter than its lease, so it stops before another node can be elected. Correctness never depends on the leader:
 * all its actions are idempotent or conditional.
 */
public class LeaderElector {

    /** The role of the planner and reaper. */
    public static final String ROLE = "housekeeping";

    private static final System.Logger LOG = System.getLogger(LeaderElector.class.getName());

    private final EngineContext ctx;

    private volatile long term = -1;
    private volatile long validUntilNanos;

    public LeaderElector(EngineContext ctx) {
        this.ctx = ctx;
    }

    /** Creates the row of the role if needed. */
    public void init() {
        ctx.tx().required(() -> ctx.tx().withConnection(c -> {
            ctx.leaders().ensureRole(c, ctx.namespace(), ROLE);
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
                    return ctx.leaders().renew(c, ctx.namespace(), ROLE, ctx.nodeId(), current,
                            now.plus(ctx.config().leaderLeaseDuration()));
                }));
                if (renewed) {
                    validUntilNanos = started + duration * 2 / 3;
                } else {
                    term = -1;
                    LOG.log(System.Logger.Level.INFO, "node {0} lost the leadership (term {1})",
                            ctx.config().nodeName(), current);
                }
                return;
            }
            OptionalLong elected = ctx.tx().required(() -> ctx.tx().withConnection(c -> {
                Instant now = ctx.clock().now(c);
                return ctx.leaders().tryAcquire(c, ctx.namespace(), ROLE, ctx.nodeId(),
                        now.plus(ctx.config().leaderLeaseDuration()), now);
            }));
            if (elected.isPresent()) {
                validUntilNanos = started + duration * 2 / 3;
                term = elected.getAsLong();
                LOG.log(System.Logger.Level.INFO, "node {0} is the leader (term {1})", ctx.config().nodeName(),
                        term);
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "leader election failed", e);
        }
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
                ctx.leaders().resign(c, ctx.namespace(), ROLE, ctx.nodeId(), current);
                return null;
            }));
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "resigning failed, the leadership expires", e);
        }
    }
}
