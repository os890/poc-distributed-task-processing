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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;

import org.os890.dtp.api.event.RunRef;
import org.os890.dtp.api.event.RunStarted;
import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.jdbc.RunStore.Candidate;
import org.os890.dtp.core.jdbc.RunStore.RunRecord;

/**
 * Claims pending runs (docs/design/05-coordination-flows.md §5.2): candidates are read without locks and shuffled
 * with the node's random source, then each claim is a short transaction that acquires the free job lease
 * conditionally. Two nodes can never hold the same lease.
 */
public class RunClaimer {

    private static final System.Logger LOG = System.getLogger(RunClaimer.class.getName());

    private final EngineContext ctx;
    private final Set<String> capabilities;
    private final Random random;

    public RunClaimer(EngineContext ctx, Set<String> capabilities, Random random) {
        this.ctx = ctx;
        this.capabilities = Set.copyOf(capabilities);
        this.random = random;
    }

    /** Signals that a claim has to be rolled back, e.g. because the run was cancelled meanwhile. */
    private static class ClaimAborted extends RuntimeException {

        private static final long serialVersionUID = 1L;

        ClaimAborted() {
            super(null, null, false, false);
        }
    }

    /**
     * Claims up to {@code max} runs.
     *
     * @param max maximum number of runs
     * @return the claimed runs, this node holds their leases
     */
    public List<ClaimedRun> claim(int max) {
        if (ctx.color().standby()) {
            // another color is active (red/green); the query checks the switch as well, so it applies at once
            return List.of();
        }
        List<Candidate> candidates = ctx.tx().withConnection(c -> ctx.runs().claimCandidates(c, ctx.namespace(),
                ctx.clock().now(c), capabilities, ctx.color().color(), max * 3));
        candidates = new ArrayList<>(candidates);
        Collections.shuffle(candidates, random);
        List<ClaimedRun> claimed = new ArrayList<>();
        for (Candidate candidate : candidates) {
            if (claimed.size() >= max) {
                break;
            }
            try {
                Optional<Claim> claim = ctx.tx().requiresNew(() -> tryClaim(candidate));
                if (claim.isPresent()) {
                    claimed.add(claim.get().run());
                    ctx.telemetry().claimed(claim.get().lag());
                } else {
                    ctx.telemetry().claimConflict(); // another node holds the lease
                }
            } catch (ClaimAborted e) {
                LOG.log(System.Logger.Level.DEBUG, "claim of run {0} aborted", candidate.runId());
            } catch (DtpException e) {
                // e.g. a lock timeout on H2 (05 §5.3): count as a conflict and try the next candidate
                LOG.log(System.Logger.Level.DEBUG, "claim conflict for run " + candidate.runId(), e);
                ctx.telemetry().claimConflict();
            }
        }
        return claimed;
    }

    /** A claimed run and the time since it was due. */
    private record Claim(ClaimedRun run, Duration lag) {
    }

    private Optional<Claim> tryClaim(Candidate candidate) {
        Duration leaseDuration = leaseDuration(candidate);
        Duration timeLimit = ctx.catalog().byId(candidate.jobId()).timeLimit();
        return ctx.tx().withConnection(c -> {
            Instant now = ctx.clock().now(c);
            OptionalLong token = ctx.leases().acquire(c, candidate.jobId(), candidate.runId(), ctx.nodeId(),
                    now.plus(leaseDuration));
            if (token.isEmpty()) {
                return Optional.empty();
            }
            Optional<Integer> attempt = ctx.runs().start(c, candidate.runId(), ctx.nodeId(), token.getAsLong(), now);
            if (attempt.isEmpty()) {
                throw new ClaimAborted();
            }
            RunRecord run = ctx.runs().load(c, candidate.runId());
            Duration lag = candidate.dueAt() == null || candidate.dueAt().isAfter(now) ? Duration.ZERO
                    : Duration.between(candidate.dueAt(), now);
            ctx.publishAfterCommit(new RunStarted(new RunRef(run.id(), ctx.catalog().byId(run.jobId()).name(),
                    attempt.get(), ctx.config().nodeName(), run.flowInstanceId())));
            return Optional.of(new Claim(new ClaimedRun(run.id(), run.jobId(), run.revision(), run.handlerClass(),
                    token.getAsLong(), attempt.get(), leaseDuration, run.slotEnd(), timeLimit == null ? null
                    : now.plus(timeLimit), run.flowInstanceId()), lag));
        });
    }

    private Duration leaseDuration(Candidate candidate) {
        Duration configured = ctx.catalog().byId(candidate.jobId()).leaseDuration();
        return configured != null ? configured : ctx.config().leaseDuration();
    }
}
