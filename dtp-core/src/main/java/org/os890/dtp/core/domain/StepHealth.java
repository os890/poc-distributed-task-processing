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
package org.os890.dtp.core.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * The health of a step in the heat map (docs/design/09-monitoring-heatmap.md §9.4): why it does or doesn't process,
 * how jammed it is, and its alert state with hysteresis, so a short spike doesn't alert and every transition is
 * reported once.
 */
public class StepHealth {

    /** Score of a jam signal that alone makes a step congested. */
    public static final int MAX_SCORE = 100;

    /** A queue that keeps growing alone doesn't reach the default threshold. */
    static final int MAX_GROWTH_SCORE = 50;

    /** Cycles a jam (or its end) has to hold before the alert state changes. */
    static final int CYCLES = 2;

    private StepHealth() {
    }

    /** Why a step does or doesn't process; only a step that should be processing can be jammed. */
    public enum StatusReason {
        /** A run holds the lease of the job. */
        PROCESSING,
        /** Items are queued for the inbox trigger, but no run is active yet (e.g. no free node). */
        WAITING_FOR_RUN,
        /**
         * Items are queued for the next run of another trigger (e.g. an import that re-imports what was routed back,
         * a cron job), or fewer than the minimum of the inbox trigger: an expected backlog.
         */
        WAITING_FOR_TRIGGER,
        /** Nothing queued. */
        IDLE,
        /** Outside the step's processing slot. */
        SLOT_CLOSED,
        /** Paused by an active occurrence of a collection window. */
        PAUSED_BY_WINDOW,
        /** Items are queued, but no active node has the handler of the step. Always a jam. */
        NO_CAPABLE_NODE,
        /** The job is disabled. */
        DISABLED;

        /**
         * {@code true} if the step should be processing, so a queue can be a jam.
         *
         * @return whether jams count
         */
        public boolean canJam() {
            return this == PROCESSING || this == WAITING_FOR_RUN;
        }
    }

    /** The alert state of a step. */
    public enum AlertState {
        OK, CONGESTED, FAILING
    }

    /**
     * The status reason of a step, checked in this order: disabled, paused, no capable node (with queued items),
     * processing, slot closed, idle, waiting for a trigger, waiting for a run.
     *
     * @param enabled    the job is enabled
     * @param paused     an active window occurrence pauses it
     * @param capable    an active node has its handler
     * @param leased     a run holds its lease
     * @param slotClosed it has processing slots and none is open
     * @param triggered  the queue starts a run by itself (ASAP, or an inbox trigger whose minimum is reached)
     * @param queued     its due items
     * @return the reason
     */
    public static StatusReason reason(boolean enabled, boolean paused, boolean capable, boolean leased,
                                      boolean slotClosed, boolean triggered, int queued) {
        if (!enabled) {
            return StatusReason.DISABLED;
        }
        if (paused) {
            return StatusReason.PAUSED_BY_WINDOW;
        }
        if (queued > 0 && !capable) {
            return StatusReason.NO_CAPABLE_NODE;
        }
        if (leased) {
            return StatusReason.PROCESSING;
        }
        if (slotClosed) {
            return StatusReason.SLOT_CLOSED;
        }
        if (queued == 0) {
            return StatusReason.IDLE;
        }
        return triggered ? StatusReason.WAITING_FOR_RUN : StatusReason.WAITING_FOR_TRIGGER;
    }

    /**
     * The jam score (0 to 100) of a step: the highest of three signals. The age of the oldest queued item relative to
     * the expected maximum (100 at the maximum); no departures within the recent window although the oldest item
     * waits longer than the window (a stuck run: 100); more arrivals than departures within the window (a growing
     * queue: at most 50). A step without a capable node scores 100, a step that shouldn't be processing 0.
     *
     * @param reason              the status reason
     * @param queued              due items
     * @param oldestQueuedMs      age of the oldest due item, or {@code null}
     * @param arrived             arrivals within the recent window
     * @param departed            departures within the recent window
     * @param expectedMaxQueueAge the age that counts as a jam
     * @param window              the recent window
     * @return the score
     */
    public static int jamScore(StatusReason reason, int queued, Long oldestQueuedMs, int arrived, int departed,
                               Duration expectedMaxQueueAge, Duration window) {
        if (reason == StatusReason.NO_CAPABLE_NODE) {
            return MAX_SCORE;
        }
        if (!reason.canJam() || queued == 0 || oldestQueuedMs == null) {
            return 0;
        }
        long age = Math.min(MAX_SCORE, oldestQueuedMs * MAX_SCORE / Math.max(1, expectedMaxQueueAge.toMillis()));
        long stuck = departed == 0 && oldestQueuedMs >= window.toMillis() ? MAX_SCORE : 0;
        long growth = arrived > departed ? MAX_GROWTH_SCORE * (long) (arrived - departed) / arrived : 0;
        return (int) Math.max(age, Math.max(stuck, growth));
    }

    /**
     * The alert state the step should have now.
     *
     * @param jamScore         its jam score
     * @param failedTechnical  its technical failures within the window
     * @param jamThreshold     the score from which it is congested
     * @param failureThreshold the failures from which it is failing
     * @return the target state
     */
    public static AlertState target(int jamScore, int failedTechnical, int jamThreshold, int failureThreshold) {
        if (failedTechnical >= failureThreshold) {
            return AlertState.FAILING;
        }
        return jamScore >= jamThreshold ? AlertState.CONGESTED : AlertState.OK;
    }

    /**
     * The stored alert state of a step and the transition it is moving to.
     *
     * @param state         the current state
     * @param since         since when
     * @param pending       the target state that held in the last cycles, or {@code null}
     * @param pendingCycles how many cycles it held
     * @param pendingSince  since when it held
     * @param changed       {@code true} if the state changed in this cycle
     * @param previous      the state before the change
     */
    public record Alert(AlertState state, Instant since, AlertState pending, int pendingCycles, Instant pendingSince,
                        boolean changed, AlertState previous) {

        /**
         * The alert of a step seen for the first time.
         *
         * @param now the DB time
         * @return OK, nothing pending
         */
        public static Alert initial(Instant now) {
            return new Alert(AlertState.OK, now, null, 0, null, false, AlertState.OK);
        }

        /**
         * The alert while the step is paused by a collection window: unchanged, so a planned pause of a few seconds
         * neither ends a jam nor starts one.
         *
         * @return the same alert, nothing changed in this cycle
         */
        public Alert hold() {
            return new Alert(state, since, pending, pendingCycles, pendingSince, false, state);
        }

        /**
         * The next alert: a failure spike changes the state at once, a jam and its end after two cycles, and a
         * failing step recovers only after the calm period (the failure window).
         *
         * @param target     the target state of this cycle
         * @param calmPeriod how long a failing step has to stay below the threshold
         * @param now        the DB time
         * @return the alert after this cycle
         */
        public Alert next(AlertState target, Duration calmPeriod, Instant now) {
            if (target == state) {
                return new Alert(state, since, null, 0, null, false, state);
            }
            int cycles = target == pending ? pendingCycles + 1 : 1;
            Instant holding = target == pending && pendingSince != null ? pendingSince : now;
            boolean move = target == AlertState.FAILING
                           || (state == AlertState.FAILING ? !now.isBefore(holding.plus(calmPeriod))
                                                           : cycles >= CYCLES);
            if (move) {
                return new Alert(target, holding, null, 0, null, true, state);
            }
            return new Alert(state, since, target, cycles, holding, false, state);
        }
    }
}
