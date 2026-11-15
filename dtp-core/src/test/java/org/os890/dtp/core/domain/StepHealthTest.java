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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.os890.dtp.core.domain.StepHealth.Alert;
import org.os890.dtp.core.domain.StepHealth.AlertState;
import org.os890.dtp.core.domain.StepHealth.StatusReason;

/** Status reasons, jam score and alert hysteresis of the heat map (docs/design/09-monitoring-heatmap.md §9.4). */
class StepHealthTest {

    private static final Duration EXPECTED = Duration.ofMinutes(10);
    private static final Duration WINDOW = Duration.ofMinutes(5);
    private static final Instant T0 = Instant.parse("2026-11-15T10:00:00Z");

    @Test
    void theReasonsAreCheckedInOrder() {
        assertThat(StepHealth.reason(false, true, false, true, true, true, 5)).isEqualTo(StatusReason.DISABLED);
        assertThat(StepHealth.reason(true, true, false, true, true, true, 5)).isEqualTo(StatusReason.PAUSED_BY_WINDOW);
        assertThat(StepHealth.reason(true, false, false, true, true, true, 5)).isEqualTo(StatusReason.NO_CAPABLE_NODE);
        // without queued items a missing handler is no problem
        assertThat(StepHealth.reason(true, false, false, false, false, true, 0)).isEqualTo(StatusReason.IDLE);
        assertThat(StepHealth.reason(true, false, true, true, true, true, 5)).isEqualTo(StatusReason.PROCESSING);
        assertThat(StepHealth.reason(true, false, true, false, true, true, 5)).isEqualTo(StatusReason.SLOT_CLOSED);
        assertThat(StepHealth.reason(true, false, true, false, false, true, 5)).isEqualTo(StatusReason.WAITING_FOR_RUN);
        // e.g. routed back to an import that runs when it is triggered: an expected backlog, no jam
        assertThat(StepHealth.reason(true, false, true, false, false, false, 5))
                .isEqualTo(StatusReason.WAITING_FOR_TRIGGER);
        assertThat(StepHealth.jamScore(StatusReason.WAITING_FOR_TRIGGER, 500, Duration.ofHours(1).toMillis(), 0, 0,
                EXPECTED, WINDOW)).isZero();
    }

    @Test
    void onlyAStepThatShouldBeProcessingCanBeJammed() {
        long old = Duration.ofHours(1).toMillis();
        assertThat(StepHealth.jamScore(StatusReason.SLOT_CLOSED, 500, old, 0, 0, EXPECTED, WINDOW)).isZero();
        assertThat(StepHealth.jamScore(StatusReason.PAUSED_BY_WINDOW, 500, old, 0, 0, EXPECTED, WINDOW)).isZero();
        assertThat(StepHealth.jamScore(StatusReason.DISABLED, 500, old, 0, 0, EXPECTED, WINDOW)).isZero();
        assertThat(StepHealth.jamScore(StatusReason.NO_CAPABLE_NODE, 1, 0L, 0, 0, EXPECTED, WINDOW)).isEqualTo(100);
        assertThat(StepHealth.jamScore(StatusReason.PROCESSING, 0, null, 10, 10, EXPECTED, WINDOW)).isZero();
    }

    @Test
    void theScoreIsTheStrongestSignal() {
        // age: half the expected maximum
        assertThat(StepHealth.jamScore(StatusReason.PROCESSING, 10, Duration.ofMinutes(5).toMillis(), 10, 10,
                EXPECTED, WINDOW)).isEqualTo(50);
        // age beyond the maximum is capped
        assertThat(StepHealth.jamScore(StatusReason.PROCESSING, 10, Duration.ofMinutes(30).toMillis(), 10, 10,
                EXPECTED, WINDOW)).isEqualTo(100);
        // stuck: nothing left within the window, the oldest item waits longer than the window
        assertThat(StepHealth.jamScore(StatusReason.WAITING_FOR_RUN, 10, Duration.ofMinutes(6).toMillis(), 0, 0,
                Duration.ofHours(1), WINDOW)).isEqualTo(100);
        // growing: 100 in, 20 out, young items: at most 50
        assertThat(StepHealth.jamScore(StatusReason.PROCESSING, 80, 1000L, 100, 20, EXPECTED, WINDOW))
                .isEqualTo(40);
    }

    @Test
    void aJamAndItsEndNeedTwoCyclesAFailureSpikeAlertsAtOnce() {
        Alert alert = Alert.initial(T0);
        alert = alert.next(AlertState.CONGESTED, WINDOW, T0.plusSeconds(10));
        assertThat(alert.state()).isEqualTo(AlertState.OK);
        assertThat(alert.pending()).isEqualTo(AlertState.CONGESTED);
        // a short spike doesn't alert
        alert = alert.next(AlertState.OK, WINDOW, T0.plusSeconds(20));
        assertThat(alert.pending()).isNull();
        alert = alert.next(AlertState.CONGESTED, WINDOW, T0.plusSeconds(30))
                     .next(AlertState.CONGESTED, WINDOW, T0.plusSeconds(40));
        assertThat(alert.changed()).isTrue();
        assertThat(alert.state()).isEqualTo(AlertState.CONGESTED);
        assertThat(alert.since()).isEqualTo(T0.plusSeconds(30));
        assertThat(alert.next(AlertState.CONGESTED, WINDOW, T0.plusSeconds(50)).changed()).isFalse();

        Alert failing = alert.next(AlertState.FAILING, WINDOW, T0.plusSeconds(50));
        assertThat(failing.changed()).isTrue();
        assertThat(failing.previous()).isEqualTo(AlertState.CONGESTED);
        // a failing step recovers only after the calm period
        Alert calm = failing.next(AlertState.OK, WINDOW, T0.plusSeconds(60))
                            .next(AlertState.OK, WINDOW, T0.plusSeconds(70))
                            .next(AlertState.OK, WINDOW, T0.plusSeconds(300));
        assertThat(calm.state()).isEqualTo(AlertState.FAILING);
        Alert recovered = calm.next(AlertState.OK, WINDOW, T0.plusSeconds(360));
        assertThat(recovered.changed()).isTrue();
        assertThat(recovered.state()).isEqualTo(AlertState.OK);
        assertThat(recovered.previous()).isEqualTo(AlertState.FAILING);
    }

    @Test
    void aPauseByAWindowHoldsTheAlert() {
        Alert congested = Alert.initial(T0).next(AlertState.CONGESTED, WINDOW, T0.plusSeconds(10))
                               .next(AlertState.CONGESTED, WINDOW, T0.plusSeconds(20));
        Alert held = congested.hold().hold();
        assertThat(held.state()).isEqualTo(AlertState.CONGESTED);
        assertThat(held.since()).isEqualTo(congested.since());
        assertThat(held.changed()).isFalse();
    }

    @Test
    void theTargetPrefersFailures() {
        assertThat(StepHealth.target(100, 10, 70, 10)).isEqualTo(AlertState.FAILING);
        assertThat(StepHealth.target(70, 9, 70, 10)).isEqualTo(AlertState.CONGESTED);
        assertThat(StepHealth.target(69, 0, 70, 10)).isEqualTo(AlertState.OK);
    }
}
