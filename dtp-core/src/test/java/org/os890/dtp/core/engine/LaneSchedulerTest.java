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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.os890.dtp.core.jdbc.ItemStore.DueLane;

/** The weighted round-robin of the lanes of an inbox. */
class LaneSchedulerTest {

    private static final DueLane VIP = new DueLane("VIP", 10, 3);
    private static final DueLane STANDARD = new DueLane("STANDARD", 0, 1);
    private static final DueLane NONE = new DueLane(null, 0, 1);

    @Test
    void weightsShareTheBatchesAndThePriorityGoesFirst() {
        LaneScheduler scheduler = new LaneScheduler();
        assertThat(picks(scheduler, List.of(VIP, STANDARD), 8))
                .containsExactly("VIP", "VIP", "STANDARD", "VIP", "VIP", "VIP", "STANDARD", "VIP");
        // equal weights: the lanes alternate, the higher priority first
        DueLane gold = new DueLane("GOLD", 5, 1);
        assertThat(picks(new LaneScheduler(), List.of(STANDARD, gold), 4))
                .containsExactly("GOLD", "STANDARD", "GOLD", "STANDARD");
        // items without lane are a lane of their own
        assertThat(picks(new LaneScheduler(), List.of(NONE, VIP), 4)).containsExactly("VIP", "VIP", null, "VIP");
    }

    @Test
    void aSingleLaneNeedsNoFilterAndAReturningLaneStartsFromZero() {
        LaneScheduler scheduler = new LaneScheduler();
        assertThat(scheduler.next(List.of())).isEmpty();
        assertThat(scheduler.next(List.of(VIP))).isEmpty();
        // standard waited alone for a while: when VIP is back, it doesn't get a burst of catch-up batches
        picks(scheduler, List.of(VIP, STANDARD), 3);
        scheduler.next(List.of(STANDARD));
        assertThat(picks(scheduler, List.of(VIP, STANDARD), 4)).containsExactly("VIP", "VIP", "STANDARD", "VIP");
        // a weight below 1 counts as 1
        assertThat(picks(new LaneScheduler(), List.of(new DueLane("A", 0, 0), new DueLane("B", 0, 1)), 2))
                .containsExactly("A", "B");
    }

    private static List<String> picks(LaneScheduler scheduler, List<DueLane> due, int count) {
        List<String> picks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            picks.add(scheduler.next(due).orElseThrow().name());
        }
        return picks;
    }
}
