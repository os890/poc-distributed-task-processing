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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.os890.dtp.core.jdbc.ItemStore.DueLane;

/**
 * Shares the batches of an inbox between its lanes by weight (DD-23), with a smooth weighted round-robin: while VIP
 * (weight 3) and standard (weight 1) both have due items, three of four batches are VIP, interleaved, and a lane that
 * runs empty gives its share to the others. The priority decides between lanes with the same credit, so the lane with
 * the higher priority is served first. One scheduler per inbox reader of a run.
 */
class LaneScheduler {

    private static final Comparator<DueLane> BY_PRIORITY = Comparator.comparingInt(DueLane::priority).reversed()
            .thenComparing(lane -> lane.name() == null ? "" : lane.name());

    private final Map<String, Long> credits = new HashMap<>();

    /**
     * The lane of the next batch.
     *
     * @param due the lanes with due items
     * @return the lane, or empty if at most one lane has due items (no filter needed)
     */
    synchronized Optional<DueLane> next(List<DueLane> due) {
        if (due.size() <= 1) {
            credits.clear();
            return Optional.empty();
        }
        // a lane without due items starts again from zero, so it doesn't come back with a burst
        credits.keySet().retainAll(due.stream().map(LaneScheduler::key).toList());
        long total = 0;
        DueLane chosen = null;
        long best = Long.MIN_VALUE;
        for (DueLane lane : due.stream().sorted(BY_PRIORITY).toList()) {
            int weight = Math.max(1, lane.weight());
            total += weight;
            long credit = credits.merge(key(lane), (long) weight, Long::sum);
            if (credit > best) {
                best = credit;
                chosen = lane;
            }
        }
        credits.merge(key(chosen), -total, Long::sum);
        return Optional.of(chosen);
    }

    private static String key(DueLane lane) {
        return lane.name() == null ? "\u0000" : lane.name();
    }
}
