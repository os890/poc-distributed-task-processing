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
package org.os890.dtp.core.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.testsupport.TestCluster;

/** A node shuts down gracefully: its run stops softly after the current batch, another node continues. */
class DrainTest {

    @ItemProcessing(batchSize = 1)
    static class SlowStep implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            item.complete();
        }
    }

    @Test
    void drainedRunIsDeferredAndAnotherNodeContinues() {
        try (TestCluster cluster = new TestCluster()) {
            DtpEngine a = cluster.start("a", new SlowStep());
            cluster.definitions().define(SlowStep.class).inbox(1).save();
            cluster.tx().required(() -> {
                for (int i = 1; i <= 60; i++) {
                    a.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-" + i).routeTo(SlowStep.class);
                }
                return null;
            });
            await().atMost(Duration.ofSeconds(10)).until(() -> a.activeRuns() == 1
                    && cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'") > 5);
            DtpEngine b = cluster.start("b", new SlowStep());
            a.stop();

            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE outcome = 'DEFERRED'")).isEqualTo(1);
            cluster.awaitIdle(Duration.ofSeconds(20));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_item WHERE state = 'COMPLETED'")).isEqualTo(60);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE owner_node_id = ? AND outcome = 'SUCCEEDED'",
                    b.nodeId())).isPositive();
        }
    }
}
