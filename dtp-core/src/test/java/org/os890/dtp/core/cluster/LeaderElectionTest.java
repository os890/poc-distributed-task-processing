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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.testsupport.TestCluster;

/** One housekeeping leader at a time; a new one is elected when it leaves or crashes (DD-3). */
class LeaderElectionTest {

    @Test
    void anotherNodeTakesOverTheLeadership() {
        try (TestCluster cluster = new TestCluster()) {
            List<DtpEngine> nodes = new ArrayList<>(List.of(cluster.start("a"), cluster.start("b"),
                    cluster.start("c")));
            await().atMost(Duration.ofSeconds(10)).until(() -> leaders(nodes).size() == 1);

            DtpEngine first = leaders(nodes).getFirst();
            first.stop();
            nodes.remove(first);
            await().atMost(Duration.ofSeconds(10)).until(() -> leaders(nodes).size() == 1);

            DtpEngine second = leaders(nodes).getFirst();
            second.kill();
            nodes.remove(second);
            await().atMost(Duration.ofSeconds(10)).until(() -> leaders(nodes).size() == 1);

            assertThat(cluster.count("SELECT term FROM dtp_leader WHERE role = 'housekeeping'"))
                    .isGreaterThanOrEqualTo(3);
            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_node"
                    + " WHERE state = 'DEAD'") == 1);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_node WHERE state = 'LEFT'")).isEqualTo(1);
        }
    }

    private static List<DtpEngine> leaders(List<DtpEngine> nodes) {
        return nodes.stream().filter(DtpEngine::isLeader).toList();
    }
}
