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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.spi.Transactions;
import org.os890.dtp.core.testsupport.TestCluster;

/** A cron job on three nodes: every slot runs once, never two runs at the same time, every effect exactly once. */
class ExactlyOnceTest {

    static class Tick implements JobHandler<NoParams, DefaultOutcome> {

        private final Transactions tx;
        private final AtomicInteger concurrent = new AtomicInteger();
        private final AtomicInteger maxConcurrent = new AtomicInteger();

        Tick(Transactions tx) {
            this.tx = tx;
        }

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            maxConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
            try {
                // joins the fenced unit of work of the run
                tx.withConnection(c -> Sql.update(c, "INSERT INTO test_effect (id) VALUES (?)", UUID.randomUUID()));
                Thread.sleep(50);
                return JobResult.success();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } finally {
                concurrent.decrementAndGet();
            }
        }
    }

    @Test
    void everyCronSlotRunsOnceOnExactlyOneNode() {
        try (TestCluster cluster = new TestCluster()) {
            Tick tick = new Tick(cluster.tx());
            DtpEngine a = cluster.start("a", tick);
            cluster.execute("CREATE TABLE test_effect (id UUID PRIMARY KEY)");
            cluster.definitions().define(Tick.class).cron("* * * * * ?").save();
            DtpEngine b = cluster.start("b", tick);
            DtpEngine c = cluster.start("c", tick);

            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_run"
                    + " WHERE outcome = 'SUCCEEDED'") >= 4);
            assertThat(List.of(a, b, c)).filteredOn(DtpEngine::isLeader).hasSize(1);
            List.of(a, b, c).forEach(DtpEngine::stop);

            int succeeded = cluster.count("SELECT COUNT(*) FROM dtp_run WHERE trigger_type = 'CRON'"
                    + " AND outcome = 'SUCCEEDED'");
            assertThat(cluster.count("SELECT COUNT(*) FROM test_effect")).isEqualTo(succeeded);
            assertThat(tick.maxConcurrent.get()).isEqualTo(1);
            assertThat(cluster.count("SELECT COUNT(*) FROM (SELECT not_before FROM dtp_run WHERE trigger_type = 'CRON'"
                    + " GROUP BY not_before HAVING COUNT(*) > 1) duplicates")).isZero();
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run_attempt WHERE end_reason <> 'FINISHED'"))
                    .isZero();
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_node WHERE state = 'LEFT'")).isEqualTo(3);
        }
    }
}
