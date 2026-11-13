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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.spi.Transactions;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * The node of a running run crashes (or hangs): another node takes the expired lease over and reruns the run. When
 * the old node wakes up, its commit is fenced (DD-12), so the committed effect exists exactly once.
 */
class TakeoverTest {

    static class SlowJob implements JobHandler<NoParams, DefaultOutcome> {

        private final Transactions tx;
        private final AtomicInteger executions = new AtomicInteger();
        private final CountDownLatch firstStarted = new CountDownLatch(1);
        private final CountDownLatch releaseFirst = new CountDownLatch(1);
        private final CountDownLatch firstDone = new CountDownLatch(1);

        SlowJob(Transactions tx) {
            this.tx = tx;
        }

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            int execution = executions.incrementAndGet();
            tx.withConnection(c -> Sql.update(c, "INSERT INTO test_effect (id, execution) VALUES (?, ?)",
                    UUID.randomUUID(), execution));
            if (execution == 1) {
                firstStarted.countDown();
                try {
                    releaseFirst.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    firstDone.countDown();
                }
            }
            return JobResult.success();
        }
    }

    @Test
    void crashedNodeIsTakenOverAndItsLateCommitIsFenced() throws InterruptedException {
        try (TestCluster cluster = new TestCluster()) {
            SlowJob job = new SlowJob(cluster.tx());
            DtpEngine a = cluster.start("a", job);
            cluster.execute("CREATE TABLE test_effect (id UUID PRIMARY KEY, execution INT NOT NULL)");
            cluster.definitions().define(SlowJob.class).external().save();
            DtpEngine b = cluster.start("b", job);

            RunHandle run = a.jobEngine().trigger(SlowJob.class, new NoParams());
            assertThat(run.newlyCreated()).isTrue();
            assertThat(job.firstStarted.await(10, TimeUnit.SECONDS)).isTrue();

            UUID runId = UUID.fromString(run.runId());
            UUID owner = cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT owner_node_id FROM dtp_run"
                    + " WHERE id = ?", rs -> Sql.uuid(rs, "owner_node_id"), runId)).orElseThrow();
            DtpEngine crashed = List.of(a, b).stream().filter(n -> n.nodeId().equals(owner)).findFirst().orElseThrow();
            crashed.kill();

            await().atMost(Duration.ofSeconds(15)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_run"
                    + " WHERE id = ? AND outcome = 'SUCCEEDED'", UUID.fromString(run.runId())) == 1);

            // the zombie wakes up and tries to commit its unit of work
            job.releaseFirst.countDown();
            assertThat(job.firstDone.await(10, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(500);

            assertThat(cluster.count("SELECT COUNT(*) FROM test_effect")).isEqualTo(1);
            assertThat(cluster.count("SELECT COUNT(*) FROM test_effect WHERE execution = 2")).isEqualTo(1);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run_attempt WHERE run_id = ? AND attempt = 1"
                    + " AND end_reason = 'LEASE_EXPIRED'", UUID.fromString(run.runId()))).isEqualTo(1);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run_attempt WHERE run_id = ? AND attempt = 2"
                    + " AND end_reason = 'FINISHED' AND node_id <> ?", UUID.fromString(run.runId()), owner))
                    .isEqualTo(1);
        }
    }
}
