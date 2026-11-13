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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.api.StopReason;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.domain.ConcurrencyPolicy;
import org.os890.dtp.core.testsupport.TestCluster;

/** Concurrency policies, idempotent triggers, cancel and soft stop. */
class PoliciesTest {

    abstract static class Blocking implements JobHandler<NoParams, DefaultOutcome> {

        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        private volatile StopReason stopped;

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            started.countDown();
            try {
                while (ctx.shouldContinue() && !release.await(20, TimeUnit.MILLISECONDS)) {
                    // waiting
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return JobResult.success();
        }

        @Override
        public void onStop(StopReason reason) {
            stopped = reason;
        }

        StopReason stopped() {
            return stopped;
        }
    }

    static class SkipJob extends Blocking {
    }

    static class QueueOneJob extends Blocking {
    }

    static class QueueAllJob extends Blocking {
    }

    static class NotExternal extends Blocking {
    }

    @Test
    void concurrencyPoliciesAndIdempotency() throws InterruptedException {
        try (TestCluster cluster = new TestCluster()) {
            SkipJob skip = new SkipJob();
            QueueOneJob queueOne = new QueueOneJob();
            QueueAllJob queueAll = new QueueAllJob();
            DtpEngine node = cluster.start("a", skip, queueOne, queueAll, new NotExternal());
            cluster.definitions().define(SkipJob.class).external().save();
            cluster.definitions().define(QueueOneJob.class).external().concurrency(ConcurrencyPolicy.QUEUE_ONE)
                   .save();
            cluster.definitions().define(QueueAllJob.class).external().concurrency(ConcurrencyPolicy.QUEUE_ALL)
                   .save();
            cluster.definitions().define(NotExternal.class).save();

            node.jobEngine().trigger(SkipJob.class, new NoParams());
            node.jobEngine().trigger(QueueOneJob.class, new NoParams());
            node.jobEngine().trigger(QueueAllJob.class, new NoParams());
            assertThat(skip.started.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(queueOne.started.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(queueAll.started.await(10, TimeUnit.SECONDS)).isTrue();

            RunHandle skipped = node.jobEngine().trigger(SkipJob.class, new NoParams());
            assertThat(skipped.newlyCreated()).isTrue();
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE outcome = 'SKIPPED'")).isEqualTo(1);

            assertThat(node.jobEngine().trigger(QueueOneJob.class, new NoParams()).newlyCreated()).isTrue();
            RunHandle coalesced = node.jobEngine().trigger(QueueOneJob.class, new NoParams());
            assertThat(coalesced.newlyCreated()).isFalse();
            assertThat(coalesced.runId()).isNull();

            node.jobEngine().trigger(QueueAllJob.class, new NoParams());
            node.jobEngine().trigger(QueueAllJob.class, new NoParams());
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run r JOIN dtp_job j ON j.id = r.job_id"
                    + " WHERE j.name = 'QueueAllJob' AND r.state = 'PENDING'")).isEqualTo(2);

            RunHandle first = node.jobEngine().trigger(QueueAllJob.class, new NoParams(), "DN-1");
            RunHandle again = node.jobEngine().trigger(QueueAllJob.class, new NoParams(), "DN-1");
            assertThat(again.newlyCreated()).isFalse();
            assertThat(again.runId()).isEqualTo(first.runId());

            assertThatThrownBy(() -> node.jobEngine().trigger(NotExternal.class, new NoParams()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("external");

            // cancel: a running run stops softly, a pending one never starts
            String running = cluster.tx().withConnection(c -> org.os890.dtp.core.jdbc.Sql.queryOne(c,
                    "SELECT r.id FROM dtp_run r JOIN dtp_job j ON j.id = r.job_id WHERE j.name = 'SkipJob'"
                    + " AND r.state = 'RUNNING'", rs -> rs.getString(1))).orElseThrow();
            assertThat(node.cancel(java.util.UUID.fromString(running))).isTrue();
            await().atMost(Duration.ofSeconds(10)).until(() -> skip.stopped() == StopReason.CANCEL);
            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_run"
                    + " WHERE outcome = 'CANCELLED'") == 1);

            queueOne.release.countDown();
            queueAll.release.countDown();
            cluster.awaitIdle(Duration.ofSeconds(20));
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run r JOIN dtp_job j ON j.id = r.job_id"
                    + " WHERE j.name = 'QueueAllJob' AND r.outcome = 'SUCCEEDED'")).isEqualTo(4);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run r JOIN dtp_job j ON j.id = r.job_id"
                    + " WHERE j.name = 'QueueOneJob' AND r.outcome = 'SUCCEEDED'")).isEqualTo(2);
        }
    }
}
