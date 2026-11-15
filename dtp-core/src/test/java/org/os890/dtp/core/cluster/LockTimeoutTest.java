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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.testsupport.MapStepRegistry;
import org.os890.dtp.core.testsupport.RecordingTelemetry;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * A lock that a hanging node holds is a lost race, not an endless wait (design 03 §3.4): the claim runs into the lock
 * timeout of the database (H2 {@code LOCK_TIMEOUT}, PostgreSQL {@code lock_timeout}), counts as a claim conflict, and
 * the run is claimed once the lock is released. On both databases (slice 17).
 */
class LockTimeoutTest {

    static class Report implements JobHandler<NoParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            return JobResult.success();
        }
    }

    @Test
    void aLockedLeaseIsAConflictNotAnEndlessWait() throws Exception {
        try (TestCluster cluster = new TestCluster()) {
            RecordingTelemetry telemetry = new RecordingTelemetry();
            cluster.migrate();
            UUID job = cluster.definitions().define(Report.class).external().save();
            DtpEngine node = cluster.start(TestCluster.config("a").build(), new MapStepRegistry(new Report()),
                    telemetry);
            // the leader creates the lease rows of new jobs
            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_job_lease"
                    + " WHERE job_id = ?", job) == 1);
            // a hanging node: an open transaction that holds the lease row of the job
            try (Connection hanging = cluster.dataSource().getConnection()) {
                hanging.setAutoCommit(false);
                try (PreparedStatement lock = hanging.prepareStatement("SELECT job_id FROM dtp_job_lease"
                        + " WHERE job_id = ? FOR UPDATE")) {
                    lock.setObject(1, job);
                    assertThat(lock.executeQuery().next()).isTrue();
                }
                RunHandle run = node.jobEngine().trigger(Report.class, new NoParams());
                await().atMost(Duration.ofSeconds(20)).until(() -> telemetry.conflicts.get() > 0);
                assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE id = ? AND state = 'PENDING'",
                        UUID.fromString(run.runId()))).isEqualTo(1);
                hanging.rollback();
                await().atMost(Duration.ofSeconds(20)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_run"
                        + " WHERE id = ? AND outcome = 'SUCCEEDED'", UUID.fromString(run.runId())) == 1);
            }
        }
    }
}
