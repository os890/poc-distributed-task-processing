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
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.jdbc.RunStore;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.jdbc.SqlDialect;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * A run that finished between the heartbeat's snapshot of the active runs and the renewal of its lease released the
 * lease itself: the failed renewal is no lease loss.
 */
class HeartbeatTest {

    static class Quick implements JobHandler<NoParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            return JobResult.success();
        }
    }

    @Test
    void onlyTheAttemptThatFinishedTheRunCountsAsFinished() {
        try (TestCluster cluster = new TestCluster()) {
            DtpEngine node = cluster.start("a", new Quick());
            cluster.definitions().define(Quick.class).external().save();
            UUID run = UUID.fromString(node.jobEngine().trigger(Quick.class, new NoParams()).runId());
            await().atMost(Duration.ofSeconds(5)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_run"
                    + " WHERE state = 'FINISHED'") == 1);

            long token = cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT fencing_token FROM dtp_run_attempt"
                    + " WHERE run_id = ?", rs -> rs.getLong(1), run).orElseThrow());
            RunStore runs = new RunStore(SqlDialect.H2);
            assertThat(finishedWith(cluster, runs, run, token)).isTrue();
            assertThat(finishedWith(cluster, runs, run, token + 1)).isFalse();

            // a taken over attempt doesn't count, even if the run is finished later (e.g. by a decision)
            cluster.execute("UPDATE dtp_run_attempt SET end_reason = 'LEASE_EXPIRED' WHERE run_id = ?", run);
            assertThat(finishedWith(cluster, runs, run, token)).isFalse();
        }
    }

    private static boolean finishedWith(TestCluster cluster, RunStore runs, UUID run, long token) {
        Boolean finished = cluster.tx().withConnection(c -> runs.finishedWith(c, run, token));
        return finished;
    }
}
