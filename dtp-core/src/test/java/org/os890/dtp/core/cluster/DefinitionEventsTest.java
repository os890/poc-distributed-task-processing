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
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.event.JobDefined;
import org.os890.dtp.api.event.JobRevised;
import org.os890.dtp.api.event.WindowDefined;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * The domain events of definitions (design 02 §2.6, slice 26): the housekeeping leader announces each job revision
 * and window once, whatever wrote it.
 */
class DefinitionEventsTest {

    static class Ingest implements JobHandler<NoParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            return JobResult.success();
        }
    }

    static class Export extends Ingest {
    }

    @Test
    void definitionsAreAnnouncedOnceWhateverWroteThem() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            UUID ingest = cluster.definitions().define(Ingest.class).external().save();
            cluster.start("a", new Ingest(), new Export());
            cluster.start("b", new Ingest(), new Export());
            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.events(JobDefined.class).size() == 1);

            // a job and a window defined while the cluster runs
            cluster.definitions().define(Export.class).external().save();
            cluster.definitions().window("export", null, Duration.ofMinutes(1), Export.class, List.of(Ingest.class));
            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.events(JobDefined.class).size() == 2
                    && cluster.events(WindowDefined.class).size() == 1);

            // a definition script revises a job; a revision written ahead of the current one waits until it's current
            for (int revision = 2; revision <= 3; revision++) {
                cluster.execute("INSERT INTO dtp_job_revision (job_id, revision, handler_class, trigger_external,"
                        + " changed_by, changed_at) VALUES (?, ?, ?, TRUE, ?, ?)", ingest, revision,
                        Ingest.class.getName(), "V" + revision + "__ingest.sql", cluster.dbNow());
            }
            cluster.execute("UPDATE dtp_job SET current_revision = 2 WHERE id = ?", ingest);
            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.events(JobRevised.class).size() == 1);
            await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(5))
                    .until(() -> cluster.events(JobRevised.class).size() == 1);
            cluster.execute("UPDATE dtp_job SET current_revision = 3 WHERE id = ?", ingest);
            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.events(JobRevised.class).size() == 2);

            // announced once, though both nodes run
            await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(5))
                    .until(() -> cluster.events(JobDefined.class).size() == 2
                           && cluster.events(JobRevised.class).size() == 2
                           && cluster.events(WindowDefined.class).size() == 1);
            assertThat(cluster.events(JobDefined.class)).extracting(JobDefined::job, JobDefined::revision,
                    JobDefined::handlerClass, JobDefined::definedBy).containsExactly(
                            tuple("Ingest", 1, Ingest.class.getName(), "builder"),
                            tuple("Export", 1, Export.class.getName(), "builder"));
            assertThat(cluster.events(JobDefined.class)).allSatisfy(e -> assertThat(e.definedAt()).isNotNull());
            assertThat(cluster.events(JobRevised.class)).extracting(JobRevised::job, JobRevised::previousRevision,
                    JobRevised::revision, JobRevised::revisedBy).containsExactly(
                            tuple("Ingest", 1, 2, "V2__ingest.sql"), tuple("Ingest", 2, 3, "V3__ingest.sql"));
            assertThat(cluster.events(WindowDefined.class)).singleElement().satisfies(e -> {
                assertThat(e.window()).isEqualTo("export");
                assertThat(e.collectorJob()).isEqualTo("Export");
                assertThat(e.scheduleCron()).isNull();
                assertThat(e.zone()).isEqualTo("UTC");
                assertThat(e.definedAt()).isNotNull();
            });
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_job_revision WHERE announced_at IS NULL")).isZero();
        }
    }
}
