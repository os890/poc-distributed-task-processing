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
package org.os890.dtp.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.os890.dtp.test.Orders.DailyReport;

/**
 * The database of a test cluster (slice 27): H2 unless the environment names a PostgreSQL database, there a schema of
 * its own per cluster, dropped with it. On PostgreSQL: {@code -Ddtp.test.jdbc-url=jdbc:postgresql://…}.
 */
class TestDatabaseTest {

    @Test
    void theEnvironmentChoosesTheDatabaseEachClusterGetsItsOwn() {
        boolean postgresql = System.getProperty(TestDatabase.URL) != null;
        try (DtpTestCluster first = new DtpTestCluster(); DtpTestCluster second = new DtpTestCluster()) {
            assertThat(first.database().isPostgresql()).isEqualTo(postgresql);
            assertThat(first.database().toString()).startsWith(postgresql ? "PostgreSQL" : "H2")
                    .isNotEqualTo(second.database().toString());
            first.start("a", new DailyReport());
            first.definitions().define(DailyReport.class).external().save();
            assertThat(first.count("SELECT COUNT(*) FROM dtp_job")).isEqualTo(1);
            second.definitions();
            assertThat(second.count("SELECT COUNT(*) FROM dtp_job")).isZero();
        }
    }

    @Test
    void anExplicitDatabaseAndWrongUrls() {
        try (DtpTestCluster cluster = new DtpTestCluster(TestDatabase.h2())) {
            cluster.start("a", new DailyReport());
            assertThat(cluster.database().isPostgresql()).isFalse();
            assertThat(cluster.runningNodes()).hasSize(1);
        }
        assertThatThrownBy(() -> TestDatabase.postgresql("jdbc:h2:mem:x", "sa", ""))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("not a PostgreSQL URL: jdbc:h2:mem:x");
        assertThatThrownBy(() -> TestDatabase.postgresql("jdbc:postgresql://127.0.0.1:1/none", "dtp", "dtp"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("can't create the schema");
    }
}
