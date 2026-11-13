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

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.spi.Transactions;
import org.os890.dtp.core.testsupport.TestCluster;

/**
 * Business vs. technical failures (DD-29): a poison item doesn't block its batch, retries follow the job's policy,
 * then the item waits for a decision. Deferred items wait without failing (DD-18).
 */
class FailureHandlingTest {

    @ItemProcessing(batchSize = 10)
    static class CreditCheck implements ItemProcessor<TestTrade, UUID> {

        private final Transactions tx;

        CreditCheck(Transactions tx) {
            this.tx = tx;
        }

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            tx.withConnection(c -> Sql.update(c, "UPDATE test_trade SET enriched = TRUE WHERE id = ?",
                    item.ref().id()));
            switch (item.businessKey()) {
                case "T-3" -> throw new IllegalStateException("scoring service timeout");
                case "T-5" -> item.fail("credit limit exceeded");
                case "T-7" -> item.defer(Duration.ofMinutes(5), "waiting for the delivery");
                default -> item.complete();
            }
        }
    }

    @Test
    void poisonItemIsIsolatedRetriedAndEscalated() {
        try (TestCluster cluster = new TestCluster()) {
            Transactions tx = cluster.tx();
            DtpEngine node = cluster.start("a", new CreditCheck(tx));
            cluster.execute(TestTrade.TABLE);
            cluster.definitions().define(CreditCheck.class).inbox(1).retry(1, Duration.ofMillis(200)).save();

            // tracked outside of a run, e.g. by a REST resource, in one transaction
            tx.required(() -> {
                for (int i = 1; i <= 10; i++) {
                    UUID id = UUID.randomUUID();
                    String number = "T-" + i;
                    tx.withConnection(c -> Sql.update(c, "INSERT INTO test_trade (id, number) VALUES (?, ?)", id,
                            number));
                    node.items().track(ItemRef.of(TestTrade.ITEM, id), number).routeTo(CreditCheck.class);
                }
                return null;
            });
            // retries are due later: wait for the escalations, then for the cluster to settle
            await().atMost(Duration.ofSeconds(20)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_item"
                    + " WHERE state = 'WAITING_FOR_DECISION'") == 2);
            cluster.awaitIdle(Duration.ofSeconds(20));

            assertThat(state("T-3", cluster)).isEqualTo("WAITING_FOR_DECISION");
            List<JourneyEntry> poison = node.queries().journey("test-trade", "T-3");
            assertThat(poison).extracting(JourneyEntry::type)
                              .containsExactly("TRACKED", "ROUTED", "FAILED", "RETRY_SCHEDULED", "FAILED", "ESCALATED");
            assertThat(poison.get(2).failureKind()).isEqualTo("TECHNICAL");
            assertThat(poison.get(2).errorType()).isEqualTo(IllegalStateException.class.getName());
            assertThat(poison.get(2).reason()).isEqualTo("scoring service timeout");

            assertThat(state("T-5", cluster)).isEqualTo("WAITING_FOR_DECISION");
            assertThat(node.queries().journey("test-trade", "T-5")).filteredOn(e -> "FAILED".equals(e.type()))
                    .hasSize(2).allSatisfy(e -> assertThat(e.failureKind()).isEqualTo("BUSINESS"));

            assertThat(state("T-7", cluster)).isEqualTo("QUEUED");
            assertThat(node.queries().journey("test-trade", "T-7").getLast().type()).isEqualTo("DEFERRED");

            for (String ok : List.of("T-1", "T-2", "T-4", "T-6", "T-8", "T-9", "T-10")) {
                assertThat(state(ok, cluster)).isEqualTo("COMPLETED");
                assertThat(node.queries().journey("test-trade", ok)).extracting(JourneyEntry::type)
                        .containsExactly("TRACKED", "ROUTED", "REPROCESSED", "COMPLETED");
            }
            // the rolled-back batch left no business effect of the poison item
            assertThat(cluster.count("SELECT COUNT(*) FROM test_trade WHERE number = 'T-3' AND enriched")).isZero();
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE outcome = 'COMPLETED_WITH_ISSUES'"))
                    .isPositive();
        }
    }

    private static String state(String businessKey, TestCluster cluster) {
        return cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT state FROM dtp_item WHERE business_key = ?",
                rs -> rs.getString(1), businessKey)).orElseThrow();
    }
}
