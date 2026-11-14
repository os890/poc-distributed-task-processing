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
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.BusinessCalendar;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.event.FlowInstanceStarted;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.domain.CalendarShift;
import org.os890.dtp.core.jdbc.Sql;
import org.os890.dtp.core.testsupport.TestCluster;

/** Cron triggers with a business calendar, and flow instances started by cron slots. */
class BusinessCalendarTest {

    static class Tick implements JobHandler<NoParams, DefaultOutcome> {

        final AtomicInteger runs = new AtomicInteger();

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            runs.incrementAndGet();
            return JobResult.success();
        }
    }

    static class Cutoff implements JobHandler<NoParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            return JobResult.success();
        }
    }

    static class Report implements JobHandler<NoParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            return JobResult.success();
        }
    }

    private static Map<LocalDate, Boolean> holidaysAroundToday() {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        return Map.of(today.minusDays(1), false, today, false, today.plusDays(1), false);
    }

    @Test
    void cronSlotsOnHolidaysAreSkippedUntilAnOperatorChangesTheCalendar() {
        try (TestCluster cluster = new TestCluster()) {
            Tick tick = new Tick();
            DtpEngine node = cluster.start("a", tick);
            cluster.definitions().calendar("OPS", "UTC", "MON-SUN", holidaysAroundToday());
            cluster.definitions().define(Tick.class).cron("* * * * * ?").calendar("OPS", CalendarShift.SKIP).save();

            BusinessCalendar ops = node.calendars().get("OPS");
            assertThat(ops.isBusinessDay(LocalDate.now(ZoneOffset.UTC))).isFalse();
            assertThat(node.calendars().names()).containsExactly("OPS");
            assertThat(node.calendars().find("NONE")).isEmpty();

            await().pollDelay(Duration.ofMillis(2500)).atMost(Duration.ofSeconds(3)).until(() -> true);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run")).as("every slot falls on a holiday").isZero();

            // an operator removes the holidays: the next refresh of the definitions applies it
            cluster.execute("DELETE FROM dtp_calendar_exception");
            await().atMost(Duration.ofSeconds(5)).until(() -> tick.runs.get() >= 2);
            assertThat(node.calendars().get("OPS").isBusinessDay(LocalDate.now(ZoneOffset.UTC))).isTrue();
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run WHERE trigger_type = 'CRON'"
                    + " AND cron_slot IS NOT NULL AND cron_slot = not_before")).isPositive();
        }
    }

    @Test
    void cronSlotsStartOneFlowInstancePerBusinessDateAcrossLeaderChanges() {
        try (TestCluster cluster = new TestCluster()) {
            List<DtpEngine> nodes = List.of(cluster.start("a", new Cutoff(), new Report()),
                    cluster.start("b", new Cutoff(), new Report()), cluster.start("c", new Cutoff(), new Report()));
            cluster.definitions().calendar("EVERY-DAY", "UTC", "MON-SUN", Map.of());
            cluster.definitions().define(Report.class).save();
            cluster.definitions().define(Cutoff.class).cron("* * * * * ?").calendar("EVERY-DAY", CalendarShift.SKIP)
                   .cronFlowInstance().on(RunOutcome.SUCCEEDED, Report.class).save();

            await().atMost(Duration.ofSeconds(10)).until(() -> cluster.count("SELECT COUNT(*) FROM dtp_flow_instance"
                    + " WHERE state = 'COMPLETED'") >= 1);
            // a new leader doesn't know which slots the old one examined: it finds the existing instance
            nodes.stream().filter(DtpEngine::isLeader).findFirst().ifPresent(DtpEngine::kill);
            await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(4)).until(() -> true);
            await().atMost(Duration.ofSeconds(5)).until(() -> nodes.stream().anyMatch(DtpEngine::isLeader));
            await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(2)).until(() -> true);

            // normally one instance; two if the test ran over midnight (UTC)
            int instances = cluster.count("SELECT COUNT(*) FROM dtp_flow_instance");
            assertThat(instances).isBetween(1, 2);
            assertThat(cluster.count("SELECT COUNT(DISTINCT business_key) FROM dtp_flow_instance"))
                    .isEqualTo(instances);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run r JOIN dtp_job j ON j.id = r.job_id"
                    + " WHERE j.name = 'Cutoff' AND r.trigger_type = 'CRON' AND r.cron_slot IS NOT NULL"
                    + " AND r.flow_instance_id IS NOT NULL")).isEqualTo(instances);
            assertThat(cluster.count("SELECT COUNT(*) FROM dtp_run r JOIN dtp_job j ON j.id = r.job_id"
                    + " WHERE j.name = 'Report'")).isEqualTo(instances);
            assertThat(cluster.events(FlowInstanceStarted.class)).hasSize(instances);
            String businessKey = cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT MIN(business_key) AS k"
                    + " FROM dtp_flow_instance", rs -> rs.getString("k")).orElseThrow());
            Instant slot = cluster.tx().withConnection(c -> Sql.queryOne(c, "SELECT MIN(cron_slot) AS s FROM dtp_run",
                    rs -> Sql.instant(rs, "s")).orElseThrow());
            assertThat(businessKey).isEqualTo(LocalDate.ofInstant(slot, ZoneOffset.UTC).toString());
        }
    }
}
