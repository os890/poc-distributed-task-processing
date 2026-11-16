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
package com.acme.trade;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.sql.DataSource;

import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.os890.dtp.api.BusinessCalendars;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;
import org.os890.dtp.core.query.DtpQueries.PackageView;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;

/** The trade flow on Quarkus: JPA changes and item routing commit together in JTA transactions. */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TradeFlowTest {

    private static final LocalDate DATE = LocalDate.of(2026, 11, 13);

    @Inject
    DtpQueries queries;

    @Inject
    TeamNotifications notifications;

    @Inject
    BusinessCalendars calendars;

    @Inject
    SettlementGateway gateway;

    @Inject
    EntityManager em;

    @Inject
    DataSource dataSource;

    @Test
    @Order(1)
    void tradesFlowThroughImportValidationAndEnrichment() {
        given().contentType(ContentType.JSON).body(new ImportParams(DATE, 8))
               .when().post("/trades/imports")
               .then().statusCode(200).body("newlyCreated", equalTo(true));

        // 4 and 8 have no ISIN, the ISIN of 7 is restricted
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle() && count("COMPLETED") == 5);
        assertThat(types("T-2026-11-13-1")).containsExactly("TRACKED", "ROUTED", "FORKED", "BRANCH_FINISHED",
                "BRANCH_FINISHED", "JOINED", "COMPLETED");
        assertThat(types("T-2026-11-13-4")).containsExactly("TRACKED", "ROUTED", "ROUTED_BACK");
        JourneyEntry restricted = queries.journey(Trade.ITEM.name(), "T-2026-11-13-7").getLast();
        assertThat(restricted.type()).isEqualTo("DISCARDED");
        assertThat(restricted.reason()).isEqualTo("restricted ISIN AT0000000007");

        // the next import without new trades re-imports the routed-back ones
        given().contentType(ContentType.JSON).body(new ImportParams(DATE, 0))
               .when().post("/trades/imports")
               .then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle() && count("COMPLETED") == 7);

        given().when().get("/trades/T-2026-11-13-4/journey")
               .then().statusCode(200)
               .body("type", equalTo(List.of("TRACKED", "ROUTED", "ROUTED_BACK", "ROUTED", "FORKED",
                       "BRANCH_FINISHED", "BRANCH_FINISHED", "JOINED", "COMPLETED")))
               .body("reason", hasItem("missing ISIN"));
        given().when().get("/trades/unknown/journey").then().statusCode(404);
        given().when().get("/dtp/runs").then().statusCode(200).body("job", hasItem("trade-import"));
        given().when().get("/dtp/items").then().statusCode(200).body("state", hasItem("COMPLETED"));
        given().when().get("/dtp/nodes").then().statusCode(200).body("leader", equalTo(List.of(true)));
        given().when().get("/q/health/ready").then().statusCode(200).body("status", equalTo("UP"));
    }

    @Test
    @Order(2)
    void flowsAreVisibleAndCanBeOverruled() throws Exception {
        String version = given().when().get("/dtp/flows")
                                .then().statusCode(200)
                                .body("name", equalTo(List.of("trade-flow")))
                                .extract().path("[0].activeVersion");
        given().when().get("/dtp/flows/trade-flow/diagram")
               .then().statusCode(200)
               .body(containsString("-.->|MISSING_ISIN|"), containsString("-->|ENRICHED| done"))
               .body(containsString("-->|VALID| fork_"));
        given().when().get("/dtp/flows/validation")
               .then().statusCode(200)
               .body("message", hasItem("trade-validation is part of a loop but has no max_item_visits"));
        assertThat(Files.readString(Path.of("target", "dtp-flows.md"))).contains("## trade-flow", "```mermaid");

        given().contentType(ContentType.JSON).body(Map.of("version", version, "by", "ops", "reason", "test"))
               .when().post("/dtp/flows/trade-flow/activations")
               .then().statusCode(204);
        given().contentType(ContentType.JSON).body(Map.of("version", "ffff", "by", "ops"))
               .when().post("/dtp/flows/trade-flow/activations")
               .then().statusCode(409);
        given().when().get("/dtp/flows/trade-flow/activations")
               .then().statusCode(200).body("activatedBy", hasItem("ops"));

        String id = given().contentType(ContentType.JSON)
                           .body(Map.of("fromJob", "trade-validation", "signal", "MISSING_ISIN", "kind", "DISCARD",
                                   "by", "ops", "reason", "ISIN feed down"))
                           .when().post("/dtp/flows/trade-flow/overrides")
                           .then().statusCode(200).extract().path("id");
        given().when().get("/dtp/flows/trade-flow/overrides")
               .then().statusCode(200).body("reason", equalTo(List.of("ISIN feed down")));
        given().when().get("/dtp/flows/trade-flow/diagram").then().body(containsString("MISSING_ISIN ⚠"));
        given().when().delete("/dtp/flows/overrides/" + id).then().statusCode(204);
        given().when().delete("/dtp/flows/overrides/" + id).then().statusCode(404);
        given().when().get("/dtp/flows/unknown/diagram").then().statusCode(404);
        given().contentType(ContentType.JSON).body(Map.of("from", version, "to", version, "by", "ops"))
               .when().post("/dtp/flows/trade-flow/migrations")
               .then().statusCode(200);
        given().when().get("/dtp/flows/export").then().statusCode(200).body(containsString("# dtp flows"));
    }

    @Test
    @Order(3)
    void largeTradesNeedTheApprovalOfTwoRiskManagers() {
        LocalDate date = LocalDate.of(2026, 11, 15);
        String large = "T-2026-11-15-10";
        given().contentType(ContentType.JSON).body(new ImportParams(date, 10))
               .when().post("/trades/imports").then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> types(large).contains("ESCALATED"));

        // the app informs the risk desk (domain event), the fallback reminds them after 1 s (PT4H in production)
        String reminder = "Reminder: still open for " + large;
        await().atMost(Duration.ofSeconds(30)).until(() -> notifications.latest().stream()
                                                                       .anyMatch(m -> m.subject().equals(reminder)));
        assertThat(notifications.latest()).anySatisfy(m -> {
            assertThat(m.audience()).isEqualTo("risk");
            assertThat(m.subject()).isEqualTo("Approval needed (2 approvers): " + large);
        });
        given().when().get("/notifications").then().statusCode(200)
               .body("find { it.subject == '%s' }.body".formatted(reminder), containsString("approvals 0 of 2"));

        String id = given().header("X-User", "rita").header("X-Groups", "risk")
                           .when().get("/dtp/decisions")
                           .then().statusCode(200)
                           .body("businessKey", equalTo(List.of(large)))
                           .body("[0].kind", equalTo("APPROVAL"))
                           .body("[0].requiredApprovals", equalTo(2))
                           .extract().path("[0].id");
        given().header("X-User", "bob").when().get("/dtp/decisions").then().statusCode(200).body("size()", is(0));
        given().when().get("/dtp/decisions").then().statusCode(401);
        given().when().get("/dtp/decisions/" + id).then().statusCode(200).body("reason",
                containsString("above the limit"));
        given().when().get("/dtp/decisions/" + UUID.randomUUID()).then().statusCode(404);

        vote(id, "rita", "risk", "APPROVE").statusCode(200).body("decided", is(false));
        vote(id, "rita", "risk", "APPROVE").statusCode(409);
        vote(id, "bob", "", "APPROVE").statusCode(409);
        vote(id, "rob", "risk", "APPROVE").statusCode(200).body("decided", is(true));
        given().header("X-User", "rob").header("X-Groups", "risk").contentType(ContentType.JSON)
               .body(Map.of("option", "APPROVED"))
               .when().post("/dtp/decisions/" + id + "/decision").then().statusCode(409);

        await().atMost(Duration.ofSeconds(30)).until(() -> types(large).contains("COMPLETED"));
        assertThat(types(large)).containsSubsequence("ESCALATED", "VOTED", "VOTED", "DECIDED", "COMPLETED");
    }

    @Test
    @Order(4)
    void theEndOfDayIsSignedOffOnceBothReportsSucceeded() {
        LocalDate date = LocalDate.of(2026, 11, 20);
        String eod = "/eod/" + date;
        given().contentType(ContentType.JSON).body(new ImportParams(date, 8))
               .when().post("/trades/imports").then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle()
                && types("T-2026-11-20-4").contains("ROUTED_BACK"));

        // trades 4 and 8 wait for their ISIN: the P&L report fails, the sign-off waits for it
        given().header("X-User", "otto").when().post(eod).then().statusCode(200).body("newlyCreated", is(true));
        given().when().post(eod).then().statusCode(200).body("newlyCreated", is(false));
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle());
        given().when().get(eod).then().statusCode(200)
               .body("name", equalTo("end-of-day"))
               .body("state", equalTo("OPEN"))
               .body("joins[0].target", equalTo("eod-sign-off"))
               .body("joins[0].waitingFor", equalTo(List.of("pnl-report")))
               .body("runs.find { it.job == 'pnl-report' }.outcome", equalTo("FAILED"));
        String failed = given().when().get(eod).then().extract().path("runs.find { it.job == 'pnl-report' }.id");

        // the next import completes the trades, then operations runs the report again
        given().contentType(ContentType.JSON).body(new ImportParams(date, 0))
               .when().post("/trades/imports").then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle()
                && types("T-2026-11-20-8").contains("COMPLETED"));
        given().when().post("/dtp/runs/" + failed + "/reruns").then().statusCode(401);
        given().header("X-User", "otto").when().post("/dtp/runs/" + failed + "/reruns").then().statusCode(200);

        // a corrected end of day: the sign-off waits for two members of product control
        await().atMost(Duration.ofSeconds(30)).until(() -> "WAITING_FOR_DECISION".equals(given().when().get(eod)
                .then().extract().path("runs.find { it.job == 'eod-sign-off' }.state")));
        given().when().get(eod).then().body("state", equalTo("OPEN"));
        await().atMost(Duration.ofSeconds(10)).until(() -> notifications.latest().stream().anyMatch(m ->
                "product_control".equals(m.audience())
                        && "Approval needed (2 approvers): end-of-day 2026-11-20".equals(m.subject())));
        String signOff = given().header("X-User", "pia").header("X-Groups", "product_control")
                                .when().get("/dtp/decisions").then().statusCode(200)
                                .body("[0].subjectType", equalTo("RUN"))
                                .body("[0].job", equalTo("eod-sign-off"))
                                .extract().path("[0].id");
        vote(signOff, "pia", "product_control", "APPROVE").statusCode(200).body("decided", is(false));
        vote(signOff, "otto", "", "APPROVE").statusCode(409);
        vote(signOff, "paul", "product_control", "APPROVE").statusCode(200).body("decided", is(true));
        await().atMost(Duration.ofSeconds(30)).until(() -> "COMPLETED".equals(
                given().when().get(eod).then().extract().path("state")));

        assertThat(notifications.latest()).anySatisfy(m -> {
            assertThat(m.audience()).isEqualTo("operations");
            assertThat(m.subject()).isEqualTo("EOD 2026-11-20 signed off");
            assertThat(m.body()).contains("Approved by pia and paul.");
        });
        given().when().get(eod).then()
               .body("runs.job", containsInAnyOrder("end-of-day", "position-report", "pnl-report", "pnl-report",
                       "eod-sign-off", "settlement-export"))
               .body("runs[0].job", equalTo("end-of-day"))
               .body("runs[-1].job", equalTo("settlement-export"))
               .body("joins[0].fired", is(true));
        given().header("X-User", "otto").when().post("/dtp/runs/" + failed + "/reruns").then().statusCode(409);
        given().when().get("/dtp/flow-instances").then().statusCode(200).body("businessKey", hasItem("2026-11-20"));
        given().when().get("/eod/2026-01-01").then().statusCode(404);
    }

    @Test
    @Order(5)
    void theEndOfDayIsOnlyForBusinessDaysAndTheEnrichmentRunsInItsSlot() throws Exception {
        given().when().post("/eod/2026-11-21").then().statusCode(409);
        given().when().post("/eod/2026-12-08").then().statusCode(409);
        assertThat(calendars.get("AT-BANKING").previousBusinessDay(LocalDate.of(2027, 1, 4)))
                .isEqualTo(LocalDate.of(2026, 12, 30));
        assertThat(calendars.names()).containsExactly("AT-BANKING");

        // revision 2 of the enrichment has a processing slot (the whole day in tests): its runs know its end
        String sql = "SELECT COUNT(*), COUNT(r.slot_end) FROM dtp_run r JOIN dtp_job j ON j.id = r.job_id"
                + " WHERE j.name = 'trade-enrichment' AND r.job_revision = 2";
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery(sql);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isPositive().isEqualTo(rs.getInt(2));
        }
    }

    @Test
    @Order(6)
    void theSettlementExportSendsOneFilePerCustodianAndARejectedFileIsDecided() {
        // after the sign-off of 2026-11-20 (order 4): seven released trades (trade 7 has a restricted ISIN), the
        // export created the files and handed them over to the sender, the custodians acknowledged them
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle() && queries.packages(20).stream()
                .filter(p -> "settlement-sender".equals(p.job())).allMatch(p -> "DONE".equals(p.state())));
        given().when().get("/dtp/packages").then().statusCode(200)
               .body("findAll { it.collector == 'settlement-export' }.itemCount.sum()", is(7))
               .body("findAll { it.collector == 'settlement-export' }.job", everyItem(equalTo("settlement-sender")))
               .body("findAll { it.collector == 'settlement-export' }.state", everyItem(equalTo("DONE")));

        // 2026-11-23: one custodian rejects its file, a retry later it still does, an operator decides
        LocalDate date = LocalDate.of(2026, 11, 23);
        given().contentType(ContentType.JSON).body(new ImportParams(date, 3))
               .when().post("/trades/imports").then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle()
                && types("T-2026-11-23-3").contains("COMPLETED"));
        String custodian = gateway.custodianOf(trade("T-2026-11-23-2"));
        given().when().post("/settlement/custodians/" + custodian + "/outage").then().statusCode(204);
        given().header("X-User", "otto").when().post("/eod/" + date).then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle() && queries.packages(20).stream()
                .anyMatch(p -> custodian.equals(p.groupKey()) && "WAITING_FOR_DECISION".equals(p.state())));
        String id = given().header("X-User", "otto").when().get("/dtp/decisions").then().statusCode(200)
                           .extract().path("find { it.subjectType == 'WORK_PACKAGE' }.id");
        given().when().get("/dtp/decisions/" + id).then()
               .body("reason", containsString(custodian + " rejected the file"))
               .body("options.name", containsInAnyOrder("RETRY", "DISCARD"));

        given().when().delete("/settlement/custodians/" + custodian + "/outage").then().statusCode(204);
        given().header("X-User", "otto").contentType(ContentType.JSON)
               .body(Map.of("option", "RETRY", "comment", "custodian is back")).when()
               .post("/dtp/decisions/" + id + "/decision").then().statusCode(204);
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle() && queries.packages(20).stream()
                .filter(p -> custodian.equals(p.groupKey())).allMatch(p -> "DONE".equals(p.state())));
        long seq = queries.packages(20).stream().filter(p -> custodian.equals(p.groupKey())).findFirst().orElseThrow()
                          .seq();
        given().when().get("/settlement/files").then().statusCode(200)
               .body("findAll { it.startsWith('settlement-" + custodian + "-" + seq + ".xml') }.size()", is(1));
        // the end of day completed with the export, sending was the sender's business
        given().when().get("/eod/" + date).then().body("state", equalTo("COMPLETED"))
               .body("runs.job", not(hasItem("settlement-sender")));
        // an instruction's business key is the id of its trade
        List<JourneyEntry> instruction = queries.journey(SettlementExport.INSTRUCTION.name(),
                trade("T-2026-11-23-2").getId().toString());
        assertThat(instruction).extracting(JourneyEntry::type).containsSubsequence("TRACKED", "PACKAGED", "COMPLETED");
        assertThat(instruction).filteredOn(e -> "PACKAGED".equals(e.type())).singleElement().satisfies(e -> {
            assertThat(e.fromJob()).isEqualTo("settlement-export");
            assertThat(e.toJob()).isEqualTo("settlement-sender");
        });
    }

    @Test
    @Order(7)
    void theTradeReportContainsTheTradesReleasedSinceTheLastReport() {
        // the first report: all trades released so far (orders 1 to 6), one file per business date
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle());
        long released = em.createQuery("SELECT COUNT(t) FROM Trade t WHERE t.released = TRUE", Long.class)
                          .getSingleResult();
        given().when().post("/reporting/occurrences").then().statusCode(401);
        given().header("X-User", "otto").when().post("/reporting/occurrences").then().statusCode(200)
               .body("window", equalTo("trade-reporting")).body("sequence", is(1)).body("triggeredBy", is("otto"));
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle() && "CLOSED".equals(
                given().when().get("/dtp/windows").then().extract().path("[0].state")));
        assertThat(reportedTrades(1)).isEqualTo(released);
        given().when().get("/reporting/files").then()
               .body("findAll { it.startsWith('trade-report-1-2026-11-23.csv') }.size()", is(1));

        // three new trades of 2026-11-24: the second report contains only these
        LocalDate date = LocalDate.of(2026, 11, 24);
        given().contentType(ContentType.JSON).body(new ImportParams(date, 3))
               .when().post("/trades/imports").then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle()
                && types("T-2026-11-24-3").contains("COMPLETED"));
        given().header("X-User", "otto").when().post("/reporting/occurrences").then().statusCode(200)
               .body("sequence", is(2));
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle() && "CLOSED".equals(
                given().when().get("/dtp/windows").then().extract().path("[0].state")));
        List<String> second = List.of("trade-report-2-2026-11-24.csv (3 trades)");
        given().when().get("/reporting/files").then().body("findAll { it.startsWith('trade-report-2-') }",
                equalTo(second));
        // the second occurrence starts at the cut-off of the first, the collector ran alone in the window
        ValidatableResponse windows = given().when().get("/dtp/windows").then().statusCode(200);
        assertThat(windows.extract().<String>path("[0].from")).isEqualTo(windows.extract().path("[1].cutOff"));
        given().when().get("/dtp/runs").then().body("job", hasItem("trade-reporting"));
        List<JourneyEntry> reported = queries.journey(TradeReporting.REPORTED.name(),
                trade("T-2026-11-24-1").getId().toString());
        assertThat(reported).extracting(JourneyEntry::type).containsSubsequence("TRACKED", "PACKAGED", "COMPLETED");
    }

    @Test
    @Order(8)
    void theHeatMapShowsTheFlowAndOperatorsHearOfTechnicalFailures() throws Exception {
        // the views of the flows so far (orders 1 to 7), as JSON and as a diagram of the trade flow
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle() && given().when()
                .get("/dtp/monitor/heatmap").then().extract().<Integer>path(
                        "steps.find { it.step == 'trade-enrichment' }.totals.departed") > 0);
        given().when().get("/dtp/monitor/heatmap?range=1h").then().statusCode(200)
                .body("version", greaterThan(0))
                .body("steps.find { it.step == 'trade-validation' }.statusReason", equalTo("IDLE"))
                .body("edges.find { it.from == 'trade-validation' && it.to == 'trade-import' }.backRoute",
                       is(true));
        given().when().get("/dtp/monitor/heatmap/diagram").then().statusCode(200)
                .body(containsString("classDef fail"), containsString("trade-enrichment"),
                       containsString("queued"));
        given().when().get("/dtp/monitor/heatmap/diagram?flow=unknown").then().statusCode(404);
        given().when().get("/dtp/monitor/heatmap?range=forever").then().statusCode(400);
        given().when().get("/dtp/monitor/steps/trade-enrichment?range=1h").then().statusCode(200)
               .body("size()", greaterThan(0));

        // the live feed sends the changed steps while a client is connected
        List<String> lines = new CopyOnWriteArrayList<>();
        HttpClient http = HttpClient.newHttpClient();
        CompletableFuture<?> stream = http.sendAsync(HttpRequest.newBuilder(URI.create("http://localhost:"
                + RestAssured.port + "/dtp/monitor/stream")).build(), HttpResponse.BodyHandlers.ofLines())
                .thenAccept(response -> response.body().forEach(lines::add));
        try {
            await().atMost(Duration.ofSeconds(10)).until(() -> lines.contains("event:connected"));
            // an outage of the market data service: the enrichment fails technically
            given().when().post("/market-data/outage").then().statusCode(204);
            LocalDate date = LocalDate.of(2026, 11, 25);
            given().contentType(ContentType.JSON).body(new ImportParams(date, 3))
                   .when().post("/trades/imports").then().statusCode(200);
            await().atMost(Duration.ofSeconds(30)).until(() -> notifications.latest().stream()
                    .anyMatch(m -> m.subject().equals("Technical failures: trade-enrichment")));
            await().atMost(Duration.ofSeconds(10)).until(() -> lines.contains("event:heatmap-update")
                    && lines.contains("event:alert"));
        } finally {
            stream.cancel(true);
        }
        given().when().get("/dtp/monitor/heatmap").then()
               .body("steps.find { it.step == 'trade-enrichment' }.alertState", equalTo("FAILING"));
        given().header("X-Groups", "dtp-operator").when().get("/dtp/monitor/steps/trade-enrichment/failures")
               .then().statusCode(200)
               .body("[0].errorType", equalTo(IllegalStateException.class.getName()))
               .body("[0].occurrences", greaterThan(2))
               .body("[0].sampleMessage", containsString("market data service unavailable"));

        // the service is back: the retry policy of the enrichment (3 retries, 10 s backoff) completes the trades,
        // the step recovers after the calm period and isn't counted as stuck when the retries become due
        given().when().delete("/market-data/outage").then().statusCode(204);
        await().atMost(Duration.ofSeconds(30)).until(() -> types("T-2026-11-25-1").contains("COMPLETED")
                && notifications.latest().stream()
                        .anyMatch(m -> m.subject().equals("Step recovered: trade-enrichment")));
        await().atMost(Duration.ofSeconds(10)).until(() -> queries.isIdle());
        given().when().get("/dtp/monitor/heatmap").then()
               .body("steps.find { it.step == 'trade-enrichment' }.alertState", equalTo("OK"));
        assertThat(notifications.latest()).noneMatch(m -> m.subject().equals("Step congested: trade-enrichment"));
    }

    @Test
    @Order(9)
    void journeyDiagramsShowThePathOfEachItem() {
        String viewer = "dtp-viewer";
        // the large trade of order 3: the approval of two risk managers, with their votes
        String approved = given().header("X-Groups", viewer)
                                 .when().get("/dtp/items/trade/T-2026-11-15-10/journey/diagram?zone=Europe/Vienna")
                                 .then().statusCode(200).contentType(containsString("text/plain"))
                                 .extract().asString();
        assertThat(approved).startsWith("flowchart LR").contains("[\"TradeValidation", "fork\" --> ",
                "{{\"Approval D-", ", quorum 2<br/>team risk<br/>", "APPROVE by rita: checked, ",
                "APPROVE by rob: checked, ", "decided APPROVED by rob: checked, ", " APPROVED\" --> ",
                "((\"COMPLETED<br/>", "class finish done")
                .doesNotContain("<br/>on ");
        // the trade without ISIN of order 1: routed back, imported again
        given().header("X-Groups", viewer).when().get("/dtp/items/trade/T-2026-11-13-4/journey/diagram")
               .then().statusCode(200)
               .body(containsString("back-route: missing ISIN\" .-> "))
               .body(containsString("[\"TradeValidation<br/>2 visits"))
               .body(containsString(" re-imported\" --> "));
        // the outage of order 8: failed attempts of the enrichment as a self-loop, for operators with the message
        given().header("X-Groups", "dtp-operator").when().get("/dtp/items/trade/T-2026-11-25-1/journey/diagram")
               .then().statusCode(200)
               .body(containsString(" retry "))
               .body(containsString("technical: IllegalStateException: market data service unavailable"))
               .body(containsString("succeeded on attempt "))
               .body(containsString("classDef retried"))
               .body(containsString("<br/>on "));
        // a settlement instruction of order 6: its work package, handed over to the sender
        String instruction = trade("T-2026-11-23-2").getId().toString();
        given().header("X-Groups", viewer)
               .when().get("/dtp/items/settlement-instruction/" + instruction + "/journey/diagram")
               .then().statusCode(200)
               .body(containsString("[/\"work package "))
               .body(containsString("handed over to SettlementSender"));

        // the folded graph for custom UIs, steps only, by id, and the journal with the numbers of the diagram
        String itemId = given().header("X-Groups", viewer)
                               .when().get("/dtp/items/trade/T-2026-11-15-10/journey/diagram?format=json")
                               .then().statusCode(200).contentType(ContentType.JSON)
                               .body("state", equalTo("COMPLETED"))
                               .body("nodes.kind", hasItem("DECISION"))
                               .body("nodes.find { it.kind == 'DECISION' }.decisionId", not(equalTo(null)))
                               .body("edges.find { it.kind == 'FORK' }.entries", equalTo(List.of(3)))
                               .extract().path("itemId");
        given().header("X-Groups", viewer)
               .when().get("/dtp/items/trade/T-2026-11-15-10/journey/diagram?detail=steps")
               .then().statusCode(200).body(not(containsString("{{")));
        given().header("X-Groups", viewer).when().get("/dtp/items/by-id/" + itemId + "/journey/diagram")
               .then().statusCode(200).body(containsString("Approval D-"));
        given().header("X-Groups", viewer).when().get("/dtp/items/trade/T-2026-11-15-10/journey")
               .then().statusCode(200).body("no", hasItem(1)).body("type", hasItem("VOTED"));

        // unknown items, formats and zones (the roles are checked with JWT: DtpSecurityTest)
        given().header("X-Groups", viewer).when().get("/dtp/items/trade/unknown/journey/diagram").then()
               .statusCode(404);
        given().header("X-Groups", viewer).when().get("/dtp/items/trade/unknown/journey").then().statusCode(404);
        given().header("X-Groups", viewer).when().get("/dtp/items/by-id/" + UUID.randomUUID() + "/journey/diagram")
               .then().statusCode(404);
        given().header("X-Groups", viewer)
               .when().get("/dtp/items/trade/T-2026-11-15-10/journey/diagram?format=png").then().statusCode(400);
        given().header("X-Groups", viewer)
               .when().get("/dtp/items/trade/T-2026-11-15-10/journey/diagram?zone=Mars/Base").then().statusCode(400);
        given().header("X-Groups", viewer)
               .when().get("/dtp/items/trade/T-2026-11-15-10/journey/diagram?detail=all").then().statusCode(400);
    }

    @Test
    @Order(10)
    void telemetryShowsTheMetricsAndTheTracesOfTheTrades() {
        // the metrics of the orders before: this single node is also the projection leader (cluster gauges)
        given().when().get("/telemetry/metrics").then().statusCode(200)
               .body("name", hasItems("dtp.items.processed", "dtp.item.wait_time", "dtp.unit_of_work.duration",
                       "dtp.unit_of_work.rollbacks", "dtp.failures.technical", "dtp.runs.finished",
                       "dtp.run.duration", "dtp.claim.duration", "dtp.leader.elections", "dtp.decisions.created",
                       "dtp.decisions.decided", "dtp.cluster.step.jam_score", "dtp.cluster.step.alert_state",
                       "dtp.cluster.nodes"))
               .body("find { it.name == 'dtp.failures.technical' }.points[0].attributes['error.type']",
                       equalTo(IllegalStateException.class.getName()))
               .body("find { it.name == 'dtp.decisions.decided' }.points.attributes['dtp.responsible.group']",
                       hasItem("risk"))
               .body("find { it.name == 'dtp.item.wait_time' }.unit", equalTo("s"));

        Number completed = given().when().get("/telemetry/metrics").then().extract()
                                  .path("find { it.name == 'dtp.items.processed' }.points.findAll {"
                                          + " it.attributes['dtp.step'] == 'trade-enrichment'"
                                          + " && it.attributes['dtp.result'] == 'completed' }.value.sum()");
        assertThat(completed.doubleValue()).isGreaterThan(10);

        // the journey of a trade links to the traces of its steps, and their spans link to the import
        List<String> traces = given().header("X-Groups", "dtp-viewer")
                                     .when().get("/dtp/items/trade/T-2026-11-25-1/journey/diagram?format=json")
                                     .then().statusCode(200)
                                     .extract().path("nodes.findAll { it.kind == 'STEP' }.traceIds.flatten()");
        assertThat(traces).isNotEmpty();
        given().when().get("/telemetry/spans?traceId=" + traces.getLast())
               .then().statusCode(200)
               .body("traceId", hasItem(traces.getLast()))
               .body("findAll { it.traceId == '" + traces.getLast() + "' }.name[0]", startsWith("dtp trade-"))
               .body("findAll { it.traceId == '" + traces.getLast() + "' }.links.flatten()", not(empty()));
        given().when().get("/telemetry/spans?limit=5").then().statusCode(200).body("size()", is(5));
    }

    @Test
    @Order(11)
    void custodiansAcknowledgeOrRejectTheirFilesOrDontAnswer() {
        LocalDate date = LocalDate.of(2026, 11, 26);
        // three trades with ISIN (the P&L report needs them), one per custodian
        given().contentType(ContentType.JSON).body(new ImportParams(date, 3))
               .when().post("/trades/imports").then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle()
                && types("T-2026-11-26-3").contains("COMPLETED"));
        List<String> custodians = SettlementGateway.CUSTODIANS;
        String rejecting = custodians.get(0);
        String silent = custodians.get(1);
        given().contentType(ContentType.TEXT).body("REJECTING").when()
               .put("/settlement/custodians/" + rejecting + "/mode").then().statusCode(204);
        given().contentType(ContentType.TEXT).body("SILENT").when()
               .put("/settlement/custodians/" + silent + "/mode").then().statusCode(204);
        given().contentType(ContentType.TEXT).body("NONSENSE").when()
               .put("/settlement/custodians/" + silent + "/mode").then().statusCode(400);
        given().header("X-User", "otto").when().post("/eod/" + date).then().statusCode(200);

        // rejected twice, and twice no answer within the timeout: both wait for a decision
        await().atMost(Duration.ofSeconds(30)).until(() -> waitingFor(rejecting) && waitingFor(silent));
        PackageView rejected = latestPackage(rejecting);
        PackageView unanswered = latestPackage(silent);
        assertThat(rejected.attempts()).isEqualTo(2);
        assertThat(rejected.lastError()).contains("unknown safekeeping account");
        assertThat(unanswered.lastError()).isEqualTo("no acknowledgement within PT1S");
        given().when().get("/settlement/answers").then().statusCode(200)
               .body("$", hasItem(rejected.reference() + ": REJECTED"));
        // a rejection is a business decision, a missing answer goes to operations
        given().header("X-User", "otto").header("X-Groups", "dtp-operator").when().get("/dtp/decisions").then()
               .statusCode(200)
               .body("find { it.subjectId == '" + unanswered.id() + "' }.responsibleGroup", equalTo("dtp-operator"))
               .body("find { it.subjectId == '" + rejected.id() + "' }.responsibleGroup", equalTo(null));
        // the journey of an instruction in the rejected file
        String instruction = given().header("X-Groups", "dtp-viewer")
                                    .when().get("/dtp/items/settlement-instruction/" + firstItemOf(rejected)
                                            + "/journey").then().statusCode(200).extract().asString();
        assertThat(instruction).contains("\"SENT\"", "\"REJECTED\"");

        // the custodians are fixed: decided RETRY, the files go out again and are acknowledged; a late answer for
        // a file that waits for a decision and one for an unknown file are refused
        given().contentType(ContentType.JSON).body(Map.of("file", unanswered.reference(), "accepted", true))
               .when().post("/settlement/acknowledgements").then().statusCode(409);
        given().contentType(ContentType.JSON).body(Map.of("file", "unknown.xml", "accepted", true))
               .when().post("/settlement/acknowledgements").then().statusCode(404);
        given().contentType(ContentType.JSON).body(Map.of("accepted", true))
               .when().post("/settlement/acknowledgements").then().statusCode(400);
        given().contentType(ContentType.TEXT).body("ACCEPTING").when()
               .put("/settlement/custodians/" + rejecting + "/mode").then().statusCode(204);
        for (PackageView pkg : List.of(rejected, unanswered)) {
            String id = given().header("X-User", "otto").header("X-Groups", "dtp-operator")
                               .when().get("/dtp/decisions").then().statusCode(200)
                               .extract().path("find { it.subjectId == '" + pkg.id() + "' }.id");
            given().header("X-User", "otto").header("X-Groups", "dtp-operator").contentType(ContentType.JSON)
                   .body(Map.of("option", "RETRY")).when().post("/dtp/decisions/" + id + "/decision")
                   .then().statusCode(204);
        }
        // the silent custodian still doesn't answer by itself: its answer arrives through the callback
        await().atMost(Duration.ofSeconds(30)).until(() -> "DONE".equals(latestPackage(rejecting).state())
                && "SENT".equals(latestPackage(silent).state()));
        given().contentType(ContentType.JSON).body(Map.of("file", unanswered.reference(), "accepted", true,
                "custodian", silent))
               .when().post("/settlement/acknowledgements").then().statusCode(200)
               .body("outcome", equalTo("ACKNOWLEDGED"));
        assertThat(latestPackage(silent).state()).isEqualTo("DONE");
        given().contentType(ContentType.TEXT).body("ACCEPTING").when()
               .put("/settlement/custodians/" + silent + "/mode").then().statusCode(204);
        assertThat(given().header("X-Groups", "dtp-viewer")
                          .when().get("/dtp/items/settlement-instruction/" + firstItemOf(rejected)
                                  + "/journey").then().statusCode(200).extract().<List<String>>path("type"))
                .containsSubsequence("PACKAGED", "SENT", "REJECTED", "SENT", "REJECTED", "SENT", "ACKNOWLEDGED",
                        "COMPLETED");
        given().when().get("/telemetry/metrics").then().statusCode(200)
               .body("find { it.name == 'dtp.packages.rejected' }.points.attributes['dtp.rejection.reason']",
                       hasItems("rejected", "timeout"))
               .body("find { it.name == 'dtp.packages.acknowledged' }.points.size()", greaterThan(0));
    }

    @Test
    @Order(12)
    void tradesHaveLanesAndAFlowVersionCanBeActiveForOneLane() throws java.sql.SQLException {
        // large tickets are institutional (served first, 3 of 4 batches), the others retail
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT business_key, lane, lane_weight FROM dtp_item WHERE business_key IN"
                    + " ('T-2026-11-15-10', 'T-2026-11-15-1') ORDER BY business_key");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("lane")).isEqualTo("RETAIL");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("lane")).isEqualTo("INSTITUTIONAL");
            assertThat(rs.getInt("lane_weight")).isEqualTo(3);
        }
        String lanes = "find { it.name == 'dtp.items.processed' }.points.attributes['dtp.lane']";
        given().when().get("/telemetry/metrics").then().statusCode(200)
               .body(lanes, hasItems("INSTITUTIONAL", "RETAIL"));

        // a version active for the institutional lane only, then the lane follows the default again
        String version = given().when().get("/dtp/flows").then().statusCode(200)
                                .extract().path("find { it.name == 'trade-flow' }.activeVersion");
        given().contentType(ContentType.JSON).body(Map.of("version", version, "by", "ops", "reason", "canary",
                "lane", "INSTITUTIONAL"))
               .when().post("/dtp/flows/trade-flow/activations").then().statusCode(204);
        given().when().get("/dtp/flows").then().statusCode(200)
               .body("find { it.name == 'trade-flow' }.laneVersions.INSTITUTIONAL", equalTo(version));
        given().when().get("/dtp/flows/trade-flow/activations").then().statusCode(200)
               .body("[0].lane", equalTo("INSTITUTIONAL"))
               .body("[0].version", equalTo(version));
        given().when().delete("/dtp/flows/trade-flow/activations/lanes/INSTITUTIONAL?by=ops&reason=done")
               .then().statusCode(204);
        given().when().get("/dtp/flows").then().statusCode(200)
               .body("find { it.name == 'trade-flow' }.laneVersions.size()", is(0));
        given().when().get("/dtp/flows/trade-flow/activations").then().statusCode(200)
               .body("[0].lane", equalTo("INSTITUTIONAL"))
               .body("[0].version", equalTo(null));
        given().contentType(ContentType.JSON).body(Map.of("version", "nonsense", "by", "ops", "lane", "RETAIL"))
               .when().post("/dtp/flows/trade-flow/activations").then().statusCode(409);
        // without a user (X-User here, the principal with JWT) nobody can be recorded
        given().when().delete("/dtp/flows/trade-flow/activations/lanes/RETAIL").then().statusCode(401);
    }

    @Test
    @Order(13)
    void anOperatorCancelsAnEndOfDayStartedByMistake() {
        LocalDate date = LocalDate.of(2026, 11, 19);
        String eod = "/eod/" + date;
        given().contentType(ContentType.JSON).body(new ImportParams(date, 4))
               .when().post("/trades/imports").then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle()
                && types("T-2026-11-19-4").contains("ROUTED_BACK"));
        given().header("X-User", "otto").when().post(eod).then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> "FAILED".equals(given().when().get(eod)
                .then().extract().path("runs.find { it.job == 'pnl-report' }.outcome")));

        // the P&L is corrected and run again: the sign-off waits for product control
        given().contentType(ContentType.JSON).body(new ImportParams(date, 0))
               .when().post("/trades/imports").then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle()
                && types("T-2026-11-19-4").contains("COMPLETED"));
        String failed = given().when().get(eod).then().extract().path("runs.find { it.job == 'pnl-report' }.id");
        given().header("X-User", "otto").when().post("/dtp/runs/" + failed + "/reruns").then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> "WAITING_FOR_DECISION".equals(given().when().get(eod)
                .then().extract().path("runs.find { it.job == 'eod-sign-off' }.state")));
        String signOff = given().header("X-User", "pia").header("X-Groups", "product_control")
                                .when().get("/dtp/decisions").then().statusCode(200)
                                .extract().path("find { it.businessKey == 'end-of-day 2026-11-19' }.id");

        // then operations notice that the date was closed by the old system: the end of day is cancelled
        String id = given().when().get(eod).then().extract().path("id");
        String cancel = "/dtp/flow-instances/" + id + "/cancel";
        Map<String, String> reason = Map.of("reason", "2026-11-19 was closed by the old system");
        given().contentType(ContentType.JSON).body(reason).when().post(cancel).then().statusCode(401);
        given().header("X-User", "otto").contentType(ContentType.JSON).body(Map.of("reason", " "))
               .when().post(cancel).then().statusCode(400);
        given().header("X-User", "otto").contentType(ContentType.JSON).body(reason)
               .when().post(cancel).then().statusCode(200).body("runs", is(1)).body("withdrawn", is(1));

        given().when().get(eod).then().statusCode(200)
               .body("state", equalTo("CANCELLED"))
               .body("cancel.by", equalTo("otto"))
               .body("cancel.reason", equalTo("2026-11-19 was closed by the old system"))
               .body("runs.find { it.job == 'eod-sign-off' }.outcome", equalTo("CANCELLED"))
               .body("runs.job", not(hasItem("settlement-export")));
        given().header("X-User", "pia").header("X-Groups", "product_control").when().get("/dtp/decisions")
               .then().statusCode(200).body("businessKey", not(hasItem("end-of-day 2026-11-19")));
        vote(signOff, "pia", "product_control", "APPROVE").statusCode(409);
        await().atMost(Duration.ofSeconds(10)).until(() -> notifications.latest().stream().anyMatch(m ->
                "operations".equals(m.audience()) && "EOD 2026-11-19 cancelled".equals(m.subject())));
        assertThat(notifications.latest()).anySatisfy(m -> {
            assertThat(m.audience()).isEqualTo("product_control");
            assertThat(m.subject()).isEqualTo("EOD 2026-11-19 cancelled");
            assertThat(m.body()).isEqualTo("Cancelled by otto: 2026-11-19 was closed by the old system."
                    + " A pending sign-off isn't needed any more.");
        });
        given().header("X-User", "otto").when().post("/dtp/runs/" + failed + "/reruns").then().statusCode(409);
        given().header("X-User", "otto").contentType(ContentType.JSON).body(reason)
               .when().post(cancel).then().statusCode(409);
    }

    private boolean waitingFor(String custodian) {
        return queries.packages(50).stream().anyMatch(p -> custodian.equals(p.groupKey())
                && "settlement-sender".equals(p.job()) && "WAITING_FOR_DECISION".equals(p.state()));
    }

    private PackageView latestPackage(String custodian) {
        return queries.packages(50).stream().filter(p -> custodian.equals(p.groupKey())
                && "settlement-sender".equals(p.job())).findFirst().orElseThrow();
    }

    private String firstItemOf(PackageView pkg) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT business_key FROM dtp_item WHERE work_package_id = '" + pkg.id()
                    + "' ORDER BY business_key LIMIT 1");
            assertThat(rs.next()).isTrue();
            return rs.getString(1);
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private long reportedTrades(long sequence) {
        List<String> files = given().when().get("/reporting/files").then().statusCode(200).extract().path(".");
        return files.stream().filter(f -> f.startsWith("trade-report-" + sequence + "-"))
                    .mapToLong(f -> Long.parseLong(f.replaceAll(".*\\((\\d+) trades\\)", "$1"))).sum();
    }

    private Trade trade(String number) {
        return em.createQuery("SELECT t FROM Trade t WHERE t.tradeNumber = :n", Trade.class)
                 .setParameter("n", number).getSingleResult();
    }

    private static ValidatableResponse vote(String id, String user, String groups, String vote) {
        return given().header("X-User", user).header("X-Groups", groups).contentType(ContentType.JSON)
                      .body(Map.of("vote", vote, "comment", "checked"))
                      .when().post("/dtp/decisions/" + id + "/votes").then();
    }

    private int count(String state) {
        return queries.itemCounts().stream().filter(c -> state.equals(c.state())).mapToInt(DtpQueries.ItemCount::count)
                      .sum();
    }

    private List<String> types(String tradeNumber) {
        return queries.journey(Trade.ITEM.name(), tradeNumber).stream().map(JourneyEntry::type).toList();
    }
}
