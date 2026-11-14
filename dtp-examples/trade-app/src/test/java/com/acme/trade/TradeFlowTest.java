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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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

import io.quarkus.test.junit.QuarkusTest;
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
        await().atMost(Duration.ofSeconds(30)).until(() -> "COMPLETED".equals(
                given().when().get(eod).then().extract().path("state")));

        assertThat(notifications.latest()).anySatisfy(m -> {
            assertThat(m.audience()).isEqualTo("operations");
            assertThat(m.subject()).isEqualTo("EOD 2026-11-20 signed off");
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
        // export created the files and handed them over to the sender
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle());
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
