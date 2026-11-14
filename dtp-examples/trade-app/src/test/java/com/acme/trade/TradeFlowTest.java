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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;

/** The trade flow on Quarkus: JPA changes and item routing commit together in JTA transactions. */
@QuarkusTest
class TradeFlowTest {

    private static final LocalDate DATE = LocalDate.of(2026, 11, 13);

    @Inject
    DtpQueries queries;

    @Test
    void tradesFlowThroughImportValidationAndEnrichment() {
        given().contentType(ContentType.JSON).body(new ImportParams(DATE, 8))
               .when().post("/trades/imports")
               .then().statusCode(200).body("newlyCreated", equalTo(true));

        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle() && count("COMPLETED") == 6);
        assertThat(types("T-2026-11-13-1")).containsExactly("TRACKED", "ROUTED", "ROUTED", "COMPLETED");
        assertThat(types("T-2026-11-13-4")).containsExactly("TRACKED", "ROUTED", "ROUTED_BACK");

        // the next import without new trades re-imports the routed-back ones
        given().contentType(ContentType.JSON).body(new ImportParams(DATE, 0))
               .when().post("/trades/imports")
               .then().statusCode(200);
        await().atMost(Duration.ofSeconds(30)).until(() -> queries.isIdle() && count("COMPLETED") == 8);

        given().when().get("/trades/T-2026-11-13-4/journey")
               .then().statusCode(200)
               .body("type", equalTo(List.of("TRACKED", "ROUTED", "ROUTED_BACK", "ROUTED", "ROUTED", "COMPLETED")))
               .body("reason", hasItem("missing ISIN"));
        given().when().get("/trades/unknown/journey").then().statusCode(404);
        given().when().get("/dtp/runs").then().statusCode(200).body("job", hasItem("trade-import"));
        given().when().get("/dtp/items").then().statusCode(200).body("state", hasItem("COMPLETED"));
        given().when().get("/dtp/nodes").then().statusCode(200).body("leader", equalTo(List.of(true)));
        given().when().get("/q/health/ready").then().statusCode(200).body("status", equalTo("UP"));
    }

    @Test
    void flowsAreVisibleAndCanBeOverruled() throws Exception {
        String version = given().when().get("/dtp/flows")
                                .then().statusCode(200)
                                .body("name", equalTo(List.of("trade-flow")))
                                .extract().path("[0].activeVersion");
        given().when().get("/dtp/flows/trade-flow/diagram")
               .then().statusCode(200)
               .body(containsString("-.->|MISSING_ISIN|"), containsString("-->|ENRICHED| done"));
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

    private int count(String state) {
        return queries.itemCounts().stream().filter(c -> state.equals(c.state())).mapToInt(DtpQueries.ItemCount::count)
                      .sum();
    }

    private List<String> types(String tradeNumber) {
        return queries.journey(Trade.ITEM.name(), tradeNumber).stream().map(JourneyEntry::type).toList();
    }
}
