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
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.smallrye.jwt.build.Jwt;

/**
 * The REST API of dtp secured with MP JWT (slice 16): roles from the groups of the token ({@code dtp-admin} implies
 * all), teams for decisions, technical details for operators, and the OpenAPI document.
 */
@QuarkusTest
@TestProfile(JwtProfile.class)
class DtpSecurityTest {

    private static final String DATE = "2026-12-07";
    private static final String LARGE = "T-" + DATE + "-10";

    @Test
    void theRestApiChecksTheRolesAndTeamsOfTheToken() {
        // without a token, with a token without role, with the role
        given().when().get("/dtp/runs").then().statusCode(401);
        as("rita", "risk").when().get("/dtp/runs").then().statusCode(403);
        as("vera", "dtp-viewer").when().get("/dtp/runs").then().statusCode(200);
        as("vera", "dtp-viewer").when().get("/dtp/jobs").then().statusCode(200)
                .body("name", hasItem("trade-import"));

        // only administrators change definitions
        as("otto", "dtp-operator").when().post("/dtp/jobs/trade-reporting/disable").then().statusCode(403);
        as("ada", "dtp-admin").when().post("/dtp/jobs/trade-reporting/disable").then().statusCode(200)
                .body("changed", is(true));
        as("ada", "dtp-admin").when().post("/dtp/jobs/trade-reporting/enable").then().statusCode(200)
                .body("changed", is(true));

        // operators trigger runs, the parameters are checked against the type of the handler
        as("vera", "dtp-viewer").contentType(ContentType.JSON).body(Map.of("businessDate", DATE, "trades", 10))
                .when().post("/dtp/jobs/trade-import/runs").then().statusCode(403);
        as("otto", "dtp-operator").contentType(ContentType.JSON).body(Map.of("businessDate", DATE, "trades", "ten"))
                .when().post("/dtp/jobs/trade-import/runs").then().statusCode(400);
        as("otto", "dtp-operator").contentType(ContentType.JSON).header("Idempotency-Key", "import-" + DATE)
                .body(Map.of("businessDate", DATE, "trades", 10))
                .when().post("/dtp/jobs/trade-import/runs").then().statusCode(200)
                .body("newlyCreated", is(true));
        await().atMost(Duration.ofSeconds(30)).until(() -> as("vera", "dtp-viewer")
                .when().get("/dtp/items/trade/" + LARGE + "/journey").then().extract().statusCode() == 200
                && as("vera", "dtp-viewer").when().get("/dtp/items/trade/" + LARGE + "/journey").then().extract()
                        .<List<String>>path("type").contains("ESCALATED"));

        // the risk desk sees the approval, others don't; members vote
        String id = as("rita", "risk", "dtp-business").when().get("/dtp/decisions").then().statusCode(200)
                .body("businessKey", hasItem(LARGE))
                .extract().path("find { it.businessKey == '" + LARGE + "' }.id");
        as("bob", "dtp-business").when().get("/dtp/decisions").then().statusCode(200)
                .body("businessKey", not(hasItem(LARGE)));
        as("bob", "dtp-business").contentType(ContentType.JSON).body(Map.of("vote", "APPROVE"))
                .when().post("/dtp/decisions/" + id + "/votes").then().statusCode(409);
        as("rita", "risk", "dtp-business").contentType(ContentType.JSON).body(Map.of("vote", "APPROVE"))
                .when().post("/dtp/decisions/" + id + "/votes").then().statusCode(200).body("decided", is(false));
        as("rob", "risk", "dtp-business").contentType(ContentType.JSON).body(Map.of("vote", "APPROVE"))
                .when().post("/dtp/decisions/" + id + "/votes").then().statusCode(200).body("decided", is(true));
        as("vera", "dtp-viewer").when().get("/dtp/decisions/" + id).then().statusCode(200)
                .body("decidedBy", equalTo("rob"));

        // journeys: operators see the nodes; failures only for operators
        String viewer = as("vera", "dtp-viewer").when().get("/dtp/items/trade/" + LARGE + "/journey/diagram")
                .then().statusCode(200).extract().asString();
        assertThat(viewer).contains("APPROVE by rita").doesNotContain("<br/>on ");
        as("otto", "dtp-operator").when().get("/dtp/items/trade/" + LARGE + "/journey/diagram").then()
                .statusCode(200).body(containsString("<br/>on "));
        as("rita", "risk").when().get("/dtp/items/trade/" + LARGE + "/journey").then().statusCode(403);
        as("vera", "dtp-viewer").when().get("/dtp/monitor/steps/trade-enrichment/failures").then().statusCode(403);
        as("otto", "dtp-operator").when().get("/dtp/monitor/steps/trade-enrichment/failures").then()
                .statusCode(200);
        as("ada", "dtp-admin").when().get("/dtp/monitor/steps/trade-enrichment/failures").then().statusCode(200);

        // only operators cancel flow instances (an unknown one: 404)
        String unknown = "/dtp/flow-instances/" + java.util.UUID.randomUUID() + "/cancel";
        as("vera", "dtp-viewer").contentType(ContentType.JSON).body(Map.of("reason", "wrong date"))
                .when().post(unknown).then().statusCode(403);
        as("otto", "dtp-operator").contentType(ContentType.JSON).body(Map.of("reason", "wrong date"))
                .when().post(unknown).then().statusCode(404);

        // the API is documented
        given().accept(ContentType.JSON).when().get("/q/openapi?format=json").then().statusCode(200)
                .body("paths", hasKey("/dtp/jobs/{name}/runs"))
                .body("paths", hasKey("/dtp/decisions/{id}/votes"))
                .body("tags.name", hasItem("dtp jobs and runs"));
    }

    private static RequestSpecification as(String user, String... groups) {
        String token = Jwt.upn(user).issuer(JwtProfile.ISSUER).groups(Set.of(groups)).sign();
        return given().auth().oauth2(token);
    }
}
