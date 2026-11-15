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
package org.os890.dtp.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

import org.junit.jupiter.api.Test;
import org.os890.dtp.api.DecisionOption;
import org.os890.dtp.api.DefaultOutcome;
import org.os890.dtp.api.FlowBuilder;
import org.os890.dtp.api.FlowDefinition;
import org.os890.dtp.api.FlowEvent;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemRef;
import org.os890.dtp.api.JobContext;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.JobResult;
import org.os890.dtp.api.NoParams;
import org.os890.dtp.api.RunHandle;
import org.os890.dtp.api.Signal;
import org.os890.dtp.api.TrackedItem;
import org.os890.dtp.core.DtpConfig;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.cluster.TestTrade;
import org.os890.dtp.core.engine.Acknowledgements.Outcome;
import org.os890.dtp.core.engine.Decisions.Vote;
import org.os890.dtp.core.engine.JobAdmin.JobView;
import org.os890.dtp.core.query.JourneyGraph;
import org.os890.dtp.core.testsupport.MapStepRegistry;
import org.os890.dtp.core.testsupport.TestCluster;

/** The REST resources on an engine with H2: access rules in both security modes and every operation. */
class RestResourcesTest {

    private static final SecurityContext VIEWER = Requests.user("vera", DtpAccess.VIEWER);
    private static final SecurityContext OPERATOR = Requests.user("otto", DtpAccess.OPERATOR);
    private static final SecurityContext ADMIN = Requests.user("ada", DtpAccess.ADMIN);
    private static final SecurityContext RITA = Requests.user("rita", "risk", DtpAccess.BUSINESS);
    private static final SecurityContext ROB = Requests.user("rob", "risk", DtpAccess.BUSINESS);
    private static final SecurityContext BOB = Requests.user("bob", DtpAccess.BUSINESS);
    private static final HttpHeaders NONE = Requests.none();

    record ExportParams(String businessDate) {
    }

    enum IntakeSignal implements Signal {
        ACCEPTED
    }

    enum ReviewSignal implements Signal {
        OK
    }

    enum ReviewOption implements DecisionOption {
        ALLOW
    }

    static class Intake implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            item.signal(IntakeSignal.ACCEPTED);
        }
    }

    /** Trades marked ASK go to the risk desk. */
    static class Review implements ItemProcessor<TestTrade, UUID> {

        @Override
        public void process(TrackedItem<TestTrade, UUID> item) {
            if (item.businessKey().contains("ASK") && item.decision(ReviewOption.class).isEmpty()) {
                item.requestDecision("unusual trade", ReviewOption.class, () -> "risk");
            } else {
                item.signal(ReviewSignal.OK);
            }
        }
    }

    static class ReviewFlow implements FlowDefinition {

        @Override
        public void define(FlowBuilder flow) {
            flow.entry(Intake.class).on(IntakeSignal.ACCEPTED).to(Review.class);
            flow.step(Review.class).on(ReviewSignal.OK).complete();
        }
    }

    /** Triggered with typed parameters. */
    static class Export implements JobHandler<ExportParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<ExportParams> ctx) {
            return JobResult.success();
        }
    }

    /** Packages its inbox into one file and waits for the acknowledgement. */
    static class Sender implements JobHandler<NoParams, DefaultOutcome> {

        @Override
        public JobResult<DefaultOutcome> execute(JobContext<NoParams> ctx) {
            ctx.packages(TestTrade.ITEM).createFromInbox(item -> "ALL", 10);
            ctx.packages(TestTrade.ITEM).forEachPending(pkg -> pkg.sent("file-" + pkg.sequence() + ".xml"));
            return JobResult.success();
        }
    }

    @Test
    void theResourcesServeTheEngineWithTheirRoles() {
        try (TestCluster cluster = new TestCluster()) {
            cluster.migrate();
            cluster.definitions().define(Export.class).external().save();
            cluster.definitions().define(Sender.class).inbox(1).acknowledged(null).save();
            DtpConfig config = TestCluster.config("a").monitoring(DtpConfig.Monitoring.builder()
                    .interval(Duration.ofMillis(200)).build()).build();
            DtpEngine node = cluster.start(config, new MapStepRegistry(new Intake(), new Review(), new Export(),
                    new Sender()).flows(new ReviewFlow()));
            cluster.tx().required(() -> {
                for (String key : List.of("T-1", "T-ASK")) {
                    node.fire(FlowEvent.start(ReviewFlow.class, ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), key));
                }
                node.items().track(ItemRef.of(TestTrade.ITEM, UUID.randomUUID()), "T-PKG").routeTo(Sender.class);
                return null;
            });
            cluster.awaitIdle(Duration.ofSeconds(20));
            DtpAccess jwt = access("jwt");

            // cluster: viewers read, nobody else
            ClusterResource clusterResource = new ClusterResource();
            clusterResource.access = jwt;
            clusterResource.queries = node.queries();
            clusterResource.instances = node.flowInstances();
            clusterResource.windows = node.windows();
            clusterResource.calendars = node.calendars();
            assertThat(status(() -> clusterResource.nodes(Requests.anonymous(), NONE))).isEqualTo(401);
            assertThat(status(() -> clusterResource.nodes(RITA, NONE))).isEqualTo(403);
            assertThat(clusterResource.nodes(VIEWER, NONE)).hasSize(1);
            assertThat(clusterResource.nodes(ADMIN, NONE)).hasSize(1);
            assertThat(clusterResource.items(VIEWER, NONE)).isNotEmpty();
            assertThat(clusterResource.flowInstances(10, VIEWER, NONE)).isEmpty();
            assertThat(clusterResource.windows(10, VIEWER, NONE)).isEmpty();
            assertThat(clusterResource.calendars(VIEWER, NONE)).isEmpty();
            assertThat(status(() -> clusterResource.startWindow("nightly", VIEWER, NONE))).isEqualTo(403);
            assertThat(status(() -> clusterResource.startWindow("nightly", OPERATOR, NONE))).isEqualTo(404);

            // jobs: definitions, enable and disable, trigger with JSON, cancel
            JobsResource jobsResource = new JobsResource();
            jobsResource.access = jwt;
            jobsResource.jobs = node.jobAdmin();
            jobsResource.queries = node.queries();
            jobsResource.instances = node.flowInstances();
            assertThat(jobsResource.jobs(VIEWER, NONE)).extracting(JobView::name).contains("Export", "Sender");
            assertThat(jobsResource.jobs(VIEWER, NONE)).filteredOn(j -> j.name().equals("Sender")).singleElement()
                    .satisfies(j -> {
                        assertThat(j.triggers()).containsExactly("inbox 1");
                        assertThat(j.ackRequired()).isTrue();
                    });
            assertThat(jobsResource.revisions("Export", VIEWER, NONE)).hasSize(1);
            assertThat(status(() -> jobsResource.revisions("nothing", VIEWER, NONE))).isEqualTo(404);
            assertThat(status(() -> jobsResource.disable("Export", OPERATOR, NONE))).isEqualTo(403);
            assertThat(jobsResource.disable("Export", ADMIN, NONE)).containsEntry("changed", true);
            assertThat(jobsResource.disable("Export", ADMIN, NONE)).containsEntry("changed", false);
            assertThat(jobsResource.enable("Export", ADMIN, NONE)).containsEntry("changed", true);
            assertThat(status(() -> jobsResource.trigger("Export", null, "{}", VIEWER, NONE))).isEqualTo(403);
            RunHandle run = jobsResource.trigger("Export", "key-1", "{\"businessDate\":\"2026-12-01\"}", OPERATOR,
                    NONE);
            assertThat(run.newlyCreated()).isTrue();
            assertThat(jobsResource.trigger("Export", "key-1", "{\"businessDate\":\"2026-12-01\"}", OPERATOR, NONE)
                    .newlyCreated()).isFalse();
            assertThat(status(() -> jobsResource.trigger("Export", null, "{\"businessDate\":[1]", OPERATOR, NONE)))
                    .isEqualTo(400);
            assertThat(status(() -> jobsResource.trigger("Sender", null, null, OPERATOR, NONE))).isEqualTo(409);
            UUID runId = UUID.fromString(run.runId());
            await().atMost(Duration.ofSeconds(10)).until(() -> "FINISHED".equals(jobsResource.run(runId, VIEWER,
                    NONE).state()));
            assertThat(jobsResource.run(runId, VIEWER, NONE).outcome()).isEqualTo("SUCCEEDED");
            assertThat(jobsResource.runs(5, VIEWER, NONE)).isNotEmpty();
            assertThat(status(() -> jobsResource.run(UUID.randomUUID(), VIEWER, NONE))).isEqualTo(404);
            assertThat(jobsResource.cancel(runId, OPERATOR, NONE)).containsEntry("cancelled", false);
            assertThat(status(() -> jobsResource.rerun(runId, OPERATOR, NONE))).isEqualTo(409);

            // decisions: only the members of the responsible team see and decide
            DecisionsResource decisionsResource = new DecisionsResource();
            decisionsResource.access = jwt;
            decisionsResource.decisions = node.decisions();
            assertThat(status(() -> decisionsResource.open(Requests.anonymous(), NONE))).isEqualTo(401);
            assertThat(decisionsResource.open(BOB, NONE)).isEmpty();
            UUID ask = decisionsResource.open(RITA, NONE).getFirst().id();
            assertThat(decisionsResource.get(ask, BOB, NONE).responsibleGroup()).isEqualTo("risk");
            assertThat(status(() -> decisionsResource.decide(ask, new DecisionsResource.DecisionRequest("ALLOW",
                    null), BOB, NONE))).isEqualTo(409);
            assertThat(status(() -> decisionsResource.vote(ask, new DecisionsResource.VoteRequest(Vote.APPROVE,
                    null), ROB, NONE))).isEqualTo(409);
            assertThat(decisionsResource.decide(ask, new DecisionsResource.DecisionRequest("ALLOW", "known client"),
                    RITA, NONE).getStatus()).isEqualTo(204);
            assertThat(status(() -> decisionsResource.get(UUID.randomUUID(), RITA, NONE))).isEqualTo(404);
            cluster.awaitIdle(Duration.ofSeconds(20));

            // packages: the receiver's answer by job and reference
            PackagesResource packagesResource = new PackagesResource();
            packagesResource.access = jwt;
            packagesResource.queries = node.queries();
            packagesResource.acknowledgements = node.acknowledgements();
            String file = packagesResource.packages(5, VIEWER, NONE).getFirst().reference();
            assertThat(status(() -> packagesResource.answer(new PackagesResource.Answer("Sender", file, true, null),
                    VIEWER, NONE))).isEqualTo(403);
            assertThat(status(() -> packagesResource.answer(new PackagesResource.Answer("Sender", "x.xml", true,
                    null), OPERATOR, NONE))).isEqualTo(404);
            assertThat(status(() -> packagesResource.answer(new PackagesResource.Answer(null, file, true, null),
                    OPERATOR, NONE))).isEqualTo(400);
            assertThat(status(() -> packagesResource.answer(new PackagesResource.Answer("Nope", file, true, null),
                    OPERATOR, NONE))).isEqualTo(404);
            assertThat(packagesResource.answer(new PackagesResource.Answer("Sender", file, true, null), OPERATOR,
                    NONE)).containsEntry("outcome", Outcome.ACKNOWLEDGED);
            assertThat(status(() -> packagesResource.answer(new PackagesResource.Answer("Sender", file, false,
                    "late"), OPERATOR, NONE))).isEqualTo(409);

            // flows: reading for viewers, activations for administrators
            FlowsResource flowsResource = new FlowsResource();
            flowsResource.access = jwt;
            flowsResource.queries = node.flowQueries();
            flowsResource.admin = node.flowAdmin();
            String version = flowsResource.flows(VIEWER, NONE).getFirst().activeVersion();
            assertThat(flowsResource.validation(VIEWER, NONE)).isNotNull();
            assertThat(flowsResource.export(VIEWER, NONE)).contains("review-flow");
            assertThat(flowsResource.diagram("review-flow", null, VIEWER, NONE)).startsWith("flowchart");
            assertThat(status(() -> flowsResource.diagram("nothing", null, VIEWER, NONE))).isEqualTo(404);
            assertThat(status(() -> flowsResource.activate("review-flow", new FlowsResource.Activation(version, null,
                    null, "canary", "VIP"), OPERATOR, NONE))).isEqualTo(403);
            assertThat(flowsResource.activate("review-flow", new FlowsResource.Activation(version, null, null,
                    "canary", "VIP"), ADMIN, NONE).getStatus()).isEqualTo(204);
            assertThat(flowsResource.activations("review-flow", VIEWER, NONE).getFirst().activatedBy())
                    .isEqualTo("ada");
            assertThat(flowsResource.flows(VIEWER, NONE).getFirst().laneVersions()).containsKey("VIP");
            assertThat(flowsResource.endLaneActivation("review-flow", "VIP", null, "done", ADMIN, NONE).getStatus())
                    .isEqualTo(204);
            assertThat(flowsResource.activate("review-flow", new FlowsResource.Activation(version, null, "ops",
                    "again", null), ADMIN, NONE).getStatus()).isEqualTo(204);
            assertThat(flowsResource.overrides("review-flow", VIEWER, NONE)).isEmpty();
            UUID override = flowsResource.override("review-flow", new FlowsResource.OverrideRequest("review", "OK",
                    org.os890.dtp.core.domain.RouteKind.DISCARD, null, null, null, null, "test"), ADMIN, NONE)
                    .get("id");
            assertThat(flowsResource.endOverride(override, ADMIN, NONE).getStatus()).isEqualTo(204);
            assertThat(status(() -> flowsResource.endOverride(override, ADMIN, NONE))).isEqualTo(404);
            assertThat(flowsResource.migrate("review-flow", new FlowsResource.MigrationRequest(version, version,
                    null), ADMIN, NONE).migrated()).isZero();

            // journeys: viewers and operators; operators see the nodes
            JourneysResource journeysResource = new JourneysResource();
            journeysResource.access = jwt;
            journeysResource.journeys = node.journeys();
            journeysResource.queries = node.queries();
            assertThat(journeysResource.journey("test-trade", "T-ASK", VIEWER, NONE)).isNotEmpty();
            assertThat(status(() -> journeysResource.journey("test-trade", "none", VIEWER, NONE))).isEqualTo(404);
            assertThat(status(() -> journeysResource.journey("test-trade", "T-ASK", BOB, NONE))).isEqualTo(403);
            String mermaid = (String) journeysResource.diagram("test-trade", "T-ASK", "mermaid", "full", "UTC",
                    VIEWER, NONE).getEntity();
            assertThat(mermaid).contains("{{\"Decision").doesNotContain("<br/>on a");
            assertThat((String) journeysResource.diagram("test-trade", "T-ASK", "mermaid", "full", null, OPERATOR,
                    NONE).getEntity()).contains("on a");
            JourneyGraph graph = (JourneyGraph) journeysResource.diagram("test-trade", "T-ASK", "json", "steps",
                    null, VIEWER, NONE).getEntity();
            assertThat(journeysResource.diagramById(graph.itemId(), "mermaid", "full", null, VIEWER, NONE)
                    .getStatus()).isEqualTo(200);
            assertThat(status(() -> journeysResource.diagram("test-trade", "T-ASK", "png", "full", null, VIEWER,
                    NONE))).isEqualTo(400);
            assertThat(status(() -> journeysResource.diagram("test-trade", "T-ASK", "mermaid", "all", null, VIEWER,
                    NONE))).isEqualTo(400);
            assertThat(status(() -> journeysResource.diagramById(UUID.randomUUID(), "mermaid", "full", null, VIEWER,
                    NONE))).isEqualTo(404);

            // monitoring: viewers; failures for operators
            MonitorResource monitorResource = new MonitorResource();
            monitorResource.access = jwt;
            monitorResource.monitoring = node.monitoring();
            monitorResource.feed = node.monitoringFeed();
            monitorResource.flows = node.flowQueries();
            await().atMost(Duration.ofSeconds(10)).until(() -> !monitorResource.heatmap("15m", VIEWER, NONE).steps()
                    .isEmpty());
            assertThat(monitorResource.diagram(null, "15m", VIEWER, NONE)).startsWith("flowchart");
            assertThat(monitorResource.series("review", "1h", VIEWER, NONE)).isNotNull();
            assertThat(status(() -> monitorResource.failures("review", "1h", VIEWER, NONE))).isEqualTo(403);
            assertThat(monitorResource.failures("review", "1h", OPERATOR, NONE)).isEmpty();
            assertThat(status(() -> monitorResource.heatmap("soon", VIEWER, NONE))).isEqualTo(400);
        }
    }

    @Test
    void withoutSecurityTheHeadersNameUserAndTeams() {
        DtpAccess off = access("off");
        HttpHeaders headers = Requests.headers(Map.of(DtpAccess.USER_HEADER, "otto", DtpAccess.GROUPS_HEADER,
                "risk, dtp-operator"));
        DtpAccess.Caller caller = off.caller(Requests.anonymous(), headers);
        assertThat(caller.require(DtpAccess.ADMIN)).isSameAs(caller);
        assertThat(caller.requireUser()).isEqualTo("otto");
        assertThat(caller.isInRole("risk")).isTrue();
        assertThat(caller.isInRole(DtpAccess.ADMIN)).isFalse();
        assertThat(caller.decider(List.of()).groups()).containsExactlyInAnyOrder("risk", DtpAccess.OPERATOR);
        // a principal wins over the header
        assertThat(off.caller(RITA, headers).requireUser()).isEqualTo("rita");
        assertThat(status(() -> off.caller(Requests.anonymous(), NONE).requireUser())).isEqualTo(401);
        // with JWT, the teams are checked one by one, and dtp-admin has every role
        DtpAccess.Caller admin = access("jwt").caller(ADMIN, headers);
        assertThat(admin.isInRole("risk")).isTrue();
        assertThat(access("jwt").caller(RITA, headers).decider(List.of("risk", "audit")).groups())
                .containsExactly("risk");
        assertThat(access(null).caller(Requests.anonymous(), headers).user()).isEmpty();
    }

    private static DtpAccess access(String mode) {
        DtpAccess access = new DtpAccess();
        access.mode = mode;
        return access;
    }

    private static int status(Runnable call) {
        try {
            call.run();
        } catch (WebApplicationException e) {
            return e.getResponse().getStatus();
        }
        return Response.Status.OK.getStatusCode();
    }

    private static int status(java.util.function.Supplier<?> call) {
        return status((Runnable) call::get);
    }
}
