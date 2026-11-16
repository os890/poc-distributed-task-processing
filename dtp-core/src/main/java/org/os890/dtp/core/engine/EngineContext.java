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
package org.os890.dtp.core.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.api.event.RunRef;
import org.os890.dtp.core.DtpConfig;
import org.os890.dtp.core.domain.FailureKind;
import org.os890.dtp.core.domain.ItemEventType;
import org.os890.dtp.core.jdbc.ColorStore;
import org.os890.dtp.core.jdbc.DbClock;
import org.os890.dtp.core.jdbc.FlowStore;
import org.os890.dtp.core.jdbc.ItemStore;
import org.os890.dtp.core.jdbc.ItemStore.ItemEvent;
import org.os890.dtp.core.jdbc.JobStore;
import org.os890.dtp.core.jdbc.LeaderStore;
import org.os890.dtp.core.jdbc.LeaseStore;
import org.os890.dtp.core.jdbc.NodeStore;
import org.os890.dtp.core.jdbc.RunStore;
import org.os890.dtp.core.jdbc.SqlDialect;
import org.os890.dtp.core.spi.DomainEventPublisher;
import org.os890.dtp.core.spi.StepRegistry;
import org.os890.dtp.core.spi.Telemetry;
import org.os890.dtp.core.spi.Transactions;

/**
 * The collaborators of one engine instance (one node), shared by its components.
 *
 * @param config   the settings
 * @param nodeId   the node id
 * @param tx       the transactions
 * @param dialect  the SQL dialect
 * @param clock    the DB clock
 * @param jobs     the job store
 * @param leases   the lease store
 * @param runs     the run store
 * @param nodes    the node store
 * @param leaders  the leader store
 * @param items    the item store
 * @param flows    the flow store
 * @param catalog  the job catalog
 * @param json     the JSON codec
 * @param registry the process steps of this node
 * @param steps    the validated step infos by class
 * @param events    the publisher of the domain events
 * @param telemetry the telemetry
 * @param colors    the store of the active color
 * @param color     the color of this node and the active color as last read (red/green)
 */
public record EngineContext(DtpConfig config, UUID nodeId, Transactions tx, SqlDialect dialect, DbClock clock,
                            JobStore jobs, LeaseStore leases, RunStore runs, NodeStore nodes, LeaderStore leaders,
                            ItemStore items, FlowStore flows, JobCatalog catalog, JsonCodec json, StepRegistry registry,
                            Map<Class<? extends ProcessStep>, StepInfo> steps, DomainEventPublisher events,
                            Telemetry telemetry, ColorStore colors, NodeColor color) {

    /** The namespace. */
    public String namespace() {
        return config.namespace();
    }

    /**
     * Publishes a domain event after the commit of the current transaction (or right away without one).
     *
     * @param event the event
     */
    public void publishAfterCommit(Object event) {
        tx.afterCommit(() -> events.publish(event));
    }

    /**
     * The run a domain event is about, as stored.
     *
     * @param c     the connection
     * @param runId the run
     * @return the reference
     * @throws SQLException on DB errors
     */
    public RunRef runRef(Connection c, UUID runId) throws SQLException {
        RunStore.RefRow row = runs.ref(c, runId);
        return new RunRef(runId, catalog.byId(row.jobId()).name(), row.attempt(), row.node(), row.flowInstanceId());
    }

    /**
     * Writes a journal entry that is the result of processing an item in a step, and reports the result to the
     * telemetry after the commit (an entry written outside of a run, e.g. by a decision, isn't a processing result).
     *
     * @param c     the connection
     * @param lane  the lane of the item, or {@code null}
     * @param event the entry
     * @param now   the DB time
     * @throws SQLException on DB errors
     */
    public void journal(Connection c, String lane, ItemEvent event, Instant now) throws SQLException {
        items.append(c, event, now);
        String result = result(event.type());
        if (event.runId() == null || event.fromJobId() == null || result == null) {
            return;
        }
        String step = catalog.byId(event.fromJobId()).name();
        String kind = event.failureKind() == null ? null : event.failureKind().name();
        tx.afterCommit(() -> {
            telemetry.itemProcessed(step, lane, result, kind);
            if (event.failureKind() == FailureKind.TECHNICAL && event.errorType() != null) {
                telemetry.technicalFailure(step, event.errorType());
            }
        });
    }

    private static String result(ItemEventType type) {
        return switch (type) {
            case ROUTED, BRANCH_FINISHED -> "routed";
            case ROUTED_BACK -> "routed_back";
            case FORKED -> "forked";
            case PACKAGED -> "packaged";
            case SENT -> "sent";
            case COMPLETED -> "completed";
            case DISCARDED -> "discarded";
            case DEFERRED -> "deferred";
            case ESCALATED -> "escalated";
            case FAILED -> "failed";
            default -> null;
        };
    }

    /** The DB time, read in the current transaction (or a short one). */
    public Instant now() {
        return tx.withConnection(clock::now);
    }

    /**
     * Step info of a handler class name.
     *
     * @param handlerClass the FQCN
     * @return the step info
     */
    public StepInfo step(String handlerClass) {
        return steps.entrySet().stream()
                    .filter(e -> e.getKey().getName().equals(handlerClass))
                    .map(Map.Entry::getValue)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("no step " + handlerClass + " on this node"));
    }
}
