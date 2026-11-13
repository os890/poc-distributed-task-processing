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

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.core.DtpConfig;
import org.os890.dtp.core.jdbc.DbClock;
import org.os890.dtp.core.jdbc.ItemStore;
import org.os890.dtp.core.jdbc.JobStore;
import org.os890.dtp.core.jdbc.LeaderStore;
import org.os890.dtp.core.jdbc.LeaseStore;
import org.os890.dtp.core.jdbc.NodeStore;
import org.os890.dtp.core.jdbc.RunStore;
import org.os890.dtp.core.jdbc.SqlDialect;
import org.os890.dtp.core.spi.StepRegistry;
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
 * @param catalog  the job catalog
 * @param json     the JSON codec
 * @param registry the process steps of this node
 * @param steps    the validated step infos by class
 */
public record EngineContext(DtpConfig config, UUID nodeId, Transactions tx, SqlDialect dialect, DbClock clock,
                            JobStore jobs, LeaseStore leases, RunStore runs, NodeStore nodes, LeaderStore leaders,
                            ItemStore items, JobCatalog catalog, JsonCodec json, StepRegistry registry,
                            Map<Class<? extends ProcessStep>, StepInfo> steps) {

    /** The namespace. */
    public String namespace() {
        return config.namespace();
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
