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
package org.os890.dtp.cdi;

import javax.sql.DataSource;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.event.Startup;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionManager;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.os890.dtp.api.Items;
import org.os890.dtp.api.JobEngine;
import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.core.DtpEngine;
import org.os890.dtp.core.jdbc.JdbcTransactions;
import org.os890.dtp.core.query.DtpQueries;
import org.os890.dtp.core.spi.Transactions;

/**
 * Starts the dtp engine with the CDI container ({@code dtp.enabled}, default {@code true}): every deployment that
 * contains the library joins the cluster. Uses the default datasource and JTA if a {@link TransactionManager} is
 * available, otherwise plain JDBC transactions.
 */
@ApplicationScoped
public class DtpBootstrap {

    @Inject
    @Any
    Instance<ProcessStep> steps;

    @Inject
    Instance<DataSource> dataSources;

    @Inject
    Instance<TransactionManager> transactionManagers;

    private DtpEngine engine;

    void onStartup(@Observes Startup event) {
        Config config = ConfigProvider.getConfig();
        if (config.getOptionalValue("dtp.enabled", Boolean.class).orElse(true)) {
            engine();
        }
    }

    void onShutdown(@Observes Shutdown event) {
        stop();
    }

    /**
     * The engine of this node, started on first access.
     *
     * @return the engine
     */
    public synchronized DtpEngine engine() {
        if (engine == null) {
            DataSource dataSource = dataSources.get();
            Transactions tx = transactionManagers.isResolvable()
                    ? new JtaTransactions(transactionManagers.get(), dataSource)
                    : new JdbcTransactions(dataSource);
            engine = DtpEngine.builder()
                    .dataSource(dataSource)
                    .transactions(tx)
                    .steps(new CdiStepRegistry(steps))
                    .config(DtpSettings.read(ConfigProvider.getConfig()))
                    .build()
                    .start();
        }
        return engine;
    }

    @PreDestroy
    synchronized void stop() {
        if (engine != null) {
            engine.stop();
        }
    }

    @Produces
    @ApplicationScoped
    JobEngine jobEngine() {
        return engine().jobEngine();
    }

    @Produces
    @ApplicationScoped
    Items items() {
        return engine().items();
    }

    @Produces
    DtpQueries queries() {
        return engine().queries();
    }
}
