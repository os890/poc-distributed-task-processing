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
package org.os890.dtp.core.spi;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Transaction port of the engine. The default implementation manages plain JDBC transactions, a CDI integration
 * can use JTA, so that the business changes of a process step (e.g. via JPA) and the item routing commit together.
 */
public interface Transactions {

    /**
     * Runs the work in the current transaction, or in a new one if none is active.
     *
     * @param work the work
     * @param <T>  result type
     * @return the result of the work
     */
    <T> T required(Work<T> work);

    /**
     * Runs the work in a new transaction. An active transaction is suspended meanwhile.
     *
     * @param work the work
     * @param <T>  result type
     * @return the result of the work
     */
    <T> T requiresNew(Work<T> work);

    /**
     * Runs JDBC work with the connection of the current transaction (or an auto-commit connection if none is active).
     *
     * @param work the JDBC work
     * @param <T>  result type
     * @return the result of the work
     */
    <T> T withConnection(ConnectionWork<T> work);

    /**
     * Runs the action after the commit of the current transaction, or right away if none is active. Nothing runs if
     * the transaction rolls back. Used for the domain events.
     *
     * @param action the action, without a transaction when it runs
     */
    void afterCommit(Runnable action);

    /**
     * Work within a transaction.
     *
     * @param <T> result type
     */
    @FunctionalInterface
    interface Work<T> {
        T run() throws Exception;
    }

    /**
     * JDBC work.
     *
     * @param <T> result type
     */
    @FunctionalInterface
    interface ConnectionWork<T> {
        T run(Connection connection) throws SQLException;
    }
}
