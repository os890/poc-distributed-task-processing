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
package org.os890.dtp.core.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;

import javax.sql.DataSource;

import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.spi.Transactions;

/** Plain JDBC transactions: one connection per transaction, bound to the current thread. */
public class JdbcTransactions implements Transactions {

    private final DataSource dataSource;

    private final ThreadLocal<Deque<Connection>> connections = ThreadLocal.withInitial(ArrayDeque::new);

    public JdbcTransactions(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public <T> T required(Work<T> work) {
        if (!connections.get().isEmpty()) {
            return call(work);
        }
        return begin(work);
    }

    @Override
    public <T> T requiresNew(Work<T> work) {
        return begin(work);
    }

    @Override
    public <T> T withConnection(ConnectionWork<T> work) {
        try {
            Connection current = connections.get().peek();
            if (current != null) {
                return work.run(current);
            }
            try (Connection c = dataSource.getConnection()) {
                return work.run(c);
            }
        } catch (SQLException e) {
            throw new DtpException(e);
        }
    }

    private <T> T begin(Work<T> work) {
        Connection c;
        try {
            c = dataSource.getConnection();
            c.setAutoCommit(false);
        } catch (SQLException e) {
            throw new DtpException(e);
        }
        Deque<Connection> stack = connections.get();
        stack.push(c);
        try {
            T result = call(work);
            c.commit();
            return result;
        } catch (SQLException e) {
            rollback(c, e);
            throw new DtpException(e);
        } catch (RuntimeException | Error e) {
            rollback(c, e);
            throw e;
        } finally {
            stack.pop();
            close(c);
        }
    }

    private static <T> T call(Work<T> work) {
        try {
            return work.run();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new DtpException(e);
        }
    }

    private static void rollback(Connection c, Throwable cause) {
        try {
            c.rollback();
        } catch (SQLException e) {
            cause.addSuppressed(e);
        }
    }

    private static void close(Connection c) {
        try {
            c.setAutoCommit(true);
            c.close();
        } catch (SQLException e) {
            // the connection is broken anyway, the pool discards it
            System.getLogger(JdbcTransactions.class.getName())
                    .log(System.Logger.Level.DEBUG, "closing a connection failed", e);
        }
    }
}
