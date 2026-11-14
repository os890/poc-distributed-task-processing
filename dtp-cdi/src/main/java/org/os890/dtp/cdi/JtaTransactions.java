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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import javax.sql.DataSource;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.os890.dtp.core.DtpException;
import org.os890.dtp.core.spi.Transactions;

/**
 * JTA transactions: the engine's statements and the application's changes (e.g. via JPA on the same datasource)
 * commit together. Connections obtained within a transaction are enlisted by the datasource (e.g. Agroal).
 *
 * <p>The fence check of a unit of work locks the lease row until the commit, so statements flushed during the
 * commit (e.g. by JPA) are covered as well.
 *
 * <p>Actions after the commit (domain events) of transactions begun here run after {@code commit()} returned, so they
 * run without a transaction. For a transaction of the application they run in {@code afterCompletion}.
 */
public class JtaTransactions implements Transactions {

    private static final System.Logger LOG = System.getLogger(JtaTransactions.class.getName());

    private final TransactionManager tm;
    private final DataSource dataSource;
    private final ThreadLocal<Deque<Begun>> begun = ThreadLocal.withInitial(ArrayDeque::new);

    private record Begun(Transaction transaction, List<Runnable> afterCommit) {
    }

    public JtaTransactions(TransactionManager tm, DataSource dataSource) {
        this.tm = tm;
        this.dataSource = dataSource;
    }

    @Override
    public <T> T required(Work<T> work) {
        if (isActive()) {
            return call(work);
        }
        return begin(work);
    }

    @Override
    public <T> T requiresNew(Work<T> work) {
        Transaction suspended = null;
        try {
            if (isActive()) {
                suspended = tm.suspend();
            }
            return begin(work);
        } catch (SystemException e) {
            throw new DtpException(e);
        } finally {
            if (suspended != null) {
                resume(suspended);
            }
        }
    }

    @Override
    public <T> T withConnection(ConnectionWork<T> work) {
        try (Connection c = dataSource.getConnection()) {
            return work.run(c);
        } catch (SQLException e) {
            throw new DtpException(e);
        }
    }

    @Override
    public void afterCommit(Runnable action) {
        if (!isActive()) {
            run(action);
            return;
        }
        try {
            Transaction current = tm.getTransaction();
            Begun top = begun.get().peek();
            if (top != null && top.transaction().equals(current)) {
                top.afterCommit().add(action);
                return;
            }
            current.registerSynchronization(new Synchronization() {
                @Override
                public void beforeCompletion() {
                    // nothing to do
                }

                @Override
                public void afterCompletion(int status) {
                    if (status == Status.STATUS_COMMITTED) {
                        run(action);
                    }
                }
            });
        } catch (RollbackException e) {
            LOG.log(System.Logger.Level.DEBUG, "the transaction rolls back, dropped an action after the commit", e);
        } catch (SystemException e) {
            throw new DtpException(e);
        }
    }

    private <T> T begin(Work<T> work) {
        Begun tx;
        try {
            tm.begin();
            tx = new Begun(tm.getTransaction(), new ArrayList<>());
        } catch (Exception e) {
            throw new DtpException("can't begin a transaction", e);
        }
        Deque<Begun> stack = begun.get();
        stack.push(tx);
        T result;
        try {
            try {
                result = call(work);
            } catch (RuntimeException | Error e) {
                rollback(e);
                throw e;
            }
            try {
                tm.commit();
            } catch (Exception e) {
                throw new DtpException("commit failed", e);
            }
        } finally {
            stack.pop();
        }
        tx.afterCommit().forEach(JtaTransactions::run);
        return result;
    }

    private static void run(Runnable action) {
        try {
            action.run();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "an action after the commit failed", e);
        }
    }

    private boolean isActive() {
        try {
            int status = tm.getStatus();
            return status == Status.STATUS_ACTIVE || status == Status.STATUS_MARKED_ROLLBACK;
        } catch (SystemException e) {
            throw new DtpException(e);
        }
    }

    private void rollback(Throwable cause) {
        try {
            tm.rollback();
        } catch (Exception e) {
            cause.addSuppressed(e);
        }
    }

    private void resume(Transaction suspended) {
        try {
            tm.resume(suspended);
        } catch (Exception e) {
            throw new DtpException("can't resume the suspended transaction", e);
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
}
