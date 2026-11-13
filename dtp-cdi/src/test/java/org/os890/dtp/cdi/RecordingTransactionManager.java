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

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.XAResource;

import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

/** Minimal transaction manager that records what happens, for unit tests of {@link JtaTransactions}. */
class RecordingTransactionManager implements TransactionManager {

    private final List<String> log = new ArrayList<>();
    private final ThreadLocal<SimpleTransaction> current = new ThreadLocal<>();
    private int counter;

    List<String> log() {
        return log;
    }

    @Override
    public void begin() {
        SimpleTransaction tx = new SimpleTransaction(++counter);
        current.set(tx);
        log.add("begin " + tx.id);
    }

    @Override
    public void commit() {
        SimpleTransaction tx = current.get();
        log.add("commit " + tx.id);
        current.remove();
    }

    @Override
    public void rollback() {
        SimpleTransaction tx = current.get();
        log.add("rollback " + tx.id);
        current.remove();
    }

    @Override
    public int getStatus() {
        SimpleTransaction tx = current.get();
        return tx == null ? Status.STATUS_NO_TRANSACTION : tx.getStatus();
    }

    @Override
    public Transaction getTransaction() {
        return current.get();
    }

    @Override
    public void resume(Transaction tx) {
        current.set((SimpleTransaction) tx);
        log.add("resume " + ((SimpleTransaction) tx).id);
    }

    @Override
    public void setRollbackOnly() {
        current.get().setRollbackOnly();
    }

    @Override
    public void setTransactionTimeout(int seconds) {
        // not needed
    }

    @Override
    public Transaction suspend() {
        SimpleTransaction tx = current.get();
        current.remove();
        log.add("suspend " + tx.id);
        return tx;
    }

    private static class SimpleTransaction implements Transaction {

        private final int id;
        private int status = Status.STATUS_ACTIVE;

        SimpleTransaction(int id) {
            this.id = id;
        }

        @Override
        public void commit() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean delistResource(XAResource resource, int flag) {
            return false;
        }

        @Override
        public boolean enlistResource(XAResource resource) {
            return false;
        }

        @Override
        public int getStatus() {
            return status;
        }

        @Override
        public void registerSynchronization(Synchronization sync) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rollback() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setRollbackOnly() {
            status = Status.STATUS_MARKED_ROLLBACK;
        }
    }
}
