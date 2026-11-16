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
package org.os890.dtp.test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.os890.dtp.core.spi.Transactions;

/**
 * The transactions of a test cluster: every transaction (and every statement outside of one) holds a read lock, so
 * the test can stop the world for a moment ({@link #pause(Duration)}) to move the clock forward while no node is in
 * the middle of a transaction.
 */
class PausingTransactions implements Transactions {

    private final Transactions delegate;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    PausingTransactions(Transactions delegate) {
        this.delegate = delegate;
    }

    @Override
    public <T> T required(Work<T> work) {
        lock.readLock().lock();
        try {
            return delegate.required(work);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public <T> T requiresNew(Work<T> work) {
        lock.readLock().lock();
        try {
            return delegate.requiresNew(work);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public <T> T withConnection(ConnectionWork<T> work) {
        lock.readLock().lock();
        try {
            return delegate.withConnection(work);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void afterCommit(Runnable action) {
        delegate.afterCommit(action);
    }

    /**
     * Waits until no transaction is open and keeps new ones from starting until the returned pause is closed.
     *
     * @param timeout how long to wait for the open transactions
     * @return the pause
     */
    Pause pause(Duration timeout) {
        if (lock.getReadHoldCount() > 0) {
            throw new IllegalStateException("the clock can't be moved inside a transaction");
        }
        try {
            if (!lock.writeLock().tryLock(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("transactions still open after " + timeout
                        + ", e.g. a unit of work that waits for the test");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for the open transactions", e);
        }
        return () -> lock.writeLock().unlock();
    }

    /** A pause of all transactions. */
    @FunctionalInterface
    interface Pause extends AutoCloseable {
        @Override
        void close();
    }
}
