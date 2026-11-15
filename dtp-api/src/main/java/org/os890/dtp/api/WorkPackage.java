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
package org.os890.dtp.api;

import java.util.List;
import java.util.UUID;

/**
 * A work package handed over by {@link WorkPackages#forEachPending}: processed as a whole in one fenced unit of work.
 *
 * @param <E>  the entity type of the items
 * @param <ID> its id type
 */
public interface WorkPackage<E, ID> {

    /** The package id. */
    UUID id();

    /** Increasing number of the package (the order of processing). */
    long sequence();

    /** The group key, e.g. the counterparty. */
    String groupKey();

    /** Failed attempts so far. */
    int attempts();

    /**
     * The items of the package. The handler may route single items (e.g. {@link TrackedItem#signal}); items it
     * doesn't route are completed by {@link #done()}.
     *
     * @return the items
     */
    List<TrackedItem<E, ID>> items();

    /** Custom, typed information about the package, e.g. the name of the file it was sent in. */
    RunInfo info();

    /** The package was processed: it is done, its items that weren't routed are completed. */
    void done();

    /**
     * The package was handed over to another system, e.g. a file sent to a custodian. If the job requires an
     * acknowledgement ({@code ack_required}), the package waits for it ({@code SENT}) with its items: the receiver's
     * acknowledgement completes them, a rejection or no acknowledgement within {@code ack_timeout} is a failed attempt
     * (retry policy, then a decision), and the package is sent again. Don't route the items before. Without
     * acknowledgement, the package is done, like {@link #done()}.
     *
     * @param reference the reference the receiver answers with, e.g. the file name or a message id
     */
    void sent(String reference);

    /**
     * A business failure of the package as a whole, e.g. the receiver rejected it: retried with the job's retry
     * policy, then a decision ({@code RETRY} or {@code DISCARD}). Its items stay in the package.
     *
     * @param reason the reason
     */
    void fail(String reason);
}
