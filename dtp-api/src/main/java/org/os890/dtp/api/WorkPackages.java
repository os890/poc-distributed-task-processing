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

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * The work packages of the current job for one item type (docs/design/04-lifecycles.md §4.4): items grouped into
 * packages, e.g. one file per counterparty, which are processed as a whole. A package is created by a run and stays
 * {@code PENDING} until a run processed it: packages left at a slot end, an item limit or a crash are carried over
 * to the next run of the job (D33).
 *
 * @param <E>  the entity type of the items
 * @param <ID> its id type
 */
public interface WorkPackages<E, ID> {

    /**
     * Moves the due items of this job's inbox into new packages. Items with the same group key share packages of at
     * most {@code maxSize} items. Each chunk of the inbox is its own fenced unit of work.
     *
     * @param groupKey the group of an item, e.g. its counterparty
     * @param maxSize  maximum number of items per package
     * @return the number of new packages
     */
    int createFromInbox(Function<TrackedItem<E, ID>, String> groupKey, int maxSize);

    /**
     * Tracks new items for this job, directly in new packages, e.g. the changes since the last export. A reference
     * that is tracked already (with this item type) is skipped, so running the same selection twice packages each
     * item once.
     *
     * @param refs     the references
     * @param groupKey the group of a reference
     * @param maxSize  maximum number of items per package
     * @return the number of new packages
     */
    int createFrom(Stream<ItemRef<E, ID>> refs, Function<ItemRef<E, ID>, String> groupKey, int maxSize);

    /**
     * Processes the pending, due packages of this job, oldest first, also those created by earlier runs. Each package
     * is one fenced unit of work, which ends with exactly one of {@link WorkPackage#done()} or
     * {@link WorkPackage#fail(String)}; an exception is a technical failure of the package. Stops softly when the run
     * is asked to stop or reached its item limit, the package in progress is finished first.
     *
     * @param pkg the processing of one package
     * @return the number of packages handed over
     */
    int forEachPending(Consumer<WorkPackage<E, ID>> pkg);
}
