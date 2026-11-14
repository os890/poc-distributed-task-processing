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

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Creates work packages from what is pending right now: the items of the job's inbox or a selection of references.
 * The packages are processed by the job they were created for: the current job ({@link WorkPackages}) or another job
 * with a {@link JobHandler} ({@link WorkPackages#forStep(Class)}), which processes them with
 * {@link WorkPackages#forEachPending(java.util.function.Consumer)} and its own retry policy.
 *
 * @param <E>  the entity type of the items
 * @param <ID> its id type
 */
public interface PackageCollector<E, ID> {

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
     * Tracks new items directly in new packages, e.g. the changes since the last export. A reference that is tracked
     * already (with this item type) is skipped, so running the same selection twice packages each item once.
     *
     * @param refs     the references
     * @param groupKey the group of a reference
     * @param maxSize  maximum number of items per package
     * @return the number of new packages
     */
    int createFrom(Stream<ItemRef<E, ID>> refs, Function<ItemRef<E, ID>, String> groupKey, int maxSize);
}
