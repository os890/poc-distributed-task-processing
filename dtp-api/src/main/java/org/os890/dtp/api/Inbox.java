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
import java.util.function.Consumer;

/**
 * Queued items of the current job. Each batch is one fenced unit of work: the handler's business changes and
 * the item routing commit together. Iteration stops softly when the run is asked to stop.
 */
public interface Inbox<E, ID> {

    void forEachBatch(int batchSize, Consumer<List<TrackedItem<E, ID>>> batch);

    /**
     * Same, but up to {@code parallelism} batches run at the same time on virtual threads of this node,
     * each in its own fenced transaction. For CPU- or IO-heavy steps within one job.
     */
    void forEachBatchInParallel(int batchSize, int parallelism, Consumer<List<TrackedItem<E, ID>>> batch);

    /** A view of this inbox that also hands over deferred items that are not yet due. */
    Inbox<E, ID> includingDeferred();

    default void forEach(Consumer<TrackedItem<E, ID>> item) {
        forEachBatch(100, batch -> batch.forEach(item));
    }
}
