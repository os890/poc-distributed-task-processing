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

import java.util.Optional;

/**
 * Everything a handler needs during a run. Only valid inside {@link JobHandler#execute(JobContext)}.
 *
 * <p>Slice 1 of the PoC: work packages and collection windows are not implemented yet.
 *
 * @param <P> type of the run parameters
 */
public interface JobContext<P> {

    /** Typed run parameters (deserialized from the trigger or the definition's defaults). */
    P parameters();

    /** Custom, typed information about this run (visible in the run history). */
    RunInfo runInfo();

    /** {@code false} once a soft stop was requested. Inbox iteration checks this automatically. */
    boolean shouldContinue();

    /**
     * The flow instance of this run, e.g. to read the business date of an end of day (empty outside an instance).
     *
     * @return the flow instance
     */
    Optional<FlowInstance> flowInstance();

    /** Tracks new items within this run. */
    Items items();

    /**
     * The inbox of this job for the given item type (only the owning run reads it).
     *
     * @param type the item type
     * @param <E>  the tracked entity type
     * @param <ID> its id type
     * @return the inbox
     */
    <E, ID> Inbox<E, ID> inbox(ItemType<E, ID> type);
}
