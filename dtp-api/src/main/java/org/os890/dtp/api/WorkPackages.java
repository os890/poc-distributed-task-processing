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

/**
 * The work packages of the current job for one item type (docs/design/04-lifecycles.md §4.4): items grouped into
 * packages, e.g. one file per counterparty, which are processed as a whole. A package is created by a run and stays
 * {@code PENDING} until a run of the job it was created for processed it: packages left at a slot end, an item limit
 * or a crash are carried over to the next run of that job (D33).
 *
 * <p>A collector can hand its packages over to other jobs, e.g. one job decides the grouping and another one sends
 * the files:
 *
 * <pre>{@code
 * ctx.packages(INSTRUCTION).forStep(SettlementSender.class)
 *     .createFromInbox(item -> custodianOf(item), 500);
 * }</pre>
 *
 * @param <E>  the entity type of the items
 * @param <ID> its id type
 */
public interface WorkPackages<E, ID> extends PackageCollector<E, ID> {

    /**
     * Processes the pending, due packages of this job, oldest first, also those created by earlier runs or by other
     * jobs for this job. Each package is one fenced unit of work, which ends with exactly one of
     * {@link WorkPackage#done()} or {@link WorkPackage#fail(String)}; an exception is a technical failure of the
     * package. Stops softly when the run is asked to stop or reached its item limit, the package in progress is
     * finished first.
     *
     * @param pkg the processing of one package
     * @return the number of packages handed over
     */
    int forEachPending(Consumer<WorkPackage<E, ID>> pkg);

    /**
     * Creates packages for another job, which processes them: its runs, retry policy, decisions, slots and limits
     * apply. The items move to that job with their package.
     *
     * @param step the job handler that processes the packages
     * @return the collector of packages for that job
     */
    PackageCollector<E, ID> forStep(Class<? extends JobHandler<?, ?>> step);

    /**
     * Creates the packages of each group for the job the group belongs to, e.g. domestic and international
     * custodians are served by different senders.
     *
     * @param stepOfGroup the job handler for a group key (the key may be {@code null})
     * @return the collector of packages for those jobs
     */
    PackageCollector<E, ID> forSteps(Function<String, Class<? extends JobHandler<?, ?>>> stepOfGroup);
}
