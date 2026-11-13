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
package org.os890.dtp.core.engine;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.os890.dtp.api.ProcessStep;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.jdbc.JobStore;
import org.os890.dtp.core.spi.Transactions;

/**
 * Cached current job definitions of the namespace. Definitions live in the DB (D1), so they are reloaded
 * periodically and on a miss.
 */
public class JobCatalog {

    private final Transactions tx;
    private final JobStore store;
    private final String namespace;
    private final long refreshNanos;

    private volatile Snapshot snapshot = new Snapshot(List.of(), 0);

    public JobCatalog(Transactions tx, JobStore store, String namespace, long refreshNanos) {
        this.tx = tx;
        this.store = store;
        this.namespace = namespace;
        this.refreshNanos = refreshNanos;
    }

    private record Snapshot(List<JobDefinition> jobs, long loadedAt) {

        Map<UUID, JobDefinition> byId() {
            return jobs.stream().collect(Collectors.toMap(JobDefinition::id, Function.identity()));
        }
    }

    /** All current job definitions of the namespace. */
    public List<JobDefinition> all() {
        return current().jobs();
    }

    /**
     * The job of a process step (one job per step in slice 1, DQ-5).
     *
     * @param step the step class
     * @return the job
     * @throws IllegalArgumentException if no job references the step
     */
    public JobDefinition byStep(Class<? extends ProcessStep> step) {
        return find(job -> job.handlerClass().equals(step.getName()))
                .orElseThrow(() -> new IllegalArgumentException("no job definition references " + step.getName()));
    }

    /**
     * A job by id.
     *
     * @param jobId the job
     * @return the job
     */
    public JobDefinition byId(UUID jobId) {
        return find(job -> job.id().equals(jobId))
                .orElseThrow(() -> new IllegalArgumentException("unknown job " + jobId));
    }

    /**
     * A specific revision of a job (runs keep the revision they were planned with, DD-10).
     *
     * @param jobId    the job
     * @param revision the revision
     * @return the definition
     */
    public JobDefinition revision(UUID jobId, int revision) {
        JobDefinition current = current().byId().get(jobId);
        if (current != null && current.revision() == revision) {
            return current;
        }
        return tx.withConnection(c -> store.load(c, jobId, revision))
                 .orElseThrow(() -> new IllegalArgumentException("unknown job " + jobId + " revision " + revision));
    }

    /** Forces a reload on the next access. */
    public void invalidate() {
        snapshot = new Snapshot(snapshot.jobs(), 0);
    }

    private Optional<JobDefinition> find(java.util.function.Predicate<JobDefinition> filter) {
        Optional<JobDefinition> found = current().jobs().stream().filter(filter).findFirst();
        if (found.isPresent()) {
            return found;
        }
        return reload().jobs().stream().filter(filter).findFirst();
    }

    private Snapshot current() {
        Snapshot s = snapshot;
        if (s.loadedAt() == 0 || System.nanoTime() - s.loadedAt() > refreshNanos) {
            return reload();
        }
        return s;
    }

    private Snapshot reload() {
        List<JobDefinition> jobs = tx.withConnection(c -> store.loadCurrent(c, namespace));
        Snapshot s = new Snapshot(jobs, System.nanoTime());
        snapshot = s;
        return s;
    }
}
