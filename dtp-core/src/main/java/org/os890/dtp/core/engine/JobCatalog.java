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
import org.os890.dtp.core.domain.CalendarDefinition;
import org.os890.dtp.core.domain.JobDefinition;
import org.os890.dtp.core.jdbc.CalendarStore;
import org.os890.dtp.core.jdbc.JobStore;
import org.os890.dtp.core.spi.Transactions;

/**
 * Cached current job definitions and business calendars of the namespace. Definitions live in the DB (D1), so they
 * are reloaded periodically and on a miss.
 */
public class JobCatalog {

    private final Transactions tx;
    private final JobStore store;
    private final String namespace;
    private final long refreshNanos;

    private final CalendarStore calendarStore = new CalendarStore();

    private volatile Snapshot snapshot = new Snapshot(List.of(), List.of(), 0);

    public JobCatalog(Transactions tx, JobStore store, String namespace, long refreshNanos) {
        this.tx = tx;
        this.store = store;
        this.namespace = namespace;
        this.refreshNanos = refreshNanos;
    }

    private record Snapshot(List<JobDefinition> jobs, List<CalendarDefinition> calendars, long loadedAt) {

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

    /** All business calendars of the namespace. */
    public List<CalendarDefinition> calendars() {
        return current().calendars();
    }

    /**
     * A business calendar by id.
     *
     * @param calendarId the calendar
     * @return the calendar
     * @throws IllegalArgumentException if it doesn't exist
     */
    public CalendarDefinition calendar(UUID calendarId) {
        java.util.function.Predicate<CalendarDefinition> filter = calendar -> calendar.id().equals(calendarId);
        return current().calendars().stream().filter(filter).findFirst()
                        .or(() -> reload().calendars().stream().filter(filter).findFirst())
                        .orElseThrow(() -> new IllegalArgumentException("unknown business calendar " + calendarId));
    }

    /**
     * A business calendar by name.
     *
     * @param name the name
     * @return the calendar, if it exists
     */
    public Optional<CalendarDefinition> calendar(String name) {
        return current().calendars().stream().filter(calendar -> calendar.name().equals(name)).findFirst();
    }

    /** Forces a reload on the next access. */
    public void invalidate() {
        snapshot = new Snapshot(snapshot.jobs(), snapshot.calendars(), 0);
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
        Snapshot s = tx.withConnection(c -> new Snapshot(store.loadCurrent(c, namespace),
                calendarStore.loadAll(c, namespace), System.nanoTime()));
        snapshot = s;
        return s;
    }
}
