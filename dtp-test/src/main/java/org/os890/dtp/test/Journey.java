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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.os890.dtp.core.query.DtpQueries.ItemView;
import org.os890.dtp.core.query.DtpQueries.JourneyEntry;

/**
 * A snapshot of the journey of an item: its journal entries and where it is now. The steps are the jobs of the
 * cluster; {@link #jobName(Class)} names the job of a process step class.
 */
public class Journey {

    private final String itemType;
    private final String businessKey;
    private final ItemView item;
    private final List<JourneyEntry> entries;
    private final Map<String, String> jobsByHandler;
    private final Map<UUID, String> jobsByRun;

    Journey(String itemType, String businessKey, ItemView item, List<JourneyEntry> entries,
            Map<String, String> jobsByHandler, Map<UUID, String> jobsByRun) {
        this.itemType = itemType;
        this.businessKey = businessKey;
        this.item = item;
        this.entries = List.copyOf(entries);
        this.jobsByHandler = Map.copyOf(jobsByHandler);
        this.jobsByRun = Map.copyOf(jobsByRun);
    }

    /** The name of the item type. */
    public String itemType() {
        return itemType;
    }

    /** The business key. */
    public String businessKey() {
        return businessKey;
    }

    /** Where the item is now, or empty if there is no such item. */
    public Optional<ItemView> item() {
        return Optional.ofNullable(item);
    }

    /** The journal entries, in order. */
    public List<JourneyEntry> entries() {
        return entries;
    }

    /** The types of the journal entries, in order, e.g. {@code TRACKED, ROUTED, COMPLETED}. */
    public List<String> types() {
        return entries.stream().map(JourneyEntry::type).toList();
    }

    /**
     * The jobs the item passed, in order: the job whose run tracked it, then every job it was routed to (also
     * back). The branch steps of a fork follow in the order their branches finished, then the join step. Repeated
     * attempts in the same job (retries) count once.
     *
     * @return the job names
     */
    public List<String> path() {
        List<String> path = new ArrayList<>();
        for (JourneyEntry entry : entries) {
            // an entry without source job (e.g. the first routing of a tracked item) comes from the job of its run;
            // an item started outside of a run (a FlowEvent of the application) has neither
            add(path, entry.fromJob() != null ? entry.fromJob()
                    : entry.runId() != null ? jobsByRun.get(entry.runId()) : null);
            // a fork and a finished branch name the join step, which the item reaches only with the JOINED entry
            if (!"FORKED".equals(entry.type()) && !"BRANCH_FINISHED".equals(entry.type())) {
                add(path, entry.toJob());
            }
        }
        if (path.isEmpty() && item != null && item.job() != null) {
            path.add(item.job());
        }
        return List.copyOf(path);
    }

    private static void add(List<String> path, String job) {
        if (job != null && (path.isEmpty() || !path.getLast().equals(job))) {
            path.add(job);
        }
    }

    /**
     * The name of the job of a process step class in this cluster.
     *
     * @param step the process step class
     * @return the job name
     * @throws IllegalArgumentException if no job runs the class
     */
    public String jobName(Class<?> step) {
        String name = jobsByHandler.get(step.getName());
        if (name == null) {
            throw new IllegalArgumentException("no job runs " + step.getName());
        }
        return name;
    }

    /**
     * The journey as text, for assertion messages.
     *
     * @return the text
     */
    public String describe() {
        StringBuilder text = new StringBuilder("journey of ").append(itemType).append(' ').append(businessKey);
        if (item == null) {
            return text.append(": no such item").toString();
        }
        text.append(" (").append(item.state());
        if (item.job() != null) {
            text.append(" in ").append(item.job());
        }
        text.append(')');
        for (JourneyEntry e : entries) {
            text.append("\n  ").append(e.no()).append(' ').append(e.type());
            if (e.fromJob() != null || e.toJob() != null) {
                text.append(' ').append(Objects.requireNonNullElse(e.fromJob(), "-")).append(" -> ")
                    .append(Objects.requireNonNullElse(e.toJob(), "-"));
            }
            if (e.reason() != null) {
                text.append(": ").append(e.reason());
            }
            if (e.node() != null) {
                text.append(" [").append(e.node()).append(']');
            }
        }
        return text.toString();
    }

    @Override
    public String toString() {
        return describe();
    }
}
