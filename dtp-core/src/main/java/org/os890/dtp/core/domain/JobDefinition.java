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
package org.os890.dtp.core.domain;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * A job with one revision of its definition (rows of {@code dtp_job} and {@code dtp_job_revision}).
 *
 * @param id                identity of the job
 * @param namespace         namespace of the application cluster
 * @param name              unique name within the namespace
 * @param revision          the revision this definition describes
 * @param enabled           disabled jobs aren't planned
 * @param handlerClass      FQCN of the process step bean
 * @param asap              continuous trigger: a new run as soon as the previous one finished
 * @param cronExpression    Quartz-style cron expression, or {@code null}
 * @param cronZone          zone of the cron expression
 * @param external          can be triggered by application code
 * @param inboxMinItems     inbox trigger: a run as soon as this many items are due, or {@code null}
 * @param concurrency       concurrency policy
 * @param takeover          takeover policy
 * @param retryMax          retries per item
 * @param retryBackoff      delay before a retry (multiplied by the attempt)
 * @param maxItemVisits     loop limit per item for this job's inbox, or {@code null}
 * @param leaseDuration     lease duration of the job's runs, or {@code null} for the engine default
 * @param defaultParameters default run parameters as JSON, or {@code null}
 * @param fallbackJobId     job of the fallback run of decision requests that stay open, or {@code null} (D38)
 * @param fallbackDelay     how long a request stays open before its fallback run, or {@code null}
 * @param transitions       successors of the job
 */
public record JobDefinition(UUID id, String namespace, String name, int revision, boolean enabled,
                            String handlerClass, boolean asap, String cronExpression, String cronZone,
                            boolean external, Integer inboxMinItems, ConcurrencyPolicy concurrency,
                            TakeoverPolicy takeover, int retryMax, Duration retryBackoff, Integer maxItemVisits,
                            Duration leaseDuration, String defaultParameters, UUID fallbackJobId,
                            Duration fallbackDelay, List<TransitionDefinition> transitions) {

    public JobDefinition {
        transitions = List.copyOf(transitions);
        if ((fallbackJobId == null) != (fallbackDelay == null)) {
            throw new IllegalArgumentException("job " + name + ": a decision fallback needs a job and a delay");
        }
    }

    /**
     * Whether the decision requests of this job get a fallback run.
     *
     * @return {@code true} with a fallback job
     */
    public boolean hasDecisionFallback() {
        return fallbackJobId != null;
    }

    /**
     * Same definition with the given transitions.
     *
     * @param newTransitions the transitions
     * @return a copy
     */
    public JobDefinition withTransitions(List<TransitionDefinition> newTransitions) {
        return new JobDefinition(id, namespace, name, revision, enabled, handlerClass, asap, cronExpression,
                cronZone, external, inboxMinItems, concurrency, takeover, retryMax, retryBackoff, maxItemVisits,
                leaseDuration, defaultParameters, fallbackJobId, fallbackDelay, newTransitions);
    }
}
