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

/**
 * A process step with full control over its run, e.g. for collectors, typed parameters or jobs without items.
 * For the common case (handling inbox items one by one) implement {@link ItemProcessor} instead.
 * Its job definition (trigger, transitions, limits, …) lives in the DB and references the handler class.
 *
 * @param <P> type of the run parameters (use {@link NoParams} if the job has none)
 * @param <R> type of the typed result status, referenced by transitions
 */
public interface JobHandler<P, R extends ResultStatus> extends ProcessStep {

    /** Executes one run, or one cycle of a continuous run. Called on exactly one node of the cluster. */
    JobResult<R> execute(JobContext<P> ctx);

    /** Optional callback when a soft stop is requested (cancel, slot end, window, drain, lease lost). */
    default void onStop(StopReason reason) {
    }
}
