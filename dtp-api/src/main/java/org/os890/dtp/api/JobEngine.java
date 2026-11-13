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

/** Injectable entry point for external triggers from application code. */
public interface JobEngine {

    /**
     * Triggers a run of the job with the given typed parameters. The compiler checks that the parameters match
     * the handler's {@code P}. Joins the caller's transaction, the run starts after commit.
     */
    <P> RunHandle trigger(Class<? extends JobHandler<P, ?>> job, P parameters);

    /** Same as {@link #trigger(Class, Object)} with an idempotency key, e.g. a delivery note number. */
    <P> RunHandle trigger(Class<? extends JobHandler<P, ?>> job, P parameters, String idempotencyKey);
}
