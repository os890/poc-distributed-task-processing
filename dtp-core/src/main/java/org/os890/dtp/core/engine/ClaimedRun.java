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

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * A run claimed by this node: it holds the job lease with the given fencing token.
 *
 * @param runId         the run
 * @param jobId         the job
 * @param revision      the job revision of the run
 * @param handlerClass  the handler class
 * @param token         the fencing token
 * @param attempt       the attempt number
 * @param leaseDuration the lease duration
 * @param slotEnd       end of the processing slot of the run, or {@code null}
 */
public record ClaimedRun(UUID runId, UUID jobId, int revision, String handlerClass, long token, int attempt,
                         Duration leaseDuration, Instant slotEnd) {
}
