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
package org.os890.dtp.api.event;

/**
 * The receiver rejected a sent work package, or didn't acknowledge it in time: a failed attempt. The package is sent
 * again after the backoff, or waits for a decision once its retries are exhausted.
 *
 * @param pkg              the package
 * @param reason           the reason
 * @param timeout          {@code true} if no acknowledgement came in time (a technical failure)
 * @param retriesExhausted {@code true} if it now waits for a decision ({@link DecisionRequested})
 */
public record PackageRejected(PackageRef pkg, String reason, boolean timeout, boolean retriesExhausted) {
}
