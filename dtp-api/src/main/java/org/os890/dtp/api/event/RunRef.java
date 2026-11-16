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

import java.util.UUID;

/**
 * The run an execution event is about.
 *
 * @param id             the run
 * @param job            the job name
 * @param attempt        the attempt (from 1), {@code 0} if it never started
 * @param node           the name of the node that runs or ran the attempt, or {@code null} if none did
 * @param flowInstanceId the flow instance the run belongs to, or {@code null}
 */
public record RunRef(UUID id, String job, int attempt, String node, UUID flowInstanceId) {
}
