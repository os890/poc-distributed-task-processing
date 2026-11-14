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

import org.os890.dtp.api.DecisionRequest;

/**
 * A request stayed open longer than the fallback delay of the requesting job: the fallback run was planned (D38). The
 * request stays open, nobody decides automatically.
 *
 * @param request     the open request
 * @param fallbackJob the job of the fallback run
 * @param runId       the fallback run
 */
public record DecisionFallbackDue(DecisionRequest request, String fallbackJob, UUID runId) {
}
