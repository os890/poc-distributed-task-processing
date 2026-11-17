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

import org.os890.dtp.api.DecisionRequest;

/**
 * An open request was withdrawn without a decision, because its subject ended otherwise: the run that asked was
 * cancelled, or its flow instance. Nobody needs to decide any more, e.g. a notification can be retracted.
 *
 * @param request the withdrawn request
 * @param by      who cancelled the subject, or {@code null}
 * @param reason  why it was withdrawn, e.g. {@code run cancelled}
 */
public record DecisionWithdrawn(DecisionRequest request, String by, String reason) {
}
