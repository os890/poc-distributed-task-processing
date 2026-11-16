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

import java.time.Duration;

import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.StopReason;

/**
 * A run ended: published by the node that ran it once the end was committed, or by the node of the operator who
 * cancelled it before it started (then without node and duration). A run that waits for a decision hasn't ended
 * ({@link DecisionRequested}); a run that an operator's decision ends is announced by {@link DecisionMade}.
 *
 * @param run          the run
 * @param outcome      the outcome, e.g. {@code SUCCEEDED}, {@code DEFERRED} or {@code FAILED}
 * @param resultStatus the result status of a job handler, or {@code null}
 * @param stopReason   why it was asked to stop, or {@code null}
 * @param elapsed      the duration of the attempt, or {@code null} if it never started
 */
public record RunFinished(RunRef run, RunOutcome outcome, String resultStatus, StopReason stopReason,
                          Duration elapsed) {
}
