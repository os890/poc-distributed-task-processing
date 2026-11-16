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

import org.os890.dtp.api.RunOutcome;
import org.os890.dtp.api.WindowOccurrence;

/**
 * An occurrence of a collection window ended: its collector run finished, or an operator skipped it. The paused jobs
 * run again.
 *
 * @param occurrence       the occurrence
 * @param skipped          {@code true} if an operator skipped it ({@code SKIP_OCCURRENCE}): its changes fall into
 *                         the next occurrence
 * @param collectorOutcome the outcome of the collector run, or {@code null} if skipped
 */
public record WindowClosed(WindowOccurrence occurrence, boolean skipped, RunOutcome collectorOutcome) {
}
