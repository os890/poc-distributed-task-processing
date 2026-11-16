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

import org.os890.dtp.api.WindowOccurrence;

/**
 * The paused jobs of a collection window stopped: the occurrence is open, its cut-off is recorded and its collector
 * run is planned.
 *
 * @param occurrence     the occurrence with its cut-off
 * @param forced         {@code true} if an operator opened it without waiting ({@code FORCE_OPEN})
 * @param collectorRunId the collector run, or {@code null} if it couldn't be planned (a decision follows)
 */
public record WindowOpened(WindowOccurrence occurrence, boolean forced, UUID collectorRunId) {
}
