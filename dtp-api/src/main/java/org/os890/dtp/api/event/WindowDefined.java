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

import java.time.Instant;

/**
 * A collection window was defined. Announced once by the housekeeping leader, shortly after the definition was
 * written.
 *
 * @param window       the window name
 * @param collectorJob the job that collects when the window opens
 * @param scheduleCron its schedule, or {@code null}: started by an operator
 * @param zone         the zone of the schedule
 * @param definedAt    when it was written
 */
public record WindowDefined(String window, String collectorJob, String scheduleCron, String zone, Instant definedAt) {
}
