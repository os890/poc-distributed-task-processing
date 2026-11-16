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
import java.time.Instant;

/**
 * The active color of a red/green deployment has no live node any more (crashed or shut down): nobody claims runs and
 * nobody leads, so nothing is planned until the color is switched. Published once for the cluster, by the node on
 * standby that noticed it first.
 *
 * @param activeColor   the active color
 * @param since         when it was noticed
 * @param lastHeartbeat the latest heartbeat of a node of the color, or {@code null} if it never had a node
 * @param noticedBy     the name of the node that noticed it
 * @param fallbackAfter after this delay a node on standby switches to its own color, or {@code null} if only an
 *                      operator switches ({@code dtp.color.fallback-after})
 */
public record ActiveColorUnavailable(String activeColor, Instant since, Instant lastHeartbeat, String noticedBy,
                                     Duration fallbackAfter) {
}
