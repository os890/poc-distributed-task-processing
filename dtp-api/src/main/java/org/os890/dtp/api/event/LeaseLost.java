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
 * A node noticed that it lost the lease of a run it was running, e.g. after a long pause: its work since the last
 * commit was rolled back, the run continues elsewhere ({@link LeaseExpired}). Published by the node that lost it.
 *
 * @param run the run, with the attempt and the node that lost the lease
 */
public record LeaseLost(RunRef run) {
}
