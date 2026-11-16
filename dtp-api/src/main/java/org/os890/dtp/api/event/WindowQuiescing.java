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

import org.os890.dtp.api.WindowOccurrence;

/**
 * An occurrence of a collection window started: the jobs it pauses are asked to stop, {@link WindowOpened} follows
 * once they did.
 *
 * @param occurrence  the occurrence, without cut-off yet
 * @param triggeredBy the operator who started it, or {@code null} for the schedule
 */
public record WindowQuiescing(WindowOccurrence occurrence, String triggeredBy) {
}
