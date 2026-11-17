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
 * A run was planned: it waits to be claimed. Each run is planned once (its run key), a skipped trigger is announced
 * with {@link RunSkipped} instead.
 *
 * @param run         the run, attempt 0 and no node yet
 * @param triggerType the trigger, e.g. {@code CRON}, {@code TRANSITION} or {@code EXTERNAL}
 * @param triggeredBy what triggered it, e.g. the cron slot, or {@code null}
 * @param notBefore   the earliest start, e.g. the start of its processing slot, or {@code null}: at once
 * @param slotEnd     the end of its processing slot, or {@code null}
 */
public record RunPlanned(RunRef run, String triggerType, String triggeredBy, Instant notBefore, Instant slotEnd) {
}
