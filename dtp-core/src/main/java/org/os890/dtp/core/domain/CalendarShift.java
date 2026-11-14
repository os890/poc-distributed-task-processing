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
package org.os890.dtp.core.domain;

/** What happens to a cron slot on a day that isn't a business day of the cron's calendar. */
public enum CalendarShift {
    /** No run for this slot. */
    SKIP,
    /** Same time on the next business day. */
    NEXT,
    /** Same time on the previous business day (planned ahead, before the slot itself). */
    PREVIOUS
}
