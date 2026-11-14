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
package org.os890.dtp.api;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * An occurrence of a collection window, read by its collector with {@link JobContext#window()}. While it is open, the
 * jobs the window pauses don't run, so everything they changed up to the cut-off is committed and nothing changes
 * during the collection. The changes of an occurrence are those in {@code (from, cutOff]}: a skipped occurrence's
 * changes fall into the next one, because {@code from} is the cut-off of the last closed occurrence.
 *
 * @param id       the occurrence
 * @param window   the name of the window
 * @param sequence its number within the window, from 1, e.g. for deterministic file names
 * @param from     the cut-off of the last closed occurrence, {@code null} for the first one
 * @param cutOff   its own cut-off
 */
public record WindowOccurrence(UUID id, String window, long sequence, Instant from, Instant cutOff) {

    /**
     * The start of the changes of this occurrence.
     *
     * @return the cut-off of the last closed occurrence, empty for the first one
     */
    public Optional<Instant> since() {
        return Optional.ofNullable(from);
    }
}
