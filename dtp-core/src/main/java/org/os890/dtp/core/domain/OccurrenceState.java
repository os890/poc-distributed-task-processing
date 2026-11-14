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

/**
 * States of a window occurrence (docs/design/04-lifecycles.md §4.6). {@code SCHEDULED} isn't stored: an occurrence
 * is created when its start time is reached (or an operator starts it) and begins with {@code QUIESCING}.
 */
public enum OccurrenceState {
    QUIESCING, OPEN, WAITING_FOR_DECISION, CLOSED, SKIPPED;

    /** Whether the occurrence pauses its jobs. */
    public boolean active() {
        return this == QUIESCING || this == OPEN || this == WAITING_FOR_DECISION;
    }
}
