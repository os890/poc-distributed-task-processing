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

/**
 * A processing lane, usually implemented by an enum (e.g. VIP vs. standard customers).
 * Every inbox serves lanes by priority, and the weight prevents starvation of lower lanes.
 */
public interface ItemLane {

    String name();

    /** Higher is served first. */
    int priority();

    /** Share of batches when several lanes have queued items, e.g. VIP 3 : standard 1. */
    default int weight() {
        return 1;
    }
}
