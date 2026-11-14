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
 * Kinds of routes of a flow: item routes of a signal, or the successor of a typed run result. A {@link #FORK} targets
 * the join step, its branches are stored separately.
 */
public enum RouteKind {
    TO, BACK, FLOW, COMPLETE, DISCARD, FORK, RUN;

    /** {@code true} for the routes of item signals (all but {@link #RUN}). */
    public boolean isItemRoute() {
        return this != RUN;
    }
}
