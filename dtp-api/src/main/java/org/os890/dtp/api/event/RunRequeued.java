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
 * A run that had started is planned again: it waits to be claimed by any node.
 *
 * @param run   the run with its last attempt, no node
 * @param cause why
 * @param by    who: the node that took the expired lease over, or the user who decided
 */
public record RunRequeued(RunRef run, Cause cause, String by) {

    /** Why a run is planned again. */
    public enum Cause {
        /** Its lease expired and the takeover policy of its job is {@code RERUN}. */
        TAKEOVER,
        /** It waited for a decision, e.g. its own question or an abandoned takeover, and the decision runs it again. */
        DECISION
    }
}
