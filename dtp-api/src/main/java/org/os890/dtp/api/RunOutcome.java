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
 * Framework outcome of a finished run, set by the engine. Not to be confused with the handler's typed result status
 * ({@link ResultStatus}, e.g. {@link DefaultOutcome#SUCCESS}). Transitions can match on either.
 */
public enum RunOutcome {
    SUCCEEDED, COMPLETED_WITH_ISSUES, DEFERRED, FAILED, CANCELLED, SKIPPED
}
