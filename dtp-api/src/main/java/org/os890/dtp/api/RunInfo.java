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

import java.util.Optional;

/**
 * Typed key/value information attached to a run or a work package. Also injectable ({@code @Inject RunInfo})
 * in process steps: the bean always refers to the current run.
 */
public interface RunInfo {

    <T> void put(RunInfoKey<T> key, T value);

    <T> Optional<T> get(RunInfoKey<T> key);

    void increment(RunInfoKey<Long> key, long delta);
}
