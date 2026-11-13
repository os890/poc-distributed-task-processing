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
package org.os890.dtp.core.engine;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.os890.dtp.api.RunInfo;
import org.os890.dtp.api.RunInfoKey;

/** Typed run info, stored as JSON when the run finishes. */
public class RunInfoImpl implements RunInfo {

    private final Map<String, Object> values = new ConcurrentHashMap<>();
    private final JsonCodec json;

    public RunInfoImpl(JsonCodec json) {
        this.json = json;
    }

    @Override
    public <T> void put(RunInfoKey<T> key, T value) {
        values.put(key.name(), value);
    }

    @Override
    public <T> Optional<T> get(RunInfoKey<T> key) {
        Object value = values.get(key.name());
        return value == null ? Optional.empty() : Optional.of(json.convert(value, key.type()));
    }

    @Override
    public void increment(RunInfoKey<Long> key, long delta) {
        values.merge(key.name(), delta, (a, b) -> ((Number) a).longValue() + ((Number) b).longValue());
    }

    /** The info as JSON, or {@code null} if empty. */
    public String toJson() {
        return values.isEmpty() ? null : json.write(new LinkedHashMap<>(values));
    }
}
