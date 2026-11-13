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

import org.os890.dtp.core.DtpException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/** JSON of run parameters, run info and errors. */
public class JsonCodec {

    private static final TypeReference<LinkedHashMap<String, Object>> MAP = new TypeReference<>() {
    };

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    /**
     * Serializes a value.
     *
     * @param value the value, may be {@code null}
     * @return the JSON, or {@code null}
     */
    public String write(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new DtpException("can't serialize " + value.getClass().getName(), e);
        }
    }

    /**
     * Deserializes a value.
     *
     * @param json the JSON, may be {@code null}
     * @param type the target type
     * @param <T>  the target type
     * @return the value, or {@code null}
     */
    public <T> T read(String json, Class<T> type) {
        if (json == null) {
            return null;
        }
        try {
            return mapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new DtpException("can't deserialize " + type.getName(), e);
        }
    }

    /**
     * Deserializes a JSON object.
     *
     * @param json the JSON, may be {@code null}
     * @return the map (empty for {@code null})
     */
    public Map<String, Object> readMap(String json) {
        if (json == null) {
            return new LinkedHashMap<>();
        }
        try {
            return mapper.readValue(json, MAP);
        } catch (JsonProcessingException e) {
            throw new DtpException("can't deserialize a map", e);
        }
    }

    /**
     * Converts a deserialized value (e.g. an {@link Integer} of a map) to the target type.
     *
     * @param value the value
     * @param type  the target type
     * @param <T>   the target type
     * @return the converted value
     */
    public <T> T convert(Object value, Class<T> type) {
        return mapper.convertValue(value, type);
    }
}
