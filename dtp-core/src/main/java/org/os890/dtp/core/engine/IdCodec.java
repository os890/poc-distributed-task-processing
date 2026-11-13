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

import java.util.UUID;

/** Encodes entity ids for {@code dtp_item.ref_id} (UUID, Long, Integer and String ids in slice 1). */
public class IdCodec {

    private IdCodec() {
    }

    /**
     * Encodes an id.
     *
     * @param id the id
     * @return the encoded id
     */
    public static String encode(Object id) {
        return id.toString();
    }

    /**
     * Decodes an id.
     *
     * @param value  the encoded id
     * @param idType the id type
     * @param <ID>   the id type
     * @return the id
     */
    public static <ID> ID decode(String value, Class<ID> idType) {
        Object id;
        if (idType == UUID.class) {
            id = UUID.fromString(value);
        } else if (idType == Long.class) {
            id = Long.valueOf(value);
        } else if (idType == Integer.class) {
            id = Integer.valueOf(value);
        } else if (idType == String.class) {
            id = value;
        } else {
            throw new IllegalArgumentException("unsupported id type " + idType.getName());
        }
        return idType.cast(id);
    }
}
