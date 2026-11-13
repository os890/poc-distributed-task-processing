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

import java.util.Objects;

/**
 * Typed descriptor of a kind of tracked item, declared once per entity as a constant.
 * The {@code name} is what gets persisted, so renaming or moving the entity class is safe.
 *
 * @param <E>  the application's entity type
 * @param <ID> the entity's id type (e.g. UUID or Long)
 */
public record ItemType<E, ID>(String name, Class<E> entityType, Class<ID> idType) {

    public ItemType {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(entityType, "entityType");
        Objects.requireNonNull(idType, "idType");
    }

    public static <E, ID> ItemType<E, ID> of(String name, Class<E> entityType, Class<ID> idType) {
        return new ItemType<>(name, entityType, idType);
    }
}
