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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.HashMap;
import java.util.Map;

/** Resolves the type arguments of a generic interface implemented by a class, e.g. {@code P} of a handler. */
public class GenericTypes {

    private GenericTypes() {
    }

    /**
     * Resolves a type argument.
     *
     * @param type      the implementation class
     * @param generic   the generic interface, e.g. {@code JobHandler.class}
     * @param index     index of the type argument
     * @return the resolved class, or {@code Object.class} if it can't be resolved
     */
    public static Class<?> typeArgument(Class<?> type, Class<?> generic, int index) {
        Type resolved = resolve(type, generic, index, new HashMap<>());
        if (resolved instanceof Class<?> c) {
            return c;
        }
        if (resolved instanceof ParameterizedType p && p.getRawType() instanceof Class<?> raw) {
            return raw;
        }
        return Object.class;
    }

    private static Type resolve(Type current, Class<?> generic, int index, Map<TypeVariable<?>, Type> bindings) {
        Class<?> raw;
        if (current instanceof ParameterizedType p) {
            raw = (Class<?>) p.getRawType();
            TypeVariable<?>[] variables = raw.getTypeParameters();
            Type[] arguments = p.getActualTypeArguments();
            for (int i = 0; i < variables.length; i++) {
                Type argument = arguments[i];
                bindings.put(variables[i], argument instanceof TypeVariable<?> v ? bindings.getOrDefault(v, v)
                                                                                   : argument);
            }
            if (raw.equals(generic)) {
                return bindings.get(variables[index]);
            }
        } else if (current instanceof Class<?> c) {
            raw = c;
        } else {
            return null;
        }
        for (Type parent : raw.getGenericInterfaces()) {
            Type found = resolve(parent, generic, index, bindings);
            if (found != null) {
                return found;
            }
        }
        Type superclass = raw.getGenericSuperclass();
        return superclass == null ? null : resolve(superclass, generic, index, bindings);
    }
}
