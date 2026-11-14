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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.os890.dtp.api.ProcessStep;

/**
 * A flow defined in Java, compiled from a {@code FlowDefinition}. Its content hash identifies the flow version in the
 * DB: the same definition on several nodes (or after a restart) is the same version.
 *
 * @param name            the flow name
 * @param definitionClass the class of the definition
 * @param entry           the entry step
 * @param steps           all steps including the entry
 * @param routes          the routes
 */
public record FlowModel(String name, Class<?> definitionClass, Class<? extends ProcessStep> entry,
                        List<Class<? extends ProcessStep>> steps, List<RouteModel> routes) {

    public FlowModel {
        steps = List.copyOf(steps);
        routes = List.copyOf(routes);
    }

    /** Canonical text of the flow (sorted, independent of the declaration order). */
    public String canonical() {
        String stepLines = steps.stream().map(Class::getName).sorted().collect(Collectors.joining("\n"));
        String routeLines = routes.stream().map(RouteModel::canonical).sorted().collect(Collectors.joining("\n"));
        return "flow " + name + "\nentry " + entry.getName() + "\n" + stepLines + "\n" + routeLines;
    }

    /** Content hash of the flow, identifies the version. */
    public String hash() {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Default name of a flow or job: the simple class name in kebab case, e.g. {@code TradeFlow} → {@code trade-flow}.
     *
     * @param type the class
     * @return the name
     */
    public static String defaultName(Class<?> type) {
        return type.getSimpleName().replaceAll("([a-z0-9])([A-Z])", "$1-$2").toLowerCase(Locale.ROOT);
    }
}
