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
 * A flow defined in Java, usually a CDI bean. At startup the framework compiles it into an immutable flow version in
 * the DB (identified by a hash of its content), generates the jobs of its inner steps and activates the version
 * unless an operator activated another version manually. Only the trigger of the entry step has to be configured in
 * the DB.
 */
public interface FlowDefinition {

    /**
     * Declares the steps and routes of the flow.
     *
     * @param flow the builder
     */
    void define(FlowBuilder flow);

    /**
     * Name of the flow. {@code null} (the default) derives it from the class name, e.g. {@code TradeFlow} becomes
     * {@code trade-flow}. Two definitions with the same name are versions of the same flow.
     *
     * @return the name or {@code null}
     */
    default String name() {
        return null;
    }
}
