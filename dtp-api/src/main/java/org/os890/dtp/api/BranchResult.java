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

import java.util.Arrays;
import java.util.Optional;

/**
 * Result of a parallel branch (DD-22), read by the join step with {@link TrackedItem#branchResults()}: the signal the
 * branch step reported.
 *
 * @param step       class name of the branch step
 * @param signalType class name of the signal
 * @param signal     name of the signal
 * @param reason     the reason given with the signal, or {@code null}
 */
public record BranchResult(String step, String signalType, String signal, String reason) {

    /**
     * {@code true} if this is the result of the given branch step.
     *
     * @param branch the branch step
     * @return {@code true} for a result of this step
     */
    public boolean isOf(Class<? extends ProcessStep> branch) {
        return branch.getName().equals(step);
    }

    /**
     * {@code true} if the branch reported this signal.
     *
     * @param expected the signal
     * @return {@code true} for this signal
     */
    public boolean is(Signal expected) {
        String type = expected instanceof Enum<?> e ? e.getDeclaringClass().getName() : expected.getClass().getName();
        return type.equals(signalType) && expected.name().equals(signal);
    }

    /**
     * The signal as constant of its enum.
     *
     * @param signals the signal enum
     * @param <S>     the signal type
     * @return the signal, empty if the branch reported a signal of another type
     */
    public <S extends Enum<S> & Signal> Optional<S> signal(Class<S> signals) {
        if (!signals.getName().equals(signalType)) {
            return Optional.empty();
        }
        return Arrays.stream(signals.getEnumConstants()).filter(s -> s.name().equals(signal)).findFirst();
    }
}
