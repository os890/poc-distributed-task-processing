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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.os890.dtp.api.ItemProcessing;
import org.os890.dtp.api.ItemProcessor;
import org.os890.dtp.api.ItemType;
import org.os890.dtp.api.JobHandler;
import org.os890.dtp.api.ProcessStep;

/**
 * What the engine knows about a process step class, resolved and validated at bootstrap.
 *
 * @param stepClass     the step class
 * @param parameterType {@code P} of a {@link JobHandler}, otherwise {@code null}
 * @param itemType      the item type of an {@link ItemProcessor} (from the entity's {@link ItemType} constant)
 * @param processing    settings of an {@link ItemProcessor}
 */
public record StepInfo(Class<? extends ProcessStep> stepClass, Class<?> parameterType, ItemType<?, ?> itemType,
                       ItemProcessing processing) {

    private static final ItemProcessing DEFAULTS = Defaults.class.getAnnotation(ItemProcessing.class);

    /**
     * Inspects a step class.
     *
     * @param stepClass the step class
     * @return the step info
     * @throws IllegalStateException if the step is invalid, e.g. an item processor without item type
     */
    public static StepInfo of(Class<? extends ProcessStep> stepClass) {
        boolean handler = JobHandler.class.isAssignableFrom(stepClass);
        boolean processor = ItemProcessor.class.isAssignableFrom(stepClass);
        if (handler == processor) {
            throw new IllegalStateException(stepClass.getName()
                    + " has to implement exactly one of JobHandler and ItemProcessor");
        }
        if (handler) {
            return new StepInfo(stepClass, GenericTypes.typeArgument(stepClass, JobHandler.class, 0), null, null);
        }
        Class<?> entity = GenericTypes.typeArgument(stepClass, ItemProcessor.class, 0);
        ItemProcessing processing = stepClass.getAnnotation(ItemProcessing.class);
        if (processing != null && (processing.batchSize() < 1 || processing.parallelism() < 1)) {
            throw new IllegalStateException("invalid @ItemProcessing of " + stepClass.getName());
        }
        return new StepInfo(stepClass, null, itemTypeOf(entity, stepClass),
                processing == null ? DEFAULTS : processing);
    }

    /** {@code true} for an {@link ItemProcessor}. */
    public boolean isItemProcessor() {
        return itemType != null;
    }

    private static ItemType<?, ?> itemTypeOf(Class<?> entity, Class<?> stepClass) {
        for (Field field : entity.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (field.getType() == ItemType.class && Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers)) {
                try {
                    ItemType<?, ?> type = (ItemType<?, ?>) field.get(null);
                    if (type != null && type.entityType() == entity) {
                        return type;
                    }
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException("can't read " + field, e);
                }
            }
        }
        throw new IllegalStateException(stepClass.getName() + ": the entity " + entity.getName()
                + " declares no public static ItemType constant (DQ-6)");
    }

    @ItemProcessing
    private static class Defaults {
    }
}
