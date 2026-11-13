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
 * The common case: a process step that handles the items in its inbox one by one. The framework iterates the inbox,
 * forms batches (each batch is one fenced transaction), stops softly, and derives the run outcome
 * ({@code SUCCEEDED} or {@code COMPLETED_WITH_ISSUES}). Optional settings: {@link ItemProcessing}.
 *
 * <p>The item type is taken from the entity's {@link ItemType} constant for {@code E}, validated at bootstrap.
 *
 * <p>An exception thrown by {@link #process(TrackedItem)} is a <em>technical</em> failure of that item: the batch is
 * rolled back and its items are processed one by one, so only the failing item is affected. It is retried according
 * to the step's retry policy and finally handed to operations. Use {@link TrackedItem#fail(String)} for
 * <em>business</em> failures.
 *
 * @param <E>  the tracked entity type
 * @param <ID> its id type
 */
public interface ItemProcessor<E, ID> extends ProcessStep {

    void process(TrackedItem<E, ID> item);
}
