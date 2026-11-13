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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Optional settings of an {@link ItemProcessor}. Without it, the defaults apply. */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ItemProcessing {

    /** Items per batch. Each batch is one fenced transaction. */
    int batchSize() default 100;

    /** Batches processed at the same time on virtual threads of the owning node. */
    int parallelism() default 1;

    /** Also hand over items that are deferred and not yet due, e.g. after an external event woke the step up. */
    boolean includeDeferred() default false;
}
