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
 * Entry point for tracking items. Available in a run via {@link JobContext#items()} and as an injectable
 * CDI bean for application code outside of runs (e.g. a REST resource). Joins the caller's transaction.
 */
public interface Items {

    <E, ID> TrackedItem<E, ID> track(ItemRef<E, ID> ref, String businessKey);

    /** Tracks the item in a lane, e.g. VIP customers. The lane is kept for the whole journey. */
    <E, ID> TrackedItem<E, ID> track(ItemRef<E, ID> ref, String businessKey, ItemLane lane);
}
