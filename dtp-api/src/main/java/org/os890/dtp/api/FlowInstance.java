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

import java.util.UUID;

/**
 * A flow instance groups the runs of one end-to-end process, e.g. the end of day of a business date: the root run
 * and its successors (transitions, run joins). Started with {@link JobEngine#startFlowInstance}; a handler reads it
 * with {@link JobContext#flowInstance()}, e.g. to know the business date.
 *
 * @param id          the instance
 * @param name        the flow name: the name of the root job
 * @param businessKey the business key, unique per flow name, e.g. {@code 2026-11-14}
 */
public record FlowInstance(UUID id, String name, String businessKey) {
}
