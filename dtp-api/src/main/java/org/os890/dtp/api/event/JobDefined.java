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
package org.os890.dtp.api.event;

import java.time.Instant;

/**
 * A new job was defined: by a definition script, the builder or a flow registration. Announced once by the
 * housekeeping leader, shortly after the definition was written.
 *
 * @param job          the job name
 * @param revision     its first revision
 * @param handlerClass the class of its process step
 * @param definedBy    who wrote it, or {@code null}
 * @param definedAt    when it was written, or {@code null}
 */
public record JobDefined(String job, int revision, String handlerClass, String definedBy, Instant definedAt) {
}
