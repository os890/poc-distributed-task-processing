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

import java.util.UUID;

/**
 * The work package a package event is about.
 *
 * @param id        the package
 * @param job       the job that processes it
 * @param sequence  its number (the order of processing)
 * @param groupKey  the group key, e.g. the custodian
 * @param items     the number of its items
 * @param reference the reference it was sent with, or {@code null}
 */
public record PackageRef(UUID id, String job, long sequence, String groupKey, int items, String reference) {
}
