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
/**
 * Domain events of dtp, fired as CDI events after the commit of the transaction that caused them: first to the
 * synchronous observers ({@code @Observes}, in the committing thread, without a transaction), then to the asynchronous
 * ones ({@code @ObservesAsync}). A rolled back transaction fires nothing. A failing synchronous observer stops the
 * remaining synchronous observers of the event (CDI), so long-running or risky work belongs into asynchronous ones.
 * The framework never notifies people itself, the application observes the events (docs/design/02-domain-model.md
 * §2.6).
 */
package org.os890.dtp.api.event;
