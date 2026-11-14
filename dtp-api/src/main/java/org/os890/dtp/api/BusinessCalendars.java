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

import java.util.List;
import java.util.Optional;

/**
 * The business calendars of the namespace. They are definitions in the DB, so a holiday added by an operator applies
 * after the next refresh of the definitions, without a deployment.
 */
public interface BusinessCalendars {

    /**
     * A calendar by name.
     *
     * @param name the name
     * @return the calendar, if it exists
     */
    Optional<BusinessCalendar> find(String name);

    /**
     * A calendar by name.
     *
     * @param name the name
     * @return the calendar
     * @throws IllegalArgumentException if it doesn't exist
     */
    default BusinessCalendar get(String name) {
        return find(name).orElseThrow(() -> new IllegalArgumentException("unknown business calendar " + name));
    }

    /** The names of all calendars. */
    List<String> names();
}
