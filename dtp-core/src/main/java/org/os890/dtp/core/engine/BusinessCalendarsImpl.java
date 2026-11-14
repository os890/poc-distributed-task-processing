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

import java.util.List;
import java.util.Optional;

import org.os890.dtp.api.BusinessCalendar;
import org.os890.dtp.api.BusinessCalendars;
import org.os890.dtp.core.domain.CalendarDefinition;

/** The business calendars of the namespace, read from the cached definitions. */
public class BusinessCalendarsImpl implements BusinessCalendars {

    private final JobCatalog catalog;

    public BusinessCalendarsImpl(JobCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public Optional<BusinessCalendar> find(String name) {
        return catalog.calendar(name).map(BusinessCalendar.class::cast);
    }

    @Override
    public List<String> names() {
        return catalog.calendars().stream().map(CalendarDefinition::name).toList();
    }
}
