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
package org.os890.dtp.core.cluster;

import java.util.UUID;

import org.os890.dtp.api.ItemType;

/** Entity of the tests (table {@code test_trade}), declares its item type once (DD-20). */
public class TestTrade {

    public static final ItemType<TestTrade, UUID> ITEM = ItemType.of("test-trade", TestTrade.class, UUID.class);

    public static final String TABLE = "CREATE TABLE IF NOT EXISTS test_trade (id UUID PRIMARY KEY,"
            + " number VARCHAR(20) NOT NULL, isin VARCHAR(20), enriched BOOLEAN DEFAULT FALSE NOT NULL)";

    private TestTrade() {
    }
}
