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
package org.os890.dtp.test;

/** Entry point of the assertions of {@code dtp-test}. */
public class DtpAssertions {

    private DtpAssertions() {
    }

    /**
     * Assertions on the journey of an item.
     *
     * @param journey the journey, e.g. {@link DtpTestCluster#journey(org.os890.dtp.api.ItemType, String)}
     * @return the assertions
     * @throws AssertionError if there is no such item
     */
    public static JourneyAssert assertThat(Journey journey) {
        return new JourneyAssert(journey);
    }
}
