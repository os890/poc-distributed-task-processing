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

import java.util.List;

/**
 * The active color of the namespace was switched (red/green deployment): from now on only the nodes of the new color
 * claim runs and lead, the nodes of the old color finish the current unit of work of their runs and stand by.
 *
 * @param previousColor  the color active before, or {@code null} if all colors claimed
 * @param activeColor    the active color, or {@code null}: all colors claim again
 * @param by             who switched
 * @param reason         why, or {@code null}
 * @param activatedFlows the flow versions activated by the switch ({@code "<flow> <version hash>"})
 */
public record ActiveColorSwitched(String previousColor, String activeColor, String by, String reason,
                                  List<String> activatedFlows) {

    public ActiveColorSwitched {
        activatedFlows = List.copyOf(activatedFlows);
    }
}
