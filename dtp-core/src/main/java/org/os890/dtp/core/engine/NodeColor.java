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

import java.util.Objects;

/**
 * The color of this node (red/green deployments, slice 21) and the active color of the namespace as last read. While
 * another color is active, the node is on standby: it doesn't claim runs, doesn't lead and doesn't activate the flow
 * versions it deploys. Correctness never depends on it (leases and fencing do): a node that reads the switch late
 * only runs a little longer.
 */
public class NodeColor {

    /** What a refresh changed. */
    public enum Change {
        NONE, BECAME_STANDBY, BECAME_ACTIVE
    }

    private final String color;
    private volatile String activeColor;
    private volatile boolean standby;

    /**
     * Creates the state of a node.
     *
     * @param color the color of the node, or {@code null}
     */
    public NodeColor(String color) {
        this.color = color;
    }

    /** The color of this node, or {@code null}. */
    public String color() {
        return color;
    }

    /** The active color as last read, or {@code null} if all colors claim. */
    public String activeColor() {
        return activeColor;
    }

    /** {@code true} while another color is active. */
    public boolean standby() {
        return standby;
    }

    /**
     * Whether a node of a color is on standby while a color is active.
     *
     * @param color       the color of the node, or {@code null}
     * @param activeColor the active color, or {@code null}
     * @return {@code true} if it's on standby
     */
    public static boolean standby(String color, String activeColor) {
        return activeColor != null && !Objects.equals(activeColor, color);
    }

    /**
     * Applies the active color read from the database.
     *
     * @param active the active color, or {@code null}
     * @return what changed for this node
     */
    public synchronized Change update(String active) {
        boolean was = standby;
        activeColor = active;
        standby = standby(color, active);
        if (standby == was) {
            return Change.NONE;
        }
        return standby ? Change.BECAME_STANDBY : Change.BECAME_ACTIVE;
    }
}
