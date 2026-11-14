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

import java.util.Objects;

/**
 * An approval request (DD-16): {@link #requiredApprovals()} distinct users have to approve, one rejection rejects.
 *
 * <pre>{@code item.requestApproval(Approval.quorum(2).assignedTo(Department.RISK), "price above limit");}</pre>
 */
public class Approval {

    private final int requiredApprovals;
    private final Responsibility team;

    private Approval(int requiredApprovals, Responsibility team) {
        if (requiredApprovals < 1) {
            throw new IllegalArgumentException("the quorum must be at least 1");
        }
        this.requiredApprovals = requiredApprovals;
        this.team = team;
    }

    /**
     * An approval by the given number of distinct users.
     *
     * @param requiredApprovals the quorum
     * @return the approval
     */
    public static Approval quorum(int requiredApprovals) {
        return new Approval(requiredApprovals, null);
    }

    /**
     * Assigns the request to a team.
     *
     * @param responsible the team
     * @return a copy with the team
     */
    public Approval assignedTo(Responsibility responsible) {
        return new Approval(requiredApprovals, Objects.requireNonNull(responsible, "responsible"));
    }

    /** The quorum. */
    public int requiredApprovals() {
        return requiredApprovals;
    }

    /** The responsible team, or {@code null} for any business user. */
    public Responsibility team() {
        return team;
    }
}
