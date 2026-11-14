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
package org.os890.dtp.core.domain;

import java.util.List;

/** Options the framework offers and applies itself (02 §2.5). */
public enum BuiltInOption {
    /** Item: back into the requesting step's inbox, attempts reset. Run: planned again. Package: pending again. */
    RETRY("Retry"),
    /** Item: back into the requesting step's inbox with its visits of that step reset (loop limit). */
    RESET_VISITS("Reset the visits and retry"),
    /** Item: ends its journey. Package: discarded with its items. */
    DISCARD("Discard"),
    /** Run: finishes as failed. */
    FAIL("Fail the run"),
    /** Run: finishes as skipped. */
    SKIP("Skip the run"),
    /** Window occurrence: keep waiting for the paused jobs, the quiesce timeout starts again. */
    KEEP_WAITING("Keep waiting"),
    /** Window occurrence: open without waiting for the paused jobs any longer. */
    FORCE_OPEN("Open the window now"),
    /** Window occurrence: skipped, its changes fall into the next occurrence. */
    SKIP_OCCURRENCE("Skip this occurrence"),
    /** Approval: quorum reached. */
    APPROVED("Approved"),
    /** Approval: a user rejected. */
    REJECTED("Rejected");

    /** Options of an escalated item. */
    public static final List<BuiltInOption> ITEM = List.of(RETRY, DISCARD);

    /** Options of an item that exceeded the loop limit of a step. */
    public static final List<BuiltInOption> ITEM_LOOP = List.of(RESET_VISITS, RETRY, DISCARD);

    /** Options of a work package whose retries are exhausted. */
    public static final List<BuiltInOption> PACKAGE = List.of(RETRY, DISCARD);

    /** Options of an abandoned run (takeover policy ABANDON). */
    public static final List<BuiltInOption> RUN = List.of(RETRY, FAIL, SKIP);

    /** Options of a window occurrence whose paused jobs didn't stop within the quiesce timeout. */
    public static final List<BuiltInOption> WINDOW_QUIESCE = List.of(KEEP_WAITING, FORCE_OPEN, SKIP_OCCURRENCE);

    /** Options of a window occurrence whose collector run failed: a new collector run, or skip. */
    public static final List<BuiltInOption> WINDOW_COLLECTOR = List.of(RETRY, SKIP_OCCURRENCE);

    private final String label;

    BuiltInOption(String label) {
        this.label = label;
    }

    /** The label shown to users. */
    public String label() {
        return label;
    }
}
